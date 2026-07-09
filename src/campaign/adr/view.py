"""CampaignView — the adapter between an AsyncCampaignManager and the ADR Operator.

All coupling to the CM lives here.  The Operator and the policies depend only
on the small ``CampaignViewProtocol`` surface below, so they can be unit-tested
against a fake view with no live CM, no asyncflow engine, and no LLM key.

Observation surface (``observe()`` → dict)::

    {
      "cycle":       int,
      "terminal":    str | None,          # terminal (deepest) stage id
      "hits":        int,                  # finished replicas of the terminal stage
      "target":      int,                  # campaign target (goal threshold)
      "free_cpus":   int, "free_gpus": int,
      "stages": {                          # one entry per workflow group
        name: {"status", "priority", "started", "running", "finished",
               "cap", "ready", "deps", "queue_depth", "bp_state"},
      },
      # Present only when a TelemetrySubscriber is wired in (real HPC runs):
      "gpu_util":             float,      # EWA GPU utilisation % (0–100)
      "cpu_util":             float,      # EWA CPU utilisation % (0–100)
      "mem_util":             float,      # EWA memory utilisation % (0–100)
      "gpu_utils_per_device": dict,       # {gpu_id: latest_gpu_pct}
      "task_fail_rate":       float,      # fraction of tasks that failed
      "avg_task_duration_s":  float|None, # rolling mean of completed task durations
    }

Action levers (the @act methods on the Operator delegate to these)::

    set_priority(stage, p)     — re-rank a stage for the CM's two-pass scheduler
    set_batch_size(stage, n)   — adjust a stage's sharder target batch size
    await trigger(stage, n)    — queue n replicas of a dependent stage
"""

from __future__ import annotations

import logging
from typing import Optional, Protocol, runtime_checkable

log = logging.getLogger(__name__)


@runtime_checkable
class CampaignViewProtocol(Protocol):
    """The minimal surface the Operator/policies require."""

    def observe(self) -> dict: ...
    def set_priority(self, stage: str, priority: int) -> bool: ...
    def set_batch_size(self, stage: str, size: int) -> bool: ...
    async def trigger(self, stage: str, replicas: int) -> int: ...


class CampaignView:
    """Live adapter over an ``AsyncCampaignManager``.

    Parameters
    ----------
    cm:        the running AsyncCampaignManager
    target:    campaign target (goal threshold); if None, read from the
               terminal stage's ``campaign_target`` when available, else 0
    terminal:  terminal stage id; if None, inferred as the deepest stage
               (the one no other stage depends on)
    """

    def __init__(
        self,
        cm,
        target: int | None = None,
        terminal: str | None = None,
        *,
        telemetry_subscriber=None,
    ) -> None:
        self._cm = cm
        self._terminal = terminal or self._infer_terminal()
        self._target = target if target is not None else self._infer_target()
        # Optional TelemetrySubscriber — provides real GPU/CPU/mem metrics on HPC.
        # None is safe: observe() simply omits the telemetry fields.
        self._telemetry: Optional[object] = telemetry_subscriber

    # ── Inference helpers ──────────────────────────────────────────────────

    def _infer_terminal(self) -> str | None:
        wfs = self._cm.state.workflows
        if not wfs:
            return None
        depended_on = {d for w in wfs.values() for d in w.dependencies}
        leaves = [name for name in wfs if name not in depended_on]
        if not leaves:
            return list(wfs)[-1]
        if len(leaves) == len(wfs):
            # Every group is a leaf — no config-level dependencies exist.
            # This happens when DAG routing is handled entirely via _on_completion
            # rather than through the config `dependencies` key.  The topology is
            # not visible to the view in that case, so terminal cannot be inferred.
            # Pass terminal=<stage_name> explicitly to CampaignView.
            log.warning(
                "CampaignView: no config dependencies found — cannot infer the "
                "terminal stage. Pass terminal='<stage_name>' explicitly when "
                "constructing CampaignView (or add dependencies: to the config)."
            )
            return list(wfs)[-1]
        # Deepest leaf = the one with the longest dependency chain.
        return leaves[-1]

    def _infer_target(self) -> int:
        plan = getattr(self._cm, "_plan", None)
        if plan is not None and self._terminal is not None:
            for stage in getattr(plan, "stages", []):
                if stage.id == self._terminal:
                    return int(getattr(stage, "campaign_target", 0) or 0)
        return 0

    # ── Observation ────────────────────────────────────────────────────────

    def observe(self) -> dict:
        st = self._cm.state
        wfs = st.workflows
        sharders = st.sharders
        bp = st.bp
        res = st.resources

        stages: dict[str, dict] = {}
        for name, w in wfs.items():
            sharder = sharders.get(name)
            queue_depth = len(sharder) if sharder is not None and hasattr(sharder, "__len__") else 0
            bp_neg = bp.get(name)
            bp_state = (
                bp_neg.state.name if bp_neg is not None and hasattr(bp_neg, "state") else "HOLD"
            )
            running = w.started_count - w.finished_replicas
            # backlog: replicas triggered but not yet started (capped/resource
            # starved). This is the TRUE bottleneck signal — unlike queue_depth
            # (the sharder buffer), which drains eagerly to ~0 between ticks.
            pending = max(0, w.replicas - w.started_count)
            cap = w.concurrency_cap
            stages[name] = {
                "status": w.status,
                "priority": w.priority,
                "started": w.started_count,
                "running": running,
                "finished": w.finished_replicas,
                "cap": cap,
                "pending": pending,
                # RESOURCE-STARVED: has work waiting but is running BELOW cap —
                # i.e. it wants more slots and can't get them (resource-contended).
                # This is the signal a priority boost can actually fix: raising its
                # priority gives it more of the contended slots.  (A stage already
                # at cap with pending is cap-limited; priority can't help it.)
                # NOTE: when DAG routing uses _on_completion (no config dependencies),
                # w.dependencies is [] for all groups, making starved always False.
                # Campaigns using _on_completion must recompute starved in the
                # operator's extract() after injecting logical topology.
                "starved": bool(pending > 0 and (cap == 0 or running < cap) and w.dependencies),
                # source stage (no upstream deps): its `pending` is the raw input
                # library, NOT a pipeline stall — must not be treated as a bottleneck.
                "is_source": not w.dependencies,
                "ready": w.ready,
                "deps": list(w.dependencies),
                "queue_depth": queue_depth,
                "bp_state": bp_state,
                # Whether this stage requires GPU slots.  Policies use this to skip
                # GPU-utilisation-based batch adjustments for CPU-only stages.
                "requires_gpu": (getattr(w, "required_gpus", 0) or 0) > 0,
            }

        hits = wfs[self._terminal].finished_replicas if self._terminal in wfs else 0
        obs = {
            "cycle": 0,  # the Operator overwrites this with snapshot.cycle
            "terminal": self._terminal,
            "hits": hits,
            "target": self._target,
            "free_cpus": getattr(res, "available_cpus", 0),
            "free_gpus": getattr(res, "available_gpus", 0),
            "stages": stages,
        }
        if self._telemetry is not None:
            obs.update(self._telemetry.snapshot())
        return obs

    # ── Action levers ──────────────────────────────────────────────────────

    def set_priority(self, stage: str, priority: int) -> bool:
        w = self._cm.state.workflows.get(stage)
        if w is None:
            return False
        w.priority = int(priority)
        return True

    def set_batch_size(self, stage: str, size: int) -> bool:
        sharder = self._cm.state.sharders.get(stage)
        if sharder is None or not hasattr(sharder, "spec"):
            return False
        spec = sharder.spec
        lo = getattr(spec, "min_size", 1)
        hi = getattr(spec, "max_size", size)
        spec.target_size = max(lo, min(hi, int(size)))
        return True

    async def trigger(self, stage: str, replicas: int) -> int:
        if replicas <= 0 or stage not in self._cm.state.workflows:
            return 0
        await self._cm.trigger_dependent(stage, replicas=replicas)
        return replicas
