"""CampaignView — the adapter between an AsyncCampaignManager and the ADR Operator.

All coupling to the CM lives here.  The Operator and the policies depend only
on the small ``CampaignViewProtocol`` surface below, so they can be unit-tested
against a fake view with no live CM, no asyncflow engine, and no LLM key.

Observation surface (``observe()`` → dict)::

    {
      "cycle":       int,
      "terminal":    list[str],            # terminal (deepest) stage ids
      "free_cpus":   int, "free_gpus": int,
      "stages": {                          # one entry per workflow group
        name: {
          "status", "priority", "started", "running", "finished",
          "cap", "pending", "ready", "deps", "queue_depth", "bp_state",
          "starved": bool,          # pending work and below cap and not a source
          "is_source": bool,        # no upstream deps or trigger routing
          "requires_gpu": bool,
          "stalls": int,            # consecutive scheduler stall cycles
          "avg_duration_s": float|None,  # mean finished-replica wall-time
          "score_p50": float|None,  # 50th-pct candidate score (recent shards)
          "score_p90": float|None,  # 90th-pct candidate score (recent shards)
        },
      },
      # Present when Monitor feature is enabled:
      "monitor_alerts": {stage_id: [kind_name, …]},  # active drift breaches
      # Present when BudgetController feature is enabled:
      "budget_controllers": {
        stage_id: {
          "frozen": bool,           # surrogate-drift freeze active
          "consecutive_bound_hits": int,
          "burn_ratio": float|None, # actual/expected node-hours (latest tick)
          "progress": float|None,   # finished/target (0–1)
          "score_at_bound": bool,   # triage nudge has hit its limit
          # present only when triage is active for this stage:
          "score_cutoff": float,    # current triage score gate (candidates below are rejected)
          "score_cutoff_bounds": [float, float],  # [lo, hi] clamp range for score_cutoff
        }
      },
      # Present when a surrogate model is configured:
      "surrogate_recall": {
        stage_id: {
          "recall_at_k": float, "mae": float,
          "consecutive_low": int, "drift_active": bool,
        }
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
    set_score_cutoff(stage, v) — absolute-set the triage score gate (clamped; repeat each tick)
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
    terminal:  terminal stage id(s); if None, inferred as the deepest leaf
               stages (those no other stage depends on).  Accepts a single
               stage name (str) or a list of names.
    """

    def __init__(
        self,
        cm,
        terminal: str | list[str] | None = None,
        *,
        telemetry_subscriber=None,
    ) -> None:
        self._cm = cm
        if isinstance(terminal, str):
            self._terminal: list[str] = [terminal]
        elif terminal is not None:
            self._terminal = list(terminal)
        else:
            self._terminal = self._infer_terminal()
        # Optional TelemetrySubscriber — provides real GPU/CPU/mem metrics on HPC.
        # None is safe: observe() simply omits the telemetry fields.
        self._telemetry: Optional[object] = telemetry_subscriber

    # ── Inference helpers ──────────────────────────────────────────────────

    def _triggered_stages(self) -> set[str]:
        """Stages that are downstream of another via trigger_* config keys.

        Covers _on_completion routing (e.g. trigger_analysis: analysis) which
        bypasses config-level dependencies: entries and is therefore invisible
        to w.dependencies.  Any trigger_<x>: <value> key whose value matches a
        known stage name is treated as a logical dependency edge.  Float-valued
        trigger keys (e.g. trigger_fraction: 0.5) are safely skipped by the
        isinstance(val, str) guard.
        """
        wfs = self._cm.state.workflows
        return {
            val
            for w in wfs.values()
            for key, val in (w.workflow_config or {}).items()
            if key.startswith("trigger_") and isinstance(val, str) and val in wfs
        }

    def _infer_terminal(self) -> list[str]:
        wfs = self._cm.state.workflows
        if not wfs:
            return []
        # Stages that trigger downstream work are upstream (not terminal).
        # _triggered_stages() returns the DOWNSTREAM targets; we need the UPSTREAM
        # producers — i.e. any stage whose workflow_config has a trigger_* key
        # pointing to a known stage name.
        triggering = {
            name
            for name, w in wfs.items()
            for key, val in (w.workflow_config or {}).items()
            if key.startswith("trigger_") and isinstance(val, str) and val in wfs
        }
        depended_on = {d for w in wfs.values() for d in w.dependencies} | triggering
        leaves = [name for name in wfs if name not in depended_on]
        if not leaves:
            return [list(wfs)[-1]]
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
            return [list(wfs)[-1]]
        return leaves

    # ── Observation ────────────────────────────────────────────────────────

    def observe(self) -> dict:
        st = self._cm.state
        wfs = st.workflows
        sharders = st.sharders
        bp = st.bp
        res = st.resources
        metrics = getattr(st, "metrics", None) or self._cm.metrics()

        # Recent candidate scores per stage (last 50 shard dispatches across
        # all stages).  Used for score_p50 / score_p90.  Keeping only the
        # tail avoids O(all-time) growth while giving enough data for percentiles.
        recent_scores: dict[str, list[float]] = {}
        for se in getattr(metrics, "shard_events", [])[-50:]:
            recent_scores.setdefault(se.group, []).extend(se.scores)

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
            # Source stages have no upstream deps: their `pending` is the raw
            # input library, NOT a pipeline stall — not a bottleneck.
            # Covers both config-level dependencies and trigger_* routing so
            # _on_completion campaigns get correct starved signals.
            is_source = not w.dependencies and name not in self._triggered_stages()

            # avg_duration_s: cheap O(1) per stage via cumulative wall-time.
            finished = w.finished_replicas
            wall_s = metrics.stage_wall_s(name) if metrics is not None else 0.0
            avg_duration_s = (wall_s / finished) if finished > 0 else None

            # score_p50 / score_p90 from recent shard dispatches (≥5 samples required).
            sc = sorted(recent_scores.get(name, []))
            n_sc = len(sc)
            score_p50 = sc[int(n_sc * 0.5)] if n_sc >= 5 else None
            score_p90 = sc[int(n_sc * 0.9)] if n_sc >= 5 else None

            stages[name] = {
                "status": w.status,
                "priority": w.priority,
                "started": w.started_count,
                "running": running,
                "finished": finished,
                "cap": cap,
                "pending": pending,
                # RESOURCE-STARVED: has backlogged work, running below cap, and
                # is not a source stage.  A priority boost can relieve this by
                # winning more scheduler slots.  A stage at cap with pending is
                # cap-limited; priority adjustment alone can't help it.
                "starved": bool(pending > 0 and (cap == 0 or running < cap) and not is_source),
                "is_source": is_source,
                "ready": w.ready,
                "deps": list(w.dependencies),
                "queue_depth": queue_depth,
                "bp_state": bp_state,
                "requires_gpu": (getattr(w, "required_gpus", 0) or 0) > 0,
                # ── New fields ──────────────────────────────────────────────
                # Consecutive scheduler stall cycles (resource-contended).
                # Resets to 0 on the first successful replica start.
                "stalls": w._consecutive_stalls,
                # Mean wall-clock time per finished replica for this stage.
                "avg_duration_s": avg_duration_s,
                # Candidate score distribution from recent shard dispatches.
                # None until ≥5 scored candidates have been dispatched.
                "score_p50": score_p50,
                "score_p90": score_p90,
                # Failed replica count and rate — expose so policies can react
                # to transient SLURM preemption vs structural config errors.
                "n_failed": w.failed_replicas,
                "fail_rate": (w.failed_replicas / w.started_count) if w.started_count > 0 else 0.0,
            }

        obs = {
            "cycle": 0,  # the Operator overwrites this with snapshot.cycle
            "terminal": self._terminal,
            "free_cpus": getattr(res, "available_cpus", 0),
            "free_gpus": getattr(res, "available_gpus", 0),
            "stages": stages,
        }

        # ── Monitor alerts (drift breaches, present when monitor enabled) ───
        monitor = getattr(st, "monitor", None)
        if monitor is not None and hasattr(monitor, "active_alerts"):
            obs["monitor_alerts"] = monitor.active_alerts()

        # ── BudgetController state (present when budget feature enabled) ────
        bc_map = getattr(st, "budget_controllers", {})
        triages = getattr(st, "triages", {})
        if bc_map or triages:
            # Index latest BudgetEventRecord per stage (scan backwards).
            latest_budget: dict[str, object] = {}
            for bev in reversed(getattr(metrics, "budget_events", [])):
                if bev.stage_id not in latest_budget:
                    latest_budget[bev.stage_id] = bev
                if len(latest_budget) == len(bc_map):
                    break
            budget_obs: dict[str, dict] = {}
            for sid in set(bc_map) | set(triages):
                bc = bc_map.get(sid)
                bev = latest_budget.get(sid)
                triage = triages.get(sid)
                budget_obs[sid] = {
                    "frozen": bc.frozen if bc is not None else False,
                    "consecutive_bound_hits": bc._consecutive_bound_hits if bc is not None else 0,
                    "burn_ratio": bev.burn_ratio if bev is not None else None,
                    "progress": bev.progress if bev is not None else None,
                    "score_at_bound": bev.score_at_bound if bev is not None else False,
                    # score_cutoff / score_cutoff_bounds present only when triage is active.
                    # BudgetController re-nudges the cutoff every scheduler cycle, so ADR
                    # must assert its target value each tick to maintain it (last write wins).
                    **({
                        "score_cutoff": triage.score_cutoff,
                        "score_cutoff_bounds": list(triage.score_cutoff_bounds),
                    } if triage is not None else {}),
                }
            obs["budget_controllers"] = budget_obs

        # ── Surrogate recall (present when a surrogate model is configured) ─
        surrogates = getattr(st, "surrogates", {})
        if surrogates:
            recall_obs: dict[str, dict] = {}
            for sid, s in surrogates.items():
                recall = s.state().get("recall")
                if recall is not None:
                    recall_obs[sid] = recall
            if recall_obs:
                obs["surrogate_recall"] = recall_obs

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

    def set_score_cutoff(self, stage: str, value: float) -> bool:
        """Absolute-set the triage score cutoff for a stage, clamped to its bounds.

        BudgetController nudges the cutoff every scheduler cycle, so call this
        every ADR tick to maintain the target (last write wins).
        Returns False if the stage has no active triage.
        """
        triage = self._cm.state.triages.get(stage)
        if triage is None:
            return False
        triage.nudge_cutoffs(value - triage.score_cutoff, 0.0)
        return True

    async def trigger(self, stage: str, replicas: int) -> int:
        if replicas <= 0 or stage not in self._cm.state.workflows:
            return 0
        await self._cm.trigger_dependent(stage, replicas=replicas)
        return replicas

    async def reactivate(self) -> None:
        """Allow the CM scheduler to run again after a between-phase completion.

        The CM sets its internal completion event (_all_done) when all
        currently-registered replicas finish.  In a phased campaign this fires
        after Phase 1 (ddsim_a + analysis), before Phase 2 is triggered.  Call
        this before triggering Phase 2 replicas so the scheduler is not blocked
        by the stale completion flag.  Safe to call when _all_done is not yet
        set (no-op in that case).
        """
        if self._cm._all_done.is_set():
            self._cm._all_done.clear()
            await self._cm._schedule()
