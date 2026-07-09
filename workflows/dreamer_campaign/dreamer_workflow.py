"""
DreamerWorkflow — emulates workflow tasks using radical.dreamer in peer mode.

Score cascade model
-------------------
Each stage computes an output score from its upstream input score and fires a
downstream trigger only if the score clears a per-stage threshold.  This models
a real drug-discovery funnel where each expensive stage refines the quality
estimate and only the most promising candidates proceed.

  s1 (root): output_score = uniform [0, 1]   — initial ligand screen
  s2–s5:     output_score = input_score + N(0, score_noise)
             only triggers downstream if output_score >= score_threshold

Three outputs are forwarded to the downstream candidate metadata:
  score         — this stage's refined quality estimate
  surrogate_pred  — cheap prediction of what the NEXT stage will produce
  surrogate_unc   — model uncertainty (high = explore this candidate)

The sharder uses all three (weighted by the active profile) to dispatch the
highest-value candidates first, so the pipeline converges on good leads faster
than FIFO (baseline) dispatch.

Config keys (per stage in config.yaml, under dreamer:):
  use_stub / simulated_duration / simulated_jitter  — timing emulation
  score_noise      float   Std of Gaussian noise added to input score (default 0.05)
  score_threshold  float   Min output score to trigger downstream (default 0.0)
  surr_decay       float   Correlation factor for surrogate prediction (default 0.90)
  surr_noise       float   Noise on surrogate prediction (default 0.05)

Config keys forwarded by run_campaign.py:
  trigger_downstream     — downstream group name
  candidate_score        — upstream score (None for root stage)
  candidate_scaffold     — upstream scaffold class
"""

import asyncio
import hashlib
import os
import random
import sys
from pathlib import Path
from typing import ClassVar

# Scaffold alphabet for diversity signal (8 classes, assigned by hash of candidate_id)
_SCAFFOLDS = ["scaf_A", "scaf_B", "scaf_C", "scaf_D", "scaf_E", "scaf_F", "scaf_G", "scaf_H"]

_dreamer_dir = os.environ.get("DREAMER_DIR")
if _dreamer_dir:
    _src = str(Path(_dreamer_dir) / "src")
    if _src not in sys.path:
        sys.path.insert(0, _src)

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.campaign import BaseWorkflow  # noqa: E402

try:
    from radical.dreamer import Resource, Workload
    from radical.dreamer.configs import ScheduleConfig
    from radical.dreamer.managers.ext.schedule import Schedule
    from radical.dreamer.managers.resource import ResourceManager

    _DREAMER_AVAILABLE = True
except Exception as _dreamer_exc:
    # Broad catch: radical.dreamer's package import can raise FileNotFoundError
    # when its VERSION file is missing (broken editable install), or other
    # non-ImportError exceptions during module init.  config.yaml's stub
    # path doesn't need the real package, so swallow and continue.
    import warnings as _warnings

    _warnings.warn(
        f"radical.dreamer unavailable ({type(_dreamer_exc).__name__}: "
        f"{_dreamer_exc}); falling back to use_stub mode.  "
        f"Real-task emulation will not run.",
        stacklevel=2,
    )
    _DREAMER_AVAILABLE = False


class DreamerWorkflow(BaseWorkflow):
    """Async wrapper that uses radical.dreamer (peer mode) to emulate tasks."""

    workflow_id = "dreamer"

    # Per-group replica counter (group_name -> replicas started). Used to drive
    # count-based ``duration_phases`` (a shifting bottleneck). Keyed on *work
    # done* rather than wall-clock so the phase boundary is reproducible and
    # policy-fair (the same Nth replica triggers the shift regardless of which
    # scheduling policy is driving).  Reset between benchmark runs.
    _group_state: ClassVar[dict] = {}

    # ── Workflow entry point ──────────────────────────────────────────────────

    async def run(self, replica_id: str) -> None:
        cfg = self.config or {}
        # Triage ADVANCE short-circuit: when the surrogate is confident the
        # candidate's score will clear the next stage's bar, skip the actual
        # simulation entirely.  The output_score / surrogate_pred logic in
        # on_replica_done still runs and triggers downstream — but the
        # wall-time cost of *this* stage's compute is saved.
        if cfg.get("candidate_triage_advance"):
            # Minimal yield so the event loop sees the replica completing
            # rather than blocking it; no sleep, no simulation.
            await asyncio.sleep(0)
            return
        # Shifting-bottleneck hook: resolve the effective duration from the
        # stage's count-based phase schedule (no-op when duration_phases unset).
        cfg = self._apply_duration_phase(cfg)
        await asyncio.to_thread(self._run_simulation, replica_id, cfg)

    def _apply_duration_phase(self, cfg: dict) -> dict:
        """Resolve simulated_duration from a count-based phase schedule.

        config (per stage's ``dreamer`` block)::

            simulated_duration: 12.0          # phase 0 (before any threshold)
            duration_phases:
              - { after: 40, duration: 3.0 }  # once 40 replicas of THIS stage
                                              # have started, drop to 3.0s

        Phases are applied in ascending ``after`` order; the last threshold the
        running count has crossed wins.  Returns cfg unchanged (same object) when
        no schedule is set, else a shallow copy with simulated_duration patched.
        """
        phases = cfg.get("duration_phases")
        if not phases:
            return cfg
        g = self._group_name or "?"
        n = DreamerWorkflow._group_state.get(g, 0) + 1
        DreamerWorkflow._group_state[g] = n
        dur = float(cfg.get("simulated_duration", 1.0))
        for ph in sorted(phases, key=lambda p: int(p.get("after", 0))):
            if n >= int(ph.get("after", 0)):
                dur = float(ph.get("duration", dur))
        patched = dict(cfg)
        patched["simulated_duration"] = dur
        return patched

    # ── Simulation (runs in a thread pool worker) ─────────────────────────────

    @staticmethod
    def _run_simulation(replica_id: str, cfg: dict) -> dict:
        if not _DREAMER_AVAILABLE or cfg.get("use_stub"):
            import time

            dur = float(cfg.get("simulated_duration", 1.0))
            jitter = float(cfg.get("simulated_jitter", 0.1))
            time.sleep(dur + random.uniform(0.0, jitter))
            return {"stub": True}

        num_cores = int(cfg.get("num_cores", 32))
        perf_dist = dict(
            cfg.get("perf_dist", {"name": "uniform", "mean": 16.0, "var_spatial": 2.0})
        )
        num_tasks = int(cfg.get("num_tasks", 64))
        ops_dist = dict(cfg.get("ops_dist", {"mean": 512.0}))
        strategy = str(cfg.get("schedule_strategy", "smallest_to_fastest"))
        early_binding = bool(cfg.get("early_binding", True))

        resource = Resource(num_cores=num_cores, perf_dist=perf_dist)
        workload = Workload(num_tasks=num_tasks, ops_dist=ops_dist)
        schedule = Schedule(
            cfg=ScheduleConfig(
                from_dict={
                    "strategy": strategy,
                    "early_binding": early_binding,
                    "is_adaptive": False,
                }
            )
        )
        ResourceManager.processing(resource=resource, workload=workload, schedule=schedule)
        return {"stub": False}

    # ── Scaffold helper ───────────────────────────────────────────────────────

    @staticmethod
    def _scaffold_for(candidate_id: str) -> str:
        """Deterministic scaffold class from candidate id hash."""
        return _SCAFFOLDS[
            int(hashlib.md5(candidate_id.encode()).hexdigest()[:2], 16) % len(_SCAFFOLDS)
        ]

    # ── Score cascade ─────────────────────────────────────────────────────────

    async def on_replica_done(self, replica_id: str, cm, final_state: str) -> None:
        """Compute output score, apply threshold gate, trigger downstream.

        Root stage (s1): generates an initial random score in [0, 1].
        Downstream stages: refine the upstream score with Gaussian noise,
        modelling each stage as a progressively more accurate quality estimate.

        Only candidates that clear score_threshold trigger the next stage.
        The downstream candidate receives output_score, surrogate_pred, and
        surrogate_unc so the sharder can dispatch highest-value candidates first.
        """
        cfg = self.config or {}
        trigger = cfg.get("trigger_downstream")
        if not trigger or final_state != "done":
            return

        # ── Read upstream context ─────────────────────────────────────────────
        input_score = cfg.get("candidate_score")  # None for root stage (s1)
        scaffold = cfg.get("candidate_scaffold") or self._scaffold_for(replica_id)

        # ── Compute this stage's output score ─────────────────────────────────
        noise_std = float(cfg.get("score_noise", 0.05))

        if input_score is None:
            # Root stage: initial screen produces a random quality score.
            output_score = random.random()
        else:
            # Downstream: refine upstream estimate with stage-specific noise.
            # Noise models imperfect correlation between successive assays.
            output_score = max(0.0, min(1.0, input_score + random.gauss(0.0, noise_std)))

        # ── Threshold gate ────────────────────────────────────────────────────
        threshold = float(cfg.get("score_threshold", 0.0))
        if output_score < threshold:
            return  # candidate does not proceed to next stage

        # ── Surrogate outputs for sharder priority ranking ────────────────────
        # surr_pred: predict what the NEXT stage will produce.
        # Modelled as a noisy, slightly-decayed version of the current score.
        surr_decay = float(cfg.get("surr_decay", 0.90))
        surr_noise = float(cfg.get("surr_noise", 0.05))
        surr_pred = max(0.0, min(1.0, output_score * surr_decay + random.gauss(0.0, surr_noise)))
        # surr_unc: uncertainty decreases for high-scoring candidates
        # (the model is more confident about good leads).
        surr_unc = 0.4 * (1.0 - output_score)

        # ── Trigger downstream with full candidate metadata ───────────────────
        cand_id = f"{replica_id}_d"
        await self._trigger_dependent(
            trigger,
            replicas=1,
            candidate_id=cand_id,
            score=output_score,
            surrogate_pred=surr_pred,
            surrogate_unc=surr_unc,
            scaffold_class=scaffold,
            source_stage=self._group_name,
        )
