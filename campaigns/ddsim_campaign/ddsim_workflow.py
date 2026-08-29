"""
Dummy DDSim campaign — two simulation flavours feeding one analysis stage.

DdSimWorkflow  — shared by ddsim_a (fast/coarse) and ddsim_b (long/fine).
                 _group_name distinguishes which flavour is running.
                 Each replica simulates an MD run, generates a score, and
                 triggers one analysis replica.

AnalysisWorkflow — triggered dynamically by both sim groups.
                   Pops the next score from the shared FIFO queue, applies a
                   flavour-aware refinement factor, and records the result.
                   Campaign stops when campaign_target analyses complete.

Score routing uses a flavour-keyed ClassVar dict so fast and slow results
are visible separately in the summary.  asyncio cooperative scheduling makes
list.append / pop(0) safe without locks.

Config keys for DdSimWorkflow (beyond CM scheduling keys):
  duration          float   simulated MD seconds per replica  (default 0.2)
  jitter            float   Gaussian σ for duration noise     (default 0.02)
  score_mean        float   centre of Gaussian score draw     (default 0.5)
  score_noise       float   σ of Gaussian score draw          (default 0.10)
  trigger_analysis  str     group to trigger per completion   (default "analysis")

Config keys for AnalysisWorkflow:
  duration          float   simulated analysis seconds        (default 0.15)
  jitter            float   Gaussian σ for duration noise     (default 0.03)
  refinement_a      float   score multiplier for fast results (default 0.85)
  refinement_b      float   score multiplier for long results (default 0.70)
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
import time
from typing import ClassVar

from src.campaign import BaseWorkflow

log = logging.getLogger(__name__)


class DdSimWorkflow(BaseWorkflow):
    """Dummy MD simulation — one class, two flavour groups (ddsim_a / ddsim_b)."""

    workflow_id = "ddsim"

    # Per-flavour score FIFOs — keyed by group name, populated dynamically.
    _scores: ClassVar[dict[str, list[float]]] = {}

    # Campaign-level stats.
    _best_raw: ClassVar[float] = float("inf")
    _n_sim: ClassVar[dict[str, int]] = {}

    # Wall-clock start time for sinusoidal duration modulation (set per run).
    _campaign_start: ClassVar[float] = 0.0

    @classmethod
    def reset_state(cls) -> None:
        cls._scores = {}
        cls._best_raw = float("inf")
        cls._n_sim = {}
        cls._campaign_start = time.time()

    # ── Compute ────────────────────────────────────────────────────────────────

    async def run(self, replica_id: str) -> None:
        cfg = self.config or {}
        duration = float(cfg.get("duration", 0.2))
        jitter   = float(cfg.get("jitter",   0.02))
        amplitude = float(cfg.get("duration_amplitude_s", 0.0))
        if amplitude > 0.0:
            t = time.time() - DdSimWorkflow._campaign_start
            period = float(cfg.get("duration_period_s",  2.0))
            phase  = float(cfg.get("duration_phase_rad", 0.0))
            duration = max(0.01, duration + amplitude * math.sin(2 * math.pi * t / period + phase))
        await asyncio.sleep(max(0.0, duration + random.gauss(0.0, jitter)))

    # ── Completion hook ────────────────────────────────────────────────────────

    async def on_replica_done(self, replica_id: str, cm, final_state: str) -> None:
        if final_state != "done":
            return

        cfg = self.config or {}
        group = self._group_name  # "ddsim_a" or "ddsim_b"

        mean = float(cfg.get("score_mean", 0.5))
        noise = float(cfg.get("score_noise", 0.10))
        score = abs(random.gauss(mean, noise))

        DdSimWorkflow._n_sim[group] = DdSimWorkflow._n_sim.get(group, 0) + 1
        is_best = score < DdSimWorkflow._best_raw
        if is_best:
            DdSimWorkflow._best_raw = score
        log.info(
            "  %s  score=%.4f  best_raw=%.4f%s",
            replica_id, score, DdSimWorkflow._best_raw, "  ★new best" if is_best else "",
        )

        # Stash score and kick off one analysis replica.
        # Pass candidate_id + score so the sharder receives real scores for
        # priority ranking and score_p50 telemetry (used by Phase1Operator).
        DdSimWorkflow._scores.setdefault(group, []).append(score)
        trigger = cfg.get("trigger_analysis", "analysis")
        await self._trigger_dependent(
            trigger,
            replicas=1,
            candidate_id=replica_id,
            score=score,
            source_stage=group,
        )


class AnalysisWorkflow(BaseWorkflow):
    """Analysis stage — refines sim scores from both flavour pools."""

    workflow_id = "analysis"

    _best_analyzed: ClassVar[float] = float("inf")
    _n_analyzed: ClassVar[dict[str, int]] = {}

    @classmethod
    def reset_state(cls) -> None:
        cls._best_analyzed = float("inf")
        cls._n_analyzed = {}

    # ── Compute ────────────────────────────────────────────────────────────────

    async def run(self, replica_id: str) -> None:
        cfg = self.config or {}
        duration = float(cfg.get("duration", 0.15))
        jitter = float(cfg.get("jitter", 0.03))
        await asyncio.sleep(max(0.0, duration + random.gauss(0.0, jitter)))

    # ── Completion hook ────────────────────────────────────────────────────────

    async def on_replica_done(self, replica_id: str, cm, final_state: str) -> None:
        if final_state != "done":
            return

        cfg = self.config or {}

        # Drain one score from any available flavour queue (insertion order).
        score, flavour = None, None
        for grp, bucket in DdSimWorkflow._scores.items():
            if bucket:
                score = bucket.pop(0)
                flavour = grp
                break

        if score is None:
            return

        # Refinement factor: look up "refinement_<suffix>" in analysis config.
        # E.g. "refinement_a" for ddsim_a, "refinement_c" for ddsim_c.
        suffix = flavour.split("_")[-1]
        factor = float(cfg.get(f"refinement_{suffix}", cfg.get("refinement_default", 0.80)))
        refined = score * factor * max(0.1, 1.0 + random.gauss(0.0, 0.02))

        AnalysisWorkflow._n_analyzed[flavour] = AnalysisWorkflow._n_analyzed.get(flavour, 0) + 1
        is_best = refined < AnalysisWorkflow._best_analyzed
        if is_best:
            AnalysisWorkflow._best_analyzed = refined
        log.info(
            "  %s  refined=%.4f  raw=%.4f  src=%s  factor=%.2f  best_analyzed=%.4f%s",
            replica_id, refined, score, flavour, factor,
            AnalysisWorkflow._best_analyzed, "  ★new best" if is_best else "",
        )
