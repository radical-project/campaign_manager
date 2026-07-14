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
import random
from typing import ClassVar

from src.campaign import BaseWorkflow


class DdSimWorkflow(BaseWorkflow):
    """Dummy MD simulation — one class, two flavour groups (ddsim_a / ddsim_b)."""

    workflow_id = "ddsim"

    # Per-flavour score FIFOs — analysis pops from the flavour that triggered it.
    _scores: ClassVar[dict[str, list[float]]] = {"ddsim_a": [], "ddsim_b": []}

    # Campaign-level stats.
    _best_raw: ClassVar[float] = float("inf")
    _n_sim: ClassVar[dict[str, int]] = {"ddsim_a": 0, "ddsim_b": 0}

    @classmethod
    def reset_state(cls) -> None:
        cls._scores = {"ddsim_a": [], "ddsim_b": []}
        cls._best_raw = float("inf")
        cls._n_sim = {"ddsim_a": 0, "ddsim_b": 0}

    # ── Compute ────────────────────────────────────────────────────────────────

    async def run(self, replica_id: str) -> None:
        cfg = self.config or {}
        duration = float(cfg.get("duration", 0.2))
        jitter = float(cfg.get("jitter", 0.02))
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
        if score < DdSimWorkflow._best_raw:
            DdSimWorkflow._best_raw = score

        # Stash score and kick off one analysis replica.
        DdSimWorkflow._scores.setdefault(group, []).append(score)
        trigger = cfg.get("trigger_analysis", "analysis")
        await self._trigger_dependent(trigger, replicas=1)


class AnalysisWorkflow(BaseWorkflow):
    """Analysis stage — refines sim scores from both flavour pools."""

    workflow_id = "analysis"

    _best_analyzed: ClassVar[float] = float("inf")
    _n_analyzed: ClassVar[dict[str, int]] = {"ddsim_a": 0, "ddsim_b": 0}

    @classmethod
    def reset_state(cls) -> None:
        cls._best_analyzed = float("inf")
        cls._n_analyzed = {"ddsim_a": 0, "ddsim_b": 0}

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

        # Drain one score from whichever flavour queue has an entry.
        # ddsim_b (long/fine) results are preferred — lower refinement needed.
        score, flavour = None, None
        for grp in ("ddsim_b", "ddsim_a"):
            bucket = DdSimWorkflow._scores.get(grp, [])
            if bucket:
                score = bucket.pop(0)
                flavour = grp
                break

        if score is None:
            return

        factor_key = "refinement_b" if flavour == "ddsim_b" else "refinement_a"
        factor = float(cfg.get(factor_key, 0.85 if flavour == "ddsim_a" else 0.70))
        refined = score * factor * max(0.1, 1.0 + random.gauss(0.0, 0.02))

        AnalysisWorkflow._n_analyzed[flavour] = AnalysisWorkflow._n_analyzed.get(flavour, 0) + 1
        if refined < AnalysisWorkflow._best_analyzed:
            AnalysisWorkflow._best_analyzed = refined
