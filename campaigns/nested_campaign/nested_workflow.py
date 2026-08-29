"""Dummy workflows for the nested-loop campaign demo.

Two workflow groups drive the nested loop:

  sim       Generates a random score, sleeps briefly, then triggers one
            analysis replica, passing ``score=`` through to the score gate.

  analysis  Sleeps briefly; the score it received from the triggering sim
            replica is available via ``self.config["score"]``.  Counts
            total hits and tracks the running-best score via ClassVars.

ClassVar pattern (from CONTRIBUTING.md):
  All mutable per-campaign state lives in ClassVars with a ``reset_state()``
  classmethod so that benchmark harnesses can reset between runs without
  re-importing the module.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import ClassVar

from src.campaign import BaseWorkflow

log = logging.getLogger(__name__)


class SimWorkflow(BaseWorkflow):
    """Dummy simulation: sleep, draw a score, trigger one analysis replica."""

    workflow_id = "sim"

    _total_sims: ClassVar[int] = 0
    _best_raw:   ClassVar[float] = 0.0

    @classmethod
    def reset_state(cls) -> None:
        cls._total_sims = 0
        cls._best_raw   = 0.0

    async def run(self, replica_id: str) -> None:
        cfg = self.config or {}
        duration = float(cfg.get("duration", 0.05))
        await asyncio.sleep(max(0.0, duration + random.gauss(0.0, duration * 0.1)))

    async def on_replica_done(self, replica_id: str, cm, final_state: str) -> None:
        if final_state != "done":
            return

        # Draw a score from a Gaussian centred at 0.5, clipped to [0, 1].
        score = min(1.0, max(0.0, random.gauss(0.5, 0.15)))

        SimWorkflow._total_sims += 1
        if score > SimWorkflow._best_raw:
            SimWorkflow._best_raw = score

        log.debug(
            "  [sim] %s  score=%.3f  best_raw=%.3f",
            replica_id, score, SimWorkflow._best_raw,
        )

        # Trigger one analysis replica.  source_stage="sim" causes the CM to log
        # this candidate in _candidate_log so the executor can inject candidate_score
        # into the analysis workflow's config.  score= feeds the sharder (for
        # score_p50 telemetry) and the score gate (for set_score_cutoff filtering).
        await self._trigger_dependent(
            "analysis",
            replicas=1,
            candidate_id=replica_id,
            score=score,
            source_stage="sim",
        )


class AnalysisWorkflow(BaseWorkflow):
    """Dummy analysis: sleep, record hit and score from triggering sim."""

    workflow_id = "analysis"

    _total_hits:  ClassVar[int]   = 0
    _best_score:  ClassVar[float] = 0.0

    @classmethod
    def reset_state(cls) -> None:
        cls._total_hits = 0
        cls._best_score = 0.0

    async def run(self, replica_id: str) -> None:
        cfg = self.config or {}
        duration = float(cfg.get("duration", 0.02))
        await asyncio.sleep(max(0.0, duration + random.gauss(0.0, duration * 0.1)))

    async def on_replica_done(self, replica_id: str, cm, final_state: str) -> None:
        if final_state != "done":
            return

        # The executor injects the candidate's score as "candidate_score"
        # (populated from _candidate_log when source_stage= was supplied).
        score = float((self.config or {}).get("candidate_score", 0.5))

        AnalysisWorkflow._total_hits += 1
        if score > AnalysisWorkflow._best_score:
            AnalysisWorkflow._best_score = score

        log.debug(
            "  [analysis] hit=%d  score=%.3f  best=%.3f",
            AnalysisWorkflow._total_hits, score, AnalysisWorkflow._best_score,
        )
