"""
DummyWorkflow — minimization campaign workflow.

Both the search and refine stages use this single class.  The CM injects
``_group_name`` ("search" or "refine") at construction time, so the workflow
knows which stage it is without needing extra config keys.

search stage  — generates a random score in [0, 1] (simulates blind candidate
                screening).  Candidates below ``refine_threshold`` are queued
                in the class-level FIFO (_refine_scores) and trigger a refine
                replica.

refine stage  — pops the next score from the FIFO queue and improves it
                (simulated: score × score_decay + small Gaussian noise).

Score passing uses a ClassVar list as a FIFO queue instead of CM config so that
concurrent search completions do not overwrite each other's init_score.  The
asyncio cooperative scheduler ensures list.append / list.pop(0) are never
interrupted mid-call.

The campaign objective is to minimise the score.  After the campaign,
``DummyWorkflow._best_score`` and ``_n_evaluated`` carry the outcome
(reset between benchmark runs via reset_state()).

Config keys (beyond CM scheduling keys):
  duration          float  simulated compute seconds per replica (default 0.5)
  jitter            float  max extra seconds of random latency   (default 0.1)
  refine_threshold  float  score gate — only candidates below this are refined
                           (default 0.35)
  trigger_refine    str    group name to trigger for refinement  (default "refine")
  score_decay       float  refine improvement factor             (default 0.6)
  score_noise       float  Gaussian σ for multiplicative noise   (default 0.03)
"""

from __future__ import annotations

import asyncio
import random
from typing import ClassVar

from src.campaign import BaseWorkflow


class DummyWorkflow(BaseWorkflow):
    """Minimization workflow — one class serves both search and refine stages."""

    workflow_id = "dummy"

    # Shared across all replicas; reset via reset_state() between benchmark runs.
    _best_score: ClassVar[float] = float("inf")
    _n_evaluated: ClassVar[int] = 0
    # FIFO queue: search appends upstream scores; refine pops them in order.
    # Using a plain list (not asyncio.Queue) because asyncio cooperative
    # scheduling means list.append / pop(0) are never interleaved mid-call.
    _refine_scores: ClassVar[list] = []

    @classmethod
    def reset_state(cls) -> None:
        """Reset shared ClassVar state between benchmark runs."""
        cls._best_score = float("inf")
        cls._n_evaluated = 0
        cls._refine_scores = []

    # ── Compute ────────────────────────────────────────────────────────────────

    async def run(self, replica_id: str) -> None:
        cfg = self.config or {}
        duration = float(cfg.get("duration", 0.5))
        jitter = float(cfg.get("jitter", 0.1))
        await asyncio.sleep(duration + random.uniform(0.0, jitter))

    # ── Completion hook ────────────────────────────────────────────────────────

    async def on_replica_done(self, replica_id: str, cm, final_state: str) -> None:
        if final_state != "done":
            return

        cfg = self.config or {}

        if self._group_name == "search":
            # Search stage: blind random score (simulates screening library).
            score = random.random()
            DummyWorkflow._n_evaluated += 1
            if score < DummyWorkflow._best_score:
                DummyWorkflow._best_score = score

            # Route promising candidates to the refine stage.
            threshold = float(cfg.get("refine_threshold", 0.35))
            trigger_group = cfg.get("trigger_refine", "refine")
            if score < threshold:
                DummyWorkflow._refine_scores.append(score)
                await self._trigger_dependent(trigger_group, replicas=1)

        else:
            # Refine stage: improve an upstream candidate score.
            # Pop the next queued score (FIFO — matches the trigger order).
            init_score = (
                DummyWorkflow._refine_scores.pop(0) if DummyWorkflow._refine_scores else 0.2
            )
            decay = float(cfg.get("score_decay", 0.6))
            noise = float(cfg.get("score_noise", 0.03))
            score = init_score * decay * max(0.1, 1.0 + random.gauss(0.0, noise))
            DummyWorkflow._n_evaluated += 1
            if score < DummyWorkflow._best_score:
                DummyWorkflow._best_score = score
