"""
CandidateLog — per-candidate result history across pipeline stages.

Workflows call trigger_dependent() with a score; the CM records it here.
Two purposes:
  1. threshold_top_fraction gating — streaming quantile cutoff decides whether
     a candidate advances to the next stage.
  2. Sharder priority signals — score / surrogate / uncertainty / enqueue_time
     stored here are read by the sharder's profile-based priority scorer.

Not thread-safe by design: all access goes through the CM's asyncio lock.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

import numpy as np


@dataclass
class StageResult:
    """One per-stage record in a candidate's history."""
    stage_id:       str
    score:          float
    surrogate_pred: float = 0.0
    surrogate_unc:  float = 0.0
    scaffold_class: str   = ""
    decision:       str   = ""   # "passed" | "filtered" | ""
    timestamp:      float = field(default_factory=time.time)


@dataclass
class CandidateHistory:
    """Accumulated per-stage results for one candidate."""
    candidate_id:   str
    scaffold_class: str   = ""
    enqueue_time:   float = field(default_factory=time.time)
    results:        list[StageResult] = field(default_factory=list)

    @property
    def latest_score(self) -> float:
        return self.results[-1].score if self.results else 0.0

    @property
    def latest_surrogate_pred(self) -> float:
        return self.results[-1].surrogate_pred if self.results else 0.0

    @property
    def latest_surrogate_unc(self) -> float:
        return self.results[-1].surrogate_unc if self.results else 0.0

    def score_at(self, stage_id: str) -> Optional[float]:
        """Most recent score recorded for stage_id, or None."""
        for r in reversed(self.results):
            if r.stage_id == stage_id:
                return r.score
        return None


class CandidateLog:
    """In-memory registry of candidate histories."""

    def __init__(self) -> None:
        self._histories: dict[str, CandidateHistory] = {}
        self._stage_scores: dict[str, list[float]] = {}   # stage_id → all scores seen

    # ── Write ─────────────────────────────────────────────────────────────────

    def register(
        self,
        candidate_id: str,
        scaffold_class: str = "",
        enqueue_time: Optional[float] = None,
    ) -> CandidateHistory:
        """Create a history record for a new candidate (idempotent)."""
        if candidate_id not in self._histories:
            self._histories[candidate_id] = CandidateHistory(
                candidate_id=candidate_id,
                scaffold_class=scaffold_class,
                enqueue_time=enqueue_time if enqueue_time is not None else time.time(),
            )
        return self._histories[candidate_id]

    def record(
        self,
        candidate_id: str,
        stage_id: str,
        score: float,
        surrogate_pred: float = 0.0,
        surrogate_unc: float = 0.0,
        scaffold_class: str = "",
        decision: str = "",
    ) -> StageResult:
        """Append a stage result. Auto-registers the candidate if unknown."""
        if candidate_id not in self._histories:
            self.register(candidate_id, scaffold_class=scaffold_class)
        result = StageResult(
            stage_id=stage_id,
            score=score,
            surrogate_pred=surrogate_pred,
            surrogate_unc=surrogate_unc,
            scaffold_class=scaffold_class,
            decision=decision,
        )
        self._histories[candidate_id].results.append(result)
        self._stage_scores.setdefault(stage_id, []).append(score)
        return result

    # ── Read ──────────────────────────────────────────────────────────────────

    def get(self, candidate_id: str) -> Optional[CandidateHistory]:
        return self._histories.get(candidate_id)

    def threshold_cutoff(self, stage_id: str, top_fraction: float) -> float:
        """Running quantile score cutoff for the top_fraction at stage_id.

        Returns -inf when fewer than 2 scores recorded so early candidates
        always pass (the distribution isn't established yet).
        """
        scores = self._stage_scores.get(stage_id, [])
        if len(scores) < 2:
            return float("-inf")
        arr = np.asarray(scores, dtype=float)
        quantile = max(0.0, min(1.0, 1.0 - top_fraction))
        return float(np.quantile(arr, quantile))

    def passes_threshold(
        self,
        candidate_id: str,
        stage_id: str,
        top_fraction: float,
    ) -> bool:
        """True if the candidate's score at stage_id is in the top_fraction.

        Always returns True when top_fraction >= 1.0 or no score is recorded.
        """
        if top_fraction >= 1.0:
            return True
        history = self._histories.get(candidate_id)
        if history is None:
            return True
        score = history.score_at(stage_id)
        if score is None:
            return True
        return score >= self.threshold_cutoff(stage_id, top_fraction)

    def stage_summary(self, stage_id: str) -> dict:
        """Score distribution stats for one stage — for monitor logging."""
        scores = self._stage_scores.get(stage_id, [])
        if not scores:
            return {"n": 0}
        arr = np.asarray(scores, dtype=float)
        return {
            "n":    len(scores),
            "mean": round(float(arr.mean()), 4),
            "p50":  round(float(np.median(arr)), 4),
            "p90":  round(float(np.quantile(arr, 0.90)), 4),
            "max":  round(float(arr.max()), 4),
        }
