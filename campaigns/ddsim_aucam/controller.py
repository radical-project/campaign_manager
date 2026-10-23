"""
MiniAppsController — AuCaM campaign controller for the MiniApps pipeline.

Two workflow types compete within each campaign:
  miniapps_fast — baseline num_step
  miniapps_slow — num_step × slow_step_multiplier

The bandit scheduling policy selects between them based on observed MSE scores.
The campaign terminates when goal_n total completions (across both types) are reached.
"""

from __future__ import annotations

import logging

from src.aucam import (
    CampaignController,
    CancelRule,
    WorkflowResult,
)

logger = logging.getLogger(__name__)

GOAL_N = 4
SCORE_CEILING = 0.95


class MiniAppsController(CampaignController):
    """Controller for a MiniApps pipeline campaign with fast and slow arms."""

    WORKFLOW_TYPES = ("miniapps_fast", "miniapps_slow")

    def __init__(
        self,
        goal_n: int = GOAL_N,
        score_ceiling: float = SCORE_CEILING,
    ) -> None:
        super().__init__(
            launch=list(self.WORKFLOW_TYPES),
            goal_type=None,  # is_goal_reached() sums both types
            goal_n=goal_n,
            rules=[
                CancelRule(
                    workflow_type=wt,
                    when=lambda r: r.score is not None and r.score > score_ceiling,
                )
                for wt in self.WORKFLOW_TYPES
            ],
        )
        self._goal_n = goal_n
        self._score_ceiling = score_ceiling
        self._scores: dict[str, list[float]] = {wt: [] for wt in self.WORKFLOW_TYPES}

    # ── Goal ──────────────────────────────────────────────────────────────────

    def is_goal_reached(self) -> bool:
        total = sum(self.completed.get(wt, 0) for wt in self.WORKFLOW_TYPES)
        return total >= self._goal_n

    # ── Lifecycle hook ────────────────────────────────────────────────────────

    def on_complete(self, result: WorkflowResult) -> None:
        super().on_complete(result)
        if result.returncode != 0:
            return
        score = result.final_metrics.get("score")
        if score is None:
            return
        wt = result.workflow_type
        if wt in self._scores:
            self._scores[wt].append(float(score))
        n_total = sum(self.completed.get(wt, 0) for wt in self.WORKFLOW_TYPES)
        logger.info(
            "[CTRL] %s done  score=%.4f  total=%d/%d",
            wt,
            score,
            n_total,
            self._goal_n,
        )

    # ── Summary ───────────────────────────────────────────────────────────────

    def get_summary(self) -> dict:
        summary: dict = {}
        all_scores: list[float] = []
        for wt in self.WORKFLOW_TYPES:
            scores = self._scores[wt]
            summary[wt] = {
                "n": len(scores),
                "best": min(scores) if scores else None,
                "avg": sum(scores) / len(scores) if scores else None,
            }
            all_scores.extend(scores)
        if all_scores:
            summary["overall"] = {
                "n": len(all_scores),
                "best": min(all_scores),
                "avg": sum(all_scores) / len(all_scores),
            }
        return summary
