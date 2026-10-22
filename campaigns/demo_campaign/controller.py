"""
Dummy campaign controller.

Demonstrates that science policy belongs to the campaign, not to the CM.
The CM is a pure orchestrator — it calls decide() and on_complete() without
knowing what SCORE_FLOOR is, which workflow types exist, or what "done" means.

All mechanics (counting, termination check, rule application) are handled
by CampaignController.  This subclass only declares:
  • what to cancel  (CancelRule — score below floor)
  • what to launch  (sim_worker, via launch= shorthand)
  • what "done" means  (goal_n analyses completed)
  • a domain log line on analysis completion
"""

from __future__ import annotations

import logging

from src.aucam import CancelRule, CampaignController
from src.aucam.types import WorkflowResult

logger = logging.getLogger(__name__)

SCORE_FLOOR   = 0.4   # simulations scoring below this are cancelled
GOAL_ANALYSES = 3     # default analyses before campaign stops


class ScoreController(CampaignController):
    """
    Science-specific policy for the dummy campaign.

    Inherits all mechanics from CampaignController.
    Only the domain knowledge (score floor, goal count) lives here.
    """

    def __init__(
        self,
        goal_analyses: int = GOAL_ANALYSES,
        use_tier2: bool = False,
        score_floor: float = SCORE_FLOOR,
    ) -> None:
        launch = ["sim_worker", "sim_worker_b"]
        if use_tier2:
            launch.append("tier2_sim_worker")
        rules = [
            CancelRule(
                workflow_type="sim_worker",
                when=lambda r: r.score is not None and r.score < score_floor,
            ),
            CancelRule(
                workflow_type="sim_worker_b",
                when=lambda r: r.score is not None and r.score < score_floor,
            ),
        ]
        if use_tier2:
            rules.append(CancelRule(
                workflow_type="tier2_sim_worker",
                when=lambda r: r.score is not None and r.score < score_floor,
            ))
        super().__init__(
            launch=launch,
            goal_type="analysis_worker",
            goal_n=goal_analyses,
            rules=rules,
        )
        self._analysis_scores: list[float] = []

    def on_complete(self, result: WorkflowResult) -> None:
        super().on_complete(result)   # base updates completed/cancelled/failed counters
        if result.returncode == 0 and result.workflow_type == "analysis_worker":
            n = self.completed.get("analysis_worker", 0)
            logger.info("[CTRL] analysis complete (%d/%d)", n, self._goal_n)
            score = result.final_metrics.get("score")
            if score is not None:
                self._analysis_scores.append(float(score))

    def get_summary(self) -> dict:
        if not self._analysis_scores:
            return {}
        return {
            "best_score": max(self._analysis_scores),
            "avg_score": sum(self._analysis_scores) / len(self._analysis_scores),
            "n_scores": len(self._analysis_scores),
            "scores": self._analysis_scores,
        }
