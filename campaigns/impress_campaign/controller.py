"""
ImpressController — AuCaM campaign controller for IMPRESS protein-binding runs.

Score semantics: `progress` from CampaignDigest — fraction of the requested Pareto
front that is satisfied (0–1; higher is better). `metrics["best"]` carries
per-objective best values (e.g. ligand_iptm, complex_plddt) for the live score report.

The controller:
  - tracks best and mean progress per completed workflow
  - logs per-objective best scores from each completed run
  - emits a budget-status line each policy cycle from obs["budget"]
  - warns once when budget_warn_fraction is consumed
  - terminates when goal_n workflows complete
  - cancels workflows whose mid-run score is below score_floor (optional early cut)
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
SCORE_FLOOR = 0.0  # progress floor to cancel mid-run (0 = keep all)
BUDGET_WARN_FRACTION = 0.80


class ImpressController(CampaignController):
    """Controller for a single-arm IMPRESS campaign."""

    WORKFLOW_TYPE = "impress"

    def __init__(
        self,
        goal_n: int = GOAL_N,
        score_floor: float = SCORE_FLOOR,
        budget_warn_fraction: float = BUDGET_WARN_FRACTION,
    ) -> None:
        rules = []
        if score_floor > 0.0:
            rules.append(
                CancelRule(
                    workflow_type=self.WORKFLOW_TYPE,
                    when=lambda r: (
                        r.score is not None
                        and isinstance(r.score, (int, float))
                        and r.score < score_floor
                    ),
                )
            )
        super().__init__(
            launch=[self.WORKFLOW_TYPE],
            goal_type=self.WORKFLOW_TYPE,
            goal_n=goal_n,
            rules=rules,
        )
        self._goal_n = goal_n
        self._budget_warn_fraction = budget_warn_fraction
        self._progress_scores: list[float] = []
        self._budget_warned = False

    # ── Goal ──────────────────────────────────────────────────────────────────

    def is_goal_reached(self) -> bool:
        return self.completed.get(self.WORKFLOW_TYPE, 0) >= self._goal_n

    # ── Per-completion hook ───────────────────────────────────────────────────

    def on_complete(self, result: WorkflowResult) -> None:
        super().on_complete(result)
        n_done = self.completed.get(self.WORKFLOW_TYPE, 0)

        if result.returncode != 0:
            over = " [over_budget]" if result.over_budget else ""
            logger.info(
                "[CTRL] impress cancelled/failed  rc=%d%s  spent=%s  total=%d/%d",
                result.returncode,
                over,
                result.spent,
                n_done,
                self._goal_n,
            )
            return

        progress = result.final_score
        metrics = result.final_metrics

        # Log per-objective best from the last CampaignDigest report
        best = metrics.get("best", {})
        best_str = "  ".join(f"{k}={v:.3f}" for k, v in sorted(best.items())) if best else "—"

        if isinstance(progress, (int, float)):
            self._progress_scores.append(float(progress))
            logger.info(
                "[CTRL] impress done  progress=%.3f  front_size=%d  best=[%s]"
                "  total=%d/%d  spent=%s",
                progress,
                metrics.get("front_size", 0),
                best_str,
                n_done,
                self._goal_n,
                result.spent,
            )
        else:
            logger.info(
                "[CTRL] impress done (no progress score)  best=[%s]  total=%d/%d  spent=%s",
                best_str,
                n_done,
                self._goal_n,
                result.spent,
            )

    # ── Policy-cycle observe hook (live score + budget monitor) ───────────────

    def observe(
        self,
        queue_snapshot=None,
        budget: dict | None = None,  # type: ignore[override]
        running: dict | None = None,
        **_kwargs,
    ) -> dict:
        """Log live budget and score state each policy cycle."""
        budget = budget or {}
        burn = budget.get("burn", {})
        spent_map = budget.get("spent", {})
        limits = budget.get("limits", {})

        # Budget line (only when something is in flight or spent)
        if spent_map or limits:
            parts = []
            for dim in ("cpu_hours", "gpu_hours"):
                if dim not in limits:
                    continue
                used = spent_map.get(dim, 0.0)
                limit = limits[dim]
                frac = used / limit if limit else 0.0
                b = burn.get(dim, {})
                rate = b.get("rate_per_hour", 0.0)
                eta = b.get("hours_to_limit")
                part = f"{dim}: {used:.4f}/{limit:.4f} ({100 * frac:.1f}%)"
                if rate:
                    part += f" @{rate:.4f}/h"
                if eta is not None:
                    part += f" eta={eta:.2f}h"
                parts.append(part)
            if parts:
                logger.info("[BUDGET] %s", "  |  ".join(parts))

        # One-time warning when budget is mostly consumed
        if not self._budget_warned and limits:
            fracs = [spent_map.get(d, 0.0) / limits[d] for d in limits if limits.get(d)]
            if fracs and max(fracs) >= self._budget_warn_fraction:
                self._budget_warned = True
                logger.warning(
                    "[BUDGET] WARNING: %.0f%% of budget consumed — approaching limit",
                    100 * max(fracs),
                )

        # Live score summary from running workflows
        by_type = budget.get("by_type", {}).get(self.WORKFLOW_TYPE, {})
        mean_score = by_type.get("mean_score")
        n_completed = by_type.get("completed", 0)
        if mean_score is not None:
            logger.debug(
                "[SCORE] running mean_progress=%.3f  completed=%d",
                mean_score,
                n_completed,
            )

        return super().observe(
            queue_snapshot=queue_snapshot, budget=budget, running=running, **_kwargs
        )

    # ── Summary ───────────────────────────────────────────────────────────────

    def get_summary(self) -> dict:
        scores = self._progress_scores
        return {
            "impress": {
                "n": len(scores),
                "best_progress": max(scores) if scores else None,
                "avg_progress": sum(scores) / len(scores) if scores else None,
            }
        }
