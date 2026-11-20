"""
IMPRESS AuCaM controllers.

ImpressController — single-arm: goal tracking, live score/budget logging.
MultiArmController — multi-arm bandit demo: per-arm stats, telemetry JSON for plots.

Score semantics: `progress` from CampaignDigest — fraction of the requested Pareto
front that is satisfied (0–1; higher is better). `metrics["best"]` carries
per-objective best values (e.g. ligand_iptm, complex_plddt) for the live score report.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

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


# ── Multi-arm controller ───────────────────────────────────────────────────────


class MultiArmController(CampaignController):
    """Controller for the three-arm bandit demo (binding_easy / medium / hard).

    Tracks per-arm completions and progress scores, logs a summary line each
    policy cycle, and writes a JSON telemetry file for plot_results.py.

    Telemetry schema (one JSON object per line):
        {"event": "complete", "t": float, "arm": str, "progress": float,
         "rc": int, "spent": dict}
        {"event": "cycle",    "t": float, "cycle": int,
         "per_arm": {arm: {"completed": int, "mean_score": float|null,
                           "in_flight": int}}}
    """

    def __init__(
        self,
        arms: list[str],
        goal_n: int = 15,
        budget_warn_fraction: float = 0.80,
        telemetry_path: str | None = None,
    ) -> None:
        super().__init__(
            launch=arms,
            goal_type=arms[0],  # goal counted across all arms via is_goal_reached()
            goal_n=goal_n,
        )
        self._arms = arms
        self._goal_n = goal_n
        self._budget_warn_fraction = budget_warn_fraction
        self._budget_warned = False
        self._scores: dict[str, list[float]] = {a: [] for a in arms}
        self._t0 = time.monotonic()
        self._cycle = 0

        self._tel_path = Path(telemetry_path) if telemetry_path else None
        self._tel_fh = None
        if self._tel_path:
            self._tel_path.parent.mkdir(parents=True, exist_ok=True)
            self._tel_fh = open(self._tel_path, "w")  # noqa: SIM115

    # ── Goal ──────────────────────────────────────────────────────────────────

    def is_goal_reached(self) -> bool:
        total = sum(self.completed.get(a, 0) for a in self._arms)
        return total >= self._goal_n

    # ── Per-completion hook ───────────────────────────────────────────────────

    def on_complete(self, result: WorkflowResult) -> None:
        super().on_complete(result)
        arm = result.workflow_type
        total = sum(self.completed.get(a, 0) for a in self._arms)

        if result.returncode != 0:
            over = " [over_budget]" if result.over_budget else ""
            logger.info(
                "[CTRL] %s cancelled/failed  rc=%d%s  spent=%s  total=%d/%d",
                arm,
                result.returncode,
                over,
                result.spent,
                total,
                self._goal_n,
            )
            self._write_event(
                {
                    "event": "complete",
                    "t": self._elapsed(),
                    "arm": arm,
                    "progress": 0.0,
                    "rc": result.returncode,
                    "spent": result.spent or {},
                }
            )
            return

        progress = result.final_score
        metrics = result.final_metrics
        best = metrics.get("best", {})
        best_str = "  ".join(f"{k}={v:.3f}" for k, v in sorted(best.items())) if best else "—"

        if isinstance(progress, (int, float)):
            self._scores[arm].append(float(progress))
            logger.info(
                "[CTRL] %s done  progress=%.3f  front_size=%d  best=[%s]  total=%d/%d  spent=%s",
                arm,
                progress,
                metrics.get("front_size", 0),
                best_str,
                total,
                self._goal_n,
                result.spent,
            )
            self._write_event(
                {
                    "event": "complete",
                    "t": self._elapsed(),
                    "arm": arm,
                    "progress": float(progress),
                    "rc": 0,
                    "spent": result.spent or {},
                }
            )

    # ── Policy-cycle observe hook ─────────────────────────────────────────────

    def observe(
        self,
        queue_snapshot=None,
        budget: dict | None = None,  # type: ignore[override]
        running: dict | None = None,
        **_kwargs,
    ) -> dict:
        self._cycle += 1
        budget = budget or {}
        spent_map = budget.get("spent", {})
        limits = budget.get("limits", {})
        burn = budget.get("burn", {})

        # Budget line
        if limits:
            parts = []
            for dim in ("cpu_hours", "gpu_hours"):
                if dim not in limits:
                    continue
                used = spent_map.get(dim, 0.0)
                limit = limits[dim]
                frac = used / limit if limit else 0.0
                b = burn.get(dim, {})
                rate = b.get("rate_per_hour", 0.0)
                part = f"{dim}: {used:.4f}/{limit:.4f} ({100 * frac:.1f}%)"
                if rate:
                    part += f" @{rate:.4f}/h"
                parts.append(part)
            if parts:
                logger.info("[BUDGET] %s", "  |  ".join(parts))

        if not self._budget_warned and limits:
            fracs = [spent_map.get(d, 0.0) / limits[d] for d in limits if limits.get(d)]
            if fracs and max(fracs) >= self._budget_warn_fraction:
                self._budget_warned = True
                logger.warning(
                    "[BUDGET] WARNING: %.0f%% of budget consumed — approaching limit",
                    100 * max(fracs),
                )

        # Per-arm score summary
        by_type = budget.get("by_type", {})
        per_arm: dict[str, dict] = {}
        arm_parts = []
        for arm in self._arms:
            info = by_type.get(arm, {})
            mean_score = info.get("mean_score")
            n_done = self.completed.get(arm, 0)
            n_running = len(
                [1 for wf in (running or {}).values() if getattr(wf, "workflow_type", None) == arm]
            )
            per_arm[arm] = {"completed": n_done, "mean_score": mean_score, "in_flight": n_running}
            label = arm.replace("binding_", "")
            score_str = f"{mean_score:.3f}" if mean_score is not None else "—"
            arm_parts.append(f"{label}: done={n_done} score={score_str}")
        logger.info("[ARMS] cycle=%d  %s", self._cycle, "  |  ".join(arm_parts))

        self._write_event(
            {"event": "cycle", "t": self._elapsed(), "cycle": self._cycle, "per_arm": per_arm}
        )

        return super().observe(
            queue_snapshot=queue_snapshot, budget=budget, running=running, **_kwargs
        )

    # ── Summary ───────────────────────────────────────────────────────────────

    def get_summary(self) -> dict:
        summary = {}
        for arm in self._arms:
            scores = self._scores[arm]
            summary[arm] = {
                "n": len(scores),
                "best_progress": max(scores) if scores else None,
                "avg_progress": sum(scores) / len(scores) if scores else None,
            }
        if self._tel_fh:
            self._tel_fh.close()
            self._tel_fh = None
            logger.info("[CTRL] telemetry written to %s", self._tel_path)
        return summary

    # ── Internal ──────────────────────────────────────────────────────────────

    def _elapsed(self) -> float:
        return round(time.monotonic() - self._t0, 3)

    def _write_event(self, event: dict) -> None:
        if self._tel_fh:
            self._tel_fh.write(json.dumps(event) + "\n")
            self._tel_fh.flush()
