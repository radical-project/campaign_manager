"""IMPRESS AuCaM controllers.

ImpressController  — one workflow type: goal tracking, score and budget logging.
MultiArmController — three types: per-type stats, cancel rules, plot telemetry.

The score both read is IMPRESS's `progress`: the fraction of the requested
Pareto front that is satisfied (0-1, higher is better).  `metrics["best"]`
carries the per-objective bests (ligand_iptm, complex_plddt, ...).
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from src.aucam import (
    CampaignController,
    CancelRule,
    CostCancelRule,
    LaunchRequest,
    LaunchRule,
    PolicyResult,
    WorkflowResult,
)

logger = logging.getLogger(__name__)

GOAL_N = 4
SCORE_FLOOR = 0.0  # progress floor to cancel mid-run (0 = keep all)
BUDGET_WARN_FRACTION = 0.80


def _log_budget(budget: dict, warn_fraction: float, already_warned: bool) -> bool:
    """One [BUDGET] line per cycle, and one warning when the budget runs low.

    Returns whether the warning has now been issued, so the caller can keep it
    to once per campaign.
    """
    spent_map = budget.get("spent", {})
    limits = budget.get("limits", {})
    burn = budget.get("burn", {})
    if not (spent_map or limits):
        return already_warned

    parts = []
    for dim in ("cpu_hours", "gpu_hours"):
        if dim not in limits:
            continue
        used, limit = spent_map.get(dim, 0.0), limits[dim]
        part = f"{dim}: {used:.4f}/{limit:.4f} ({100 * (used / limit if limit else 0):.1f}%)"
        if rate := burn.get(dim, {}).get("rate_per_hour", 0.0):
            part += f" @{rate:.4f}/h"
        parts.append(part)
    if parts:
        logger.info("[BUDGET] %s", "  |  ".join(parts))

    if not already_warned and limits:
        fracs = [spent_map.get(d, 0.0) / limits[d] for d in limits if limits.get(d)]
        if fracs and max(fracs) >= warn_fraction:
            logger.warning(
                "[BUDGET] WARNING: %.0f%% of budget consumed — approaching limit",
                100 * max(fracs),
            )
            return True
    return already_warned


class ImpressController(CampaignController):
    """Controller for a campaign of one IMPRESS workflow type."""

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
        self._budget_warned = _log_budget(budget, self._budget_warn_fraction, self._budget_warned)

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


# ── Three-type controller ──────────────────────────────────────────────────────


class MultiArmController(CampaignController):
    """Controller for the three workflow types (tractable/borderline/refractory).

    The scheduling policy calls a workflow type an ARM, which is why the figures
    and the bandit log lines use that word; everything here is per TYPE.
    Tracks per-type completions and progress scores, logs a summary line each
    policy cycle, and writes the JSON telemetry plot_results.py reads:

        {"event": "complete", "t": float, "wf_type": str, "progress": float,
         "rc": int, "spent": dict}
        {"event": "cycle",    "t": float, "cycle": int, "per_type": {...}}
        {"event": "select",   "t": float, "wf_type": str, "c_eff": float|null}
    """

    def __init__(
        self,
        workflow_types: list[str],
        goal_n: int = 15,
        budget_warn_fraction: float = 0.80,
        telemetry_path: str | None = None,
        in_flight_per_type: int = 2,
        score_floor: float = 0.0,
        cost_cancel: dict[str, float] | None = None,
    ) -> None:
        # A CancelRule per type: AuCaM checks the live score each policy cycle
        # and cancels a run whose progress is below the floor, without the
        # controller having to decide anything.
        rules: list = [
            CancelRule(
                workflow_type=wf_type,
                when=lambda r, floor=score_floor: (
                    isinstance(r.score, (int, float)) and r.score < floor
                ),
            )
            for wf_type in (workflow_types if score_floor > 0.0 else [])
        ]
        # Spending without improving: AuCaM tracks each run's best score and
        # what it has spent since, and cancels it when the gap grows too large.
        rules += [
            CostCancelRule(
                workflow_type=wf_type,
                max_without_improvement=dict(cost_cancel or {}),
                direction="maximize",
            )
            for wf_type in (workflow_types if cost_cancel else [])
        ]
        super().__init__(
            launch=workflow_types,
            goal_type=workflow_types[0],  # counted across all types in is_goal_reached()
            goal_n=goal_n,
            rules=rules,
        )
        self._types = workflow_types
        self._per_type = max(1, in_flight_per_type)
        self._goal_n = goal_n
        self._budget_warn_fraction = budget_warn_fraction
        self._budget_warned = False
        self._scores: dict[str, list[float]] = {t: [] for t in workflow_types}
        self._t0 = time.monotonic()
        self._cycle = 0

        self._tel_path = Path(telemetry_path) if telemetry_path else None
        self._tel_fh = None
        if self._tel_path:
            self._tel_path.parent.mkdir(parents=True, exist_ok=True)
            self._tel_fh = open(self._tel_path, "w")  # noqa: SIM115

    # ── Goal ──────────────────────────────────────────────────────────────────

    def results(self) -> int:
        """Results across every type: runs that found a design satisfying every objective.

        What the campaign is actually for. Completions are not: a run that ends
        having satisfied nothing has consumed allocation and produced no design,
        and counting it would make "the goal" something any scheduler reaches
        equally well by launching anything at all.
        """
        return sum(1 for t in self._types for score in self._scores[t] if score > 0.0)

    def is_goal_reached(self) -> bool:
        return self.results() >= self._goal_n

    # ── Per-completion hook ───────────────────────────────────────────────────

    def on_complete(self, result: WorkflowResult) -> None:
        super().on_complete(result)
        wf_type = result.workflow_type
        if wf_type not in self._types:
            return  # not one of our types: nothing to update
        if result.returncode != 0:
            over = " [over_budget]" if result.over_budget else ""
            logger.info(
                "[CTRL] %s cancelled/failed  rc=%d%s  spent=%s  results=%d/%d",
                wf_type,
                result.returncode,
                over,
                result.spent,
                self.results(),
                self._goal_n,
            )
            self._write_event(
                {
                    "event": "complete",
                    "t": self._elapsed(),
                    "wf_type": wf_type,
                    "progress": 0.0,
                    "rc": result.returncode,
                    "spent": result.spent or {},
                    # Whether the campaign was already finished when this run was
                    # stopped.  A run cancelled because a rule judged it hopeless and
                    # a run cancelled because the campaign reached its goal are both
                    # rc<0, and only the first is AuCaM saving anything.
                    "after_goal": self.is_goal_reached(),
                }
            )
            return

        progress = result.final_score
        metrics = result.final_metrics
        best = metrics.get("best", {})
        best_str = "  ".join(f"{k}={v:.3f}" for k, v in sorted(best.items())) if best else "—"

        if isinstance(progress, (int, float)):
            self._scores[wf_type].append(float(progress))
            logger.info(
                "[CTRL] %s done  progress=%.3f  front_size=%d  best=[%s]  results=%d/%d  spent=%s",
                wf_type,
                progress,
                metrics.get("front_size", 0),
                best_str,
                self.results(),
                self._goal_n,
                result.spent,
            )
            self._write_event(
                {
                    "event": "complete",
                    "t": self._elapsed(),
                    "wf_type": wf_type,
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
        self._budget_warned = _log_budget(budget, self._budget_warn_fraction, self._budget_warned)

        # Per-type score summary
        by_type = budget.get("by_type", {})
        per_type: dict[str, dict] = {}
        type_parts = []
        for wf_type in self._types:
            info = by_type.get(wf_type, {})
            mean_score = info.get("mean_score")
            n_done = self.completed.get(wf_type, 0)
            # `running` is {workflow_type: count}, which is what AuCaM passes.
            n_running = int((running or {}).get(wf_type, 0))
            per_type[wf_type] = {
                "completed": n_done,
                "mean_score": mean_score,
                "in_flight": n_running,
            }
            label = wf_type.replace("binding_", "")
            score_str = f"{mean_score:.3f}" if mean_score is not None else "—"
            type_parts.append(f"{label}: done={n_done} score={score_str}")
        logger.info("[TYPES] cycle=%d  %s", self._cycle, "  |  ".join(type_parts))

        self._write_event(
            {"event": "cycle", "t": self._elapsed(), "cycle": self._cycle, "per_type": per_type}
        )

        return super().observe(
            queue_snapshot=queue_snapshot, budget=budget, running=running, **_kwargs
        )

    # ── The decision ──────────────────────────────────────────────────────────

    def decide(self, obs: dict) -> PolicyResult:
        """One policy cycle: keep every type topped up, and stop at the goal.

        The cancel rules are applied first, because a custom `decide()` would
        otherwise skip them.  LaunchRules are left out: the launching is done
        here, and applying both would queue every type twice.
        """
        res = self._apply_rules([r for r in self._rules if not isinstance(r, LaunchRule)], obs)
        queued = obs["queue_snapshot"].by_type if obs.get("queue_snapshot") else {}
        running = obs.get("running") or {}

        for wf_type in self._types:
            want = self._per_type - int(running.get(wf_type, 0)) - int(queued.get(wf_type, 0))
            for _ in range(max(0, want)):
                res.launch_requests.append(
                    LaunchRequest.make(wf_type, context={"wf_type": wf_type})
                )

        res.terminate = self.is_goal_reached()
        return res

    def get_summary(self) -> dict:
        summary = {}
        for wf_type in self._types:
            scores = self._scores[wf_type]
            summary[wf_type] = {
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

    def record(self, event: dict) -> None:
        """Write one telemetry event, stamped with the campaign clock.

        Public because the controller is not the only thing worth recording: the
        scheduler decides which type runs next, and `RecordingPolicy` writes those
        decisions through here so every event in the file shares one clock and one
        file handle.
        """
        self._write_event({**event, "t": self._elapsed()})

    def _write_event(self, event: dict) -> None:
        if self._tel_fh:
            self._tel_fh.write(json.dumps(event) + "\n")
            self._tel_fh.flush()
