"""Experiment 2: N good designs, for the fewest gpu_hours.

A design is *good* once its quality reaches a bar. IMPRESS keeps improving a run
for as long as it is allowed to, so the controller decides, every policy cycle,
whether each run is worth its next cycle. The scheduler learns which type
delivers good designs cheapest (AuCaM's cost-aware bandit).

    design_quality()   — one number per design, from the objectives' bars
    QualityController  — keeps a run while it pays, stops it when it does not
    good_design_reward — the bandit's reward: 1 for a good design, else 0
"""

from __future__ import annotations

import logging
import math
from pathlib import Path

import yaml

from src.aucam import CampaignController, CampaignDecision, PolicyResult, WorkflowResult

from controller import MultiArmController

logger = logging.getLogger(__name__)

DIM = "gpu_hours"


def objective_bars(spec_path: str | Path) -> dict[str, float]:
    """Each bounded objective's `min`, read from an IMPRESS spec."""
    spec = yaml.safe_load(Path(spec_path).read_text())
    return {o["name"]: float(o["min"]) for o in spec.get("objectives", []) if "min" in o}


def design_quality(best: dict | None, bars: dict[str, float]) -> float | None:
    """How far a design is above its bars: 0 just at them, 1 perfect, None if no design.

    Per objective, the margin above its bar as a fraction of the room left above
    it (these metrics top out at 1.0); the design's quality is its weakest margin,
    so one poor objective cannot be bought back by a strong one.
    """
    if not best or any(k not in best for k in bars):
        return None
    return min((best[k] - bar) / (1.0 - bar) for k, bar in bars.items())


def good_design_reward(bars: dict[str, float], bar: float):
    """The bandit's reward for a finished run: 1.0 if it left a good design, else 0.0."""

    def reward(result: WorkflowResult) -> float:
        q = design_quality((result.final_metrics or {}).get("best"), bars)
        return 1.0 if q is not None and q >= bar else 0.0

    return reward


class QualityController(MultiArmController):
    """Decides, each policy cycle, whether every running design run is worth continuing.

    A run whose design reaches the quality bar is ended with FINISH: it is good,
    and more cycles add little, so AuCaM counts it as completed. A run is ended
    with CANCEL, its design kept, when:

      * it has no design after `no_design` gpu_hours — nothing to refine;
      * it has not improved for `patience` gpu_hours — it has plateaued;
      * what it still needs to reach the bar (judged by its type's good designs
        so far) is more than its share of the budget left (budget left ÷ good
        designs still needed).

    With `stops=False` it never stops a run: each one runs until IMPRESS ends it,
    which is the baseline the figures compare against.
    """

    def __init__(
        self,
        workflow_types: list[str],
        *,
        bars: dict[str, float],
        quality_bar: float,
        goal_n: int,
        no_design: float,
        patience: float,
        stops: bool = True,
        telemetry_path: str | None = None,
        in_flight_per_type: int = 2,
        budget_warn_fraction: float = 0.80,
    ) -> None:
        super().__init__(
            workflow_types=workflow_types,
            goal_n=goal_n,
            budget_warn_fraction=budget_warn_fraction,
            telemetry_path=telemetry_path,
            in_flight_per_type=in_flight_per_type,
        )
        self._bars = bars
        self._bar = quality_bar
        self._no_design = no_design
        self._patience = patience
        self._stops = stops
        self._runs: dict[str, dict] = {}  # wf_id -> best quality, spend at best, last spend
        self._stopped: set[str] = set()  # runs already ended, so not decided twice
        self._good: dict[str, list[float]] = {
            t: [] for t in workflow_types
        }  # spend per good design
        self._quality: dict[str, list[float]] = {t: [] for t in workflow_types}

    # ── Goal ──────────────────────────────────────────────────────────────────

    def results(self) -> int:
        """Good designs found so far, across every type."""
        return sum(len(v) for v in self._good.values())

    # ── The decision ──────────────────────────────────────────────────────────

    def decide(self, obs: dict) -> PolicyResult:
        res = PolicyResult()
        reports = obs.get("live_reports") or {}
        spend = obs.get("wf_spend") or {}
        share = self._budget_share(obs.get("budget") or {})

        for wf_id, wf_type in (obs.get("wf_types") or {}).items():
            if wf_type not in self._types or wf_id in self._stopped:
                continue
            report = reports.get(wf_id)
            q = design_quality((report.metrics or {}).get("best") if report else None, self._bars)
            spent = float((spend.get(wf_id) or {}).get(DIM, 0.0))
            run = self._track(wf_id, wf_type, q, spent)
            why = self._stop_reason(wf_type, run, spent, share) if self._stops else None
            if why:
                self._stopped.add(wf_id)
                done = run["best"] is not None and run["best"] >= self._bar
                res.decisions[wf_id] = CampaignDecision.FINISH if done else CampaignDecision.CANCEL
                res.reasons[wf_id] = why

        self._top_up(res, obs)
        res.terminate = self.is_goal_reached()
        return res

    def _track(self, wf_id: str, wf_type: str, q: float | None, spent: float) -> dict:
        """Update one run's best quality, and record a new point of its trajectory."""
        run = self._runs.setdefault(wf_id, {"best": None, "at": 0.0, "last": None})
        if q is not None and (run["best"] is None or q > run["best"]):
            run["best"], run["at"] = q, spent
        if spent != run["last"]:
            run["last"] = spent
            self.record(
                {"event": "progress", "wf_id": wf_id, "wf_type": wf_type, DIM: spent, "quality": q}
            )
        return run

    def _stop_reason(self, wf_type: str, run: dict, spent: float, share: float) -> str | None:
        best = run["best"]
        if best is not None and best >= self._bar:
            return f"reached the bar (quality {best:.2f})"
        if best is None:
            if spent >= self._no_design:
                return f"no design after {spent:.3f} {DIM}"
            return None
        if spent - run["at"] >= self._patience:
            return f"no improvement for {spent - run['at']:.3f} {DIM} (quality {best:.2f})"
        typical = self._good[wf_type]
        if typical:
            still_needed = sum(typical) / len(typical) - spent
            if still_needed > share:
                return (
                    f"needs ~{still_needed:.3f} more {DIM} to reach the bar, "
                    f"over the {share:.3f} left per design"
                )
        return None

    def _budget_share(self, budget: dict) -> float:
        """Budget left ÷ good designs still needed (infinite without a limit)."""
        limit = (budget.get("limits") or {}).get(DIM)
        if not limit:
            return math.inf
        left = limit - (budget.get("spent") or {}).get(DIM, 0.0)
        return max(0.0, left) / max(1, self._goal_n - self.results())

    # ── Per-completion hook ───────────────────────────────────────────────────

    def on_complete(self, result: WorkflowResult) -> None:
        CampaignController.on_complete(self, result)
        wf_type, wf_id = result.workflow_type, result.workflow_id
        if wf_type not in self._types:
            return
        after_goal = self.is_goal_reached()
        q = design_quality((result.final_metrics or {}).get("best"), self._bars)
        spent = float((result.spent or {}).get(DIM, 0.0))
        good = q is not None and q >= self._bar and not after_goal
        if good:
            self._good[wf_type].append(spent)
        if q is not None:
            self._quality[wf_type].append(q)

        self._stopped.discard(wf_id)
        if result.finished:
            how = "finished: " + (result.reason or "")
        elif result.returncode == 0:
            how = "ran to the end"
        elif result.reason:
            how = "cancelled: " + result.reason
        elif after_goal:
            how = "cancelled: campaign done"
        else:
            how = f"failed (rc={result.returncode})"
        self._runs.pop(wf_id, None)

        logger.info(
            "[CTRL] %s %s  quality=%s  good=%s  spent=%.3f  good designs=%d/%d",
            wf_id,
            how,
            "—" if q is None else f"{q:.2f}",
            good,
            spent,
            self.results(),
            self._goal_n,
        )
        self._write_event(
            {
                "event": "complete",
                "t": self._elapsed(),
                "wf_id": wf_id,
                "wf_type": wf_type,
                "quality": q,
                "good": good,
                "progress": 1.0 if good else 0.0,  # the bandit's reward, for figure 2
                "rc": result.returncode,
                "how": how,
                "spent": result.spent or {},
                "after_goal": after_goal,
            }
        )

    def get_summary(self) -> dict:
        summary = {
            t: {
                "n": len(self._good[t]),
                "best_progress": max(self._quality[t]) if self._quality[t] else None,
                "avg_progress": (
                    sum(self._quality[t]) / len(self._quality[t]) if self._quality[t] else None
                ),
            }
            for t in self._types
        }
        if self._tel_fh:
            self._tel_fh.close()
            self._tel_fh = None
            logger.info("[CTRL] telemetry written to %s", self._tel_path)
        return summary
