"""FINISH — the controller ends a running workflow because it is done.

It is stopped like a cancel, but counted as completed: returncode 0,
WorkflowResult.finished, its last report kept, and the controller's reason
carried into the result.
"""

from __future__ import annotations

import asyncio
import time

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignDecision,
    CampaignManager,
    CampaignSpec,
    PolicyResult,
    WorkflowConfig,
    WorkflowReporter,
)


def worker(steps: int = 500, step_s: float = 0.02):
    """Reports a rising score; runs far longer than the tests wait."""

    async def wf(reporter: WorkflowReporter) -> None:
        for i in range(steps):
            await asyncio.sleep(step_s)
            await reporter.emit(score=float(i), metrics={"step": i, "cpu_hours": 0.0})

    return wf


class EndAfter(CampaignController):
    """Ends every running workflow with `decision` once it has reported a score
    of at least `at`; stops when all have ended."""

    def __init__(self, types: list[str], decision: CampaignDecision, at: float = 3) -> None:
        super().__init__()
        self.types = types
        self.decision = decision
        self.at = at
        self.decided: set[str] = set()
        self.results = {}

    def on_complete(self, result) -> None:
        super().on_complete(result)
        self.results[result.workflow_type] = result

    def decide(self, obs: dict) -> PolicyResult:
        res = PolicyResult()
        for wf_id, report in obs["live_reports"].items():
            if wf_id not in self.decided and (report.score or 0) >= self.at:
                self.decided.add(wf_id)
                res.decisions[wf_id] = self.decision
                res.reasons[wf_id] = f"score {report.score:g} is enough"
        res.terminate = len(self.results) == len(self.types)
        return res


def _run(ctl, grace_s: float = 0.0):
    spec = CampaignSpec(
        name="finish",
        budget=CampaignBudget(
            capacity={"cpus": 64},
            max_concurrent_workflows=4,
            max_concurrency_per_workflow=4,
        ),
        controller=ctl,
        workflows=[
            WorkflowConfig(
                name=t,
                block=worker(),
                spending_limit={"cpu_hours": 100.0},
                resources={"cpus": 1},
                cancel_grace_s=grace_s,
            )
            for t in ctl.types
        ],
        policy_interval_s=0.02,
        shutdown_timeout_s=5.0,
    )
    t0 = time.monotonic()
    out = asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))
    return out, time.monotonic() - t0


def test_finish_counts_as_completed_with_reason():
    ctl = EndAfter(["sim"], CampaignDecision.FINISH)
    out, elapsed = _run(ctl)
    r = ctl.results["sim"]
    assert r.returncode == 0 and r.finished
    assert r.cancel is None
    assert r.reason and r.reason.endswith("is enough")
    assert r.final_score >= 3  # its last report is kept
    assert elapsed < 5.0  # stopped early, not run to its end
    assert out.completed == {"sim": 1} and out.finished == {"sim": 1}
    assert not out.cancelled.get("sim")


def test_finish_with_grace_period_is_still_completed():
    ctl = EndAfter(["sim"], CampaignDecision.FINISH)
    _run(ctl, grace_s=0.2)  # the worker ignores the signal: forced after the grace
    r = ctl.results["sim"]
    assert r.returncode == 0 and r.finished


def test_cancel_carries_its_reason_and_is_not_finished():
    ctl = EndAfter(["sim"], CampaignDecision.CANCEL)
    out, _ = _run(ctl)
    r = ctl.results["sim"]
    assert r.returncode == -1 and not r.finished
    assert r.reason and r.reason.endswith("is enough")
    assert out.cancelled == {"sim": 1} and not out.finished.get("sim")
