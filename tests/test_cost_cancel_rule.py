"""B4 — CostCancelRule: cancel a workflow that has spent a set amount since its
score last improved."""

from __future__ import annotations

import asyncio

import pytest

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    CostCancelRule,
    LaunchRequest,
    PolicyResult,
    WorkflowConfig,
    WorkflowReporter,
)

H = 3600.0


def test_check_tracks_best_score_and_spending_since():
    rule = CostCancelRule("sim", max_without_improvement={"cpu_hours": 3.0})
    assert rule.check("w", None, {"cpu_hours": 2.0}) is None  # no score yet, 2 < 3
    assert rule.check("w", 0.5, {"cpu_hours": 2.5}) is None  # first score: best 0.5 at 2.5
    assert rule.check("w", 0.5, {"cpu_hours": 5.0}) is None  # 2.5 since best
    assert rule.check("w", 0.6, {"cpu_hours": 5.4}) is None  # improved: best at 5.4
    assert rule.check("w", 0.4, {"cpu_hours": 8.0}) is None  # 2.6 since best
    why = rule.check("w", 0.4, {"cpu_hours": 8.5})  # 3.1 since best
    assert why is not None and "cpu_hours" in why


def test_check_no_score_and_minimize_and_min_delta():
    rule = CostCancelRule("sim", max_without_improvement={"gpu_hours": 1.0})
    assert rule.check("a", None, {"gpu_hours": 1.0}) is not None  # never scored
    rule = CostCancelRule(
        "sim", max_without_improvement={"gpu_hours": 1.0}, direction="minimize", min_delta=0.1
    )
    rule.check("b", 5.0, {"gpu_hours": 0.0})
    assert rule.check("b", 4.95, {"gpu_hours": 0.9}) is None  # within min_delta: no improvement
    assert rule.check("b", 4.95, {"gpu_hours": 1.0}) is not None
    assert rule.check("c", 5.0, {}) is None  # no spending data: never cancelled
    with pytest.raises(ValueError):
        CostCancelRule("sim", {"gpu_hours": 1.0}, direction="up")


def make(improving: bool):
    async def sim(reporter: WorkflowReporter) -> None:
        for step in range(1, 11):
            await asyncio.sleep(0.04)
            score = 0.1 * step if improving else 0.3
            await reporter.emit(score=score, metrics={"cpu_hours": step * 1.0 / H})

    return sim


class Once(CampaignController):
    def __init__(self) -> None:
        super().__init__(
            rules=[
                CostCancelRule("stuck", max_without_improvement={"cpu_hours": 3.0 / H}),
                CostCancelRule("improving", max_without_improvement={"cpu_hours": 3.0 / H}),
            ]
        )
        self.sent = False
        self.done: dict[str, int] = {}
        self.spend_seen = False

    def on_complete(self, result) -> None:
        super().on_complete(result)
        self.done[result.workflow_type] = result.returncode

    def decide(self, obs: dict) -> PolicyResult:
        self.spend_seen |= any(obs["wf_spend"].values())
        res = super().decide(obs)
        if not self.sent:
            self.sent = True
            res.launch_requests += [LaunchRequest.make("stuck"), LaunchRequest.make("improving")]
        res.terminate = len(self.done) == 2
        return res


def test_stuck_workflow_is_cancelled_improving_one_completes():
    ctl = Once()
    spec = CampaignSpec(
        name="costcancel",
        budget=CampaignBudget(max_concurrent_workflows=2, max_concurrency_per_workflow=1),
        controller=ctl,
        workflows=[
            WorkflowConfig(name="stuck", block=make(False)),
            WorkflowConfig(name="improving", block=make(True)),
        ],
        policy_interval_s=0.02,
        shutdown_timeout_s=5.0,
    )
    asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))
    assert ctl.spend_seen
    assert ctl.done == {"stuck": -1, "improving": 0}
