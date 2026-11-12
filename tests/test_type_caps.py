"""W1 — a concurrency cap per workflow type (WorkflowConfig.max_running)."""

from __future__ import annotations

import asyncio

import pytest

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    FIFOSchedulingPolicy,
    LaunchPriority,
    LaunchRequest,
    PolicyResult,
    WorkflowConfig,
    WorkflowReporter,
)


async def work(reporter: WorkflowReporter) -> None:
    await asyncio.sleep(0.1)
    await reporter.emit(score=1.0)


class Fill(CampaignController):
    def __init__(self, priority=LaunchPriority.NORMAL) -> None:
        super().__init__()
        self.priority = priority
        self.sent = False
        self.peak: dict[str, int] = {}
        self.done = 0

    def on_complete(self, result) -> None:
        super().on_complete(result)
        self.done += 1

    def decide(self, obs: dict) -> PolicyResult:
        for t, n in obs["running"].items():
            self.peak[t] = max(self.peak.get(t, 0), n)
        reqs = []
        if not self.sent:
            self.sent = True
            reqs = [LaunchRequest.make(t, priority=self.priority) for t in ["big", "small"] * 5]
        return PolicyResult(launch_requests=reqs, terminate=self.done >= 10)


@pytest.mark.parametrize("policy", [None, "fifo"])
@pytest.mark.parametrize("priority", [LaunchPriority.NORMAL, LaunchPriority.CRITICAL])
def test_each_type_keeps_its_own_cap(policy, priority):
    ctl = Fill(priority)
    spec = CampaignSpec(
        name="caps",
        budget=CampaignBudget(max_concurrent_workflows=5, max_concurrency_per_workflow=3),
        controller=ctl,
        workflows=[
            WorkflowConfig(name="big", block=work, launch_on_start=False, max_running=1),
            WorkflowConfig(name="small", block=work, launch_on_start=False),
        ],
        scheduling_policy=FIFOSchedulingPolicy() if policy == "fifo" else None,
        policy_interval_s=0.01,
        shutdown_timeout_s=5.0,
    )
    asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))
    assert ctl.peak["big"] == 1
    assert ctl.peak["small"] == 3


def test_max_running_validation():
    with pytest.raises(ValueError):
        WorkflowConfig(name="x", block=work, max_running=0)
