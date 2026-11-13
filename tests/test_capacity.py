"""Allocation design note, step 3: the campaign's capacity (total CPUs / GPUs),
required in CampaignBudget; workflows start only if their resources fit what is
free."""

from __future__ import annotations

import asyncio

import pytest

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    LaunchRequest,
    PolicyResult,
    WorkflowConfig,
    WorkflowReporter,
)


async def work(reporter: WorkflowReporter, context: dict) -> None:
    await asyncio.sleep(0.1)
    await reporter.emit(score=1.0)


def test_capacity_is_required_and_checked():
    with pytest.raises(TypeError):
        CampaignBudget(max_concurrent_workflows=1, max_concurrency_per_workflow=1)  # type: ignore[call-arg]
    for bad in ({}, {"nodes": 2}, {"cpus": -1}):
        with pytest.raises(ValueError):
            CampaignBudget(max_concurrent_workflows=1, max_concurrency_per_workflow=1, capacity=bad)


class Fill(CampaignController):
    def __init__(self, requests: list[LaunchRequest]) -> None:
        super().__init__()
        self.requests = requests
        self.sent = False
        self.peak_cpus = 0.0
        self.views: list[dict] = []
        self.results = []

    def on_complete(self, result) -> None:
        super().on_complete(result)
        self.results.append(result)

    def decide(self, obs: dict) -> PolicyResult:
        cap = obs["budget"]["capacity"]["cpus"]
        self.views.append(cap)
        self.peak_cpus = max(self.peak_cpus, cap["in_use"])
        reqs = [] if self.sent else self.requests
        self.sent = True
        return PolicyResult(launch_requests=reqs)


def _run(ctl, capacity, wfc):
    spec = CampaignSpec(
        name="cap",
        budget=CampaignBudget(
            max_concurrent_workflows=10, max_concurrency_per_workflow=10, capacity=capacity
        ),
        controller=ctl,
        workflows=[wfc],
        policy_interval_s=0.01,
        shutdown_timeout_s=5.0,
        idle_exit_s=0.3,
    )
    return asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))


def test_workflows_start_only_within_capacity():
    ctl = Fill([LaunchRequest.make("w") for _ in range(6)])
    wfc = WorkflowConfig(
        spending_limit={"cpu_hours": 1000.0},
        name="w",
        block=work,
        launch_on_start=False,
        resources={"cpus": 2},
    )
    _run(ctl, {"cpus": 4}, wfc)
    assert len(ctl.results) == 6  # all ran, two at a time
    assert ctl.peak_cpus == 4
    assert all(v["total"] == 4 and v["free"] == v["total"] - v["in_use"] for v in ctl.views)


def test_more_than_the_whole_capacity_is_dropped():
    ctl = Fill([LaunchRequest.make("w", resources={"cpus": 8})])
    wfc = WorkflowConfig(
        spending_limit={"cpu_hours": 1000.0},
        resources={"cpus": 1},
        name="w",
        block=work,
        launch_on_start=False,
    )
    out = _run(ctl, {"cpus": 4}, wfc)
    assert ctl.results == [] and out.unfit_dropped == 1


def test_gpus_not_in_capacity_count_as_zero():
    ctl = Fill([LaunchRequest.make("w", resources={"gpus": 1})])
    wfc = WorkflowConfig(
        spending_limit={"cpu_hours": 1000.0},
        resources={"cpus": 1},
        name="w",
        block=work,
        launch_on_start=False,
    )
    out = _run(ctl, {"cpus": 4}, wfc)
    assert ctl.results == [] and out.unfit_dropped == 1
