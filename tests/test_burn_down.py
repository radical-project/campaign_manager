"""B1 — burn-down in the controller's observation: spending rate, projected
spending, hours to the limit, and per-type outcomes and costs."""

from __future__ import annotations

import asyncio

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

CAPACITY = {"cpus": 1024, "gpus": 64}  # the allocation tests run on


H = 3600.0


def make(cpu_s: float, score: float, fail: bool = False):
    async def wf(reporter: WorkflowReporter) -> None:
        await asyncio.sleep(0.05)
        await reporter.emit(score=score, metrics={"cpu_hours": cpu_s / H})
        if fail:
            raise RuntimeError("failed on purpose")

    return wf


class Watch(CampaignController):
    def __init__(self) -> None:
        super().__init__(launch=["good", "bad"])
        self.views: list[dict] = []

    def decide(self, obs: dict) -> PolicyResult:
        self.views.append(obs["budget"])
        res = super().decide(obs)
        res.terminate = self.completed.get("good", 0) >= 4
        return res


def test_burn_down_fields():
    ctl = Watch()
    spec = CampaignSpec(
        name="burn",
        budget=CampaignBudget(
            capacity=CAPACITY,
            max_concurrent_workflows=2,
            max_concurrency_per_workflow=1,
            spending={"cpu_hours": 1000.0 / H},
        ),
        controller=ctl,
        workflows=[
            WorkflowConfig(
                name="good",
                block=make(2.0, 0.8),
                spending_limit={"cpu_hours": 2.0 / H},
                resources={"cpus": 1},
            ),
            WorkflowConfig(
                name="bad",
                block=make(1.0, 0.1, fail=True),
                spending_limit={"cpu_hours": 1.0 / H},
                resources={"cpus": 1},
            ),
        ],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )
    asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))

    first, last = ctl.views[0], ctl.views[-1]
    assert first["burn"]["cpu_hours"]["rate_per_hour"] == 0.0
    assert first["burn"]["cpu_hours"]["hours_to_limit"] is None
    b = last["burn"]["cpu_hours"]
    assert b["rate_per_hour"] > 0
    assert b["hours_to_limit"] == b["remaining"] / b["rate_per_hour"]
    spent = last["spent"]["cpu_hours"]
    assert b["projected"] >= spent + last["in_flight"].get("cpu_hours", 0.0) - 1e-12

    good, bad = last["by_type"]["good"], last["by_type"]["bad"]
    assert good["completed"] >= 3 and good["failed"] == 0
    assert abs(good["cost_per_completed"]["cpu_hours"] * H - 2.0) < 1e-6
    assert abs(good["mean_score"] - 0.8) < 1e-9
    assert bad["completed"] == 0 and bad["failed"] >= 1
    assert bad["cost_per_completed"] == {} and bad["mean_score"] is None
    assert abs(bad["avg_cost"]["cpu_hours"] * H - 1.0) < 1e-6


def test_projected_includes_queued_requests():
    async def long(reporter: WorkflowReporter) -> None:
        await asyncio.sleep(0.3)
        await reporter.emit(score=1.0, metrics={"cpu_hours": 2.0 / H})

    class Q(CampaignController):
        def __init__(self) -> None:
            super().__init__()
            self.views: list[tuple[int, dict]] = []
            self.sent = False

        def decide(self, obs: dict) -> PolicyResult:
            self.views.append((obs["queue_snapshot"].total_queued, obs["budget"]))
            reqs = [] if self.sent else [LaunchRequest.make("w") for _ in range(3)]
            self.sent = True
            return PolicyResult(launch_requests=reqs, terminate=len(self.views) > 6)

    ctl = Q()
    spec = CampaignSpec(
        name="proj",
        budget=CampaignBudget(
            capacity=CAPACITY,
            max_concurrent_workflows=1,
            max_concurrency_per_workflow=1,
            spending={"cpu_hours": 100.0 / H},
        ),
        controller=ctl,
        workflows=[
            WorkflowConfig(
                name="w",
                block=long,
                launch_on_start=False,
                spending_limit={"cpu_hours": 2.0 / H},
                resources={"cpus": 1},
            )
        ],
        policy_interval_s=0.03,
        shutdown_timeout_s=5.0,
    )
    asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))
    queued, view = next((q, v) for q, v in ctl.views if q == 2)
    b = view["burn"]["cpu_hours"]
    expected = view["spent"].get("cpu_hours", 0.0) + view["in_flight"]["cpu_hours"] + 2 * 2.0 / H
    assert abs(b["projected"] - expected) < 1e-12
