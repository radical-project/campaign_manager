"""U6 — WorkflowConfig.bounds: every launch of a type is sized from its learned
cost (the range's max until 3 have finished); obs["learned_cost"] shows the
same data to the controller."""

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

H = 3600.0


async def sim(reporter: WorkflowReporter, context: dict) -> None:
    await asyncio.sleep(0.03)
    await reporter.emit(score=1.0, metrics={"cpu_hours": 4.0 / H})


class Collect(CampaignController):
    def __init__(self, n: int, override_at: int | None = None) -> None:
        super().__init__(launch=["sim"])
        self.n = n
        self.override_at = override_at
        self.results = []
        self.learned: list[dict] = []
        self.sent_override = False

    def on_complete(self, result) -> None:
        super().on_complete(result)
        self.results.append(result)

    def decide(self, obs: dict) -> PolicyResult:
        self.learned.append(obs["learned_cost"])
        res = super().decide(obs)
        if (
            self.override_at is not None
            and len(self.results) >= self.override_at
            and not self.sent_override
        ):
            self.sent_override = True
            res.launch_requests = [LaunchRequest.make("sim", bounds={"cpu_hours": (1 / H, 3 / H)})]
        res.terminate = len(self.results) >= self.n
        return res


def _run(ctl):
    spec = CampaignSpec(
        name="typebounds",
        budget=CampaignBudget(max_concurrent_workflows=1, max_concurrency_per_workflow=1),
        controller=ctl,
        workflows=[WorkflowConfig(name="sim", block=sim, bounds={"cpu_hours": (2 / H, 10 / H)})],
        policy_interval_s=0.02,
        shutdown_timeout_s=5.0,
    )
    asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))
    return [round(r.spending_limit["cpu_hours"] * H, 6) for r in ctl.results]


def test_type_bounds_size_every_launch_from_learned_cost():
    ctl = Collect(n=5)
    limits = _run(ctl)
    assert limits[:3] == [10.0, 10.0, 10.0]  # range max until 3 have finished
    assert limits[3:] == [5.0, 5.0]  # learned 4 × 1.25
    assert ctl.learned[0] == {}
    assert abs(ctl.learned[-1]["sim"]["cpu_hours"] * H - 4.0) < 1e-6


def test_request_bounds_override_type_bounds():
    ctl = Collect(n=5, override_at=3)
    limits = _run(ctl)
    assert 3.0 in limits  # learned 5 clamped to the request's max of 3


def test_bounds_validation():
    with pytest.raises(ValueError):
        WorkflowConfig(name="x", block=sim, bounds={"cpu_hours": (5.0, 1.0)})
