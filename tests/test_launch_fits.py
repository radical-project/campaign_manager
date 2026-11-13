"""B2 — launch only what fits: a request starts only if spent + in-flight + its
cost fits the campaign's spending limit; one that does not fit stays queued, a
cheaper one may start instead, and the campaign ends when nothing queued fits
and nothing is running."""

from __future__ import annotations

import asyncio
import logging

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    LaunchPriority,
    LaunchRequest,
    PolicyResult,
    WorkflowConfig,
    WorkflowReporter,
)

CAPACITY = {"cpus": 1024, "gpus": 64}  # the allocation tests run on


H = 3600.0


def make(cpu_s: float):
    """A workflow that runs 0.1 s and reports cpu_s CPU-seconds."""

    async def wf(reporter: WorkflowReporter) -> None:
        await asyncio.sleep(0.1)
        await reporter.emit(score=1.0, metrics={"cpu_hours": cpu_s / H})

    return wf


def cfg(name: str, cpu_s: float) -> WorkflowConfig:
    return WorkflowConfig(
        name=name,
        block=make(cpu_s),
        spending_limit={"cpu_hours": cpu_s / H},  # starting limit = its cost
        # Before its only report (at the end), AuCaM counts 0.1 CPU × elapsed,
        # which stays well below the limit even on a slow machine.
        resources={"cpus": 0.1},
    )


class Queue(CampaignController):
    """Enqueue the given requests once; record in-flight + spent each cycle."""

    def __init__(self, requests: list[LaunchRequest]) -> None:
        super().__init__()
        self._requests = requests
        self._sent = False
        self.held: list[float] = []
        self.launched: list[str] = []

    def on_complete(self, result) -> None:
        super().on_complete(result)
        self.launched.append(result.workflow_type)

    def decide(self, obs: dict) -> PolicyResult:
        b = obs["budget"]
        self.held.append(
            (b["spent"].get("cpu_hours", 0.0) + b["in_flight"].get("cpu_hours", 0.0)) * H
        )
        reqs = [] if self._sent else self._requests
        self._sent = True
        return PolicyResult(launch_requests=reqs)


def _run(workflows, requests, limit_cpu_s, slots=4):
    ctl = Queue(requests)
    spec = CampaignSpec(
        name="fits",
        budget=CampaignBudget(
            capacity=CAPACITY,
            max_concurrent_workflows=slots,
            max_concurrency_per_workflow=slots,
            spending={"cpu_hours": limit_cpu_s / H},
            on_limit="finish",  # B2 only; B6 is tested in test_graceful_cancel
        ),
        controller=ctl,
        workflows=workflows,
        policy_interval_s=0.02,
        shutdown_timeout_s=5.0,
        idle_exit_s=None,
    )
    out = asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))
    return ctl, out


def test_last_launch_does_not_overshoot_the_limit():
    # 4 CPU·s each, limit 10: two fit (8), a third would make 12.
    ctl, out = _run([cfg("sim", 4.0)], [LaunchRequest.make("sim") for _ in range(6)], 10.0)
    assert out.exit_reason == "spending_limit"
    # In-flight grows a little if a workflow runs past its declared 0.1 s.
    assert max(ctl.held) <= 10.0 * 1.1
    assert len(ctl.launched) == 2
    assert out.spent["cpu_hours"] * H <= 10.0 + 1e-9


def test_cheaper_request_starts_when_the_expensive_one_does_not_fit(caplog):
    # limit 10: big (8) runs first; then 2 left — big does not fit, small (1) does.
    reqs = [
        LaunchRequest.make("big", priority=LaunchPriority.HIGH),
        LaunchRequest.make("big", priority=LaunchPriority.HIGH),
        LaunchRequest.make("small"),
        LaunchRequest.make("small"),
    ]
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        ctl, out = _run([cfg("big", 8.0), cfg("small", 1.0)], reqs, 10.0)
    assert sorted(ctl.launched) == ["big", "small", "small"]
    assert out.exit_reason == "spending_limit"
    assert "WAIT big" in caplog.text
    assert out.spent["cpu_hours"] * H <= 10.0 + 1e-9


def test_critical_requests_are_checked_too():
    reqs = [LaunchRequest.make("big", priority=LaunchPriority.CRITICAL) for _ in range(3)]
    ctl, out = _run([cfg("big", 4.0)], reqs, 10.0)
    assert ctl.launched == ["big", "big"]
    assert out.exit_reason == "spending_limit"
