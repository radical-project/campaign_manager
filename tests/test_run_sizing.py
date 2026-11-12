"""What each run gets: the scheduler sizes its spending limit and resources at
launch (SchedulingPolicy.size), and AuCaM enforces resources in use at once."""

from __future__ import annotations

import asyncio
import json
import sys

from src.aucam import (
    Allocation,
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    LaunchPriority,
    LaunchRequest,
    PolicyResult,
    SchedulingPolicy,
    SizingContext,
    WorkflowConfig,
    WorkflowReporter,
)

# ── SchedulingPolicy.size(), the default sizing step ──────────────────────────


def _ctx(spend=None, res=None, learned=None, remaining=None, queued=1) -> SizingContext:
    return SizingContext(
        proposal=Allocation(spending_limit=spend or {}, resources=res or {}),
        learned_cost=learned or {},
        remaining=remaining or {},
        queued=queued,
    )


def _req(bounds=None) -> LaunchRequest:
    return LaunchRequest.make("sim", bounds=bounds)


def test_explicit_amounts_are_kept() -> None:
    a = SchedulingPolicy().size(_req(), _ctx({"cpu_hours": 2.0}, {"cpus": 4}))
    assert a.spending_limit == {"cpu_hours": 2.0} and a.resources == {"cpus": 4}
    assert not a.capped


def test_range_uses_learned_cost_with_margin() -> None:
    p = SchedulingPolicy()
    req = _req({"cpu_hours": (0.5, 4.0), "cpus": (2, 8)})
    before = p.size(req, _ctx())  # nothing learned yet: the range's maximum
    assert before.spending_limit == {"cpu_hours": 4.0} and before.resources == {"cpus": 8.0}
    after = p.size(req, _ctx(learned={"cpu_hours": 1.0}))
    assert after.spending_limit == {"cpu_hours": 1.25}  # 1.0 × (1 + 0.25)


def test_amounts_stay_within_the_range() -> None:
    a = SchedulingPolicy().size(
        _req({"cpu_hours": (0.5, 4.0), "cpus": (2, 8)}), _ctx({"cpu_hours": 9.0}, {"cpus": 16})
    )
    assert a.spending_limit == {"cpu_hours": 4.0} and a.resources == {"cpus": 8.0}


def test_capped_at_the_remaining_budget_within_a_range() -> None:
    req = _req({"cpu_hours": (1.0, 5.0)})
    a = SchedulingPolicy().size(req, _ctx({"cpu_hours": 5.0}, remaining={"cpu_hours": 1.5}))
    assert a.spending_limit == {"cpu_hours": 1.5} and a.capped


def test_without_a_range_a_request_that_does_not_fit_gets_none() -> None:
    a = SchedulingPolicy().size(_req(), _ctx({"cpu_hours": 5.0}, remaining={"cpu_hours": 1.5}))
    assert a is None  # it waits, or is dropped when nothing else runs


def test_dropped_when_the_minimum_does_not_fit() -> None:
    req = _req({"cpu_hours": (2.0, 5.0)})
    assert SchedulingPolicy().size(req, _ctx(remaining={"cpu_hours": 1.0})) is None


def test_fair_share_splits_the_remaining_budget() -> None:
    p = SchedulingPolicy()
    p.fair_share = True
    req = _req({"cpu_hours": (0.5, 5.0)})
    a = p.size(req, _ctx({"cpu_hours": 5.0}, remaining={"cpu_hours": 4.0}, queued=4))
    assert a.spending_limit == {"cpu_hours": 1.0} and a.capped


# ── In a running campaign ──────────────────────────────────────────────────────


class LaunchOnce(CampaignController):
    def __init__(self, requests: list[LaunchRequest]) -> None:
        super().__init__()
        self._requests, self._sent, self.results = requests, False, []

    def on_complete(self, result) -> None:
        super().on_complete(result)
        self.results.append(result)

    def decide(self, obs: dict) -> PolicyResult:
        reqs = [] if self._sent else self._requests
        self._sent = True
        return PolicyResult(
            launch_requests=reqs, terminate=len(self.results) >= len(self._requests)
        )


def _run(workflows, requests, spending=None):
    ctl = LaunchOnce(requests)
    spec = CampaignSpec(
        name="sizing",
        budget=CampaignBudget(
            max_concurrent_workflows=4, max_concurrency_per_workflow=4, spending=spending or {}
        ),
        controller=ctl,
        workflows=workflows,
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )
    out = asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=60.0))
    return ctl, out


def test_python_workflow_receives_its_resources() -> None:
    seen: list = []

    async def reads(context: dict) -> None:
        seen.append(context.get("resources"))

    wf = WorkflowConfig(name="reads", block=reads, launch_on_start=False, resources={"cpus": 8})
    ctl, _ = _run([wf], [LaunchRequest.make("reads", resources={"cpus": 4, "gpus": 1})])
    assert seen == [{"cpus": 4.0, "gpus": 1.0}]
    assert ctl.results[0].resources == {"cpus": 4.0, "gpus": 1.0}


def test_command_line_workflow_receives_its_resources(tmp_path) -> None:
    out = tmp_path / "env.json"
    script = (
        "import json, os, sys; "
        "json.dump({k: v for k, v in os.environ.items() if k.startswith('CM_RESOURCES_')}, "
        "open(sys.argv[1], 'w'))"
    )
    wf = WorkflowConfig(
        name="prog",
        launch_on_start=False,
        launch_cmd=[sys.executable, "-c", script, str(out)],
        resources={"cpus": 16, "gpus": 2},
    )
    _run([wf], [LaunchRequest.make("prog", LaunchPriority.NORMAL)])
    env = json.loads(out.read_text())
    assert float(env["CM_RESOURCES_CPUS"]) == 16 and float(env["CM_RESOURCES_GPUS"]) == 2


def test_run_using_more_resources_is_cancelled() -> None:
    async def greedy(reporter: WorkflowReporter) -> None:
        for step in range(40):
            await asyncio.sleep(0.05)
            await reporter.emit(score=1.0, iteration=step, metrics={"cpus_in_use": 12})

    wf = WorkflowConfig(name="greedy", block=greedy, launch_on_start=False, resources={"cpus": 8})
    ctl, out = _run([wf], [LaunchRequest.make("greedy")])
    (r,) = ctl.results
    assert r.returncode == -1 and r.over_resources and not r.over_budget
    assert out.resource_cancels == 1


def test_run_is_capped_at_the_campaign_budget() -> None:
    async def quick() -> None:
        await asyncio.sleep(0.05)

    wf = WorkflowConfig(name="quick", block=quick, launch_on_start=False)
    ctl, _ = _run(
        [wf],
        [LaunchRequest.make("quick", bounds={"cpu_hours": (0.5, 5.0)})],
        spending={"cpu_hours": 1.0},
    )
    (r,) = ctl.results
    assert r.capped and r.spending_limit == {"cpu_hours": 1.0}


def test_request_that_does_not_fit_is_dropped() -> None:
    async def quick() -> None:
        await asyncio.sleep(0.05)

    wf = WorkflowConfig(name="quick", block=quick, launch_on_start=False)
    ctl, out = _run(
        [wf],
        [LaunchRequest.make("quick", bounds={"cpu_hours": (2.0, 5.0)})],
        spending={"cpu_hours": 1.0},
    )
    assert ctl.results == []
    assert out.unfit_dropped == 1
    assert out.exit_reason == "spending_limit"
