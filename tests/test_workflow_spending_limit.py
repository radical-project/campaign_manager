"""Per-run spending limits: assigned at launch, held against the campaign limit,
passed to the workflow, enforced by cancelling the run once it spends past it."""

from __future__ import annotations

import asyncio
import json
import sys

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


class LaunchOnce(CampaignController):
    """Launch the given requests once; stop when all of them have finished.

    Records the campaign's in-flight spending at every policy cycle.
    """

    def __init__(self, requests: list[LaunchRequest]) -> None:
        super().__init__()
        self._requests = requests
        self._sent = False
        self.results: list = []
        self.in_flight: list[float] = []

    def on_complete(self, result) -> None:
        super().on_complete(result)
        self.results.append(result)

    def decide(self, obs: dict) -> PolicyResult:
        self.in_flight.append(obs["budget"]["in_flight"].get("cpu_hours", 0.0))
        reqs: list[LaunchRequest] = []
        if not self._sent:
            self._sent = True
            reqs = self._requests
        return PolicyResult(
            launch_requests=reqs, terminate=len(self.results) >= len(self._requests)
        )


def _run(workflows, requests, spending=None):
    ctl = LaunchOnce(requests)
    spec = CampaignSpec(
        name="limits",
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


async def spender(reporter: WorkflowReporter) -> None:
    """Reports a running total of 0.1 CPU-hours per step, for 10 steps."""
    for step in range(1, 11):
        await asyncio.sleep(0.05)
        await reporter.emit(score=1.0, iteration=step, metrics={"cpu_hours": 0.1 * step})


def _req(wf_type: str, limit: dict | None = None) -> LaunchRequest:
    return LaunchRequest.make(wf_type, LaunchPriority.NORMAL, spending_limit=limit)


def test_run_over_its_limit_is_cancelled() -> None:
    wf = WorkflowConfig(
        name="spender", block=spender, launch_on_start=False, spending_limit={"cpu_hours": 0.25}
    )
    ctl, out = _run([wf], [_req("spender")])
    (r,) = ctl.results
    assert r.returncode == -1
    assert r.over_budget is True
    assert r.spending_limit == {"cpu_hours": 0.25}
    assert out.budget_cancels == 1


def test_launch_request_overrides_the_type_limit() -> None:
    wf = WorkflowConfig(
        name="spender", block=spender, launch_on_start=False, spending_limit={"cpu_hours": 0.25}
    )
    ctl, out = _run([wf], [_req("spender", {"cpu_hours": 5.0})])
    (r,) = ctl.results
    assert r.returncode == 0
    assert r.over_budget is False
    assert r.spending_limit == {"cpu_hours": 5.0}
    assert out.budget_cancels == 0


def test_no_limit_runs_to_the_end() -> None:
    wf = WorkflowConfig(name="spender", block=spender, launch_on_start=False)
    ctl, out = _run([wf], [_req("spender")])
    assert ctl.results[0].returncode == 0
    assert ctl.results[0].spending_limit == {}
    assert out.budget_cancels == 0


def test_prorated_spending_is_enforced_without_reports() -> None:
    async def silent() -> None:
        await asyncio.sleep(2.0)

    # Declared 1 CPU-hour per 0.1 s of wall time: the limit is passed after ~0.03 s.
    wf = WorkflowConfig(
        name="silent",
        block=silent,
        launch_on_start=False,
        declared_spending={"wall_hours": 0.1 / 3600, "cpu_hours": 1.0},
        spending_limit={"cpu_hours": 0.3},
    )
    ctl, out = _run([wf], [_req("silent")])
    (r,) = ctl.results
    assert r.returncode == -1 and r.over_budget
    assert r.wall_time_s < 1.5
    assert out.budget_cancels == 1


def test_limit_is_held_against_the_campaign() -> None:
    async def short() -> None:
        await asyncio.sleep(0.3)

    wf = WorkflowConfig(
        name="short",
        block=short,
        launch_on_start=False,
        declared_spending={"wall_hours": 1.0, "cpu_hours": 0.01},
        spending_limit={"cpu_hours": 0.6},
    )
    ctl, _ = _run([wf], [_req("short")], spending={"cpu_hours": 10.0})
    # While the run is going, the campaign holds its full limit, not the 0.01 estimate.
    assert max(ctl.in_flight) == 0.6


def test_python_workflow_receives_its_limit() -> None:
    seen: list = []

    async def reads(context: dict) -> None:
        seen.append(context.get("spending_limit"))

    wf = WorkflowConfig(
        name="reads", block=reads, launch_on_start=False, spending_limit={"gpu_hours": 2.0}
    )
    _run([wf], [_req("reads")])
    assert seen == [{"gpu_hours": 2.0}]


def test_command_line_workflow_receives_its_limit(tmp_path) -> None:
    out = tmp_path / "env.json"
    script = (
        "import json, os, sys; "
        "json.dump({k: v for k, v in os.environ.items() if k.startswith('CM_SPENDING_LIMIT_')}, "
        "open(sys.argv[1], 'w'))"
    )
    wf = WorkflowConfig(
        name="prog",
        launch_on_start=False,
        launch_cmd=[sys.executable, "-c", script, str(out)],
        spending_limit={"cpu_hours": 1.5, "llm_tokens": 20000},
    )
    ctl, _ = _run([wf], [_req("prog")])
    assert ctl.results[0].returncode == 0
    env = json.loads(out.read_text())
    assert float(env["CM_SPENDING_LIMIT_CPU_HOURS"]) == 1.5
    assert float(env["CM_SPENDING_LIMIT_LLM_TOKENS"]) == 20000.0
