"""Every workflow gets an allocation (allocation design note, step 1): the
type's starting value (as given), or declared_spending × (1 + size_margin);
learned values once the type has enough finished workflows; and, after a
workflow is cancelled at its own limit, the next ones get 25 % more."""

from __future__ import annotations

import asyncio
from pathlib import Path

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    LaunchRequest,
    PolicyResult,
    WorkflowConfig,
    WorkflowReporter,
    checkpoint_path,
    read_checkpoint,
)

H = 3600.0


def worker(need_cpu_s: float, cpus_in_use: float | None = None):
    """Spends `need_cpu_s` in 10 steps, reporting a running total."""

    async def wf(reporter: WorkflowReporter) -> None:
        for i in range(1, 11):
            await asyncio.sleep(0.02)
            metrics = {"cpu_hours": need_cpu_s * i / 10 / H}
            if cpus_in_use is not None:
                metrics["cpus_in_use"] = cpus_in_use
            await reporter.emit(score=1.0, metrics=metrics)

    return wf


class Collect(CampaignController):
    """Keep one 'sim' queued; stop after `ok` successes (or `n` results)."""

    def __init__(self, ok: int, n: int = 50, request: LaunchRequest | None = None) -> None:
        super().__init__(launch=[] if request else ["sim"])
        self.ok, self.n, self.request = ok, n, request
        self.results = []

    def on_complete(self, result) -> None:
        super().on_complete(result)
        self.results.append(result)

    def decide(self, obs: dict) -> PolicyResult:
        res = super().decide(obs)
        if (
            self.request is not None
            and not obs["running"]
            and obs["queue_snapshot"].total_queued == 0
        ):
            res.launch_requests.append(self.request)
        done = sum(r.returncode == 0 for r in self.results)
        res.terminate = done >= self.ok or len(self.results) >= self.n
        return res


def _run(ctl, wfc, tmp_path=None):
    spec = CampaignSpec(
        name="start",
        budget=CampaignBudget(max_concurrent_workflows=1, max_concurrency_per_workflow=1),
        controller=ctl,
        workflows=[wfc],
        policy_interval_s=0.01,
        shutdown_timeout_s=5.0,
        checkpoint_dir=str(tmp_path) if tmp_path else None,
    )
    asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=60.0))
    return [round(r.spending_limit.get("cpu_hours", 0) * H, 4) for r in ctl.results]


def test_declared_plus_margin_then_learned():
    ctl = Collect(ok=5)
    wfc = WorkflowConfig(
        name="sim",
        block=worker(2.0),
        launch_on_start=False,
        declared_spending={"cpu_hours": 3.0 / H},
    )
    limits = _run(ctl, wfc)
    assert limits[:3] == [3.75, 3.75, 3.75]  # declared 3 × 1.25
    assert limits[3:5] == [2.5, 2.5]  # learned 2 × 1.25


def test_explicit_starting_value_is_used_as_given():
    ctl = Collect(ok=1)
    wfc = WorkflowConfig(
        name="sim",
        block=worker(2.0),
        launch_on_start=False,
        declared_spending={"cpu_hours": 3.0 / H},
        spending_limit={"cpu_hours": 2.2 / H},
    )
    assert _run(ctl, wfc) == [2.2]


def test_the_request_s_own_amount_wins():
    ctl = Collect(ok=1, request=LaunchRequest.make("sim", spending_limit={"cpu_hours": 9.0 / H}))
    wfc = WorkflowConfig(
        name="sim",
        block=worker(2.0),
        launch_on_start=False,
        spending_limit={"cpu_hours": 2.2 / H},
    )
    assert _run(ctl, wfc) == [9.0]


def test_limit_raised_after_cancels_until_it_fits(tmp_path: Path):
    # Needs 5 CPU·s, starts at 2: each cancel at the limit raises the next by 25 %.
    ctl = Collect(ok=1)
    wfc = WorkflowConfig(
        name="sim", block=worker(5.0), launch_on_start=False, spending_limit={"cpu_hours": 2.0 / H}
    )
    limits = _run(ctl, wfc, tmp_path)
    assert limits[0] == 2.0 and len(limits) >= 4
    for prev, nxt in zip(limits, limits[1:], strict=False):
        assert abs(nxt - prev * 1.25) < 1e-3
    # Cancelled at its limit until one is given enough to finish.
    assert [r.over_budget for r in ctl.results] == [True] * (len(limits) - 1) + [False]
    assert ctl.results[-1].returncode == 0
    ck = read_checkpoint(checkpoint_path(str(tmp_path), "start"))
    assert abs(ck.type_raised["spending"]["sim"]["cpu_hours"] * H - limits[-1]) < 1e-3


def test_resources_raised_after_overuse():
    ctl = Collect(ok=1, n=3)
    wfc = WorkflowConfig(
        name="sim", block=worker(1.0, cpus_in_use=3), launch_on_start=False, resources={"cpus": 2}
    )
    _run(ctl, wfc)
    cpus = [r.resources["cpus"] for r in ctl.results]
    assert cpus[0] == 2 and ctl.results[0].over_resources
    assert cpus[1] == 3  # ceil(2 × 1.25)
    assert ctl.results[1].returncode == 0
