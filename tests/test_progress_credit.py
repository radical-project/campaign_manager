"""Allocation design note, step 2: progress toward the goal, credit passed back
along the chain of workflows, and the allocation history."""

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
    WorkflowDependency,
    WorkflowReporter,
    checkpoint_path,
    read_checkpoint,
)

CAPACITY = {"cpus": 1024, "gpus": 64}  # the allocation tests run on


H = 3600.0


def make(score: float = 1.0, fail: bool = False, sleep: float = 0.03):
    async def wf(reporter: WorkflowReporter) -> None:
        await asyncio.sleep(sleep)
        await reporter.emit(score=score, metrics={"cpu_hours": 1.0 / H})
        if fail:
            raise RuntimeError("failed on purpose")

    return wf


class Watch(CampaignController):
    """goal_type="analysis"; records obs["allocations"]; stops after `n` results."""

    def __init__(self, n: int, partial: float = 0.0, extra=None) -> None:
        super().__init__(goal_type="analysis", goal_n=99)
        self.n, self.partial, self.extra = n, partial, extra or []
        self.results, self.seen = [], []

    def on_complete(self, result) -> None:
        super().on_complete(result)
        self.results.append(result)

    def progress(self, result) -> float:
        base = super().progress(result)
        return base + (
            self.partial if result.workflow_type == "sim" and result.returncode == 0 else 0
        )

    def decide(self, obs: dict) -> PolicyResult:
        self.seen.append(obs["allocations"])
        res = super().decide(obs)
        if self.extra:
            res.launch_requests += self.extra(self, obs)
        res.terminate = len(self.results) >= self.n
        return res


def _run(ctl, workflows, tmp_path=None):
    spec = CampaignSpec(
        name="credit",
        budget=CampaignBudget(
            capacity=CAPACITY, max_concurrent_workflows=2, max_concurrency_per_workflow=2
        ),
        controller=ctl,
        workflows=workflows,
        dependencies=[WorkflowDependency(source="sim", target="analysis")],
        policy_interval_s=0.01,
        shutdown_timeout_s=5.0,
        idle_exit_s=0.3,
        checkpoint_dir=str(tmp_path) if tmp_path else None,
    )
    asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))
    return {r["workflow_id"]: r for r in ctl.seen[-1]}


def test_analysis_progress_is_credited_to_the_sim_that_led_to_it(tmp_path: Path):
    ctl = Watch(n=2)
    recs = _run(
        ctl,
        [
            WorkflowConfig(name="sim", block=make()),
            WorkflowConfig(name="analysis", block=make(), launch_on_start=False),
        ],
        tmp_path,
    )
    sim = next(r for r in recs.values() if r["workflow_type"] == "sim")
    ana = next(r for r in recs.values() if r["workflow_type"] == "analysis")
    assert ana["source"] == sim["workflow_id"]
    assert ana["progress"] == 1.0 and ana["credit"] == 0.0
    assert sim["progress"] == 0.0 and sim["credit"] == 1.0
    assert sim["outcome"] == ana["outcome"] == "completed"
    ck = read_checkpoint(checkpoint_path(str(tmp_path), "credit"))
    assert {r["workflow_id"] for r in ck.allocations} == {sim["workflow_id"], ana["workflow_id"]}


def test_failed_analysis_gives_no_credit_and_pending_while_it_runs():
    ctl = Watch(n=2)
    _run(
        ctl,
        [
            WorkflowConfig(name="sim", block=make()),
            WorkflowConfig(
                name="analysis", block=make(fail=True, sleep=0.1), launch_on_start=False
            ),
        ],
    )
    # While the analysis ran, the sim's record was pending.
    pending_views = [
        v for v in ctl.seen if any(r["workflow_type"] == "sim" and r["pending"] for r in v)
    ]
    assert pending_views
    final = {r["workflow_type"]: r for r in ctl.seen[-1]}
    assert final["analysis"]["outcome"] == "failed" and final["sim"]["credit"] == 0.0


def test_controller_progress_adds_to_the_credit():
    ctl = Watch(n=2, partial=0.3)
    final = {
        r["workflow_type"]: r
        for r in _run(
            ctl,
            [
                WorkflowConfig(name="sim", block=make()),
                WorkflowConfig(name="analysis", block=make(), launch_on_start=False),
            ],
        ).values()
    }
    assert final["sim"]["progress"] == 0.3 and final["sim"]["credit"] == 1.0


def test_controller_launch_with_source_gets_credit_before_its_source_finishes():
    # "refine" is launched by the controller while its sim is still running,
    # and finishes first: the credit waits for the sim and reaches it then.
    def extra(ctl, obs):
        running_sims = [w for w, t in obs["wf_types"].items() if t == "sim"]
        if running_sims and not getattr(ctl, "sent", False):
            ctl.sent = True
            return [LaunchRequest.make("analysis", context={"source": running_sims[0]})]
        return []

    ctl = Watch(n=3, extra=extra)
    recs = _run(
        ctl,
        [
            WorkflowConfig(name="sim", block=make(sleep=0.2)),
            WorkflowConfig(name="analysis", block=make(), launch_on_start=False),
        ],
    )
    sim = next(r for r in recs.values() if r["workflow_type"] == "sim")
    assert sim["credit"] >= 1.0  # from the controller's analysis (and the dependency's)
