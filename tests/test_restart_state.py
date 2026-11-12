"""R2 — dependency state and requests' own amounts survive a restart."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

from src.aucam import (
    CampaignBudget,
    CampaignCheckpoint,
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
    write_checkpoint,
)
from src.aucam.campaign import _request_from_record, _request_record
from src.aucam.checkpoint import (
    ActiveWorkflowRecord,
    QueuedRequestRecord,
    _from_json,
    _to_json,
)

CAPACITY = {"cpus": 1024, "gpus": 64}  # the allocation tests run on


H = 3600.0


def test_request_record_roundtrip_keeps_amounts():
    req = LaunchRequest.make(
        "sim",
        spending_limit={"cpu_hours": 2.0},
        resources={"cpus": 4},
    )
    back = _request_from_record(_request_record(req))
    assert back.spending_limit == {"cpu_hours": 2.0}
    assert back.resources == {"cpus": 4}

    ckpt = CampaignCheckpoint(
        campaign_name="c",
        dep_triggered=[["c.sim.1", "analysis"]],
        dep_pending=[_request_record(LaunchRequest.make("analysis"))],
        active=[
            ActiveWorkflowRecord(
                wf_id="c.sim.2", workflow_type="sim", started_at=1.0, request=_request_record(req)
            )
        ],
    )
    again = _from_json(_to_json(ckpt))
    assert again.dep_triggered == [["c.sim.1", "analysis"]]
    assert again.dep_pending[0].workflow_type == "analysis"
    assert again.active[0].request.spending_limit == {"cpu_hours": 2.0}


async def sim(reporter: WorkflowReporter) -> None:
    await asyncio.sleep(0.03)
    await reporter.emit(score=1.0)


async def analysis(reporter: WorkflowReporter) -> None:
    await asyncio.sleep(0.02)
    await reporter.emit(score=1.0)


class Until(CampaignController):
    def __init__(self, analyses: int) -> None:
        super().__init__()
        self.analyses = analyses
        self.results = []

    def on_complete(self, result) -> None:
        super().on_complete(result)
        self.results.append(result)

    def decide(self, obs: dict) -> PolicyResult:
        done = sum(r.workflow_type == "analysis" for r in self.results)
        return PolicyResult(terminate=done >= self.analyses)


def test_held_requests_and_triggers_are_restored(tmp_path: Path):
    path = checkpoint_path(str(tmp_path), "restart")
    write_checkpoint(
        path,
        CampaignCheckpoint(
            campaign_name="restart",
            # One analysis was waiting for the first successful sim, and a sim
            # that finished before the restart already triggered once.
            dep_pending=[
                QueuedRequestRecord(
                    workflow_type="analysis",
                    priority="NORMAL",
                    enqueued_at=time.time(),
                    context={"held": True},
                    requester="test",
                    spending_limit={"cpu_hours": 7.0 / H},
                )
            ],
            dep_triggered=[["restart.sim.old", "analysis"]],
            queue=[
                QueuedRequestRecord(
                    workflow_type="sim",
                    priority="NORMAL",
                    enqueued_at=time.time(),
                    context={},
                    requester="test",
                    spending_limit={"cpu_hours": 3.0 / H},
                )
            ],
        ),
    )
    ctl = Until(analyses=1)
    spec = CampaignSpec(
        name="restart",
        budget=CampaignBudget(
            capacity=CAPACITY, max_concurrent_workflows=2, max_concurrency_per_workflow=2
        ),
        controller=ctl,
        workflows=[
            WorkflowConfig(name="sim", block=sim, launch_on_start=False),
            WorkflowConfig(name="analysis", block=analysis, launch_on_start=False),
        ],
        dependencies=[WorkflowDependency(source="sim", target="analysis")],
        policy_interval_s=0.02,
        shutdown_timeout_s=5.0,
        checkpoint_dir=str(tmp_path),
    )
    asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))

    sims = [r for r in ctl.results if r.workflow_type == "sim"]
    ans = [r for r in ctl.results if r.workflow_type == "analysis"]
    assert round(sims[0].spending_limit["cpu_hours"] * H, 6) == 3.0  # queued request's own limit
    # The held analysis was released by the first successful sim, with its own limit.
    assert len(ans) == 1 and round(ans[0].spending_limit["cpu_hours"] * H, 6) == 7.0
    final = read_checkpoint(path)
    assert ["restart.sim.old", "analysis"] in final.dep_triggered
