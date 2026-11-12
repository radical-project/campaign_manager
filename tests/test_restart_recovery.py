"""R1 — restart: workflows that died with AuCaM are charged what they used;
command-line workflows that outlived it are adopted; goals and the scheduling
policy's and controller's own state survive."""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

from src.aucam import (
    BanditSchedulingPolicy,
    CampaignBudget,
    CampaignCheckpoint,
    CampaignController,
    CampaignDecision,
    CampaignManager,
    CampaignSpec,
    Goal,
    LocalDictBackend,
    PolicyResult,
    WorkflowConfig,
    WorkflowReport,
    WorkflowReporter,
    checkpoint_path,
    read_checkpoint,
    write_checkpoint,
)
from src.aucam.checkpoint import ActiveWorkflowRecord

H = 3600.0


class RemoteLike:
    """LocalDictBackend that looks remote (endpoint, no synchronous read)."""

    def __init__(self) -> None:
        self._b = LocalDictBackend()

    def endpoint(self) -> str:
        return "fake:0"

    def put(self, wf_id: str, **kw) -> None:
        self._b._store[wf_id] = WorkflowReport(wf_id=wf_id, **kw)

    def __getattr__(self, name):
        if name == "sync_get_report":
            raise AttributeError(name)
        return getattr(self._b, name)


async def sim(reporter: WorkflowReporter) -> None:
    await asyncio.sleep(0.03)
    await reporter.emit(score=1.0, metrics={"cpu_hours": 1.0 / H})


class Until(CampaignController):
    """Ends when `n` workflows have finished (counting only this run)."""

    def __init__(self, n: int, **kw) -> None:
        super().__init__(**kw)
        self.n = n
        self.results = []

    def on_complete(self, result) -> None:
        super().on_complete(result)
        self.results.append(result)

    def decide(self, obs: dict) -> PolicyResult:
        res = super().decide(obs)
        res.terminate = res.terminate or len(self.results) >= self.n
        return res


def _spec(ctl, tmp_path, workflows, **kw):
    return CampaignSpec(
        name="r1",
        budget=CampaignBudget(max_concurrent_workflows=2, max_concurrency_per_workflow=2),
        controller=ctl,
        workflows=workflows,
        policy_interval_s=0.02,
        shutdown_timeout_s=5.0,
        checkpoint_dir=str(tmp_path),
        **kw,
    )


def _ckpt(tmp_path, **kw):
    write_checkpoint(
        checkpoint_path(str(tmp_path), "r1"), CampaignCheckpoint(campaign_name="r1", **kw)
    )


def _run(spec):
    return asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))


def test_lost_workflow_is_charged_and_run_again(tmp_path: Path):
    _ckpt(
        tmp_path,
        active=[
            ActiveWorkflowRecord(
                wf_id="r1.sim.dead",
                workflow_type="sim",
                started_at=time.time() - 5,
                spent_so_far={"cpu_hours": 2.0 / H},
            )
        ],
    )
    ctl = Until(1)
    out = _run(_spec(ctl, tmp_path, [WorkflowConfig(name="sim", block=sim, launch_on_start=False)]))
    assert ctl.results[0].workflow_id != "r1.sim.dead"  # a new run
    assert abs(out.spent["cpu_hours"] * H - 3.0) < 1e-6  # 2 lost + 1 re-run


def test_goals_policy_and_controller_state_survive(tmp_path: Path):
    _ckpt(
        tmp_path,
        goals=[{"name": "best", "metric": "best", "threshold": 99.0, "direction": "maximize"}],
        policy_state={"arms": {"sim": {"n": 7, "mean": 0.5}}, "total_pulls": 7, "cost_mean": {}},
        controller_state={"completed": {"sim": 2}},
    )
    policy = BanditSchedulingPolicy()
    ctl = Until(99, launch=["sim"], goal_type="sim", goal_n=3)
    out = _run(
        _spec(ctl, tmp_path, [WorkflowConfig(name="sim", block=sim)], scheduling_policy=policy)
    )
    assert out.goals == [Goal("best", "best", 99.0, "maximize")]
    assert policy.arm_stats()["sim"]["n"] >= 7
    assert sum(r.returncode == 0 for r in ctl.results) == 1  # 2 before the restart + 1 = goal_n


def _adopt_case(tmp_path, feed, stale=None, n=1):
    backend = RemoteLike()
    _ckpt(
        tmp_path,
        active=[
            ActiveWorkflowRecord(
                wf_id="r1.cmd.alive",
                workflow_type="cmd",
                started_at=time.time() - 5,
                spending_limit={"cpu_hours": 9.0 / H},
            )
        ],
    )
    ctl = Until(n)
    spec = _spec(
        ctl,
        tmp_path,
        [
            WorkflowConfig(
                name="cmd",
                launch_cmd=[sys.executable, "-c", "pass"],
                launch_on_start=False,
                stale_timeout_s=stale,
            )
        ],
        data_backend=backend,
    )

    async def main():
        task = asyncio.ensure_future(feed(backend))
        out = await asyncio.wait_for(CampaignManager().run(spec), timeout=30.0)
        task.cancel()
        return out

    return ctl, asyncio.run(main())


def test_running_command_line_workflow_is_adopted(tmp_path: Path):
    async def feed(backend):
        for i in range(3):
            backend.put("r1.cmd.alive", score=0.5, metrics={"cpu_hours": (i + 1) / H})
            await asyncio.sleep(0.05)
        backend.put(
            "r1.cmd.alive", score=0.9, metrics={"cpu_hours": 4.0 / H}, done=True, returncode=0
        )

    ctl, out = _adopt_case(tmp_path, feed)
    r = ctl.results[0]
    assert r.workflow_id == "r1.cmd.alive" and r.returncode == 0  # adopted, not re-run
    assert round(r.spending_limit["cpu_hours"] * H, 6) == 9.0
    assert abs(out.spent["cpu_hours"] * H - 4.0) < 1e-6


def test_adopted_failure_keeps_its_exit_code(tmp_path: Path):
    async def feed(backend):
        backend.put("r1.cmd.alive", score=0.1, metrics={}, done=True, returncode=3)

    ctl, _ = _adopt_case(tmp_path, feed)
    assert ctl.results[0].workflow_id == "r1.cmd.alive"
    assert ctl.results[0].returncode == 3


def test_silent_adopted_workflow_is_run_again(tmp_path: Path):
    async def feed(backend):
        pass  # it died with AuCaM: no reports

    ctl, out = _adopt_case(tmp_path, feed, stale=0.2, n=2)
    first, second = ctl.results
    assert first.workflow_id == "r1.cmd.alive" and first.returncode == -1
    assert second.workflow_id != "r1.cmd.alive" and second.returncode == 0  # run again


def test_checkpoint_records_running_workflows_spending(tmp_path: Path):
    async def slow(reporter: WorkflowReporter) -> None:
        for i in range(1, 30):
            await asyncio.sleep(0.02)
            await reporter.emit(score=0.5, metrics={"cpu_hours": i / H})

    class Peek(CampaignController):
        def __init__(self) -> None:
            super().__init__()
            self.seen: list[dict] = []

        def decide(self, obs: dict) -> PolicyResult:
            ck = read_checkpoint(checkpoint_path(str(tmp_path), "r1"))
            if ck is not None and ck.active:
                self.seen.append(ck.active[0].spent_so_far)
            return PolicyResult(terminate=len(self.seen) >= 5)

    ctl = Peek()
    _run(_spec(ctl, tmp_path, [WorkflowConfig(name="slow", block=slow)]))
    assert ctl.seen and ctl.seen[-1]["cpu_hours"] > 0
    assert ctl.seen[-1]["cpu_hours"] >= ctl.seen[0]["cpu_hours"]


def test_cancelling_an_adopted_workflow_sends_cancel(tmp_path: Path):
    class CancelIt(Until):
        def decide(self, obs: dict) -> PolicyResult:
            res = super().decide(obs)
            if "r1.cmd.alive" in obs["wf_types"]:
                res.decisions["r1.cmd.alive"] = CampaignDecision.CANCEL
            return res

    backend = RemoteLike()

    async def feed(b):
        while True:  # keeps running and reporting; never sends done
            b.put("r1.cmd.alive", score=0.5, metrics={})
            await asyncio.sleep(0.02)

    _ckpt(
        tmp_path,
        active=[
            ActiveWorkflowRecord(wf_id="r1.cmd.alive", workflow_type="cmd", started_at=time.time())
        ],
    )
    ctl = CancelIt(1)
    spec = _spec(
        ctl,
        tmp_path,
        [
            WorkflowConfig(
                name="cmd", launch_cmd=[sys.executable, "-c", "pass"], launch_on_start=False
            )
        ],
        data_backend=backend,
    )

    async def main():
        task = asyncio.ensure_future(feed(backend))
        await asyncio.wait_for(CampaignManager().run(spec), timeout=30.0)
        task.cancel()

    asyncio.run(main())
    r = ctl.results[0]
    assert r.workflow_id == "r1.cmd.alive" and r.returncode == -1 and r.cancel == "forced"
    assert backend._b._signals.get("r1.cmd.alive") == CampaignDecision.CANCEL  # asked to stop
