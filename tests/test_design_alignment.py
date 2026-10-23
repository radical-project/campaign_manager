"""Behaviour described in the AuCaM design document (doc ↔ code alignment).

1. SIGTERM / SIGINT shuts campaigns down gracefully (final checkpoint, exit reason).
2. The scheduler enforces max_concurrency_per_workflow for every request,
   including CRITICAL ones and whatever a custom policy returns.
3. A restored campaign does not add duplicate initial launch requests.
4. CampaignOutcome.goal_reached is True when the campaign's goals were met.
5. A controller's budget observation covers only its own campaign.
6. Remote backends: the final report is read when the workflow ends
   (WorkflowResult.final_metrics / final_score).
7. Node telemetry correlation recognises TaskCanceled.
8. submit_campaign() applies the admission check.
9. Removed settings are rejected / gone.
10. A scheduling-policy instance belongs to one campaign (no cross-campaign learning).
"""

from __future__ import annotations

import asyncio
import json

import pytest

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    GlobalBudget,
    Goal,
    LaunchPriority,
    LaunchRequest,
    LocalDictBackend,
    PolicyResult,
    SchedulingPolicy,
    WorkflowConfig,
)
from src.aucam.campaign import _correlate_events


def _spec(name, controller, workflows, **kw) -> CampaignSpec:
    budget = kw.pop("budget", None) or CampaignBudget(
        max_concurrent_workflows=4, max_concurrency_per_workflow=4
    )
    return CampaignSpec(
        name=name,
        budget=budget,
        controller=controller,
        workflows=workflows,
        policy_interval_s=kw.pop("policy_interval_s", 0.05),
        shutdown_timeout_s=5.0,
        **kw,
    )


async def sleeper(reporter) -> None:
    await reporter.emit(score=0.0)
    await asyncio.sleep(30)


# ── 1. graceful shutdown on signal ───────────────────────────────────────────


def test_signal_shuts_campaign_down_gracefully(tmp_path) -> None:
    spec = _spec(
        "sig",
        CampaignController(),
        [WorkflowConfig(name="s", block=sleeper)],
        checkpoint_dir=str(tmp_path),
    )
    cm = CampaignManager()
    cm.submit_campaign(spec)

    async def main() -> None:
        async def stop() -> None:
            await asyncio.sleep(0.5)
            cm._shutdown_event.set()  # what the SIGTERM handler does

        asyncio.ensure_future(stop())
        await asyncio.wait_for(cm.run(), timeout=20.0)

    asyncio.run(main())
    inst = cm._active_instances["sig"]
    assert inst.exit_reason == "signal"
    ckpt = json.loads((tmp_path / "sig.json").read_text())
    assert ckpt["cancelled"] == 1  # final checkpoint written after cancel
    assert ckpt["active"] == []


# ── 2. per-type cap enforced by the scheduler ────────────────────────────────

running = {"n": 0, "max": 0}


async def counted(reporter) -> None:
    running["n"] += 1
    running["max"] = max(running["max"], running["n"])
    try:
        await asyncio.sleep(0.15)
    finally:
        running["n"] -= 1


class GreedyPolicy(SchedulingPolicy):
    """Ignores the per-type cap."""

    def select(self, candidates, ctx):
        return list(candidates)


class Burst(CampaignController):
    def __init__(self, priority: LaunchPriority) -> None:
        super().__init__(goal_type="c", goal_n=4)
        self._priority = priority
        self._sent = False

    def decide(self, obs):
        reqs = []
        if not self._sent:
            self._sent = True
            reqs = [LaunchRequest.make("c", self._priority) for _ in range(4)]
        return PolicyResult(launch_requests=reqs, terminate=self.is_goal_reached())


@pytest.mark.parametrize("priority", [LaunchPriority.CRITICAL, LaunchPriority.NORMAL])
def test_per_type_cap_enforced(priority) -> None:
    running.update(n=0, max=0)
    spec = _spec(
        "cap",
        Burst(priority),
        [WorkflowConfig(name="c", block=counted, launch_on_start=False)],
        budget=CampaignBudget(max_concurrent_workflows=4, max_concurrency_per_workflow=1),
        scheduling_policy=GreedyPolicy(),
    )
    cm = CampaignManager()
    cm.submit_campaign(spec)
    asyncio.run(asyncio.wait_for(cm.run(), timeout=30.0))
    assert running["max"] == 1


# ── 3. no duplicate initial requests after restore ───────────────────────────


async def quick(reporter) -> None:
    await asyncio.sleep(0.1)


class StopWhenIdle(CampaignController):
    def decide(self, obs):
        idle = not obs["running"] and obs["queue_snapshot"].total_queued == 0
        return PolicyResult(terminate=idle and sum(self.completed.values()) > 0)


def test_restore_does_not_duplicate_initial_requests(tmp_path) -> None:
    (tmp_path / "rst.json").write_text(
        json.dumps(
            {
                "campaign_name": "rst",
                "queue": [
                    {
                        "workflow_type": "q",
                        "priority": "NORMAL",
                        "enqueued_at": 0.0,
                        "context": {},
                        "requester": "cm:init",
                    }
                ],
                "active": [],
            }
        )
    )
    ctl = StopWhenIdle()
    spec = _spec("rst", ctl, [WorkflowConfig(name="q", block=quick)], checkpoint_dir=str(tmp_path))
    cm = CampaignManager()
    cm.submit_campaign(spec)
    asyncio.run(asyncio.wait_for(cm.run(), timeout=30.0))
    assert ctl.completed == {"q": 1}


# ── 4. goal_reached for Goal-based campaigns ────────────────────────────────


class GoalCtl(CampaignController):
    def goals(self):
        return [Goal(name="g", metric="m", threshold=1.0, direction="maximize")]

    def observe(self, queue_snapshot, budget, running=None, **kw):
        obs = super().observe(queue_snapshot, budget, running, **kw)
        obs["m"] = 1.0
        return obs


def test_goal_reached_when_goals_satisfied() -> None:
    out = {}

    async def main() -> None:
        cm = CampaignManager()
        cm.expect_driver()

        async def driver() -> None:
            try:
                res = await cm.submit(
                    _spec("goal", GoalCtl(), [WorkflowConfig(name="s", block=sleeper)])
                )
                out["o"] = await res.wait()
            finally:
                cm.driver_done()

        asyncio.ensure_future(driver())
        await asyncio.wait_for(cm.run(), timeout=30.0)

    asyncio.run(main())
    assert out["o"].goal_reached is True


# ── 5. budget observation is the campaign's own ledger entry ────────────────


class BudgetSpy(CampaignController):
    def __init__(self) -> None:
        super().__init__()
        self.seen: dict | None = None

    def decide(self, obs):
        self.seen = obs["budget"]
        return PolicyResult(terminate=True)


def test_budget_observation_is_own_campaign_only() -> None:
    spies = [BudgetSpy(), BudgetSpy()]
    cm = CampaignManager()
    for i, spy in enumerate(spies):
        cm.submit_campaign(
            _spec(
                f"b{i}",
                spy,
                [WorkflowConfig(name="s", block=sleeper)],
                budget=CampaignBudget(
                    max_concurrent_workflows=1,
                    max_concurrency_per_workflow=1,
                    spending={"cpu_hours": float(i + 1)},
                ),
            )
        )
    asyncio.run(asyncio.wait_for(cm.run(), timeout=30.0))
    for i, spy in enumerate(spies):
        assert set(spy.seen) == {"limits", "spent", "in_flight", "global_spent", "burn"}
        assert spy.seen["limits"] == {"cpu_hours": float(i + 1)}


# ── 6. final report read at the end for remote backends ─────────────────────


class RemoteLikeBackend:
    """LocalDictBackend without the synchronous read (like Redis / Dragon)."""

    def __init__(self) -> None:
        self._b = LocalDictBackend()

    def __getattr__(self, name):
        if name == "sync_get_report":
            raise AttributeError(name)
        return getattr(self._b, name)


async def emit_and_exit(reporter) -> None:
    await reporter.emit(score={"energy": -8.5}, metrics={"cpu_hours": 0.01})


class Collect(CampaignController):
    def __init__(self) -> None:
        super().__init__(goal_type="e", goal_n=1)
        self.results: list = []

    def on_complete(self, result) -> None:
        super().on_complete(result)
        self.results.append(result)


def test_final_report_read_for_remote_backend() -> None:
    ctl = Collect()
    spec = _spec(
        "fin",
        ctl,
        [WorkflowConfig(name="e", block=emit_and_exit)],
        data_backend=RemoteLikeBackend(),
        policy_interval_s=30.0,
    )
    cm = CampaignManager()
    cm.submit_campaign(spec)

    async def main() -> None:
        async def stop() -> None:
            while not ctl.results:
                await asyncio.sleep(0.05)
            cm._shutdown_event.set()

        asyncio.ensure_future(stop())
        await asyncio.wait_for(cm.run(), timeout=20.0)

    asyncio.run(main())
    r = ctl.results[0]
    assert r.final_score == {"energy": -8.5}
    assert r.final_metrics == {"cpu_hours": 0.01}


# ── 7. TaskCanceled ends the correlation window ─────────────────────────────


def test_correlation_recognises_task_canceled() -> None:
    events = [
        {"event_type": "TaskStarted", "event_time": 0.0, "task_id": "w"},
        {"event_type": "ResourceUpdate", "event_time": 1.0, "cpu_percent": 50.0},
        {"event_type": "TaskCanceled", "event_time": 2.0, "task_id": "w"},
        {"event_type": "ResourceUpdate", "event_time": 50.0, "cpu_percent": 100.0},
    ]
    prof, _ = _correlate_events("w", 100.0, events)
    assert prof["end_time"] == 2.0
    assert prof["cpu_percent_avg"] == 50.0


# ── 8. submit_campaign() admission ──────────────────────────────────────────


def test_submit_campaign_admission() -> None:
    cm = CampaignManager(
        global_budget=GlobalBudget(max_concurrent_campaigns=2, spending={"gpu_hours": 10.0})
    )
    wf = [WorkflowConfig(name="s", block=sleeper)]

    def bud(gpu: float) -> CampaignBudget:
        return CampaignBudget(
            max_concurrent_workflows=1, max_concurrency_per_workflow=1, spending={"gpu_hours": gpu}
        )

    assert cm.submit_campaign(_spec("a", CampaignController(), wf, budget=bud(6.0))).accepted
    r = cm.submit_campaign(_spec("b", CampaignController(), wf, budget=bud(6.0)))
    assert not r.accepted and "spending" in r.reason
    assert cm.submit_campaign(_spec("c", CampaignController(), wf, budget=bud(1.0))).accepted
    r = cm.submit_campaign(_spec("d", CampaignController(), wf, budget=bud(1.0)))
    assert not r.accepted and "max_concurrent_campaigns" in r.reason
    assert [s.name for s in cm._pending] == ["a", "c"]


# ── 9. removed settings ─────────────────────────────────────────────────────


def test_removed_manager_settings() -> None:
    for kw in ("slurm_account", "qos", "telemetry_interval_s"):
        with pytest.raises(TypeError):
            CampaignManager(**{kw: "x"})
    wfc = WorkflowConfig(name="w", block=sleeper)
    assert not hasattr(wfc, "metrics_cls") and not hasattr(wfc, "score_directions")


# ── 10. no cross-campaign learning ─────────────────────────────────────────────


def test_policy_instance_cannot_be_shared() -> None:
    from src.aucam import BanditSchedulingPolicy

    shared = BanditSchedulingPolicy()
    cm = CampaignManager()
    wf = [WorkflowConfig(name="s", block=sleeper)]
    assert cm.submit_campaign(
        _spec("p1", CampaignController(), wf, scheduling_policy=shared)
    ).accepted
    r = cm.submit_campaign(_spec("p2", CampaignController(), wf, scheduling_policy=shared))
    assert not r.accepted and "already used by campaign 'p1'" in r.reason
    # A fresh instance (or None) is fine.
    assert cm.submit_campaign(
        _spec("p3", CampaignController(), wf, scheduling_policy=BanditSchedulingPolicy())
    ).accepted
    assert cm.submit_campaign(_spec("p4", CampaignController(), wf)).accepted


def test_bandit_has_no_scope_option() -> None:
    from src.aucam import BanditSchedulingPolicy

    with pytest.raises(TypeError):
        BanditSchedulingPolicy(scope="global")
