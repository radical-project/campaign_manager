"""P1 — graceful cancel, PAUSE timeout; U4 — graceful cancel for workflows over
their own limit; B6 — the campaign limit cancels running workflows."""

from __future__ import annotations

import asyncio
import logging
import time

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignDecision,
    CampaignManager,
    CampaignSpec,
    PolicyResult,
    WorkflowConfig,
    WorkflowReporter,
)

H = 3600.0


def worker(polite: bool, steps: int = 200, step_s: float = 0.02, cpu_per_step: float = 0.0):
    """Runs `steps` steps; a polite one stops when it gets CANCEL."""

    async def wf(reporter: WorkflowReporter) -> None:
        spent = 0.0
        for i in range(steps):
            if polite:
                sig = await reporter.poll()
                if sig == CampaignDecision.CANCEL:
                    await reporter.emit(score=0.5, metrics={"saved": True, "cpu_hours": spent})
                    return
            await asyncio.sleep(step_s)
            spent += cpu_per_step
            await reporter.emit(score=0.5, metrics={"step": i, "cpu_hours": spent})

    return wf


class CancelAfter(CampaignController):
    """Each type starts once (launch_on_start); cancel (or pause) each after
    `after_s`; stop when all have ended."""

    def __init__(self, types: list[str], after_s: float = 0.15, pause: bool = False) -> None:
        super().__init__()
        self.types = types
        self.after_s = after_s
        self.pause = pause
        self.t0 = time.monotonic()
        self.decided: set[str] = set()
        self.results = {}

    def on_complete(self, result) -> None:
        super().on_complete(result)
        self.results[result.workflow_type] = result

    def decide(self, obs: dict) -> PolicyResult:
        res = PolicyResult()
        if time.monotonic() - self.t0 > self.after_s:
            for wf_id in obs["wf_types"]:
                if wf_id not in self.decided:
                    self.decided.add(wf_id)
                    res.decisions[wf_id] = (
                        CampaignDecision.PAUSE if self.pause else CampaignDecision.CANCEL
                    )
        res.terminate = len(self.results) == len(self.types)
        return res


def _run(ctl, workflows, spending=None, on_limit="cancel", **spec_kw):
    spec = CampaignSpec(
        name="graceful",
        budget=CampaignBudget(
            max_concurrent_workflows=4,
            max_concurrency_per_workflow=4,
            spending=spending or {},
            on_limit=on_limit,
        ),
        controller=ctl,
        workflows=workflows,
        policy_interval_s=0.02,
        shutdown_timeout_s=5.0,
        **spec_kw,
    )
    t0 = time.monotonic()
    out = asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))
    return out, time.monotonic() - t0


def test_graceful_exit_counts_as_cancelled():
    ctl = CancelAfter(["polite"])
    _run(ctl, [WorkflowConfig(name="polite", block=worker(True), cancel_grace_s=3.0)])
    r = ctl.results["polite"]
    assert r.returncode == -1 and r.cancel == "graceful"
    assert r.final_metrics.get("saved") is True
    assert r.wall_time_s < 2.0  # well before the grace period ended


def test_workflow_ignoring_cancel_is_forced_after_grace():
    ctl = CancelAfter(["deaf"])
    _run(ctl, [WorkflowConfig(name="deaf", block=worker(False), cancel_grace_s=0.3)])
    r = ctl.results["deaf"]
    assert r.returncode == -1 and r.cancel == "forced"
    assert r.wall_time_s >= 0.3


def test_no_grace_cancels_at_once_and_campaign_default_applies():
    ctl = CancelAfter(["a", "b"])
    _run(
        ctl,
        [
            WorkflowConfig(name="a", block=worker(True)),  # campaign default: 2 s
            WorkflowConfig(name="b", block=worker(True), cancel_grace_s=0),  # at once
        ],
        cancel_grace_s=2.0,
    )
    assert ctl.results["a"].cancel == "graceful"
    assert ctl.results["b"].cancel == "forced"


def test_over_own_limit_is_cancelled_gracefully():
    ctl = CancelAfter(["hog"], after_s=99)  # the controller never cancels
    out, _ = _run(
        ctl,
        [
            WorkflowConfig(
                name="hog",
                block=worker(True, cpu_per_step=1.0 / H),
                spending_limit={"cpu_hours": 5.0 / H},
                cancel_grace_s=2.0,
            )
        ],
    )
    r = ctl.results["hog"]
    assert r.over_budget and r.cancel == "graceful"
    assert out.budget_cancels == 1


def test_shutdown_cancels_gracefully():
    class StopSoon(CancelAfter):
        def decide(self, obs):
            res = super().decide(obs)
            res.decisions = {}
            res.terminate = time.monotonic() - self.t0 > 0.15
            return res

    ctl = StopSoon(["polite"])
    _run(ctl, [WorkflowConfig(name="polite", block=worker(True), cancel_grace_s=3.0)])
    assert ctl.results["polite"].cancel == "graceful"


def test_pause_not_acknowledged_is_cancelled(caplog):
    ctl = CancelAfter(["deaf"], pause=True)
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        out, _ = _run(
            ctl,
            [
                WorkflowConfig(
                    name="deaf", block=worker(False), supports_pause=True, pause_timeout_s=0.2
                )
            ],
        )
    assert ctl.results["deaf"].returncode == -1
    assert "did not acknowledge PAUSE" in caplog.text


def test_paused_workflow_gets_cancel_through_wait_for_resume():
    async def pausable(reporter: WorkflowReporter) -> None:
        while True:
            sig = await reporter.poll()
            if sig == CampaignDecision.PAUSE:
                if await reporter.wait_for_resume() == CampaignDecision.CANCEL:
                    await reporter.emit(score=0.1, metrics={"saved": True})
                    return
            await asyncio.sleep(0.02)

    class PauseThenCancel(CancelAfter):
        def decide(self, obs):
            res = super().decide(obs)
            if time.monotonic() - self.t0 > 0.4:
                res.decisions = {w: CampaignDecision.CANCEL for w in obs["wf_types"]}
            return res

    ctl = PauseThenCancel(["p"], pause=True)
    _run(
        ctl,
        [WorkflowConfig(name="p", block=pausable, supports_pause=True, cancel_grace_s=3.0)],
    )
    r = ctl.results["p"]
    assert r.cancel == "graceful" and r.final_metrics.get("saved") is True


def test_campaign_limit_cancels_running_workflows():
    # Two workflows with no estimate (so no limit of their own) that would
    # each spend 20 CPU·s; the campaign limit stops both near 10.
    wfs = [
        WorkflowConfig(
            name=n,
            block=worker(True, steps=40, cpu_per_step=0.5 / H),
            cancel_grace_s=1.0,
        )
        for n in ("a", "b")
    ]
    ctl = CancelAfter(["a", "b"], after_s=99)
    out, _ = _run(ctl, wfs, spending={"cpu_hours": 10.0 / H})
    assert out.exit_reason == "spending_limit"
    assert {r.cancel for r in ctl.results.values()} == {"graceful"}
    assert out.spent["cpu_hours"] * H <= 12.0  # stopped within about one cycle


def test_on_limit_finish_lets_them_finish():
    # No estimate at all: the only workflows without a limit of their own.
    wfs = [
        WorkflowConfig(name=n, block=worker(True, steps=20, step_s=0.01, cpu_per_step=0.5 / H))
        for n in ("a", "b")
    ]
    ctl = CancelAfter(["a", "b"], after_s=99)
    out, _ = _run(ctl, wfs, spending={"cpu_hours": 10.0 / H}, on_limit="finish")
    assert all(r.returncode == 0 for r in ctl.results.values())
    assert abs(out.spent["cpu_hours"] * H - 20.0) < 1e-6


def test_over_resources_is_cancelled_gracefully():
    async def greedy(reporter: WorkflowReporter, context: dict) -> None:
        cpus = 2 * context["resources"]["cpus"]  # uses twice what it was given
        for _ in range(200):
            if await reporter.poll() == CampaignDecision.CANCEL:
                return
            await asyncio.sleep(0.02)
            await reporter.emit(score=0.5, metrics={"cpus_in_use": cpus})

    ctl = CancelAfter(["greedy"], after_s=99)
    out, _ = _run(
        ctl,
        [WorkflowConfig(name="greedy", block=greedy, resources={"cpus": 2}, cancel_grace_s=2.0)],
    )
    r = ctl.results["greedy"]
    assert r.over_resources and r.cancel == "graceful"
    assert out.resource_cancels == 1
