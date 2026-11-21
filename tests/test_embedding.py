"""AuCaM embedded in a host process: signal handlers are optional, and goals
are met by the metrics successful workflows report (also after a restart)."""

from __future__ import annotations

import asyncio
import signal

import pytest

from src.aucam import (
    CampaignBudget,
    CampaignCheckpoint,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    Goal,
    WorkflowConfig,
    checkpoint_path,
    write_checkpoint,
)


def _spec(ctl, workflows, **kw) -> CampaignSpec:
    return CampaignSpec(
        name="emb",
        budget=CampaignBudget(
            capacity={"cpus": 64}, max_concurrent_workflows=2, max_concurrency_per_workflow=2
        ),
        controller=ctl,
        workflows=workflows,
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
        idle_exit_s=0.5,  # without a met goal the campaign ends idle
        **kw,
    )


def _wf(block, **kw) -> WorkflowConfig:
    return WorkflowConfig(
        spending_limit={"cpu_hours": 1.0}, resources={"cpus": 1}, name="w", block=block, **kw
    )


def _run(spec, cm=None):
    cm = cm or CampaignManager()
    return asyncio.run(asyncio.wait_for(cm.run(spec), timeout=30.0))


# ── signal handlers ─────────────────────────────────────────────────────────


async def sleeper(reporter) -> None:
    await reporter.emit(score=0.0)
    await asyncio.sleep(30)


def test_host_signal_handler_survives_run() -> None:
    cm = CampaignManager(install_signal_handlers=False)

    async def main():
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, lambda: None)  # the host's
        loop.call_later(0.5, cm.stop)
        out = await asyncio.wait_for(cm.run(_spec(CampaignController(), [_wf(sleeper)])), 20.0)
        return out, loop.remove_signal_handler(signal.SIGTERM)

    out, still_installed = asyncio.run(main())
    assert out.exit_reason == "signal"
    assert still_installed


# ── goals on workflow metrics ──────────────────────────────────────────────


async def reports_m(reporter) -> None:
    await reporter.emit(score=0.0, metrics={"m": 5.0})


async def reports_m_then_fails(reporter) -> None:
    await reporter.emit(score=0.0, metrics={"m": 5.0})
    raise RuntimeError("workflow failed")


async def reports_score(reporter) -> None:
    await reporter.emit(score=0.9)


class Goals(CampaignController):
    def __init__(self, goal: Goal, obs_m=None) -> None:
        super().__init__()
        self._goal, self._obs_m = goal, obs_m

    def goals(self):
        return [self._goal]

    def observe(self, queue_snapshot, budget, running=None, **kw):
        obs = super().observe(queue_snapshot, budget, running, **kw)
        if self._obs_m is not None:
            obs["m"] = self._obs_m
        return obs


M_GOAL = Goal("g", metric="m", threshold=4.0, direction="maximize")


@pytest.mark.parametrize(
    "block, goal, obs_m, exit_reason",
    [
        (reports_m, M_GOAL, None, "goals_satisfied"),
        (reports_score, Goal("q", "score", 0.8, "maximize"), None, "goals_satisfied"),
        (reports_m_then_fails, M_GOAL, None, "campaign_idle"),  # failed: no value
        (reports_m, M_GOAL, 0.0, "campaign_idle"),  # the observation's value wins
    ],
)
def test_goal_met_by_workflow_metrics(block, goal, obs_m, exit_reason) -> None:
    out = _run(_spec(Goals(goal, obs_m), [_wf(block)]))
    assert out.exit_reason == exit_reason


async def no_metrics(reporter) -> None:
    await reporter.emit(score=0.0)


def test_goal_met_by_restored_metric_range(tmp_path) -> None:
    write_checkpoint(
        checkpoint_path(str(tmp_path), "emb"),
        CampaignCheckpoint(campaign_name="emb", metric_range={"m": [5.0, 5.0]}),
    )
    spec = _spec(
        Goals(M_GOAL),
        [_wf(no_metrics, launch_on_start=False)],
        checkpoint_dir=str(tmp_path),
    )
    assert _run(spec).exit_reason == "goals_satisfied"
