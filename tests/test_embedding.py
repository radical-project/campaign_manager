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


def _spec(ctl, block, **kw) -> CampaignSpec:
    return CampaignSpec(
        name="emb",
        budget=CampaignBudget(
            capacity={"cpus": 64}, max_concurrent_workflows=2, max_concurrency_per_workflow=2
        ),
        controller=ctl,
        workflows=[
            WorkflowConfig(
                spending_limit={"cpu_hours": 1.0},
                resources={"cpus": 1},
                name="w",
                block=block,
                launch_on_start=kw.pop("launch_on_start", True),
            )
        ],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
        idle_exit_s=0.5,  # without a met goal the campaign ends idle
        **kw,
    )


def _run(spec):
    return asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))


async def sleeper(reporter) -> None:
    await reporter.emit(score=0.0)
    await asyncio.sleep(30)


def test_host_signal_handler_survives_run() -> None:
    cm = CampaignManager(install_signal_handlers=False)

    async def main():
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, lambda: None)  # the host's
        loop.call_later(0.5, cm.stop)
        out = await asyncio.wait_for(cm.run(_spec(CampaignController(), sleeper)), 20.0)
        return out, loop.remove_signal_handler(signal.SIGTERM)

    out, still_installed = asyncio.run(main())
    assert out.exit_reason == "signal"
    assert still_installed


async def reports_m(reporter) -> None:
    await reporter.emit(score=0.0, metrics={"m": 5.0})


async def reports_m_then_fails(reporter) -> None:
    await reporter.emit(score=0.0, metrics={"m": 5.0})
    raise RuntimeError("workflow failed")


async def reports_score(reporter) -> None:
    await reporter.emit(score=0.9)


async def no_metrics(reporter) -> None:
    await reporter.emit(score=0.0)


class Goals(CampaignController):
    def __init__(self, goal: Goal) -> None:
        super().__init__()
        self._goal = goal

    def goals(self):
        return [self._goal]


M_GOAL = Goal("g", metric="m", threshold=4.0, direction="maximize")


@pytest.mark.parametrize(
    "block, goal, exit_reason",
    [
        (reports_m, M_GOAL, "goals_satisfied"),
        (reports_score, Goal("q", "score", 0.8, "maximize"), "goals_satisfied"),
        (reports_m_then_fails, M_GOAL, "campaign_idle"),  # a failed workflow does not count
    ],
)
def test_goal_met_by_workflow_metrics(block, goal, exit_reason) -> None:
    assert _run(_spec(Goals(goal), block)).exit_reason == exit_reason


def test_goal_met_by_restored_metric_range(tmp_path) -> None:
    write_checkpoint(
        checkpoint_path(str(tmp_path), "emb"),
        CampaignCheckpoint(campaign_name="emb", metric_range={"m": [5.0, 5.0]}),
    )
    spec = _spec(Goals(M_GOAL), no_metrics, launch_on_start=False, checkpoint_dir=str(tmp_path))
    assert _run(spec).exit_reason == "goals_satisfied"


async def reports_big_int_then_m(reporter) -> None:
    """A value that cannot be a float comes first: it must not stop "m" being
    recorded, nor the rest of the completion handling."""
    await reporter.emit(score=0.0, metrics={"big": 10**400, "m": 5.0})


def test_unusable_metric_value_does_not_stop_the_rest() -> None:
    assert _run(_spec(Goals(M_GOAL), reports_big_int_then_m)).exit_reason == "goals_satisfied"


def test_goal_on_a_built_in_observation_key_is_unmet_not_fatal() -> None:
    # obs["running"] is a dict: comparing it to the threshold would raise
    # inside the policy cycle, which no caller catches.
    goal = Goal("g", metric="running", threshold=1.0, direction="maximize")
    assert _run(_spec(Goals(goal), reports_m)).exit_reason == "campaign_idle"


def test_malformed_checkpoint_metric_range_is_dropped(tmp_path) -> None:
    write_checkpoint(
        checkpoint_path(str(tmp_path), "emb"),
        CampaignCheckpoint(campaign_name="emb", metric_range={"m": "not-a-pair"}),
    )
    spec = _spec(Goals(M_GOAL), no_metrics, launch_on_start=False, checkpoint_dir=str(tmp_path))
    assert _run(spec).exit_reason == "campaign_idle"


def test_host_signal_handler_is_put_back_by_default() -> None:
    """With the default install_signal_handlers=True AuCaM takes SIGTERM over
    for the run, and gives the host's own handler back when the run ends."""
    cm = CampaignManager()  # the default

    def host() -> None:
        pass

    async def main():
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, host)
        loop.call_later(0.5, cm.stop)
        out = await asyncio.wait_for(cm.run(_spec(CampaignController(), sleeper)), 20.0)
        # asyncio has no getter; read its bookkeeping to check whose handler it is.
        handlers = getattr(loop, "_signal_handlers", None)
        handle = handlers.get(signal.SIGTERM) if isinstance(handlers, dict) else None
        return out, loop.remove_signal_handler(signal.SIGTERM), getattr(handle, "_callback", None)

    out, still_installed, callback = asyncio.run(main())
    assert out.exit_reason == "signal"
    assert still_installed
    assert callback is host
