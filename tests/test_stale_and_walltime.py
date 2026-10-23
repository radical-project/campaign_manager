"""
Stale reporter and max_wall_time_s auto-cancellation tests.

Stale reporter
--------------
  stale_worker emits one report then goes silent.
  With stale_timeout_s=0.3 and policy_interval_s=0.1, the CM auto-cancels
  it after the gap exceeds the threshold.
  A good_worker running alongside completes normally.

max_wall_time_s
---------------
  walltime_worker runs an infinite loop with no reporter.
  With max_wall_time_s=0.25 and policy_interval_s=0.1, the CM auto-cancels
  it once the elapsed time exceeds the limit.
  A fast_worker alongside completes normally.

Both
----
  One campaign with both workflow types, both auto-cancel paths fire.
"""

from __future__ import annotations

import asyncio

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    PolicyResult,
    WorkflowConfig,
    WorkflowReporter,
    WorkflowResult,
)

# ── Workers ───────────────────────────────────────────────────────────────────


async def good_worker(reporter: WorkflowReporter) -> None:
    for i in range(4):
        await asyncio.sleep(0.05)
        await reporter.emit(score=0.9, metrics={"iter": i})


async def stale_worker(reporter: WorkflowReporter) -> None:
    """Emits once then stops reporting — should be auto-cancelled."""
    await reporter.emit(score=0.7, metrics={"iter": 0})
    await asyncio.sleep(999)  # hangs; reporter goes silent


async def walltime_worker() -> None:
    """No reporter, just runs forever — should be auto-cancelled by wall time."""
    await asyncio.sleep(999)


async def fast_worker() -> None:
    """No reporter, completes quickly."""
    await asyncio.sleep(0.1)


# ── Minimal controller ────────────────────────────────────────────────────────


class PassthroughController(CampaignController):
    """Issues no decisions; relies entirely on CM auto-cancels."""

    def __init__(self, target_completions: int = 1) -> None:
        self.completions = 0
        self.target = target_completions

    def decide(self, obs: dict) -> PolicyResult:
        running = obs.get("running", {})
        q = obs["queue_snapshot"]
        terminate = self.completions >= self.target and not running and q.total_queued == 0
        return PolicyResult(terminate=terminate)


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_stale_reporter_auto_cancel() -> None:
    """
    stale_worker emits once then goes silent.
    CM auto-cancels it after stale_timeout_s.
    good_worker completes normally.
    """
    asyncio.run(_run_stale())


def test_max_wall_time_auto_cancel() -> None:
    """
    walltime_worker runs forever (no reporter).
    CM auto-cancels it once max_wall_time_s is exceeded.
    fast_worker completes normally.
    """
    asyncio.run(_run_walltime())


def test_both_auto_cancels() -> None:
    """Both stale-reporter and wall-time paths fire in the same campaign."""
    asyncio.run(_run_both())


# ── Async runners ─────────────────────────────────────────────────────────────


async def _run_stale() -> None:
    ctrl = PassthroughController(target_completions=2)

    def _on_good(r: WorkflowResult) -> None:
        if r.returncode == 0:
            ctrl.completions += 1

    stale_cancelled: list[str] = []

    class _Ctrl(PassthroughController):
        def decide(self, obs: dict) -> PolicyResult:
            # Track when stale_worker disappears from running
            for wf_id in list(obs.get("live_reports", {})):
                if "stale_worker" in wf_id and wf_id not in stale_cancelled:
                    pass  # will be auto-cancelled by CM
            return super().decide(obs)

    ctrl2 = _Ctrl(target_completions=1)

    def _on_good2(r: WorkflowResult) -> None:
        if r.returncode == 0:
            ctrl2.completions += 1

    spec = CampaignSpec(
        name="stale_test",
        budget=CampaignBudget(max_concurrent_workflows=3, max_concurrency_per_workflow=2),
        controller=ctrl2,
        workflows=[
            WorkflowConfig(
                name="good_worker",
                block=good_worker,
                launch_on_start=True,
                on_completion=_on_good2,
            ),
            WorkflowConfig(
                name="stale_worker",
                block=stale_worker,
                launch_on_start=True,
                stale_timeout_s=0.3,  # auto-cancel if silent > 0.3 s
            ),
        ],
        policy_interval_s=0.1,
        shutdown_timeout_s=5.0,
    )

    cm = CampaignManager()
    cm.submit_campaign(spec)
    await asyncio.wait_for(cm.run(), timeout=15.0)

    assert ctrl2.completions >= 1, "Expected ≥1 good_worker completion"
    # stale_worker must have been cancelled (returncode -1); it never completes (rc=0)
    # Verified by the fact that the campaign terminated — if stale_worker had
    # blocked forever without being cancelled the test would timeout.


async def _run_walltime() -> None:
    ctrl = PassthroughController(target_completions=1)

    def _on_fast(r: WorkflowResult) -> None:
        if r.returncode == 0:
            ctrl.completions += 1

    spec = CampaignSpec(
        name="walltime_test",
        budget=CampaignBudget(max_concurrent_workflows=3, max_concurrency_per_workflow=2),
        controller=ctrl,
        workflows=[
            WorkflowConfig(
                name="fast_worker",
                block=fast_worker,
                launch_on_start=True,
                on_completion=_on_fast,
            ),
            WorkflowConfig(
                name="walltime_worker",
                block=walltime_worker,
                launch_on_start=True,
                max_wall_time_s=0.25,  # auto-cancel after 0.25 s
            ),
        ],
        policy_interval_s=0.1,
        shutdown_timeout_s=5.0,
    )

    cm = CampaignManager()
    cm.submit_campaign(spec)
    await asyncio.wait_for(cm.run(), timeout=15.0)

    assert ctrl.completions >= 1, "Expected ≥1 fast_worker completion"


async def _run_both() -> None:
    ctrl = PassthroughController(target_completions=1)
    completions: list[int] = [0]

    def _on_fast(r: WorkflowResult) -> None:
        if r.returncode == 0:
            completions[0] += 1
            ctrl.completions += 1

    spec = CampaignSpec(
        name="both_test",
        budget=CampaignBudget(max_concurrent_workflows=4, max_concurrency_per_workflow=2),
        controller=ctrl,
        workflows=[
            WorkflowConfig(
                name="fast_worker",
                block=fast_worker,
                launch_on_start=True,
                on_completion=_on_fast,
            ),
            WorkflowConfig(
                name="stale_worker",
                block=stale_worker,
                launch_on_start=True,
                stale_timeout_s=0.3,
            ),
            WorkflowConfig(
                name="walltime_worker",
                block=walltime_worker,
                launch_on_start=True,
                max_wall_time_s=0.25,
            ),
        ],
        policy_interval_s=0.1,
        shutdown_timeout_s=5.0,
    )

    cm = CampaignManager()
    cm.submit_campaign(spec)
    await asyncio.wait_for(cm.run(), timeout=15.0)

    assert completions[0] >= 1, "Expected ≥1 fast_worker completion"
