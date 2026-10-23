"""
Dummy campaign integration test for Phase 1.

Two Tier-2 workflows (fast_job and slow_job, launched as block coroutines)
run concurrently.  A third (follow_up) is gated behind a DAG dependency on
fast_job completing successfully.  The controller terminates after all three
have finished.

Exercises: scheduler loop, DAG dependency, on-completion hook, policy cycle,
graceful shutdown, BudgetAllocator.
"""

import asyncio
import time

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    LaunchRequest,
    PolicyResult,
    WorkflowConfig,
    WorkflowDependency,
    WorkflowResult,
)

# ── Dummy workflows ──────────────────────────────────────────────────────────


async def fast_job() -> str:
    await asyncio.sleep(0.1)
    return "fast_done"


async def slow_job() -> str:
    await asyncio.sleep(0.3)
    return "slow_done"


async def follow_up() -> str:
    await asyncio.sleep(0.05)
    return "follow_up_done"


# ── Controller ───────────────────────────────────────────────────────────────


class DummyController(CampaignController):
    """Terminates when all three workflow types have each run at least once."""

    def __init__(self) -> None:
        self.completed: dict[str, int] = {}
        self.hook_fired = False

    def decide(self, obs: dict) -> PolicyResult:
        running = obs.get("running", {})
        queue_snap = obs["queue_snapshot"]

        all_done = (
            self.completed.get("fast_job", 0) >= 1
            and self.completed.get("slow_job", 0) >= 1
            and self.completed.get("follow_up", 0) >= 1
            and queue_snap.total_queued == 0
            and not running
        )

        return PolicyResult(terminate=all_done)


# ── Test 1: full campaign with DAG dependency ─────────────────────────────────


def test_dummy_campaign_runs_and_terminates() -> None:
    asyncio.run(_run_dummy_campaign())


async def _run_dummy_campaign() -> None:
    controller = DummyController()

    def _on_fast_done(result: WorkflowResult) -> None:
        controller.completed[result.workflow_type] = (
            controller.completed.get(result.workflow_type, 0) + 1
        )
        controller.hook_fired = True

    def _on_slow_done(result: WorkflowResult) -> None:
        controller.completed[result.workflow_type] = (
            controller.completed.get(result.workflow_type, 0) + 1
        )

    def _on_follow_done(result: WorkflowResult) -> None:
        controller.completed[result.workflow_type] = (
            controller.completed.get(result.workflow_type, 0) + 1
        )

    spec = CampaignSpec(
        name="dummy-campaign",
        budget=CampaignBudget(
            max_concurrent_workflows=3,
            max_concurrency_per_workflow=1,
        ),
        controller=controller,
        workflows=[
            WorkflowConfig(
                name="fast_job",
                block=fast_job,
                launch_on_start=True,
                on_completion=_on_fast_done,
            ),
            WorkflowConfig(
                name="slow_job",
                block=slow_job,
                launch_on_start=True,
                on_completion=_on_slow_done,
            ),
            WorkflowConfig(
                name="follow_up",
                block=follow_up,
                launch_on_start=False,
                on_completion=_on_follow_done,
            ),
        ],
        dependencies=[
            WorkflowDependency(
                source="fast_job",
                target="follow_up",
                condition=lambda r: r.returncode == 0,
            ),
        ],
        policy_interval_s=0.2,
        shutdown_timeout_s=5.0,
    )

    cm = CampaignManager()
    cm.submit_campaign(spec)

    start = time.monotonic()
    await asyncio.wait_for(cm.run(), timeout=20.0)
    elapsed = time.monotonic() - start

    assert elapsed < 10.0, "campaign took too long"
    assert controller.hook_fired, "on_completion hook for fast_job did not fire"
    assert controller.completed.get("fast_job", 0) >= 1
    assert controller.completed.get("slow_job", 0) >= 1
    assert controller.completed.get("follow_up", 0) >= 1


# ── Test 2: concurrency cap enforced ─────────────────────────────────────────


def test_budget_limits_concurrent_workflows() -> None:
    asyncio.run(_run_sequential_campaign())


async def _run_sequential_campaign() -> None:
    launch_times: list[float] = []
    completed_count = 0

    async def timed_job() -> str:
        launch_times.append(time.monotonic())
        await asyncio.sleep(0.15)
        return "done"

    class SequentialController(CampaignController):
        def decide(self, obs: dict) -> PolicyResult:
            nonlocal completed_count
            running = obs.get("running", {})
            q = obs["queue_snapshot"]
            # Terminate after 2 completions.
            if completed_count >= 2 and q.total_queued == 0 and not running:
                return PolicyResult(terminate=True)
            # Re-enqueue a second run if the first just finished.
            if (
                completed_count == 1
                and q.by_type.get("timed_job", 0) == 0
                and not running.get("timed_job")
            ):
                return PolicyResult(
                    launch_requests=[LaunchRequest.make("timed_job", requester="test")]
                )
            return PolicyResult()

    def _count(r: WorkflowResult) -> None:
        nonlocal completed_count
        completed_count += 1

    spec = CampaignSpec(
        name="seq-campaign",
        budget=CampaignBudget(
            max_concurrent_workflows=1,
            max_concurrency_per_workflow=1,
        ),
        controller=SequentialController(),
        workflows=[
            WorkflowConfig(
                name="timed_job",
                block=timed_job,
                launch_on_start=True,
                on_completion=_count,
            ),
        ],
        policy_interval_s=0.1,
        shutdown_timeout_s=5.0,
    )

    cm = CampaignManager()
    cm.submit_campaign(spec)
    await asyncio.wait_for(cm.run(), timeout=20.0)

    assert completed_count >= 2
    if len(launch_times) >= 2:
        gap = launch_times[1] - launch_times[0]
        assert gap >= 0.1, f"second job launched too soon (gap={gap:.3f}s)"
