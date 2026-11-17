"""Task-level spending accounting: requested resources × duration from AsyncFlow events.

Needs a radical.asyncflow that puts attributes["resources"] on task events
(workflow_manager.requested_resources); skipped otherwise.
"""

from __future__ import annotations

import asyncio
import sys

import pytest

wm = pytest.importorskip("radical.asyncflow.workflow_manager")
if not hasattr(wm, "requested_resources"):
    pytest.skip("radical.asyncflow without task resource telemetry", allow_module_level=True)

from src.aucam import (  # noqa: E402
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    WorkflowConfig,
)

CAPACITY = {"cpus": 1024, "gpus": 64}  # the allocation tests run on


SLEEP_S = 0.3


async def gpu_workflow(flow=None) -> None:
    """Python workflow submitting one AsyncFlow task that asks for 2 GPUs."""

    @flow.function_task
    async def step(task_description={"ranks": 2, "gpus_per_rank": 1}):  # noqa: B006
        await asyncio.sleep(SLEEP_S)

    await step()


def test_task_level_accounting_charges_requested_resources() -> None:
    spec = CampaignSpec(
        name="acct",
        budget=CampaignBudget(
            capacity=CAPACITY, max_concurrent_workflows=2, max_concurrency_per_workflow=1
        ),
        controller=CampaignController(goal_type="cmd", goal_n=1),
        workflows=[
            WorkflowConfig(
                name="gpu",
                block=gpu_workflow,
                # deliberately too many GPUs: task-level accounting must win over
                # resources × run time
                spending_limit={"gpu_hours": 100.0},
                resources={"cpus": 1, "gpus": 50},
            ),
            WorkflowConfig(
                name="cmd",
                launch_cmd=[sys.executable, "-c", f"import time; time.sleep({SLEEP_S})"],
                task_description={"ranks": 1, "cores_per_rank": 4},  # also its resources
                spending_limit={"cpu_hours": 100.0},
            ),
        ],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )
    cm = CampaignManager(enable_telemetry=True)
    outcome = asyncio.run(asyncio.wait_for(cm.run(spec), timeout=60.0))
    spent = outcome.spent
    # gpu: 2 GPUs × ~0.3 s;  cmd: 4 cores × ~0.3 s  (generous bounds for CI jitter)
    assert 2 * SLEEP_S / 3600 * 0.8 <= spent.get("gpu_hours", 0.0) <= 2 * 3.0 / 3600
    assert 4 * SLEEP_S / 3600 * 0.8 <= spent.get("cpu_hours", 0.0) <= 4 * 3.0 / 3600


def test_task_counts_reach_the_controller() -> None:
    """With telemetry on, each policy cycle's observation carries AsyncFlow task
    counts; the running GPU task shows up as running."""
    seen: list[dict] = []

    class Recorder(CampaignController):
        def decide(self, obs):
            if obs.get("tasks") is not None:
                seen.append(obs["tasks"])
            return super().decide(obs)

    spec = CampaignSpec(
        name="counts",
        budget=CampaignBudget(
            capacity=CAPACITY, max_concurrent_workflows=1, max_concurrency_per_workflow=1
        ),
        controller=Recorder(launch=["gpu"], goal_type="gpu", goal_n=1),
        workflows=[
            WorkflowConfig(
                spending_limit={"cpu_hours": 1000.0},
                resources={"cpus": 1},
                name="gpu",
                block=gpu_workflow,
            )
        ],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )

    cm = CampaignManager(enable_telemetry=True)
    asyncio.run(asyncio.wait_for(cm.run(spec), timeout=60.0))
    assert seen, "no observation carried task counts"
    assert set(seen[0]) == {"waiting", "submitted", "running"}
    assert max(s["running"] for s in seen) >= 1
