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
        budget=CampaignBudget(max_concurrent_workflows=2, max_concurrency_per_workflow=1),
        controller=CampaignController(goal_type="cmd", goal_n=1),
        workflows=[
            WorkflowConfig(
                name="gpu",
                block=gpu_workflow,
                # deliberately wrong declaration: task-level accounting must win
                declared_spending={"wall_hours": 1.0, "gpu_hours": 100.0},
            ),
            WorkflowConfig(
                name="cmd",
                launch_cmd=[sys.executable, "-c", f"import time; time.sleep({SLEEP_S})"],
                task_description={"ranks": 1, "cores_per_rank": 4},
                declared_spending={"wall_hours": 1.0, "cpu_hours": 100.0},
            ),
        ],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )
    outcome: dict = {}

    async def main() -> None:
        cm = CampaignManager(enable_telemetry=True)
        cm.expect_driver()

        async def driver() -> None:
            try:
                res = await cm.submit(spec)
                outcome["o"] = await res.wait()
            finally:
                cm.driver_done()

        asyncio.ensure_future(driver())
        await asyncio.wait_for(cm.run(), timeout=60.0)

    asyncio.run(main())
    spent = outcome["o"].spent
    # gpu: 2 GPUs × ~0.3 s;  cmd: 4 cores × ~0.3 s  (generous bounds for CI jitter)
    assert 2 * SLEEP_S / 3600 * 0.8 <= spent.get("gpu_hours", 0.0) <= 2 * 3.0 / 3600
    assert 4 * SLEEP_S / 3600 * 0.8 <= spent.get("cpu_hours", 0.0) <= 4 * 3.0 / 3600
