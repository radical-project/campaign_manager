"""Integration test: spending accounting through the ledger, returned in the outcome."""

from __future__ import annotations

import asyncio

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    WorkflowConfig,
)

CAPACITY = {"cpus": 1024, "gpus": 64}  # the allocation tests run on


async def quick_job() -> None:
    await asyncio.sleep(0.05)


def _spec(name: str, limit_cpu_h: float) -> CampaignSpec:
    return CampaignSpec(
        name=name,
        budget=CampaignBudget(
            capacity=CAPACITY,
            max_concurrent_workflows=2,
            max_concurrency_per_workflow=2,
            spending={"cpu_hours": limit_cpu_h},
        ),
        controller=CampaignController(launch=["job"], goal_type="job", goal_n=2),
        workflows=[
            WorkflowConfig(
                name="job",
                block=quick_job,
                # 1 CPU, reporting nothing: charged 1 CPU × run time (~1.4e-5 per 0.05 s)
                spending_limit={"cpu_hours": 1.0},
                resources={"cpus": 1},
            )
        ],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )


def test_spending_is_charged_and_returned_in_outcome() -> None:
    out = asyncio.run(asyncio.wait_for(CampaignManager().run(_spec("a", 6.0)), timeout=30.0))
    assert out.goal_reached
    assert out.exit_reason == "goals_satisfied"
    assert 0.0 < out.spent.get("cpu_hours", 0.0) < 1e-3
