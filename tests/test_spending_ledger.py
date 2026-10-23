"""Integration test: spending admission and accounting through the ledger."""

from __future__ import annotations

import asyncio

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    GlobalBudget,
    WorkflowConfig,
)


async def quick_job() -> None:
    await asyncio.sleep(0.05)


def _spec(name: str, limit_cpu_h: float) -> CampaignSpec:
    return CampaignSpec(
        name=name,
        budget=CampaignBudget(
            max_concurrent_workflows=2,
            max_concurrency_per_workflow=2,
            spending={"cpu_hours": limit_cpu_h},
        ),
        controller=CampaignController(launch=["job"], goal_type="job", goal_n=2),
        workflows=[
            WorkflowConfig(
                name="job",
                block=quick_job,
                # declared 1 cpu_hour per hour of wall time → ~1.4e-5 per 0.05 s run
                declared_spending={"wall_hours": 1.0, "cpu_hours": 1.0},
            )
        ],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )


def test_global_spending_admission_and_outcome() -> None:
    results: dict = {}

    async def driver(cm: CampaignManager) -> None:
        try:
            ok = await cm.submit(_spec("a", 6.0))
            rejected = await cm.submit(_spec("b", 5.0))  # 6 + 5 > 10 → rejected
            results["rejected"] = rejected
            results["outcome"] = await ok.wait()
            # a closed: its tiny actual spending is global; its 6.0 no longer committed
            results["after"] = await cm.submit(_spec("c", 5.0))
            if results["after"].accepted:
                await results["after"].wait()
        finally:
            cm.driver_done()

    async def main() -> CampaignManager:
        cm = CampaignManager(global_budget=GlobalBudget(spending={"cpu_hours": 10.0}))
        cm.expect_driver()
        asyncio.ensure_future(driver(cm))
        await asyncio.wait_for(cm.run(), timeout=30.0)
        return cm

    cm = asyncio.run(main())

    assert results["rejected"].accepted is False
    assert "global spending[cpu_hours]" in results["rejected"].reason
    out = results["outcome"]
    assert out.goal_reached
    assert 0.0 < out.spent.get("cpu_hours", 0.0) < 1e-3
    assert results["after"].accepted
    gs = cm._allocator.global_spent()["cpu_hours"]
    assert gs > out.spent["cpu_hours"]  # a + c both charged
    assert cm._allocator.snapshot()["campaigns"] == {}  # every campaign closed
