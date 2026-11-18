"""CampaignManager(backend=[...]): several named backends behind one engine.

The first backend is the default route; a task picks another with
``backend="<name>"``.  A Python workflow that declares ``flow`` gets that one
engine, so its tasks can use every route, and AuCaM leaves backends it was
given running when the campaign ends.
"""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

from radical.asyncflow import LocalExecutionBackend

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    WorkflowConfig,
    WorkflowReporter,
)

ran_on: dict[str, str] = {}


async def _thread() -> str:
    return threading.current_thread().name


async def two_routes(reporter: WorkflowReporter, flow) -> None:
    default = flow.function_task(_thread)
    local = flow.function_task(_thread, backend="local")
    ran_on["default"] = await default()
    ran_on["local"] = await local()
    await reporter.emit(score=1.0)


async def _backends():
    compute = await LocalExecutionBackend(
        ThreadPoolExecutor(max_workers=2, thread_name_prefix="compute"), name="compute"
    )
    local = await LocalExecutionBackend(
        ThreadPoolExecutor(max_workers=2, thread_name_prefix="local"), name="local"
    )
    return [compute, local]


def test_a_workflow_routes_tasks_to_named_backends_of_one_engine():
    async def main():
        backends = await _backends()
        out = await CampaignManager(backend=backends).run(
            CampaignSpec(
                name="routes",
                budget=CampaignBudget(
                    capacity={"cpus": 8},
                    max_concurrent_workflows=1,
                    max_concurrency_per_workflow=1,
                ),
                controller=CampaignController(
                    launch=["two_routes"], goal_type="two_routes", goal_n=1
                ),
                workflows=[
                    WorkflowConfig(
                        name="two_routes",
                        block=two_routes,
                        spending_limit={"cpu_hours": 1.0},
                        resources={"cpus": 1},
                    )
                ],
                policy_interval_s=0.05,
                shutdown_timeout_s=5.0,
            )
        )
        return out, backends

    out, backends = asyncio.run(asyncio.wait_for(main(), timeout=30.0))
    assert out.completed == {"two_routes": 1}, out
    assert ran_on["default"].startswith("compute"), "the first backend is the default route"
    assert ran_on["local"].startswith("local")
    # Backends the caller passed are the caller's: AuCaM does not shut them down.
    for b in backends:
        b.executor.submit(lambda: None).result(timeout=5)
