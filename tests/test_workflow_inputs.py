"""Python workflows get their inputs as arguments, never via CM's process environment."""
from __future__ import annotations

import asyncio
import os

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    LaunchPriority,
    LaunchRequest,
    PolicyResult,
    WorkflowConfig,
)

seen: list[dict] = []


async def ctx_worker(reporter, context=None) -> None:
    # Overlap the two instances so a shared-environment bug would show up.
    await asyncio.sleep(0.2)
    seen.append({
        "wf_id": reporter.wf_id,
        "context": dict(context or {}),
        "env_wf_id": os.environ.get("CM_WORKFLOW_ID"),
    })
    await reporter.emit(score=1.0)


class TwoLaunches(CampaignController):
    def __init__(self) -> None:
        super().__init__(goal_type="w", goal_n=2)
        self._sent = False

    def decide(self, obs: dict) -> PolicyResult:
        reqs = []
        if not self._sent:
            self._sent = True
            reqs = [
                LaunchRequest.make("w", LaunchPriority.NORMAL, context={"seed": i})
                for i in (1, 2)
            ]
        return PolicyResult(launch_requests=reqs, terminate=self.is_goal_reached())


def test_python_workflow_gets_context_argument_not_env() -> None:
    seen.clear()
    spec = CampaignSpec(
        name="inputs",
        budget=CampaignBudget(max_concurrent_workflows=2, max_concurrency_per_workflow=2),
        controller=TwoLaunches(),
        workflows=[WorkflowConfig(name="w", block=ctx_worker, launch_on_start=False)],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )
    cm = CampaignManager()
    cm.submit_campaign(spec)
    asyncio.run(asyncio.wait_for(cm.run(), timeout=30.0))

    assert sorted(s["context"]["seed"] for s in seen) == [1, 2]
    assert len({s["wf_id"] for s in seen}) == 2
    # CM no longer writes workflow variables into its own (shared) environment.
    assert all(s["env_wf_id"] is None for s in seen)
