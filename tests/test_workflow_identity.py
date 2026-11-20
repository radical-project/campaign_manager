"""A Python workflow can name itself: context["workflow_id"] / ["campaign_id"].

The same identity a command-line workflow already got as CM_WORKFLOW_ID and
CM_CAMPAIGN_ID.  A workflow needs it to key its own per-instance state on - a
scratch directory, a log prefix, or the seed that makes two instances of one
workflow type independent samples rather than the same draw twice (which is
what IMPRESS-A uses it for, as its campaign's `seed_salt`).
"""

from __future__ import annotations

import asyncio

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    LaunchRequest,
    PolicyResult,
    WorkflowConfig,
)

CAPACITY = {"cpus": 64}

seen: list[tuple[str | None, str | None]] = []


async def sim(reporter, context) -> None:
    seen.append((context.get("workflow_id"), context.get("campaign_id")))
    await reporter.emit(score=1.0)


class LaunchThree(CampaignController):
    def decide(self, obs: dict) -> PolicyResult:
        res = PolicyResult()
        if not self.completed and not obs["running"].get("sim", 0):
            res.launch_requests += [LaunchRequest.make("sim") for _ in range(3)]
        res.terminate = sum(self.completed.values()) >= 3
        return res


def test_a_python_workflow_is_told_which_instance_it_is():
    seen.clear()
    spec = CampaignSpec(
        name="identity",
        budget=CampaignBudget(
            capacity=CAPACITY, max_concurrent_workflows=3, max_concurrency_per_workflow=1
        ),
        controller=LaunchThree(),
        workflows=[
            WorkflowConfig(
                name="sim",
                block=sim,
                spending_limit={"cpu_hours": 1000.0},
                resources={"cpus": 1},
                launch_on_start=False,
            )
        ],
        policy_interval_s=0.02,
        shutdown_timeout_s=5.0,
    )
    asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))

    assert len(seen) == 3
    wf_ids = [w for w, _ in seen]
    # Every instance gets an id, and no two instances of one type share it - which is the
    # property anything seeding per-instance state depends on.
    assert all(wf_ids), f"a workflow was launched without an id: {seen}"
    assert len(set(wf_ids)) == 3, f"instances share an id: {wf_ids}"
    assert all(w.startswith("identity.sim.") for w in wf_ids), wf_ids
    assert {c for _, c in seen} == {"identity"}
