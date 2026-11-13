"""U2 — cost-aware bandit reward, the value() hook for sizing, and learned
resource sizes."""

from __future__ import annotations

import asyncio

from src.aucam import (
    Allocation,
    BanditSchedulingPolicy,
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    LaunchRequest,
    PolicyResult,
    SchedulingPolicy,
    SizingContext,
    WorkflowConfig,
    WorkflowReporter,
    WorkflowResult,
)

CAPACITY = {"cpus": 1024, "gpus": 64}  # the allocation tests run on


def _result(wf_type: str, cpu: float, rc: int = 0) -> WorkflowResult:
    return WorkflowResult(
        workflow_id=f"c.{wf_type}.x",
        workflow_type=wf_type,
        returncode=rc,
        wall_time_s=1.0,
        spent={"cpu_hours": cpu},
    )


def _feed(policy, outcomes):
    for wf_type, cpu in outcomes:
        policy.on_outcome(LaunchRequest.make(wf_type), _result(wf_type, cpu))
    return {k: v["mean"] for k, v in policy.arm_stats().items()}


def test_cost_aware_reward_prefers_the_cheaper_type():
    outcomes = [("cheap", 1.0), ("pricey", 3.0)] * 5
    plain = _feed(BanditSchedulingPolicy(), outcomes)
    assert plain["cheap"] == plain["pricey"] == 1.0
    aware = _feed(BanditSchedulingPolicy(cost_aware=True), outcomes)
    assert aware["cheap"] > aware["pricey"]
    assert 0.0 < aware["pricey"] < aware["cheap"] <= 1.0
    # A failure still earns 0, whatever it cost.
    p = BanditSchedulingPolicy(cost_aware=True)
    p.on_outcome(LaunchRequest.make("x"), _result("x", 0.1, rc=1))
    assert p.arm_stats()["x"]["mean"] == 0.0


def _ctx(**kw) -> SizingContext:
    base = dict(proposal=Allocation(), learned_cost={}, remaining={}, queued=1)
    base.update(kw)
    return SizingContext(**base)


def test_value_hook_scales_the_learned_amount():
    class Generous(SchedulingPolicy):
        def value(self, workflow_type, ctx):
            return 2.0 if workflow_type == "good" else 1.0

    req_good = LaunchRequest.make("good")
    req_other = LaunchRequest.make("other")
    ctx = _ctx(learned_cost={"cpu_hours": 2.0})
    assert SchedulingPolicy().size(req_good, ctx).spending_limit["cpu_hours"] == 2.5
    assert Generous().size(req_good, ctx).spending_limit["cpu_hours"] == 5.0
    assert Generous().size(req_other, ctx).spending_limit["cpu_hours"] == 2.5
    # Still subject to the remaining budget.
    short = _ctx(learned_cost={"cpu_hours": 2.0}, remaining={"cpu_hours": 3.0})
    assert Generous().size(req_good, short) is None  # 5.0 does not fit 3.0


def test_resources_use_learned_peak_use():
    req = LaunchRequest.make("sim")
    start = Allocation(resources={"cpus": 16})
    assert (
        SchedulingPolicy().size(req, _ctx(start=start)).resources["cpus"] == 16
    )  # not learned yet
    alloc = SchedulingPolicy().size(req, _ctx(start=start, learned_resources={"cpus": 2.4}))
    assert alloc.resources["cpus"] == 3  # ceil(2.4 × 1.25)


async def worker(reporter: WorkflowReporter, context: dict) -> None:
    await asyncio.sleep(0.03)
    await reporter.emit(score=1.0, metrics={"cpus_in_use": 3})


class Collect(CampaignController):
    def __init__(self, n: int) -> None:
        super().__init__(launch=["sim"])
        self.n = n
        self.results = []

    def on_complete(self, result) -> None:
        super().on_complete(result)
        self.results.append(result)

    def decide(self, obs: dict) -> PolicyResult:
        res = super().decide(obs)
        res.terminate = len(self.results) >= self.n
        return res


def test_campaign_learns_resource_size_from_reports():
    ctl = Collect(5)
    spec = CampaignSpec(
        name="learnres",
        budget=CampaignBudget(
            capacity=CAPACITY, max_concurrent_workflows=1, max_concurrency_per_workflow=1
        ),
        controller=ctl,
        workflows=[
            WorkflowConfig(
                spending_limit={"cpu_hours": 1000.0},
                name="sim",
                block=worker,
                resources={"cpus": 16},
            )
        ],
        policy_interval_s=0.02,
        shutdown_timeout_s=5.0,
    )
    asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))
    cpus = [r.resources["cpus"] for r in ctl.results]
    assert cpus[:3] == [16, 16, 16]
    assert cpus[3:] == [4, 4]  # ceil(3 × 1.25)
