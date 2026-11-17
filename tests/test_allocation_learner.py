"""Allocation design note, step 4: the demo's example learner picks, per
workflow type, the spending factor with the most progress per hour."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from src.aucam import Allocation, AllocationRecord, LaunchRequest, SizingContext

DEMO = Path(__file__).resolve().parents[1] / "campaigns" / "demo_campaign"


def _learner_cls():
    spec = importlib.util.spec_from_file_location("demo_scheduler", DEMO / "scheduler.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.AllocationLearner


def _ctx(records) -> SizingContext:
    return SizingContext(
        proposal=Allocation(),
        learned_cost={},
        remaining={},
        queued=1,
        requested=Allocation(),
        start=Allocation(spending_limit={"cpu_hours": 1.0}),
        allocations=records,
    )


def test_tries_every_factor_then_keeps_the_best(monkeypatch):
    learner = _learner_cls()(types=("sim",))
    req = LaunchRequest.make("sim")
    records = []
    # Each factor's workflows: progress 1 in `wall` seconds.
    wall = {0.5: 100.0, 1.0: 20.0, 1.5: 30.0}
    for _ in range(6):  # 3 factors × MIN_TRIALS
        alloc = learner.size(req, _ctx(records))
        limit = alloc.spending_limit["cpu_hours"]
        factor = round(limit, 6)
        records.append(
            AllocationRecord(
                workflow_id=f"w{len(records)}",
                workflow_type="sim",
                outcome="completed",
                wall_time_s=wall[factor],
                spending_limit={"cpu_hours": limit},
                credit=1.0,
            )
        )
    assert sorted({round(r.spending_limit["cpu_hours"], 6) for r in records}) == [0.5, 1.0, 1.5]
    monkeypatch.setattr(learner, "EPSILON", 0.0)
    assert learner.size(req, _ctx(records)).spending_limit["cpu_hours"] == 1.0  # fastest progress


def test_pending_records_and_other_types_are_ignored():
    learner = _learner_cls()(types=("sim",))
    other = learner.size(LaunchRequest.make("analysis"), _ctx([]))
    assert other.spending_limit["cpu_hours"] == 1.0  # not learned for: default amount
    rec = AllocationRecord(
        workflow_id="w0",
        workflow_type="sim",
        outcome="completed",
        wall_time_s=1.0,
        spending_limit={"cpu_hours": 0.5},
        pending=True,
    )
    learner._issued[("sim", 0.5)] = 0.5
    assert learner._rates("sim", _ctx([rec]))[0.5] == []


def test_the_controller_s_amount_is_not_scaled():
    learner = _learner_cls()(types=("sim",))
    ctx = _ctx([])
    ctx.requested = Allocation(spending_limit={"cpu_hours": 3.0})
    assert learner.size(LaunchRequest.make("sim"), ctx).spending_limit["cpu_hours"] == 3.0
