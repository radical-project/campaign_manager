"""Unit tests for the BudgetAllocator spending ledger and WorkflowQueue (no AsyncFlow)."""

import time

import pytest

from src.aucam import BudgetAllocator, LaunchPriority, LaunchRequest, WorkflowQueue

# ── BudgetAllocator (spending ledger) ───────────────────────────────────────


def test_ledger_admission_against_global_cap() -> None:
    alloc = BudgetAllocator()
    cap = {"gpu_hours": 100.0}
    assert alloc.admit("camp-a", {"gpu_hours": 60.0}, cap) is None
    assert alloc.admit("camp-b", {"gpu_hours": 40.0}, cap) is None
    reason = alloc.admit("camp-c", {"gpu_hours": 1.0}, cap)
    assert reason is not None and "global spending[gpu_hours]" in reason
    assert alloc.committed("gpu_hours") == pytest.approx(100.0)


def test_ledger_close_moves_actual_spending_to_global() -> None:
    alloc = BudgetAllocator()
    cap = {"gpu_hours": 100.0}
    alloc.admit("camp-a", {"gpu_hours": 60.0}, cap)
    alloc.admit("camp-b", {"gpu_hours": 40.0}, cap)
    alloc.record("camp-a", {"gpu_hours": 10.0})
    assert alloc.close("camp-a") == {"gpu_hours": 10.0}
    assert alloc.global_spent() == {"gpu_hours": 10.0}
    # 10 spent + 40 committed (camp-b) → 50 left
    assert alloc.global_remaining("gpu_hours", 100.0) == pytest.approx(50.0)
    assert alloc.admit("camp-c", {"gpu_hours": 50.0}, cap) is None
    assert alloc.admit("camp-d", {"gpu_hours": 0.5}, cap) is not None


def test_ledger_in_flight_counts_toward_campaign_limit() -> None:
    alloc = BudgetAllocator()
    alloc.open("c1", {"gpu_hours": 10.0})
    alloc.record("c1", {"gpu_hours": 6.0})
    assert alloc.limit_reached("c1") is None
    alloc.set_in_flight("c1", "wf1", {"wall_hours": 1.0, "gpu_hours": 4.0})
    hit = alloc.limit_reached("c1")
    assert hit is not None and hit[0] == "gpu_hours"
    assert alloc.in_flight("c1") == {"gpu_hours": 4.0}
    # The workflow finishes having used less than declared.
    alloc.clear_in_flight("c1", "wf1")
    alloc.record("c1", {"gpu_hours": 1.5})
    assert alloc.limit_reached("c1") is None
    assert alloc.spent("c1") == {"gpu_hours": 7.5}


def test_ledger_snapshot_and_late_charge() -> None:
    alloc = BudgetAllocator()
    alloc.open("c1", {"cpu_hours": 5.0})
    alloc.set_in_flight("c1", "wf1", {"cpu_hours": 2.0})
    snap = alloc.snapshot()
    assert snap["campaigns"]["c1"] == {
        "limits": {"cpu_hours": 5.0},
        "spent": {},
        "in_flight": {"cpu_hours": 2.0},
    }
    alloc.close("c1")
    # A workflow that finishes after its campaign was closed is charged globally.
    alloc.record("c1", {"cpu_hours": 1.0})
    assert alloc.global_spent() == {"cpu_hours": 1.0}
    assert "c1" not in alloc.snapshot()["campaigns"]


# ── WorkflowQueue ────────────────────────────────────────────────────────────


def _req(wf_type: str, priority: LaunchPriority = LaunchPriority.NORMAL) -> LaunchRequest:
    return LaunchRequest(
        workflow_type=wf_type,
        priority=priority,
        enqueued_at=time.time(),
        context={},
        requester="test",
    )


def test_queue_priority_order() -> None:
    q = WorkflowQueue()
    q.push(_req("low", LaunchPriority.LOW))
    q.push(_req("high", LaunchPriority.HIGH))
    q.push(_req("normal", LaunchPriority.NORMAL))
    q.push(_req("critical", LaunchPriority.CRITICAL))

    assert q.pop().workflow_type == "critical"
    assert q.pop().workflow_type == "high"
    assert q.pop().workflow_type == "normal"
    assert q.pop().workflow_type == "low"


def test_queue_fifo_within_same_priority() -> None:
    q = WorkflowQueue()
    t0 = time.time()
    q.push(LaunchRequest("first", LaunchPriority.NORMAL, t0, {}, "t"))
    q.push(LaunchRequest("second", LaunchPriority.NORMAL, t0 + 0.001, {}, "t"))

    assert q.pop().workflow_type == "first"
    assert q.pop().workflow_type == "second"


def test_queue_remove_by_type() -> None:
    q = WorkflowQueue()
    for _ in range(3):
        q.push(_req("worker"))
    q.push(_req("other"))

    removed = q.remove_by_type("worker", 2)
    assert removed == 2
    assert len(q) == 2

    remaining_types = []
    while q:
        remaining_types.append(q.pop().workflow_type)
    assert remaining_types.count("worker") == 1
    assert remaining_types.count("other") == 1


def test_queue_snapshot() -> None:
    q = WorkflowQueue()
    q.push(_req("a", LaunchPriority.HIGH))
    q.push(_req("b", LaunchPriority.NORMAL))
    q.push(_req("a", LaunchPriority.NORMAL))

    snap = q.snapshot()
    assert snap.total_queued == 3
    assert snap.by_type["a"] == 2
    assert snap.by_type["b"] == 1


def test_queue_aging(monkeypatch) -> None:
    q = WorkflowQueue(age_threshold_s=1.0)
    old_time = time.time() - 2.0
    q.push(LaunchRequest("worker", LaunchPriority.NORMAL, old_time, {}, "t"))

    # After aging, NORMAL → HIGH, so it should come out before a freshly pushed NORMAL.
    q.push(_req("fresh", LaunchPriority.NORMAL))

    first = q.pop()
    assert first.workflow_type == "worker", "aged request should be promoted to HIGH"
