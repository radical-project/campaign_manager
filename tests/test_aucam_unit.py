"""Unit tests for BudgetAllocator and WorkflowQueue (no AsyncFlow)."""

import time

import pytest

from src.aucam import BudgetAllocator, LaunchPriority, LaunchRequest, WorkflowQueue


# ── BudgetAllocator ──────────────────────────────────────────────────────────

def test_budget_reserve_and_release() -> None:
    alloc = BudgetAllocator({"gpu_h": 100.0})
    assert alloc.reserve("camp-a", {"gpu_h": 60.0})
    assert alloc.reserve("camp-b", {"gpu_h": 40.0})
    assert not alloc.reserve("camp-c", {"gpu_h": 1.0})

    alloc.release("camp-a")
    assert alloc.reserve("camp-c", {"gpu_h": 60.0})


def test_budget_headroom() -> None:
    alloc = BudgetAllocator({"gpu_h": 100.0})
    alloc.reserve("c1", {"gpu_h": 80.0})
    alloc.charge("c1", {"gpu_h": 20.0})
    hw = alloc.headroom("c1")
    assert hw["gpu_h"] == pytest.approx(60.0)


def test_budget_fits() -> None:
    alloc = BudgetAllocator({"gpu_h": 100.0})
    alloc.reserve("c1", {"gpu_h": 50.0})
    alloc.charge("c1", {"gpu_h": 45.0})
    assert alloc.fits("c1", {"gpu_h": 5.0})
    assert not alloc.fits("c1", {"gpu_h": 6.0})


def test_budget_empty_resources_always_fits() -> None:
    alloc = BudgetAllocator({"gpu_h": 100.0})
    alloc.reserve("c1", {"gpu_h": 50.0})
    alloc.charge("c1", {"gpu_h": 50.0})
    assert alloc.fits("c1", {})


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
