"""
Checkpointing and recovery tests.

1. Unit — CampaignCheckpoint round-trips through JSON correctly.
2. Unit — write_checkpoint writes an atomic file; read_checkpoint loads it.
3. Integration — checkpoint_dir triggers file creation during a campaign run.
4. Integration — checkpoint content matches campaign state (queue, spent, counters).
5. Recovery — loading a checkpoint restores queue and counters;
              re-enqueued active workflows complete on the recovered campaign.
6. Corruption resilience — read_checkpoint returns None for corrupt files.
"""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import pytest

from src.aucam import (
    CampaignBudget,
    CampaignCheckpoint,
    CampaignController,
    CampaignDecision,
    CampaignManager,
    CampaignSpec,
    LaunchPriority,
    LaunchRequest,
    PolicyResult,
    WorkflowConfig,
    WorkflowReporter,
    WorkflowResult,
    checkpoint_path,
    read_checkpoint,
    write_checkpoint,
)
from src.aucam.checkpoint import (
    ActiveWorkflowRecord,
    QueuedRequestRecord,
    _from_json,
    _to_json,
)


# ── Workers ───────────────────────────────────────────────────────────────────

async def fast_worker(reporter: WorkflowReporter) -> None:
    await asyncio.sleep(0.05)
    await reporter.emit(score=0.8, metrics={"iter": 1})


async def slow_worker(reporter: WorkflowReporter) -> None:
    for i in range(5):
        await asyncio.sleep(0.05)
        await reporter.emit(score=float(i), metrics={"iter": i})


# ── Controller helpers ────────────────────────────────────────────────────────

class _SimpleCtrl(CampaignController):
    def __init__(self, target: int = 1) -> None:
        self.completions = 0
        self.target = target

    def decide(self, obs: dict) -> PolicyResult:
        running = obs.get("running", {})
        q = obs["queue_snapshot"]
        return PolicyResult(
            terminate=(
                self.completions >= self.target
                and not running
                and q.total_queued == 0
            )
        )


async def _run(spec: CampaignSpec) -> None:
    cm = CampaignManager()
    cm.submit_campaign(spec)
    await cm.run()


# ── 1. Unit: CampaignCheckpoint JSON round-trip ───────────────────────────────

def test_checkpoint_json_roundtrip() -> None:
    original = CampaignCheckpoint(
        campaign_name="test_camp",
        queue=[
            QueuedRequestRecord(
                workflow_type="worker_a",
                priority="NORMAL",
                enqueued_at=1234.5,
                context={"key": "val"},
                requester="policy",
            )
        ],
        active=[
            ActiveWorkflowRecord(
                wf_id="test_camp.worker_a.abc123",
                workflow_type="worker_a",
                started_at=1234.0,
                resources={"gpu": 4.0},
            )
        ],
        paused_wf_ids=["test_camp.worker_a.abc123"],
        spent={"gpu_hours": 0.042},
        instances_run=3,
        completed=2,
        cancelled=1,
        failed=0,
        stale_cancels=0,
        walltime_cancels=1,
        policy_cancels=0,
        policy_cycles=7,
    )

    d = _to_json(original)
    restored = _from_json(d)

    assert restored.campaign_name == "test_camp"
    assert len(restored.queue) == 1
    assert restored.queue[0].workflow_type == "worker_a"
    assert restored.queue[0].context == {"key": "val"}
    assert len(restored.active) == 1
    assert restored.active[0].resources == {"gpu": 4.0}
    assert restored.paused_wf_ids == ["test_camp.worker_a.abc123"]
    assert restored.spent == {"gpu_hours": 0.042}
    assert restored.policy_cycles == 7
    assert restored.walltime_cancels == 1


# ── 2. Unit: atomic file write/read ───────────────────────────────────────────

def test_write_read_checkpoint(tmp_path: Path) -> None:
    ckpt = CampaignCheckpoint(
        campaign_name="my_camp",
        spent={"gpu_hours": 1.5},
        policy_cycles=10,
    )
    path = tmp_path / "my_camp.json"
    write_checkpoint(path, ckpt)

    assert path.exists()
    loaded = read_checkpoint(path)
    assert loaded is not None
    assert loaded.campaign_name == "my_camp"
    assert loaded.spent == {"gpu_hours": 1.5}
    assert loaded.policy_cycles == 10


def test_read_checkpoint_missing_file(tmp_path: Path) -> None:
    result = read_checkpoint(tmp_path / "nonexistent.json")
    assert result is None


def test_read_checkpoint_corrupt_file(tmp_path: Path) -> None:
    path = tmp_path / "corrupt.json"
    path.write_text("this is not json {{{{")
    result = read_checkpoint(path)
    assert result is None


def test_checkpoint_path_helper(tmp_path: Path) -> None:
    p = checkpoint_path(str(tmp_path), "fold_sweep")
    assert p == tmp_path / "fold_sweep.json"


# ── 3. Integration: file created during campaign run ─────────────────────────

def test_checkpoint_file_created(tmp_path: Path) -> None:
    ctrl = _SimpleCtrl(target=1)

    def _on(r: WorkflowResult) -> None:
        if r.returncode == 0:
            ctrl.completions += 1

    spec = CampaignSpec(
        name="ckpt_create_test",
        budget=CampaignBudget(max_concurrent_workflows=1, max_concurrency_per_workflow=1),
        controller=ctrl,
        workflows=[
            WorkflowConfig(
                name="fast_worker",
                block=fast_worker,
                launch_on_start=True,
                on_completion=_on,
            ),
        ],
        policy_interval_s=0.1,
        shutdown_timeout_s=5.0,
        checkpoint_dir=str(tmp_path),
    )

    asyncio.run(asyncio.wait_for(_run(spec), timeout=15.0))

    ckpt_file = checkpoint_path(str(tmp_path), "ckpt_create_test")
    assert ckpt_file.exists(), f"Checkpoint file not found at {ckpt_file}"


# ── 4. Integration: checkpoint content matches campaign state ─────────────────

def test_checkpoint_content_counters(tmp_path: Path) -> None:
    ctrl = _SimpleCtrl(target=1)
    completions = [0]

    def _on(r: WorkflowResult) -> None:
        if r.returncode == 0:
            completions[0] += 1
            ctrl.completions += 1

    spec = CampaignSpec(
        name="ckpt_content_test",
        budget=CampaignBudget(max_concurrent_workflows=2, max_concurrency_per_workflow=2),
        controller=ctrl,
        workflows=[
            WorkflowConfig(
                name="fast_worker",
                block=fast_worker,
                launch_on_start=True,
                on_completion=_on,
            ),
        ],
        policy_interval_s=0.1,
        shutdown_timeout_s=5.0,
        checkpoint_dir=str(tmp_path),
    )

    asyncio.run(asyncio.wait_for(_run(spec), timeout=15.0))

    ckpt = read_checkpoint(checkpoint_path(str(tmp_path), "ckpt_content_test"))
    assert ckpt is not None
    assert ckpt.campaign_name == "ckpt_content_test"
    assert ckpt.completed >= 1, f"Expected ≥1 completed in checkpoint; got {ckpt.completed}"
    assert ckpt.policy_cycles >= 1, "Expected ≥1 policy cycle in checkpoint"


# ── 5. Recovery: restore queue + counters → campaign completes ────────────────

def test_recovery_restores_queue_and_completes(tmp_path: Path) -> None:
    """
    Write a checkpoint with one queued request, then start a fresh campaign
    with checkpoint_dir pointing to that file.  The CM restores the queue
    and the workflow completes normally.
    """
    # Build a checkpoint with one queued request (as if a previous run died)
    ckpt = CampaignCheckpoint(
        campaign_name="recovery_test",
        queue=[
            QueuedRequestRecord(
                workflow_type="fast_worker",
                priority="HIGH",
                enqueued_at=time.time(),
                context={"recovered_from": "recovery_test.fast_worker.deadbeef"},
                requester="cm:recovery",
            )
        ],
        active=[],       # previous active workflows already re-enqueued in queue
        spent={"gpu_hours": 0.001},
        instances_run=1,
        completed=0,
        cancelled=1,
        failed=0,
        policy_cycles=5,
    )
    write_checkpoint(checkpoint_path(str(tmp_path), "recovery_test"), ckpt)

    # Now start a fresh campaign — it will load the checkpoint
    ctrl = _SimpleCtrl(target=1)
    completions = [0]

    def _on(r: WorkflowResult) -> None:
        if r.returncode == 0:
            completions[0] += 1
            ctrl.completions += 1

    spec = CampaignSpec(
        name="recovery_test",
        budget=CampaignBudget(max_concurrent_workflows=2, max_concurrency_per_workflow=2),
        controller=ctrl,
        workflows=[
            WorkflowConfig(
                name="fast_worker",
                block=fast_worker,
                launch_on_start=False,   # don't auto-enqueue — queue comes from checkpoint
                on_completion=_on,
            ),
        ],
        policy_interval_s=0.1,
        shutdown_timeout_s=5.0,
        checkpoint_dir=str(tmp_path),
    )

    asyncio.run(asyncio.wait_for(_run(spec), timeout=15.0))

    assert completions[0] >= 1, "Recovered workflow did not complete"

    # Check that counters were merged correctly: previous 5 cycles + at least 1 new
    final_ckpt = read_checkpoint(checkpoint_path(str(tmp_path), "recovery_test"))
    assert final_ckpt is not None
    assert final_ckpt.policy_cycles > 5, (
        f"Expected policy_cycles > 5 after recovery; got {final_ckpt.policy_cycles}"
    )
    # Spent from checkpoint was preserved
    assert "gpu_hours" in final_ckpt.spent


def test_recovery_active_workflows_reenqueued(tmp_path: Path) -> None:
    """
    Active workflows in the checkpoint are re-enqueued with HIGH priority.
    They should complete on the recovered campaign.
    """
    ckpt = CampaignCheckpoint(
        campaign_name="active_recovery_test",
        queue=[],
        active=[
            ActiveWorkflowRecord(
                wf_id="active_recovery_test.fast_worker.deadbeef",
                workflow_type="fast_worker",
                started_at=time.time() - 10,
                resources={},
            )
        ],
        policy_cycles=3,
    )
    write_checkpoint(checkpoint_path(str(tmp_path), "active_recovery_test"), ckpt)

    ctrl = _SimpleCtrl(target=1)

    def _on(r: WorkflowResult) -> None:
        if r.returncode == 0:
            ctrl.completions += 1

    spec = CampaignSpec(
        name="active_recovery_test",
        budget=CampaignBudget(max_concurrent_workflows=2, max_concurrency_per_workflow=2),
        controller=ctrl,
        workflows=[
            WorkflowConfig(
                name="fast_worker",
                block=fast_worker,
                launch_on_start=False,
                on_completion=_on,
            ),
        ],
        policy_interval_s=0.1,
        shutdown_timeout_s=5.0,
        checkpoint_dir=str(tmp_path),
    )

    asyncio.run(asyncio.wait_for(_run(spec), timeout=15.0))
    assert ctrl.completions >= 1, "Re-enqueued active workflow did not complete"
