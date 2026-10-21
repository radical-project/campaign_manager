"""Tests for features added in the easy-to-do batch:

  B0     — any budget dimension in set_in_flight / _compute_workflow_spent
  B1     — burn entry in campaign_view
  B3     — learned cost estimates replace declared_spending after min_samples
  idle   — campaign exits with reason="campaign_idle" when queue drains
  Gap1   — dep_satisfied persisted in checkpoint and restored before re-push
  R1     — inner-block uid propagation in _on_task_event
  live   — live in-flight update for non-R1 paths (proration and reported)
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
    CampaignManager,
    CampaignSpec,
    LaunchRequest,
    PolicyResult,
    WorkflowConfig,
    WorkflowReporter,
    WorkflowResult,
    checkpoint_path,
    write_checkpoint,
)
from src.aucam.budget import BudgetAllocator
from src.aucam.campaign import _CampaignInstance
from src.aucam.checkpoint import (
    ActiveWorkflowRecord,
    QueuedRequestRecord,
    _from_json,
    _to_json,
)


# ── helpers ───────────────────────────────────────────────────────────────────

async def quick(reporter: WorkflowReporter) -> None:
    await asyncio.sleep(0.05)
    await reporter.emit(score=1.0)


async def _run_campaign(spec: CampaignSpec, timeout: float = 20.0) -> None:
    cm = CampaignManager()
    cm.submit_campaign(spec)
    await asyncio.wait_for(cm.run(), timeout=timeout)


# ── B0: any budget dimension ──────────────────────────────────────────────────

def test_b0_custom_dim_tracked_in_flight() -> None:
    """set_in_flight stores and in_flight() sums arbitrary non-wall_hours dims."""
    alloc = BudgetAllocator()
    alloc.open("c", {"llm_tokens": 1000.0})
    alloc.set_in_flight("c", "wf1", {"llm_tokens": 400.0, "wall_hours": 2.0})
    # wall_hours must be excluded from in-flight
    assert alloc.in_flight("c") == {"llm_tokens": 400.0}


def test_b0_custom_dim_burn_view() -> None:
    """campaign_view burn entry is populated for custom dimensions."""
    alloc = BudgetAllocator()
    alloc.open("c", {"llm_tokens": 1000.0})
    alloc.record("c", {"llm_tokens": 300.0})
    view = alloc.campaign_view("c")
    assert "burn" in view
    assert "llm_tokens" in view["burn"]
    assert abs(view["burn"]["llm_tokens"]["remaining"] - 700.0) < 1e-6
    assert abs(view["burn"]["llm_tokens"]["fraction_left"] - 0.7) < 1e-6


# ── B1: burn entry ─────────────────────────────────────────────────────────────

def test_b1_burn_zero_limit_dimension_excluded() -> None:
    """Dimensions with limit=0 are not included in burn (avoids ZeroDivisionError)."""
    alloc = BudgetAllocator()
    alloc.open("c", {"cpu_hours": 0.0, "gpu_hours": 10.0})
    alloc.record("c", {"gpu_hours": 3.0})
    view = alloc.campaign_view("c")
    assert "cpu_hours" not in view["burn"]
    assert "gpu_hours" in view["burn"]
    assert abs(view["burn"]["gpu_hours"]["fraction_left"] - 0.7) < 1e-6


def test_b1_burn_in_observation() -> None:
    """Controller receives burn in its obs['budget'] dict."""
    burn_seen: list[dict] = []

    class BurnObserver(CampaignController):
        def decide(self, obs: dict) -> PolicyResult:
            burn_seen.append(obs["budget"].get("burn", {}))
            return PolicyResult(terminate=True)

    spec = CampaignSpec(
        name="burn_obs",
        budget=CampaignBudget(
            max_concurrent_workflows=1,
            max_concurrency_per_workflow=1,
            spending={"cpu_hours": 10.0},
        ),
        controller=BurnObserver(),
        workflows=[WorkflowConfig(name="w", block=quick)],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )
    asyncio.run(_run_campaign(spec))
    assert burn_seen, "controller never observed budget"
    assert "cpu_hours" in burn_seen[0]
    assert "remaining" in burn_seen[0]["cpu_hours"]
    assert "fraction_left" in burn_seen[0]["cpu_hours"]
    assert 0.0 <= burn_seen[0]["cpu_hours"]["fraction_left"] <= 1.0


# ── B3: learned cost estimates ────────────────────────────────────────────────

def test_b3_learned_cost_unit() -> None:
    """_learned_cost() returns declared until min_samples reached, then mean."""
    from unittest.mock import MagicMock
    from src.aucam.campaign import _CampaignInstance
    from src.aucam import CampaignSpec, CampaignBudget, CampaignController, WorkflowConfig

    spec = CampaignSpec(
        name="lc",
        budget=CampaignBudget(max_concurrent_workflows=1, max_concurrency_per_workflow=1),
        controller=CampaignController(),
        workflows=[WorkflowConfig(name="wf", block=quick)],
        policy_interval_s=60.0,
        shutdown_timeout_s=5.0,
    )
    alloc = BudgetAllocator()
    flow_mock = MagicMock()
    inst = _CampaignInstance(spec, alloc, flow_mock)

    declared = {"wall_hours": 1.0, "gpu_hours": 2.0}

    # Before min_samples: declared values returned unchanged
    result = inst._learned_cost("wf", declared, min_samples=3)
    assert result["gpu_hours"] == 2.0

    # Feed 2 samples (not enough yet)
    inst._update_type_cost("wf", {"gpu_hours": 1.0})
    inst._update_type_cost("wf", {"gpu_hours": 1.5})
    result = inst._learned_cost("wf", declared, min_samples=3)
    assert result["gpu_hours"] == 2.0  # still declared

    # Third sample — min_samples reached, mean kicks in
    inst._update_type_cost("wf", {"gpu_hours": 2.0})
    result = inst._learned_cost("wf", declared, min_samples=3)
    expected_mean = (1.0 + 1.5 + 2.0) / 3
    assert abs(result["gpu_hours"] - expected_mean) < 1e-9

    # wall_hours should always pass through as declared (not replaced by learned)
    assert result["wall_hours"] == 1.0


# ── campaign idle exit ────────────────────────────────────────────────────────

def test_campaign_idle_exit() -> None:
    """Campaign exits with reason='campaign_idle' when queue empties naturally."""
    outcomes: list = []

    class IdleCtrl(CampaignController):
        """Issues one launch on start, then does nothing (no terminate signal)."""
        _sent = False

        def decide(self, obs: dict) -> PolicyResult:
            reqs = []
            if not self._sent:
                self._sent = True
                reqs = [LaunchRequest.make("w")]
            return PolicyResult(launch_requests=reqs)

    spec = CampaignSpec(
        name="idle_exit",
        budget=CampaignBudget(max_concurrent_workflows=2, max_concurrency_per_workflow=2),
        controller=IdleCtrl(),
        workflows=[WorkflowConfig(name="w", block=quick, launch_on_start=False)],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )

    cm = CampaignManager()
    cm.submit_campaign(spec)
    asyncio.run(asyncio.wait_for(cm.run(), timeout=15.0))
    inst = cm._active_instances["idle_exit"]
    assert inst.exit_reason == "campaign_idle", f"got: {inst.exit_reason}"


# ── Gap 1: dep_satisfied in checkpoint ───────────────────────────────────────

def test_gap1_dep_satisfied_roundtrip() -> None:
    """dep_satisfied persists through CampaignCheckpoint JSON round-trip."""
    ckpt = CampaignCheckpoint(
        campaign_name="dep_test",
        dep_satisfied=["dep_test.source.abc", "dep_test.source.def"],
    )
    d = _to_json(ckpt)
    assert "dep_satisfied" in d
    restored = _from_json(d)
    assert set(restored.dep_satisfied) == {"dep_test.source.abc", "dep_test.source.def"}


def test_gap1_dep_satisfied_missing_in_old_checkpoint() -> None:
    """Checkpoints without dep_satisfied field (old format) default to empty."""
    d = {
        "campaign_name": "old_format",
        "queue": [],
        "active": [],
    }
    restored = _from_json(d)
    assert restored.dep_satisfied == []


def test_gap1_dep_satisfied_restored_before_queue(tmp_path: Path) -> None:
    """After recovery, dep_satisfied is restored so queued target-type requests
    launch immediately without waiting for a redundant source completion."""
    from src.aucam import WorkflowDependency

    completed: list[str] = []

    class DoneCtrl(CampaignController):
        def decide(self, obs: dict) -> PolicyResult:
            running = obs.get("running", {})
            q = obs["queue_snapshot"]
            done = sum(self.completed.values()) >= 1
            return PolicyResult(terminate=done and not running and q.total_queued == 0)

    # Write a checkpoint that has: source already done (dep_satisfied contains
    # the source wf_id), and target queued.
    source_wf_id = "dep_restored.source.fake000"
    ckpt = CampaignCheckpoint(
        campaign_name="dep_restored",
        queue=[
            QueuedRequestRecord(
                workflow_type="target",
                priority="NORMAL",
                enqueued_at=time.time(),
                context={},
                requester="cm:policy",
            )
        ],
        active=[],
        dep_satisfied=[source_wf_id],
        instances_run=1,
        completed=1,
    )
    write_checkpoint(checkpoint_path(str(tmp_path), "dep_restored"), ckpt)

    ctrl = DoneCtrl()
    spec = CampaignSpec(
        name="dep_restored",
        budget=CampaignBudget(max_concurrent_workflows=2, max_concurrency_per_workflow=2),
        controller=ctrl,
        workflows=[
            WorkflowConfig(name="source", block=quick, launch_on_start=False),
            WorkflowConfig(
                name="target",
                block=quick,
                launch_on_start=False,
                # depends on source; satisfied by checkpoint dep_satisfied list
                dependencies=[WorkflowDependency(source="source", target="target")],
            ),
        ],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
        checkpoint_dir=str(tmp_path),
    )

    asyncio.run(asyncio.wait_for(_run_campaign(spec), timeout=15.0))
    # target should have completed (dep was pre-satisfied from checkpoint)
    assert ctrl.completed.get("target", 0) >= 1, (
        f"target did not complete; completions={ctrl.completed}"
    )


# ── R1: inner-block uid propagation ──────────────────────────────────────────

def test_inner_block_uid_registered_in_wf_tags() -> None:
    """TaskCreated (no resources) for an inner block propagates the parent's
    wf_id mapping to the child block uid so tasks inside nested blocks are
    charged to the correct workflow."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from src.aucam.campaign import _CampaignInstance
    from src.aucam import CampaignSpec, CampaignBudget, CampaignController, WorkflowConfig

    spec = CampaignSpec(
        name="inner",
        budget=CampaignBudget(max_concurrent_workflows=1, max_concurrency_per_workflow=1),
        controller=CampaignController(),
        workflows=[WorkflowConfig(name="w", block=quick)],
        policy_interval_s=60.0,
        shutdown_timeout_s=5.0,
    )
    inst = _CampaignInstance(spec, BudgetAllocator(), MagicMock())

    wf_id = "inner.w.abc123"
    outer_block = "block.000001"
    inner_block = "block.000003"

    # Seed the outer block mapping (as _run_as_block → _block would do)
    inst._wf_tags[wf_id] = wf_id
    inst._wf_tags[outer_block] = wf_id

    # Simulate the TaskCreated event asyncflow emits when the inner block is
    # registered inside the outer block's execution context.
    inner_block_created = SimpleNamespace(
        event_type="TaskCreated",
        task_id=inner_block,
        attributes={"asyncflow.workflow_id": outer_block},  # parent uid as tag
    )
    inst._on_task_event(inner_block_created)

    assert inst._wf_tags.get(inner_block) == wf_id, (
        f"inner block not registered; _wf_tags={inst._wf_tags}"
    )

    # A task inside the inner block should now be chargeable.
    task_event = SimpleNamespace(
        event_type="TaskCompleted",
        task_id="task.000004",
        duration_seconds=1.0,
        event_time=None,
        attributes={
            "asyncflow.workflow_id": inner_block,
            "resources": {"cpus": 4.0, "gpus": 0.0},
        },
    )
    inst._on_task_event(task_event)
    assert wf_id in inst._task_level
    assert inst._task_usage.get(wf_id, {}).get("cpu_hours", 0.0) > 0.0


def test_inner_block_cross_campaign_not_registered() -> None:
    """A campaign ignores inner-block TaskCreated events whose parent block
    belongs to a different campaign (parent not in this campaign's _wf_tags)."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from src.aucam.campaign import _CampaignInstance
    from src.aucam import CampaignSpec, CampaignBudget, CampaignController, WorkflowConfig

    spec = CampaignSpec(
        name="other",
        budget=CampaignBudget(max_concurrent_workflows=1, max_concurrency_per_workflow=1),
        controller=CampaignController(),
        workflows=[WorkflowConfig(name="w", block=quick)],
        policy_interval_s=60.0,
        shutdown_timeout_s=5.0,
    )
    inst = _CampaignInstance(spec, BudgetAllocator(), MagicMock())

    # This campaign only knows about its own block
    inst._wf_tags["other.w.xyz"] = "other.w.xyz"
    inst._wf_tags["block.000002"] = "other.w.xyz"

    # Event from a DIFFERENT campaign's inner block (parent = block.000001, unknown here)
    foreign_inner = SimpleNamespace(
        event_type="TaskCreated",
        task_id="block.000003",
        attributes={"asyncflow.workflow_id": "block.000001"},
    )
    inst._on_task_event(foreign_inner)

    # Must not pollute this campaign's _wf_tags
    assert "block.000003" not in inst._wf_tags


# ── live in-flight update for non-R1 paths ───────────────────────────────────

def test_live_inflight_proration_shrinks_over_time() -> None:
    """For a non-R1 workflow, _collect_telemetry() reduces in_flight by proration
    as time passes (declared * remaining_wall_fraction), not stuck at declared."""

    async def slow_wf(reporter):
        await asyncio.sleep(0.4)
        await reporter.emit(score=1.0)

    declared = {"wall_hours": 1.0 / 3600.0, "cpu_hours": 4.0}  # 1s wall, 4 cpu_h
    spec = CampaignSpec(
        name="live_prorate",
        budget=CampaignBudget(
            max_concurrent_workflows=1,
            max_concurrency_per_workflow=1,
            spending={"cpu_hours": 10.0},
        ),
        controller=CampaignController(),
        workflows=[WorkflowConfig(name="w", block=slow_wf,
                                  declared_spending=declared)],
        policy_interval_s=0.1,
        shutdown_timeout_s=5.0,
    )

    inflight_samples: list[float] = []

    class SamplingController(CampaignController):
        def decide(self, obs):
            inflight = obs["budget"]["in_flight"].get("cpu_hours", 0.0)
            inflight_samples.append(inflight)
            return PolicyResult(terminate=sum(self.completed.values()) >= 1)

    spec = CampaignSpec(
        name="live_prorate",
        budget=CampaignBudget(
            max_concurrent_workflows=1,
            max_concurrency_per_workflow=1,
            spending={"cpu_hours": 10.0},
        ),
        controller=SamplingController(),
        workflows=[WorkflowConfig(name="w", block=slow_wf,
                                  declared_spending=declared)],
        policy_interval_s=0.1,
        shutdown_timeout_s=5.0,
    )
    asyncio.run(asyncio.wait_for(_run_campaign(spec), timeout=15.0))

    assert len(inflight_samples) >= 2, "too few policy cycles"
    # First sample: workflow just launched — in_flight near 4.0 cpu_h
    # Later samples: elapsed > 0 → in_flight < 4.0
    assert inflight_samples[0] <= 4.0 + 0.01
    # At least one sample should be strictly less than 4.0 (shrinkage happened)
    assert any(s < 3.9 for s in inflight_samples), (
        f"in_flight never shrank: {inflight_samples}"
    )


def test_live_inflight_reported_path_uses_workflow_report() -> None:
    """For a non-R1 workflow that reports cpu_hours, in_flight shrinks by
    remaining = declared - reported (Path 1), not by wall-time proration."""

    async def reporting_wf(reporter):
        await asyncio.sleep(0.1)
        await reporter.emit(score=1.0, metrics={"cpu_hours": 1.5})
        await asyncio.sleep(0.3)

    declared = {"wall_hours": 1.0 / 3600.0, "cpu_hours": 4.0}

    samples: list[float] = []

    class SamplingController(CampaignController):
        def decide(self, obs):
            samples.append(obs["budget"]["in_flight"].get("cpu_hours", 0.0))
            return PolicyResult(terminate=sum(self.completed.values()) >= 1)

    spec = CampaignSpec(
        name="live_reported",
        budget=CampaignBudget(
            max_concurrent_workflows=1,
            max_concurrency_per_workflow=1,
            spending={"cpu_hours": 10.0},
        ),
        controller=SamplingController(),
        workflows=[WorkflowConfig(name="w", block=reporting_wf,
                                  declared_spending=declared)],
        policy_interval_s=0.1,
        shutdown_timeout_s=5.0,
    )
    asyncio.run(asyncio.wait_for(_run_campaign(spec), timeout=15.0))

    assert len(samples) >= 2
    # After the workflow emits cpu_hours=1.5, remaining = 4.0 - 1.5 = 2.5
    # At least one sample should be ≤ 2.5 + small tolerance
    assert any(s <= 2.6 for s in samples), (
        f"in_flight never reflected reported cpu_hours=1.5: {samples}"
    )
