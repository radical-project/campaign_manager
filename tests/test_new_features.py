"""Tests for budget, idle-exit and checkpoint features (IDs as in docs/known-gaps.md):

B0     — any budget dimension in set_in_flight / _compute_workflow_spent
B1     — burn entry in campaign_view
B3     — learned cost estimates replace declared_spending after min_samples
idle   — campaign exits with reason="campaign_idle" when queue drains
R2     — dep_satisfied persisted in checkpoint and restored before re-push
tasks  — inner-block uid propagation in _on_task_event (per-task accounting)
live   — live in-flight update for workflows not charged per task
"""

from __future__ import annotations

import asyncio
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

    from src.aucam import CampaignBudget, CampaignController, CampaignSpec, WorkflowConfig
    from src.aucam.campaign import _CampaignInstance

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


def _idle_spec(name: str, **kw) -> CampaignSpec:
    class OneShot(CampaignController):
        _sent = False

        def decide(self, obs: dict) -> PolicyResult:
            if not self._sent:
                self._sent = True
                return PolicyResult(launch_requests=[LaunchRequest.make("w")])
            return PolicyResult()

    return CampaignSpec(
        name=name,
        budget=CampaignBudget(max_concurrent_workflows=1, max_concurrency_per_workflow=1),
        controller=OneShot(),
        workflows=[WorkflowConfig(name="w", block=quick, launch_on_start=False)],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
        **kw,
    )


def test_idle_exit_s_values() -> None:
    """idle_exit_s: 'auto' = max(5 s, policy_interval_s); a number is used as
    is; None disables; negative values are rejected."""
    assert _idle_spec("a").idle_exit_after() == 5.0
    assert (
        _idle_spec("b", idle_exit_s="auto")
        .model_copy(update={"policy_interval_s": 30.0})
        .idle_exit_after()
        == 30.0
    )
    assert _idle_spec("c", idle_exit_s=0.5).idle_exit_after() == 0.5
    assert _idle_spec("d", idle_exit_s=None).idle_exit_after() is None
    with pytest.raises(ValueError):
        _idle_spec("e", idle_exit_s=-1.0)


def test_idle_exit_s_custom_value_ends_campaign() -> None:
    """With idle_exit_s=0.5 an idle campaign ends well before the 5 s default."""
    cm = CampaignManager()
    cm.submit_campaign(_idle_spec("idle_custom", idle_exit_s=0.5))
    t0 = time.time()
    asyncio.run(asyncio.wait_for(cm.run(), timeout=15.0))
    assert cm._active_instances["idle_custom"].exit_reason == "campaign_idle"
    assert time.time() - t0 < 4.5


def test_idle_exit_s_none_keeps_campaign_waiting() -> None:
    """With idle_exit_s=None an idle campaign keeps waiting until stopped."""

    async def run() -> str:
        cm = CampaignManager()
        cm.submit_campaign(_idle_spec("idle_never", idle_exit_s=None))
        task = asyncio.ensure_future(cm.run())
        await asyncio.sleep(6.5)  # longer than the 5 s 'auto' window
        inst = cm._active_instances["idle_never"]
        still_running = not task.done() and inst.exit_reason == "unknown"
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
        return "waiting" if still_running else inst.exit_reason

    assert asyncio.run(asyncio.wait_for(run(), timeout=20.0)) == "waiting"


# ── R2: dep_satisfied in checkpoint ──────────────────────────────────────────


def test_dep_satisfied_roundtrip() -> None:
    """dep_satisfied persists through CampaignCheckpoint JSON round-trip."""
    ckpt = CampaignCheckpoint(
        campaign_name="dep_test",
        dep_satisfied=["dep_test.source.abc", "dep_test.source.def"],
    )
    d = _to_json(ckpt)
    assert "dep_satisfied" in d
    restored = _from_json(d)
    assert set(restored.dep_satisfied) == {"dep_test.source.abc", "dep_test.source.def"}


def test_dep_satisfied_missing_in_old_checkpoint() -> None:
    """Checkpoints without dep_satisfied field (old format) default to empty."""
    d = {
        "campaign_name": "old_format",
        "queue": [],
        "active": [],
    }
    restored = _from_json(d)
    assert restored.dep_satisfied == []


def test_dep_satisfied_restored_before_queue(tmp_path: Path) -> None:
    """After recovery, dep_satisfied is restored so queued target-type requests
    launch immediately without waiting for a redundant source completion."""
    from src.aucam import WorkflowDependency

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


# ── Per-task accounting: inner-block uid propagation────────────────────────────────────────


def test_inner_block_uid_registered_in_wf_tags() -> None:
    """TaskCreated (no resources) for an inner block propagates the parent's
    wf_id mapping to the child block uid so tasks inside nested blocks are
    charged to the correct workflow."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from src.aucam import CampaignBudget, CampaignController, CampaignSpec, WorkflowConfig
    from src.aucam.campaign import _CampaignInstance

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

    from src.aucam import CampaignBudget, CampaignController, CampaignSpec, WorkflowConfig
    from src.aucam.campaign import _CampaignInstance

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


# ── live in-flight update, not charged per task─────────────────────────────────


def test_live_inflight_proration_holds_full_cost_and_grows_on_overrun() -> None:
    """A workflow not charged per task is charged only when it ends, so while it runs the
    ledger must hold its full expected cost: in_flight never drops below the
    launch estimate, and grows when the workflow runs past its declared wall
    time (declared x elapsed / declared_wall)."""

    async def slow_wf(reporter):
        await asyncio.sleep(0.5)
        await reporter.emit(score=1.0)

    # declared wall 0.1 s, actual ~0.5 s -> overrun up to ~5x
    declared = {"wall_hours": 0.1 / 3600.0, "cpu_hours": 4.0}
    samples: list[tuple[float, float]] = []

    class SamplingController(CampaignController):
        def decide(self, obs):
            b = obs["budget"]
            if obs.get("running"):
                samples.append(
                    (b["spent"].get("cpu_hours", 0.0), b["in_flight"].get("cpu_hours", 0.0))
                )
            return PolicyResult(terminate=sum(self.completed.values()) >= 1)

    spec = CampaignSpec(
        name="live_prorate",
        budget=CampaignBudget(
            max_concurrent_workflows=1,
            max_concurrency_per_workflow=1,
            spending={"cpu_hours": 100.0},
        ),
        controller=SamplingController(),
        workflows=[WorkflowConfig(name="w", block=slow_wf, declared_spending=declared)],
        policy_interval_s=0.1,
        shutdown_timeout_s=5.0,
    )
    asyncio.run(asyncio.wait_for(_run_campaign(spec), timeout=15.0))

    assert len(samples) >= 2, "too few policy cycles"
    for spent, inflight in samples:
        assert spent == 0.0
        assert inflight >= 4.0 - 1e-9, f"in_flight dropped below estimate: {samples}"
    assert any(inflight > 4.5 for _, inflight in samples), (
        f"in_flight did not grow on overrun: {samples}"
    )


def test_live_inflight_reported_path_raises_estimate() -> None:
    """For a workflow not charged per task that reports cumulative cpu_hours, in_flight is
    max(estimate, reported): a report below the estimate leaves it unchanged,
    a report above it raises in_flight to the reported value."""

    async def reporting_wf(reporter):
        await asyncio.sleep(0.1)
        await reporter.emit(score=0.5, metrics={"cpu_hours": 1.5})
        await asyncio.sleep(0.3)
        await reporter.emit(score=1.0, metrics={"cpu_hours": 6.0})
        await asyncio.sleep(0.3)

    declared = {"wall_hours": 1.0, "cpu_hours": 4.0}
    samples: list[float] = []

    class SamplingController(CampaignController):
        def decide(self, obs):
            if obs.get("running"):
                samples.append(obs["budget"]["in_flight"].get("cpu_hours", 0.0))
            return PolicyResult(terminate=sum(self.completed.values()) >= 1)

    spec = CampaignSpec(
        name="live_reported",
        budget=CampaignBudget(
            max_concurrent_workflows=1,
            max_concurrency_per_workflow=1,
            spending={"cpu_hours": 100.0},
        ),
        controller=SamplingController(),
        workflows=[WorkflowConfig(name="w", block=reporting_wf, declared_spending=declared)],
        policy_interval_s=0.1,
        shutdown_timeout_s=5.0,
    )
    asyncio.run(asyncio.wait_for(_run_campaign(spec), timeout=15.0))

    assert len(samples) >= 2
    assert min(samples) >= 4.0 - 1e-9, f"in_flight dropped below estimate: {samples}"
    assert any(abs(s - 6.0) < 1e-6 for s in samples), (
        f"in_flight never reflected reported cpu_hours=6.0: {samples}"
    )


# ── fixes: task-level edge cases and learned costs ────────────────────────────


def _bare_instance(name: str = "fx", wf_type: str = "w") -> _CampaignInstance:
    from unittest.mock import MagicMock

    spec = CampaignSpec(
        name=name,
        budget=CampaignBudget(
            max_concurrent_workflows=2,
            max_concurrency_per_workflow=2,
            spending={"cpu_hours": 100.0},
        ),
        controller=CampaignController(),
        workflows=[
            WorkflowConfig(
                name=wf_type, block=quick, declared_spending={"wall_hours": 1.0, "cpu_hours": 8.0}
            )
        ],
        policy_interval_s=60.0,
        shutdown_timeout_s=5.0,
    )
    alloc = BudgetAllocator()
    alloc.open(name, {"cpu_hours": 100.0})
    return _CampaignInstance(spec, alloc, MagicMock())


def test_task_level_workflow_without_charges_finishes() -> None:
    """A workflow marked task-level (TaskCreated with resources) that ends
    before any task is charged must still produce a WorkflowResult (no KeyError
    on _task_usage), with empty spending."""
    results: list[WorkflowResult] = []

    async def cancelled_mid_task(reporter):
        await asyncio.sleep(0.05)
        raise asyncio.CancelledError

    class Ctrl(CampaignController):
        def on_complete(self, result):
            super().on_complete(result)
            results.append(result)

        def decide(self, obs):
            return PolicyResult(terminate=len(results) >= 1)

    spec = CampaignSpec(
        name="tl_nocharge",
        budget=CampaignBudget(max_concurrent_workflows=1, max_concurrency_per_workflow=1),
        controller=Ctrl(),
        workflows=[WorkflowConfig(name="w", block=cancelled_mid_task)],
        policy_interval_s=0.1,
        shutdown_timeout_s=5.0,
    )

    orig_launch = _CampaignInstance._launch

    def launch_and_mark(self, request):
        orig_launch(self, request)
        for wf_id in self._active:
            self._task_level.add(wf_id)  # as a TaskCreated with resources would

    _CampaignInstance._launch = launch_and_mark
    try:
        asyncio.run(asyncio.wait_for(_run_campaign(spec), timeout=15.0))
    finally:
        _CampaignInstance._launch = orig_launch

    assert len(results) == 1
    assert results[0].spent == {}


def test_task_level_inflight_uses_launch_estimate() -> None:
    """For a task-level workflow, in_flight = launch estimate - charged, using
    the learned estimate (not declared_spending) once one exists."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    inst = _bare_instance()
    wf_id = "fx.w.1"
    inst._wf_tags[wf_id] = wf_id
    inst._active[wf_id] = MagicMock(declared_spending={"wall_hours": 1.0, "cpu_hours": 8.0})
    inst._estimates[wf_id] = {"wall_hours": 1.0, "cpu_hours": 2.0}  # learned
    inst._allocator.set_in_flight("fx", wf_id, inst._estimates[wf_id])

    ev = SimpleNamespace(
        event_type="TaskCompleted",
        task_id="task.000001",
        duration_seconds=900.0,
        event_time=None,
        attributes={"asyncflow.workflow_id": wf_id, "resources": {"cpus": 4.0, "gpus": 0.0}},
    )
    inst._on_task_event(ev)  # charges 4 cpus x 0.25 h = 1.0 cpu_h
    assert abs(inst._allocator.spent("fx")["cpu_hours"] - 1.0) < 1e-9
    assert abs(inst._allocator.in_flight("fx")["cpu_hours"] - 1.0) < 1e-9  # 2.0 - 1.0


def test_learned_cost_only_from_successful_runs() -> None:
    """Cancelled and failed runs do not update the per-type cost model; for a
    task-level workflow the model gets its actual per-task charges."""
    results: list[WorkflowResult] = []

    async def ok(reporter):
        await asyncio.sleep(0.05)
        await reporter.emit(score=1.0)

    async def boom(reporter):
        await asyncio.sleep(0.05)
        raise RuntimeError("fail")

    class Ctrl(CampaignController):
        def __init__(self):
            super().__init__()
            self._sent = False

        def on_complete(self, result):
            super().on_complete(result)
            results.append(result)

        def decide(self, obs):
            if not self._sent:
                self._sent = True
                return PolicyResult(
                    launch_requests=[LaunchRequest.make("ok"), LaunchRequest.make("boom")]
                )
            return PolicyResult(terminate=len(results) >= 2)

    decl = {"wall_hours": 1.0, "cpu_hours": 3.0}
    spec = CampaignSpec(
        name="lc_rc",
        budget=CampaignBudget(max_concurrent_workflows=2, max_concurrency_per_workflow=1),
        controller=Ctrl(),
        workflows=[
            WorkflowConfig(name="ok", block=ok, declared_spending=decl, launch_on_start=False),
            WorkflowConfig(name="boom", block=boom, declared_spending=decl, launch_on_start=False),
        ],
        policy_interval_s=0.1,
        shutdown_timeout_s=5.0,
    )
    captured: dict = {}
    orig_shutdown = _CampaignInstance._shutdown

    async def capture(self, reason="external"):
        captured.update(self._type_cost_stats)
        return await orig_shutdown(self, reason=reason)

    _CampaignInstance._shutdown = capture
    try:
        asyncio.run(asyncio.wait_for(_run_campaign(spec), timeout=15.0))
    finally:
        _CampaignInstance._shutdown = orig_shutdown

    assert {r.workflow_type for r in results} == {"ok", "boom"}
    assert "ok" in captured and captured["ok"]["cpu_hours"]["n"] == 1
    assert "boom" not in captured
