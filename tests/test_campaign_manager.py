"""Tests for CampaignManager and AsyncCampaignManager."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from src.campaign import (
    AsyncCampaignManager,
    BaseWorkflow,
    CampaignManager,
    ResourcePool,
    WorkflowStats,
)

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    """Run all async tests with the asyncio backend only."""
    return "asyncio"


# ---------------------------------------------------------------------------
# Workflow stubs
# ---------------------------------------------------------------------------


class NullWorkflow(BaseWorkflow):
    """No-op async workflow."""

    workflow_id = "null"

    async def run(self, replica_id: str) -> None:
        pass


class SleepWorkflow(BaseWorkflow):
    """Sleeps briefly so concurrency effects are observable."""

    workflow_id = "sleep"

    async def run(self, replica_id: str) -> None:
        await asyncio.sleep(0.02)


class RecordingWorkflow(BaseWorkflow):
    """Appends each replica_id it runs to a class-level list."""

    workflow_id = "recording"
    ran: list = []

    async def run(self, replica_id: str) -> None:
        RecordingWorkflow.ran.append(replica_id)


class SignalDoneWorkflow(BaseWorkflow):
    """Fires _signal_done() immediately then finishes after a tiny sleep."""

    workflow_id = "signal_done"

    async def run(self, replica_id: str) -> None:
        await self._signal_done()
        await asyncio.sleep(0.01)


class TriggerWorkflow(BaseWorkflow):
    """Triggers a dependent group named 'downstream' then finishes."""

    workflow_id = "trigger"
    dependent_name: str = "downstream"
    dependent_replicas: int = 1

    async def run(self, replica_id: str) -> None:
        await self._trigger_dependent(self.dependent_name, replicas=self.dependent_replicas)


class HookWorkflow(BaseWorkflow):
    """Records (replica_id, final_state) tuples in on_replica_done."""

    workflow_id = "hook"
    calls: list = []

    async def run(self, replica_id: str) -> None:
        pass

    async def on_replica_done(self, replica_id, cm, final_state):
        HookWorkflow.calls.append((replica_id, final_state))


class FailingHookWorkflow(BaseWorkflow):
    """Raises during run; records (replica_id, final_state) in on_replica_done."""

    workflow_id = "failing_hook"
    calls: list = []

    async def run(self, replica_id: str) -> None:
        raise RuntimeError("deliberate failure")

    async def on_replica_done(self, replica_id, cm, final_state):
        FailingHookWorkflow.calls.append((replica_id, final_state))


# Sync variants for CampaignManager (thread-pool) tests


class SyncRecordingWorkflow(BaseWorkflow):
    workflow_id = "sync_rec"
    ran: list = []

    def run(self, replica_id: str) -> None:
        SyncRecordingWorkflow.ran.append(replica_id)


class SyncHookWorkflow(BaseWorkflow):
    workflow_id = "sync_hook"
    calls: list = []

    def run(self, replica_id: str) -> None:
        pass

    def on_replica_done(self, replica_id, cm, final_state):
        SyncHookWorkflow.calls.append((replica_id, final_state))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_class_state():
    """Clear class-level recording lists before every test."""
    RecordingWorkflow.ran = []
    HookWorkflow.calls = []
    FailingHookWorkflow.calls = []
    SyncRecordingWorkflow.ran = []
    SyncHookWorkflow.calls = []
    yield


@pytest.fixture
async def acm():
    """AsyncCampaignManager with a mock asyncflow engine.

    asyncflow lifecycle is caller-owned: start() requires _asyncflow to be set,
    so we inject a mock directly. The test workflows execute their run()/start()
    via the CM and never touch the engine, so a mock is sufficient.
    """
    cm = AsyncCampaignManager()
    cm._asyncflow = AsyncMock()
    yield cm
    await cm.close()


# ---------------------------------------------------------------------------
# BaseWorkflow
# ---------------------------------------------------------------------------


class TestBaseWorkflow:
    async def test_signal_done_no_cm_is_noop(self):
        wf = NullWorkflow()
        await wf._signal_done()  # must not raise

    async def test_trigger_dependent_no_cm_is_noop(self):
        wf = NullWorkflow()
        await wf._trigger_dependent("some_group", replicas=2)  # must not raise

    async def test_signal_done_calls_cm(self):
        cm_mock = AsyncMock()
        wf = NullWorkflow(_cm=cm_mock, _group_name="mygroup")
        await wf._signal_done()
        cm_mock.signal_done.assert_awaited_once_with("mygroup")

    async def test_trigger_dependent_calls_cm(self):
        cm_mock = AsyncMock()
        wf = NullWorkflow(_cm=cm_mock)
        await wf._trigger_dependent("dep", replicas=3)
        cm_mock.trigger_dependent.assert_awaited_once_with("dep", replicas=3)

    def test_base_run_raises_not_implemented(self):
        wf = BaseWorkflow()
        with pytest.raises(NotImplementedError):
            wf.run("r0")

    def test_resolve_entry_point_both_raises(self):
        class BothWorkflow(BaseWorkflow):
            workflow_id = "both"

            def run(self, replica_id):
                pass

            def start(self, replica_id):
                pass

        with pytest.raises(ValueError, match="both 'run' and 'start'"):
            AsyncCampaignManager._resolve_entry_point(BothWorkflow)

    def test_resolve_entry_point_neither_raises(self):
        class NeitherWorkflow(BaseWorkflow):
            workflow_id = "neither"

        with pytest.raises(ValueError, match="must define either"):
            AsyncCampaignManager._resolve_entry_point(NeitherWorkflow)


# ---------------------------------------------------------------------------
# AsyncCampaignManager
# ---------------------------------------------------------------------------


class TestAsyncCampaignManager:
    async def test_single_replica_completes(self, acm):
        acm.register_workflow("a", NullWorkflow, replicas=1)
        await acm.start()
        assert await acm.wait(timeout=3.0)
        s = acm.status()
        assert s["groups"]["a"]["status"] == "done"
        assert s["groups"]["a"]["replicas_finished"] == 1

    async def test_all_replicas_run(self, acm):
        acm.register_workflow("a", RecordingWorkflow, replicas=4, concurrency_cap=4)
        await acm.start()
        assert await acm.wait(timeout=3.0)
        assert sorted(RecordingWorkflow.ran) == ["a_0", "a_1", "a_2", "a_3"]

    async def test_concurrency_cap_cap_respected(self, acm):
        """Concurrent running count must never exceed concurrency_cap."""
        peak = []

        class PeakObserver(BaseWorkflow):
            workflow_id = "peak"
            _active = 0

            async def run(self, replica_id: str) -> None:
                PeakObserver._active += 1
                peak.append(PeakObserver._active)
                await asyncio.sleep(0.02)
                PeakObserver._active -= 1

        acm.register_workflow("a", PeakObserver, replicas=6, concurrency_cap=2)
        await acm.start()
        assert await acm.wait(timeout=5.0)
        assert max(peak) <= 2

    async def test_dependency_count_based(self, acm):
        """Group B must not start until A has dep_threshold finished replicas."""
        order = []

        class A(BaseWorkflow):
            workflow_id = "A"

            async def run(self, replica_id: str) -> None:
                order.append(("A", replica_id))

        class B(BaseWorkflow):
            workflow_id = "B"

            async def run(self, replica_id: str) -> None:
                order.append(("B", replica_id))

        acm.register_workflow("a", A, replicas=2)
        acm.register_workflow("b", B, replicas=1, dependencies=["a"], dep_threshold=2)
        await acm.start()
        assert await acm.wait(timeout=3.0)

        b_idx = next(i for i, (wf, _) in enumerate(order) if wf == "B")
        assert all(wf == "A" for wf, _ in order[:b_idx])

    async def test_dependency_via_signal_done(self, acm):
        """_signal_done() unblocks B even before all of A's replicas finish."""
        acm.register_workflow("a", SignalDoneWorkflow, replicas=1)
        acm.register_workflow(
            "b",
            NullWorkflow,
            replicas=1,
            dependencies=["a"],
            dep_threshold=999,  # count-based fallback would never fire
        )
        await acm.start()
        assert await acm.wait(timeout=3.0)

        s = acm.status()
        assert s["groups"]["a"]["ready"] is True
        assert s["groups"]["b"]["status"] == "done"

    async def test_dag_join_waits_for_all_upstreams(self, acm):
        """Fan-in / join: a group depending on [a, b] must wait for BOTH (AND semantics).

        Confirms the CM is a general DAG orchestrator, not just a linear cascade —
        the join stage runs only after every upstream is satisfied.
        """
        order = []

        class Rec(BaseWorkflow):
            workflow_id = "rec"

            async def run(self, replica_id: str) -> None:
                order.append(replica_id.split("_")[0])

        acm.register_workflow("a", Rec, replicas=1)
        acm.register_workflow("b", Rec, replicas=1)
        acm.register_workflow("join", Rec, replicas=1, dependencies=["a", "b"], dep_threshold=1)
        await acm.start()
        assert await acm.wait(timeout=3.0)

        # join must appear only after BOTH a and b have run.
        join_idx = order.index("join")
        assert "a" in order[:join_idx] and "b" in order[:join_idx]
        assert acm.status()["groups"]["join"]["status"] == "done"

    async def test_dag_fanout_signal_done_activates_all_dependents(self, acm):
        """Fan-out: one upstream _signal_done() routes +1 replica to every dependent."""
        acm.register_workflow("root", SignalDoneWorkflow, replicas=1)
        acm.register_workflow(
            "left", NullWorkflow, replicas=0, dependencies=["root"], dep_threshold=999
        )
        acm.register_workflow(
            "right", NullWorkflow, replicas=0, dependencies=["root"], dep_threshold=999
        )
        await acm.start()
        assert await acm.wait(timeout=3.0)

        s = acm.status()
        # both branches were activated and completed off the single signal.
        assert s["groups"]["left"]["status"] == "done"
        assert s["groups"]["right"]["status"] == "done"
        assert s["groups"]["left"]["replicas_finished"] == 1
        assert s["groups"]["right"]["replicas_finished"] == 1

    async def test_trigger_dependent_activates_group(self, acm):
        """Parent workflow calls _trigger_dependent to start a replicas=0 group."""
        acm.register_workflow("upstream", TriggerWorkflow, replicas=1)
        acm.register_workflow("downstream", RecordingWorkflow, replicas=0)
        await acm.start()
        assert await acm.wait(timeout=3.0)

        s = acm.status()
        assert s["groups"]["downstream"]["status"] == "done"
        assert "downstream_0" in RecordingWorkflow.ran

    async def test_untriggered_group_does_not_block_completion(self, acm):
        """A replicas=0 group that is never triggered must not prevent _all_done."""
        acm.register_workflow("a", NullWorkflow, replicas=1)
        acm.register_workflow("never_triggered", NullWorkflow, replicas=0)
        await acm.start()
        assert await acm.wait(timeout=3.0)
        assert acm.status()["groups"]["a"]["status"] == "done"

    async def test_on_replica_done_hook_called(self, acm):
        acm.register_workflow("a", HookWorkflow, replicas=2)
        await acm.start()
        assert await acm.wait(timeout=3.0)
        assert len(HookWorkflow.calls) == 2
        assert {rid for rid, _ in HookWorkflow.calls} == {"a_0", "a_1"}
        assert all(st == "done" for _, st in HookWorkflow.calls)

    async def test_run_exception_marks_replica_failed(self, acm):
        """An exception in run() sets final_state="failed"; campaign still completes."""
        acm.register_workflow("a", FailingHookWorkflow, replicas=2)
        await acm.start()
        assert await acm.wait(timeout=3.0)
        assert len(FailingHookWorkflow.calls) == 2
        assert all(st == "failed" for _, st in FailingHookWorkflow.calls)
        assert acm.status()["groups"]["a"]["status"] == "done"

    async def test_status_transitions_pending_running_done(self, acm):
        """Status progresses: pending before start → running during → done after."""
        acm.register_workflow("a", SleepWorkflow, replicas=1)
        assert acm.status()["groups"]["a"]["status"] == "pending"
        await acm.start()
        await asyncio.sleep(0.005)  # yield to let the replica task begin
        assert acm.status()["groups"]["a"]["status"] == "running"
        assert await acm.wait(timeout=3.0)
        assert acm.status()["groups"]["a"]["status"] == "done"

    async def test_empty_campaign_finishes_immediately(self, acm):
        await acm.start()
        assert await acm.wait(timeout=1.0)

    async def test_status_snapshot_fields(self, acm):
        acm.register_workflow("a", NullWorkflow, replicas=2, concurrency_cap=1)
        s = acm.status()["groups"]["a"]
        assert s["status"] == "pending"
        assert s["replicas_total"] == 2
        assert s["concurrency_cap"] == 1
        assert s["dependencies"] == []

    async def test_stats_reflect_finished_count(self, acm):
        acm.register_workflow("a", NullWorkflow, replicas=3)
        await acm.start()
        assert await acm.wait(timeout=3.0)
        st = acm.stats()
        assert st["a"].replicas_started == 3
        assert st["a"].replicas_finished == 3
        assert isinstance(st["a"], WorkflowStats)

    async def test_from_config_registers_groups(self):
        config = {
            "workflows": {
                "x": {"replicas": 2, "concurrency_cap": 1},
                # y has dependencies → replicas defaults to 0 (triggered group)
                "y": {"dependencies": ["x"], "dependency_threshold": 2},
            }
        }
        cm = AsyncCampaignManager.from_config(config, {"x": NullWorkflow, "y": NullWorkflow})
        s = cm.status()["groups"]
        assert s["x"]["replicas_total"] == 2
        assert s["x"]["concurrency_cap"] == 1
        assert s["y"]["replicas_total"] == 0  # triggered group: not yet activated
        assert s["y"]["dependencies"] == ["x"]
        assert s["y"]["dep_threshold"] == 2

    async def test_unknown_group_skipped_in_from_config(self):
        config = {"workflows": {"unknown": {"replicas": 1}}}
        cm = AsyncCampaignManager.from_config(config, {})  # empty registry
        assert "unknown" not in cm.status()["groups"]


# ---------------------------------------------------------------------------
# CampaignManager (sync / thread-pool)
# ---------------------------------------------------------------------------


class TestCampaignManager:
    @pytest.fixture
    def cm(self):
        manager = CampaignManager()
        yield manager
        manager.close()

    def test_single_replica_runs(self, cm):
        cm.register_workflow("a", SyncRecordingWorkflow, replicas=1)
        cm.start()
        assert cm.wait(timeout=5.0)
        assert SyncRecordingWorkflow.ran == ["a_0"]

    def test_multiple_replicas_all_run(self, cm):
        cm.register_workflow("a", SyncRecordingWorkflow, replicas=3)
        cm.start()
        assert cm.wait(timeout=5.0)
        assert sorted(SyncRecordingWorkflow.ran) == ["a_0", "a_1", "a_2"]

    def test_sliding_window_concurrency_cap(self, cm):
        cm.register_workflow("a", SyncRecordingWorkflow, replicas=4, concurrency_cap=2)
        cm.start()
        assert cm.wait(timeout=5.0)
        assert sorted(SyncRecordingWorkflow.ran) == ["a_0", "a_1", "a_2", "a_3"]

    def test_dependency_respected(self, cm):
        """Group B must start only after group A completes."""
        order = []

        class A(BaseWorkflow):
            workflow_id = "A"

            def run(self, replica_id: str) -> None:
                order.append(("A", replica_id))

        class B(BaseWorkflow):
            workflow_id = "B"

            def run(self, replica_id: str) -> None:
                order.append(("B", replica_id))

        cm.register_workflow("a", A, replicas=2)
        cm.register_workflow("b", B, replicas=1, dependencies=["a"])
        cm.start()
        assert cm.wait(timeout=5.0)

        b_idx = next(i for i, (wf, _) in enumerate(order) if wf == "B")
        assert all(wf == "A" for wf, _ in order[:b_idx])

    def test_on_replica_done_hook_called(self, cm):
        cm.register_workflow("a", SyncHookWorkflow, replicas=2)
        cm.start()
        assert cm.wait(timeout=5.0)
        assert len(SyncHookWorkflow.calls) == 2
        assert {rid for rid, _ in SyncHookWorkflow.calls} == {"a_0", "a_1"}

    def test_status_snapshot_fields(self, cm):
        cm.register_workflow("a", SyncRecordingWorkflow, replicas=1, concurrency_cap=1)
        s = cm.status()["groups"]["a"]
        assert s["status"] == "pending"
        assert s["replicas_total"] == 1
        assert s["concurrency_cap"] == 1

    def test_stats_reflect_finished_count(self, cm):
        cm.register_workflow("a", SyncRecordingWorkflow, replicas=3)
        cm.start()
        assert cm.wait(timeout=5.0)
        st = cm.stats()
        assert st["a"].replicas_finished == 3
        assert isinstance(st["a"], WorkflowStats)

    def test_from_config_registers_groups(self):
        config = {
            "workflows": {
                "alpha": {"replicas": 3, "concurrency_cap": 2},
                # beta has dependencies → replicas defaults to 0 (triggered group)
                "beta": {"dependencies": ["alpha"]},
            }
        }
        cm = CampaignManager.from_config(
            config, {"alpha": SyncRecordingWorkflow, "beta": SyncRecordingWorkflow}
        )
        s = cm.status()["groups"]
        cm.close()
        assert "alpha" in s
        assert s["alpha"]["replicas_total"] == 3
        assert s["alpha"]["concurrency_cap"] == 2
        assert s["beta"]["replicas_total"] == 0  # triggered group: not yet activated

    def test_unknown_group_skipped_in_from_config(self):
        config = {"workflows": {"ghost": {"replicas": 1}}}
        cm = CampaignManager.from_config(config, {})
        cm.close()
        assert "ghost" not in cm.status()["groups"]


# ---------------------------------------------------------------------------
# ResourcePool unit tests
# ---------------------------------------------------------------------------


class TestResourcePool:
    def test_initial_available_equals_total(self):
        rp = ResourcePool(total_cpus=16, total_gpus=4)
        assert rp.available_cpus == 16
        assert rp.available_gpus == 4

    def test_can_fit_within_budget(self):
        rp = ResourcePool(total_cpus=8, total_gpus=2)
        assert rp.can_fit(8, 2)
        assert rp.can_fit(1, 0)
        assert rp.can_fit(0, 1)

    def test_cannot_fit_over_budget(self):
        rp = ResourcePool(total_cpus=4, total_gpus=1)
        assert not rp.can_fit(5, 0)
        assert not rp.can_fit(0, 2)

    def test_zero_total_means_unlimited(self):
        rp = ResourcePool(total_cpus=0, total_gpus=0)
        assert rp.can_fit(9999, 9999)

    def test_allocate_decrements_available(self):
        rp = ResourcePool(total_cpus=16, total_gpus=4)
        rp.allocate(4, 1)
        assert rp.available_cpus == 12
        assert rp.available_gpus == 3

    def test_release_increments_available(self):
        rp = ResourcePool(total_cpus=16, total_gpus=4)
        rp.allocate(4, 1)
        rp.release(4, 1)
        assert rp.available_cpus == 16
        assert rp.available_gpus == 4

    def test_as_dict_keys(self):
        rp = ResourcePool(total_cpus=8, total_gpus=2)
        d = rp.as_dict()
        assert set(d) == {
            "total_cpus",
            "available_cpus",
            "total_gpus",
            "available_gpus",
            "total_memory_gb",
            "available_memory_gb",
        }

    def test_usage_str_tracks_used(self):
        rp = ResourcePool(total_cpus=8, total_gpus=4)
        rp.allocate(3, 2)
        s = rp.usage_str()
        assert "3/8" in s
        assert "2/4" in s

    def test_available_str_tracks_free(self):
        rp = ResourcePool(total_cpus=8, total_gpus=4)
        rp.allocate(3, 2)
        s = rp.available_str()
        assert "5/8" in s
        assert "2/4" in s

    def test_unlimited_usage_str_returns_dash(self):
        rp = ResourcePool(total_cpus=0, total_gpus=0)
        assert rp.usage_str() == "—"


# ---------------------------------------------------------------------------
# Resource-aware scheduling tests (AsyncCampaignManager)
# ---------------------------------------------------------------------------


class TestAsyncCampaignManagerResources:
    @pytest.fixture
    async def racm(self):
        """AsyncCampaignManager with 4 CPUs and 2 GPUs, asyncflow mocked."""
        cm = AsyncCampaignManager(total_cpus=4, total_gpus=2)
        cm._asyncflow = AsyncMock()
        yield cm
        await cm.close()

    async def test_resource_limits_concurrency(self, racm):
        """With 2 GPUs and 1 GPU/replica, at most 2 replicas run concurrently."""
        peak = []

        class GpuWorkflow(BaseWorkflow):
            workflow_id = "gpu"
            _active = 0

            async def run(self, replica_id: str) -> None:
                GpuWorkflow._active += 1
                peak.append(GpuWorkflow._active)
                await asyncio.sleep(0.02)
                GpuWorkflow._active -= 1

        racm.register_workflow("g", GpuWorkflow, replicas=6, concurrency_cap=6, required_gpus=1)
        await racm.start()
        assert await racm.wait(timeout=5.0)
        assert max(peak) <= 2  # only 2 GPUs available

    async def test_resources_released_after_replica(self, racm):
        """Available resources return to full after all replicas complete."""
        racm.register_workflow("g", NullWorkflow, replicas=2, required_cpus=2, required_gpus=1)
        await racm.start()
        assert await racm.wait(timeout=3.0)
        s = racm.status()["resources"]
        assert s["available_cpus"] == 4  # total_cpus restored
        assert s["available_gpus"] == 2  # total_gpus restored

    async def test_status_includes_resource_snapshot(self, racm):
        racm.register_workflow("g", NullWorkflow, replicas=1, required_cpus=2, required_gpus=1)
        s = racm.status()
        assert "resources" in s
        assert s["resources"]["total_cpus"] == 4
        assert s["resources"]["total_gpus"] == 2
        assert s["resources"]["available_cpus"] == 4
        assert s["resources"]["available_gpus"] == 2

    async def test_from_config_parses_resources(self):
        config = {
            "resources": {"total_cpus": 64, "total_gpus": 8},
            "workflows": {
                "a": {"replicas": 1, "required_cpus": 4, "required_gpus": 2},
            },
        }
        cm = AsyncCampaignManager.from_config(config, {"a": NullWorkflow})
        s = cm.status()
        assert s["resources"]["total_cpus"] == 64
        assert s["resources"]["total_gpus"] == 8
        assert s["groups"]["a"]["required_cpus"] == 4
        assert s["groups"]["a"]["required_gpus"] == 2

    async def test_resource_constrained_scheduling(self, racm):
        """Both groups run to completion despite resource contention."""
        started_order = []

        class TrackWorkflow(BaseWorkflow):
            workflow_id = "track"

            async def run(self, replica_id: str) -> None:
                started_order.append(replica_id)
                await asyncio.sleep(0.01)

        racm.register_workflow("lo", TrackWorkflow, replicas=2, required_gpus=1)
        racm.register_workflow("hi", TrackWorkflow, replicas=2, required_gpus=1)
        await racm.start()
        assert await racm.wait(timeout=3.0)
        # All 4 replicas should complete
        assert len(started_order) == 4
        assert sum(1 for r in started_order if r.startswith("lo")) == 2
        assert sum(1 for r in started_order if r.startswith("hi")) == 2


# ---------------------------------------------------------------------------
# Resource-aware scheduling tests (CampaignManager sync)
# ---------------------------------------------------------------------------


class TestCampaignManagerResources:
    @pytest.fixture
    def rcm(self):
        cm = CampaignManager(total_cpus=4, total_gpus=2)
        yield cm
        cm.close()

    def test_resource_limits_concurrency(self, rcm):
        """With 2 GPUs and 1 GPU/replica, at most 2 run concurrently."""
        import threading

        peak = []
        lock = threading.Lock()

        class GpuWorkflow(BaseWorkflow):
            workflow_id = "gpu"
            _active = 0

            def run(self, replica_id: str) -> None:
                with lock:
                    GpuWorkflow._active += 1
                    peak.append(GpuWorkflow._active)
                import time

                time.sleep(0.02)
                with lock:
                    GpuWorkflow._active -= 1

        rcm.register_workflow("g", GpuWorkflow, replicas=6, concurrency_cap=6, required_gpus=1)
        rcm.start()
        assert rcm.wait(timeout=5.0)
        assert max(peak) <= 2

    def test_resources_released_after_replica(self, rcm):
        rcm.register_workflow(
            "g", SyncRecordingWorkflow, replicas=2, required_cpus=2, required_gpus=1
        )
        rcm.start()
        assert rcm.wait(timeout=3.0)
        s = rcm.status()["resources"]
        assert s["available_cpus"] == 4
        assert s["available_gpus"] == 2

    def test_status_includes_resource_snapshot(self, rcm):
        rcm.register_workflow(
            "g", SyncRecordingWorkflow, replicas=1, required_cpus=1, required_gpus=0
        )
        s = rcm.status()
        assert "resources" in s
        assert s["resources"]["total_cpus"] == 4
        assert s["resources"]["total_gpus"] == 2

    def test_from_config_parses_resources(self):
        config = {
            "resources": {"total_cpus": 32, "total_gpus": 4},
            "workflows": {
                "a": {"replicas": 1, "required_cpus": 8, "required_gpus": 1},
            },
        }
        cm = CampaignManager.from_config(config, {"a": SyncRecordingWorkflow})
        s = cm.status()
        cm.close()
        assert s["resources"]["total_cpus"] == 32
        assert s["resources"]["total_gpus"] == 4
        assert s["groups"]["a"]["required_cpus"] == 8
        assert s["groups"]["a"]["required_gpus"] == 1
