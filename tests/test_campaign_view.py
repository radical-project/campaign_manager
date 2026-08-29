"""Tests for CampaignView and TelemetrySubscriber.

Extracted from test_adr_bridge.py so they run without radical.adr installed.
CampaignView and TelemetrySubscriber have no radical.adr dependency.
"""

import pytest

from src.campaign.adr.telemetry import TelemetrySubscriber
from src.campaign.adr.view import CampaignView

# ---------------------------------------------------------------------------
# Fake CM / workflow helpers
# ---------------------------------------------------------------------------


class _FakeWf:
    def __init__(
        self,
        deps=(),
        replicas=0,
        started=0,
        finished=0,
        cap=4,
        priority=0,
        ready=True,
        status="running",
        stalls=0,
        workflow_config=None,
    ):
        self.dependencies = list(deps)
        self.replicas = replicas
        self.started_count = started
        self.finished_replicas = finished
        self.failed_replicas = 0
        self.concurrency_cap = cap
        self.priority = priority
        self.ready = ready
        self.status = status
        self._consecutive_stalls = stalls
        self.workflow_config = workflow_config


class _FakeResources:
    available_cpus = 32
    available_gpus = 2


class _FakeMetrics:
    shard_events = []
    budget_events = []

    def stage_wall_s(self, stage: str) -> float:
        return 0.0


class _FakeState:
    def __init__(self, workflows, triages=None, budget_controllers=None):
        self.workflows = workflows
        self.sharders = {}
        self.bp = {}
        self.resources = _FakeResources()
        self.triages = triages or {}
        self.budget_controllers = budget_controllers or {}


class _FakeCM:
    def __init__(self, workflows, triages=None, budget_controllers=None):
        self.state = _FakeState(workflows, triages=triages, budget_controllers=budget_controllers)
        self._plan = None

    def metrics(self) -> _FakeMetrics:
        return _FakeMetrics()


# ---------------------------------------------------------------------------
# TestCampaignView
# ---------------------------------------------------------------------------


class TestCampaignView:
    def _view(self, telemetry_subscriber=None):
        wfs = {
            "s1": _FakeWf(deps=(), replicas=100, started=4, finished=2, cap=4),
            "s2": _FakeWf(deps=["s1"], replicas=20, started=8, finished=2, cap=16),
            "s3": _FakeWf(deps=["s2"], replicas=2, started=2, finished=2, cap=12),
        }
        return CampaignView(
            _FakeCM(wfs), terminal="s3", telemetry_subscriber=telemetry_subscriber
        )

    def test_terminal_is_list(self):
        obs = self._view().observe()
        assert isinstance(obs["terminal"], list)
        assert obs["terminal"] == ["s3"]

    def test_no_hits_or_target_in_obs(self):
        obs = self._view().observe()
        assert "hits" not in obs
        assert "target" not in obs

    def test_exposes_pending_and_backlog(self):
        obs = self._view().observe()
        assert obs["stages"]["s2"]["pending"] == 12
        assert obs["stages"]["s1"]["pending"] == 96

    def test_is_source_flag(self):
        st = self._view().observe()["stages"]
        assert st["s1"]["is_source"] is True
        assert st["s2"]["is_source"] is False

    def test_starved_only_for_resource_limited_dependent(self):
        st = self._view().observe()["stages"]
        assert st["s2"]["starved"] is True
        assert st["s1"]["starved"] is False
        assert st["s3"]["starved"] is False

    def test_telemetry_fields_merged_when_subscriber_present(self):
        sub = TelemetrySubscriber(None)
        sub._gpu_util, sub._cpu_util = 77.0, 40.0
        obs = self._view(telemetry_subscriber=sub).observe()
        assert obs["gpu_util"] == 77.0
        assert obs["cpu_util"] == 40.0
        assert "task_fail_rate" in obs

    def test_no_telemetry_fields_without_subscriber(self):
        obs = self._view().observe()
        assert "gpu_util" not in obs

    def test_terminal_inferred_as_list(self):
        wfs = {
            "s1": _FakeWf(deps=()),
            "s2": _FakeWf(deps=["s1"]),
            "s3": _FakeWf(deps=["s2"]),
        }
        obs = CampaignView(_FakeCM(wfs)).observe()
        assert obs["terminal"] == ["s3"]

    def test_terminal_string_coerced_to_list(self):
        wfs = {"a": _FakeWf(), "b": _FakeWf()}
        view = CampaignView(_FakeCM(wfs), terminal="a")
        assert view.observe()["terminal"] == ["a"]

    def test_terminal_list_accepted_directly(self):
        wfs = {"a": _FakeWf(), "b": _FakeWf()}
        view = CampaignView(_FakeCM(wfs), terminal=["a", "b"])
        assert view.observe()["terminal"] == ["a", "b"]


# ---------------------------------------------------------------------------
# TestTelemetrySubscriber
# ---------------------------------------------------------------------------


class _Ev:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class TestTelemetrySubscriber:
    def test_none_telemetry_is_noop_zeros(self):
        snap = TelemetrySubscriber(None).snapshot()
        assert snap["gpu_util"] == 0.0
        assert snap["cpu_util"] == 0.0
        assert snap["task_fail_rate"] == 0.0
        assert snap["avg_task_duration_s"] is None

    def test_resource_and_task_events_aggregate(self):
        s = TelemetrySubscriber(None, alpha=1.0)
        s._on_event(
            _Ev(event_type="ResourceUpdate", resource_scope="per_gpu", gpu_id=0, gpu_percent=80.0)
        )
        s._on_event(
            _Ev(event_type="ResourceUpdate", resource_scope="per_gpu", gpu_id=1, gpu_percent=60.0)
        )
        s._on_event(
            _Ev(
                event_type="ResourceUpdate",
                resource_scope="per_node",
                gpu_id=None,
                gpu_percent=None,
                cpu_percent=45.0,
                memory_percent=70.0,
            )
        )
        s._on_event(_Ev(event_type="TaskCompleted", duration_seconds=2.0))
        s._on_event(_Ev(event_type="TaskCompleted", duration_seconds=4.0))
        s._on_event(_Ev(event_type="TaskFailed"))
        snap = s.snapshot()
        assert snap["gpu_util"] == pytest.approx(70.0)
        assert snap["cpu_util"] == pytest.approx(45.0)
        assert snap["mem_util"] == pytest.approx(70.0)
        assert snap["avg_task_duration_s"] == pytest.approx(3.0)
        assert snap["task_fail_rate"] == pytest.approx(1 / 3)

    def test_subscribe_wired_when_telemetry_given(self):
        captured = []

        class _Mgr:
            def subscribe(self, cb):
                captured.append(cb)

        TelemetrySubscriber(_Mgr())
        assert len(captured) == 1

    def test_per_gpu_fallback_to_node_level(self):
        """When per_gpu events absent, node-level gpu_percent is used."""
        s = TelemetrySubscriber(None, alpha=1.0)
        s._on_event(
            _Ev(
                event_type="ResourceUpdate",
                resource_scope="per_node",
                cpu_percent=50.0,
                memory_percent=30.0,
                gpu_percent=90.0,
            )
        )
        snap = s.snapshot()
        assert snap["gpu_util"] == pytest.approx(90.0)

    def test_task_fail_rate_zero_when_no_tasks(self):
        snap = TelemetrySubscriber(None).snapshot()
        assert snap["task_fail_rate"] == 0.0

    def test_avg_duration_none_when_no_completed_tasks(self):
        s = TelemetrySubscriber(None)
        s._on_event(_Ev(event_type="TaskFailed"))
        assert s.snapshot()["avg_task_duration_s"] is None
