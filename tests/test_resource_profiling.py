"""Tests for block resource profiling: BlockProfiler, TaskProfiler, and
_compute_workflow_spent (reported profiler data and telemetry JSONL correlation)."""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.aucam import BlockProfiler, TaskProfiler, WorkflowResult
from src.aucam.campaign import _CampaignInstance

# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_campaign(telemetry_mock=None):
    """Minimal object exposing _correlate_from_jsonl and _compute_workflow_spent.

    Both methods only use self._flow._telemetry, so we skip full construction
    and bind the unbound methods to a lightweight namespace instead.
    """
    flow = MagicMock()
    flow._telemetry = telemetry_mock
    obj = SimpleNamespace(_flow=flow, _spent={})
    obj._correlate_from_jsonl = _CampaignInstance._correlate_from_jsonl.__get__(obj)
    obj._compute_workflow_spent = _CampaignInstance._compute_workflow_spent.__get__(obj)
    return obj


def _make_jsonl(tmp_path: Path, events: list[dict]) -> tuple[Path, MagicMock]:
    """Write events to a JSONL file and return (jsonl_path, telemetry_mock)."""
    session_id = "asyncflow.session.testtest"
    ts = int(time.time())
    fname = f"{session_id}.{ts}.telemetry.jsonl"
    jsonl_path = tmp_path / fname
    with open(jsonl_path, "w") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")

    tel = SimpleNamespace(_checkpoint_path=str(tmp_path), _session_id=session_id)
    return jsonl_path, tel


# ── BlockProfiler / TaskProfiler ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_block_profiler_basic():
    async with BlockProfiler(interval=0.05) as prof:
        await asyncio.sleep(0.2)
    m = prof.metrics
    assert m["duration_s"] >= 0.15
    assert "cpu_percent_avg" in m
    assert "memory_percent_avg" in m
    assert m["node_id"] == os.uname().nodename
    assert m["n_samples"] >= 1


def test_task_profiler_basic():
    with TaskProfiler(interval=0.05) as prof:
        time.sleep(0.2)
    m = prof.metrics
    assert m["duration_s"] >= 0.15
    assert "cpu_percent_avg" in m
    assert m["n_samples"] >= 1


@pytest.mark.asyncio
async def test_block_profiler_very_short():
    """Blocks shorter than the interval still return a metrics dict."""
    async with BlockProfiler(interval=10.0) as prof:
        pass  # exits before first tick
    m = prof.metrics
    assert "duration_s" in m
    assert m["n_samples"] == 0  # no ticks fired


# ── JSONL correlation ────────────────────────────────────────────────────────


def test_jsonl_correlation_basic(tmp_path):
    """_correlate_from_jsonl finds block window and ResourceUpdates."""
    t0 = time.time()
    events = [
        {"event_type": "TaskStarted", "task_id": "test-campaign.sim_worker.aabb", "event_time": t0},
        {
            "event_type": "ResourceUpdate",
            "task_id": None,
            "event_time": t0 + 1.0,
            "node_id": "cn001",
            "cpu_percent": 60.0,
            "memory_percent": 40.0,
        },
        {
            "event_type": "ResourceUpdate",
            "task_id": None,
            "event_time": t0 + 2.0,
            "node_id": "cn001",
            "cpu_percent": 80.0,
            "memory_percent": 45.0,
        },
        {
            "event_type": "TaskCompleted",
            "task_id": "test-campaign.sim_worker.aabb",
            "event_time": t0 + 3.0,
        },
    ]
    _, tel = _make_jsonl(tmp_path, events)
    camp = _make_campaign(tel)

    prof, spending = camp._correlate_from_jsonl("test-campaign.sim_worker.aabb", 3.0, tel)

    assert prof["duration_s"] == pytest.approx(3.0, abs=0.01)
    assert prof["cpu_percent_avg"] == pytest.approx(70.0)  # (60+80)/2
    assert prof["cpu_percent_peak"] == pytest.approx(80.0)
    assert prof["n_samples"] == 2
    assert "cn001" in prof["nodes"]
    assert prof["source"] == "telemetry_jsonl"
    assert spending["cpu_hours"] == pytest.approx(70.0 / 100.0 * 3.0 / 3600.0)


def test_jsonl_no_events_for_wf(tmp_path):
    """Returns empty dicts when wf_id is not in the JSONL."""
    t0 = time.time()
    events = [
        {"event_type": "TaskStarted", "task_id": "other.wf.xxxx", "event_time": t0},
        {"event_type": "TaskCompleted", "task_id": "other.wf.xxxx", "event_time": t0 + 2.0},
        {
            "event_type": "ResourceUpdate",
            "task_id": None,
            "event_time": t0 + 1.0,
            "cpu_percent": 50.0,
            "memory_percent": 30.0,
        },
    ]
    _, tel = _make_jsonl(tmp_path, events)
    camp = _make_campaign(tel)

    prof, spending = camp._correlate_from_jsonl("test-campaign.sim_worker.missing", 2.0, tel)
    assert prof == {}
    assert spending == {}


def test_jsonl_no_resource_updates_in_window(tmp_path):
    """Returns empty dicts when ResourceUpdates fall outside the block window."""
    t0 = time.time()
    events = [
        {"event_type": "TaskStarted", "task_id": "tc.sim_worker.abcd", "event_time": t0},
        {"event_type": "TaskCompleted", "task_id": "tc.sim_worker.abcd", "event_time": t0 + 1.0},
        {
            "event_type": "ResourceUpdate",
            "task_id": None,
            "event_time": t0 + 10.0,  # outside
            "cpu_percent": 50.0,
            "memory_percent": 30.0,
        },
    ]
    _, tel = _make_jsonl(tmp_path, events)
    camp = _make_campaign(tel)

    prof, spending = camp._correlate_from_jsonl("tc.sim_worker.abcd", 1.0, tel)
    assert prof == {}


def test_jsonl_uses_wall_s_when_no_completed(tmp_path):
    """Falls back to wall_s for end_time when TaskCompleted is absent."""
    t0 = time.time()
    events = [
        {"event_type": "TaskStarted", "task_id": "tc.sim_worker.abcd", "event_time": t0},
        {
            "event_type": "ResourceUpdate",
            "task_id": None,
            "event_time": t0 + 1.0,
            "cpu_percent": 55.0,
            "memory_percent": 30.0,
        },
    ]
    _, tel = _make_jsonl(tmp_path, events)
    camp = _make_campaign(tel)

    prof, _ = camp._correlate_from_jsonl("tc.sim_worker.abcd", 2.0, tel)
    # end_t = t0 + 2.0, ResourceUpdate at t0+1 is inside → should be found
    assert prof["n_samples"] == 1
    assert prof["duration_s"] == pytest.approx(2.0, abs=0.01)


# ── _compute_workflow_spent integration ──────────────────────────────────────


def test_compute_spent_reported_profiler_data(tmp_path):
    """Reported: cpu_hours/gpu_hours reported directly in final_metrics are used as-is."""
    camp = _make_campaign()
    final_metrics = {
        "cpu_hours": 0.5,
        "gpu_hours": 0.1,
        "resource_usage": {
            "node_id": "head",
            "start_time": 0.0,
            "end_time": 3600.0,
            "duration_s": 3600.0,
            "cpu_percent_avg": 50.0,
            "memory_percent_avg": 30.0,
            "n_samples": 10,
        },
    }
    spending, profile = camp._compute_workflow_spent("wf1", {}, 3600.0, final_metrics)
    assert spending["cpu_hours"] == pytest.approx(0.5)
    assert spending["gpu_hours"] == pytest.approx(0.1)
    # resource_usage becomes the profile when no JSONL telemetry
    assert profile["cpu_percent_avg"] == 50.0


def test_compute_spent_jsonl(tmp_path):
    """JSONL correlation when no profiler data in final_metrics."""
    t0 = time.time()
    events = [
        {"event_type": "TaskStarted", "task_id": "tc.sim_worker.aabb", "event_time": t0},
        {
            "event_type": "ResourceUpdate",
            "task_id": None,
            "event_time": t0 + 1.0,
            "node_id": "cn001",
            "cpu_percent": 70.0,
            "memory_percent": 40.0,
        },
        {"event_type": "TaskCompleted", "task_id": "tc.sim_worker.aabb", "event_time": t0 + 2.0},
    ]
    _, tel = _make_jsonl(tmp_path, events)
    camp = _make_campaign(tel)

    spending, profile = camp._compute_workflow_spent("tc.sim_worker.aabb", {"cpu": 2.0}, 2.0, {})
    assert profile["cpu_percent_avg"] == pytest.approx(70.0)
    assert profile["source"] == "telemetry_jsonl"
    assert spending["cpu_hours"] > 0


def test_compute_spent_fallback_no_telemetry():
    """Fallback: prorate declared cpu_hours/gpu_hours by actual/declared wall time."""
    camp = _make_campaign(telemetry_mock=None)
    # declared: 4 cpu_hours over a declared wall of 1h; actual wall = 2h → ratio = 2.0
    declared = {"wall_hours": 1.0, "cpu_hours": 4.0}
    spending, profile = camp._compute_workflow_spent("wf1", declared, 7200.0, {})
    assert spending == {"cpu_hours": pytest.approx(8.0)}  # 4 × (2h / 1h) = 8 cpu_hours
    assert profile == {}


# ── WorkflowResult carries resource_profile ──────────────────────────────────


def test_workflow_result_resource_profile_field():
    r = WorkflowResult(
        workflow_id="wf1",
        workflow_type="sim_worker",
        returncode=0,
        wall_time_s=5.0,
        resource_profile={"cpu_percent_avg": 62.5},
    )
    assert r.resource_profile["cpu_percent_avg"] == 62.5


def test_workflow_result_resource_profile_default_empty():
    r = WorkflowResult("wf1", "sim_worker", 0, 5.0)
    assert r.resource_profile == {}
