"""L1 — load-aware launching: with telemetry, how many workflows may run
follows node utilization and the tasks waiting in the backend; without
telemetry nothing changes.  Telemetry is faked per policy cycle."""

from __future__ import annotations

import asyncio
import logging

import pytest

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    LaunchRequest,
    NodeResources,
    WorkflowConfig,
    WorkflowReporter,
)
from src.aucam import campaign as campaign_mod

RUNNING = {"now": 0, "peak": 0}  # workflows running at once, measured inside them


async def work(reporter: WorkflowReporter) -> None:
    RUNNING["now"] += 1
    RUNNING["peak"] = max(RUNNING["peak"], RUNNING["now"])
    try:
        await asyncio.sleep(0.08)
        await reporter.emit(score=1.0)
    finally:
        RUNNING["now"] -= 1


class Watch(CampaignController):
    """Keeps 4 'sim' requests queued; records per cycle the load and how many run."""

    def __init__(self, cycles: int) -> None:
        super().__init__()
        self.cycles = cycles
        self.seen: list[tuple[dict | None, int]] = []

    def decide(self, obs):
        self.seen.append((obs.get("load"), sum(obs["running"].values())))
        res = super().decide(obs)
        queued = obs["queue_snapshot"].by_type.get("sim", 0)
        res.launch_requests += [LaunchRequest.make("sim") for _ in range(4 - queued)]
        res.terminate = len(self.seen) >= self.cycles
        return res


def _fake_telemetry(monkeypatch, cpu_percent=None, waiting=None):
    """Node CPU use and waiting-task count the campaign sees every cycle."""
    orig = campaign_mod._CampaignInstance._collect_telemetry

    def telemetry(self):
        snap = orig(self)
        if cpu_percent is not None:
            snap.node_resources = {
                f"dragon/n{i}": NodeResources(
                    backend="dragon", node_id=f"n{i}", cpu_percent=cpu_percent
                )
                for i in range(2)
            }
        return snap

    monkeypatch.setattr(campaign_mod._CampaignInstance, "_collect_telemetry", telemetry)
    if waiting is not None:
        monkeypatch.setattr(
            campaign_mod._CampaignInstance,
            "_task_observation",
            lambda self: {"waiting": waiting, "submitted": 0, "running": 1},
        )


def _run(ctl, cpus=None, capacity=None, **budget):
    RUNNING.update(now=0, peak=0)
    spec = CampaignSpec(
        name="load",
        budget=CampaignBudget(
            capacity=capacity or {"cpus": 1024},
            max_concurrent_workflows=4,
            max_concurrency_per_workflow=4,
            **budget,
        ),
        controller=ctl,
        workflows=[WorkflowConfig(name="sim", block=work, resources=cpus or {})],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )
    return asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))


def test_without_telemetry_nothing_changes(caplog):
    ctl = Watch(cycles=10)
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        _run(ctl)
    assert all(load is None for load, _ in ctl.seen)
    assert "allowed=" not in caplog.text
    assert RUNNING["peak"] == 4


def test_many_waiting_tasks_allow_fewer_workflows(monkeypatch, caplog):
    _fake_telemetry(monkeypatch, cpu_percent=30.0, waiting=50)  # > 2 × 4
    ctl = Watch(cycles=30)
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        _run(ctl)
    allowed = [load["allowed"] for load, _ in ctl.seen]
    assert allowed[0] == 3 and allowed[-1] == 1  # 4 → 3 → 2 → 1
    assert max(n for _, n in ctl.seen[10:]) <= 1
    assert "[LOAD-load] workflows allowed 4 → 3" in caplog.text
    assert "allowed=1/4" in caplog.text


def test_room_and_few_waiting_tasks_allow_one_more_per_cycle():
    inst = campaign_mod._CampaignInstance.__new__(campaign_mod._CampaignInstance)
    inst.spec = type("Spec", (), {})()
    inst.spec.budget = CampaignBudget(
        capacity={"cpus": 8}, max_concurrent_workflows=4, max_concurrency_per_workflow=4
    )
    inst.name, inst._allowed, inst._util, inst._room, inst._load = "x", 1, None, False, None
    nodes = {"a": NodeResources("dragon", "a", cpu_percent=30)}
    snap = type("Snap", (), {"node_resources": nodes})()
    for expected in (2, 3, 4, 4):  # never above max_concurrent_workflows
        inst._update_load(snap, {"waiting": 0, "submitted": 0})
        assert inst._allowed == expected and inst._room
    inst._update_load(snap, {"waiting": 6, "submitted": 0})  # > 4 // 2, not > 8: keep
    assert inst._allowed == 4 and not inst._room


def test_room_lets_the_capacity_count_overcommit_times(monkeypatch):
    # 4 CPUs per workflow, capacity 8: two fit; with room and overcommit 1.5, three.
    _fake_telemetry(monkeypatch, cpu_percent=20.0, waiting=0)
    ctl = Watch(cycles=25)
    _run(ctl, cpus={"cpus": 4}, capacity={"cpus": 8}, overcommit=1.5)
    assert RUNNING["peak"] == 3


def test_busy_nodes_give_no_room_to_overcommit(monkeypatch):
    _fake_telemetry(monkeypatch, cpu_percent=95.0, waiting=0)
    ctl = Watch(cycles=25)
    _run(ctl, cpus={"cpus": 4}, capacity={"cpus": 8}, overcommit=1.5)
    assert RUNNING["peak"] == 2
    assert all(load["allowed"] == 4 and not load["room"] for load, _ in ctl.seen)


def test_target_utilization_none_turns_it_off(monkeypatch):
    _fake_telemetry(monkeypatch, cpu_percent=20.0, waiting=50)
    ctl = Watch(cycles=10)
    _run(ctl, target_utilization=None)
    assert all(load is None for load, _ in ctl.seen)


def test_gpu_use_counts_when_the_capacity_has_gpus():
    nodes = {"a": NodeResources("dragon", "a", cpu_percent=10, gpu_percent=90)}
    assert campaign_mod._node_utilization(nodes, gpus=True) == pytest.approx(0.9)
    assert campaign_mod._node_utilization(nodes, gpus=False) == pytest.approx(0.1)
    assert campaign_mod._node_utilization({}, gpus=True) is None


@pytest.mark.parametrize(
    "kw",
    [
        {"target_utilization": 0},
        {"target_utilization": 1.5},
        {"max_waiting_tasks": -1},
        {"overcommit": 0.5},
    ],
)
def test_settings_are_checked(kw):
    with pytest.raises(ValueError):
        CampaignBudget(
            capacity={"cpus": 1}, max_concurrent_workflows=1, max_concurrency_per_workflow=1, **kw
        )
