"""B3 follow-ups — the number of finished workflows before learned costs are
used is a setting (CampaignSpec.min_cost_samples), and what was learned about
each workflow type survives a restart (checkpoint)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    PolicyResult,
    WorkflowConfig,
    WorkflowReporter,
    checkpoint_path,
    read_checkpoint,
)

CAPACITY = {"cpus": 1024, "gpus": 64}  # the allocation tests run on


H = 3600.0


async def sim(reporter: WorkflowReporter, context: dict) -> None:
    await asyncio.sleep(0.03)
    await reporter.emit(score=0.7, metrics={"cpu_hours": 4.0 / H, "cpus_in_use": 3})


class Collect(CampaignController):
    def __init__(self, n: int) -> None:
        super().__init__(launch=["sim"])
        self.n = n
        self.results = []
        self.learned: list[dict] = []

    def on_complete(self, result) -> None:
        super().on_complete(result)
        self.results.append(result)

    def decide(self, obs: dict) -> PolicyResult:
        self.learned.append(obs["learned_cost"])
        res = super().decide(obs)
        res.terminate = len(self.results) >= self.n
        return res


def _spec(ctl, tmp_path=None, **kw):
    return CampaignSpec(
        name="learned",
        budget=CampaignBudget(
            capacity=CAPACITY, max_concurrent_workflows=1, max_concurrency_per_workflow=1
        ),
        controller=ctl,
        workflows=[
            WorkflowConfig(
                name="sim", block=sim, bounds={"cpu_hours": (1 / H, 10 / H), "cpus": (1, 16)}
            )
        ],
        policy_interval_s=0.02,
        shutdown_timeout_s=5.0,
        checkpoint_dir=str(tmp_path) if tmp_path else None,
        **kw,
    )


def _run(spec):
    asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))


def test_min_cost_samples_setting():
    ctl = Collect(3)
    _run(_spec(ctl, min_cost_samples=1))
    limits = [round(r.spending_limit["cpu_hours"] * H, 6) for r in ctl.results]
    assert limits == [10.0, 5.0, 5.0]  # learned after the first one
    assert [r.resources["cpus"] for r in ctl.results] == [16, 4, 4]
    with pytest.raises(ValueError):
        _spec(Collect(1), min_cost_samples=0)


def test_learned_state_survives_a_restart(tmp_path: Path):
    _run(_spec(Collect(3), tmp_path))
    ckpt = read_checkpoint(checkpoint_path(str(tmp_path), "learned"))
    assert ckpt.type_cost_stats["sim"]["cpu_hours"]["n"] == 3
    assert ckpt.type_resource_stats["sim"]["cpus"]["mean"] == 3
    assert ckpt.type_totals["sim"]["completed"] == 3

    # A restarted campaign sizes from what was learned at once.
    ctl = Collect(2)
    _run(_spec(ctl, tmp_path))
    assert "sim" in ctl.learned[0]
    assert round(ctl.results[0].spending_limit["cpu_hours"] * H, 6) == 5.0
    assert ctl.results[0].resources["cpus"] == 4
