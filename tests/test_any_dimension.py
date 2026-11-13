"""B0 — dimensions other than CPU/GPU hours (e.g. llm_tokens) are charged from
the workflow's reports, also when its CPU/GPU hours come from task telemetry."""

from __future__ import annotations

import asyncio
import logging

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    PolicyResult,
    WorkflowConfig,
    WorkflowReporter,
)
from src.aucam.campaign import _CampaignInstance

CAPACITY = {"cpus": 1024, "gpus": 64}  # the allocation tests run on


async def agent(reporter: WorkflowReporter) -> None:
    await asyncio.sleep(0.03)
    await reporter.emit(score=1.0, metrics={"llm_tokens": 1500, "cpu_hours": 0.01})


class N(CampaignController):
    def __init__(self, n: int) -> None:
        super().__init__(launch=["agent"])
        self.n = n
        self.results = []

    def on_complete(self, result) -> None:
        super().on_complete(result)
        self.results.append(result)

    def decide(self, obs: dict) -> PolicyResult:
        res = super().decide(obs)
        res.terminate = len(self.results) >= self.n
        return res


def _run(ctl, spending):
    spec = CampaignSpec(
        name="dims",
        budget=CampaignBudget(
            capacity=CAPACITY,
            max_concurrent_workflows=1,
            max_concurrency_per_workflow=1,
            spending=spending,
        ),
        controller=ctl,
        workflows=[
            WorkflowConfig(
                spending_limit={"cpu_hours": 1000.0},
                resources={"cpus": 1},
                name="agent",
                block=agent,
            )
        ],  # nothing declared
        policy_interval_s=0.02,
        shutdown_timeout_s=5.0,
    )
    return asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))


def test_reported_dimension_of_the_campaign_limit_is_charged(caplog):
    ctl = N(2)
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        out = _run(ctl, {"llm_tokens": 1e6})
    assert out.spent["llm_tokens"] == 3000
    assert ctl.results[0].spent["llm_tokens"] == 1500
    assert "llm_tokens=1500" in caplog.text  # [SPEND] wf_done
    assert "spent_llm_tokens=" in caplog.text  # [SPEND] campaign


def test_task_charged_workflow_still_gets_other_dimensions(monkeypatch):
    # Pretend AsyncFlow telemetry charged this workflow's CPU/GPU per task.
    orig = _CampaignInstance._launch

    def launch(self, request, alloc=None):
        orig(self, request, alloc)
        for wf_id in self._active:
            self._task_level.add(wf_id)
            self._task_usage.setdefault(wf_id, {"cpu_hours": 0.5})

    monkeypatch.setattr(_CampaignInstance, "_launch", launch)
    ctl = N(1)
    out = _run(ctl, {"llm_tokens": 1e6})
    r = ctl.results[0]
    assert r.spent == {"cpu_hours": 0.5, "llm_tokens": 1500}
    assert out.spent["llm_tokens"] == 1500
    assert "cpu_hours" not in out.spent or out.spent["cpu_hours"] != 0.01  # not double-charged
