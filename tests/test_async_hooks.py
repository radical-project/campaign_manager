"""S1 step 1 — decide ahead, apply fast: a scheduling policy's async refresh()
runs in the background, an async decide_async() is awaited with the decide
timeout, and model usage (take_usage()) is counted and charged only when
opted in."""

from __future__ import annotations

import asyncio
import logging

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    LaunchRequest,
    PolicyResult,
    SchedulingPolicy,
    WorkflowConfig,
    WorkflowReporter,
)

CAPACITY = {"cpus": 1024, "gpus": 64}  # the allocation tests run on


async def quick(reporter: WorkflowReporter) -> None:
    await asyncio.sleep(0.02)
    await reporter.emit(score=1.0)


class Recorder(SchedulingPolicy):
    """FIFO; refresh() records what it got, optionally misbehaves, reports tokens."""

    def __init__(self, mode: str = "ok", tokens: float = 0.0) -> None:
        self.mode = mode
        self.tokens = tokens
        self.calls: list[tuple[list[str], set]] = []
        self._usage = 0.0

    async def refresh(self, ctx, queued, obs) -> None:
        self.calls.append(([r.workflow_type for r in queued], set(obs)))
        self._usage += self.tokens
        if self.mode == "raise":
            raise RuntimeError("model down")
        if self.mode == "slow":
            await asyncio.sleep(10)

    def take_usage(self) -> dict[str, float]:
        usage, self._usage = self._usage, 0.0
        return {"llm_tokens": usage} if usage else {}


def _spec(policy=None, controller=None, spending=None, decide_timeout_s=None, n=4):
    return CampaignSpec(
        name="s1",
        budget=CampaignBudget(
            capacity=CAPACITY,
            max_concurrent_workflows=1,
            max_concurrency_per_workflow=1,
            spending=spending or {},
        ),
        controller=controller or CampaignController(launch=["quick"], goal_type="quick", goal_n=n),
        workflows=[
            WorkflowConfig(
                spending_limit={"cpu_hours": 1000.0},
                resources={"cpus": 1},
                name="quick",
                block=quick,
            )
        ],
        scheduling_policy=policy,
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
        decide_timeout_s=decide_timeout_s,
    )


def _run(spec):
    return asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))


# ── refresh() ──────────────────────────────────────────────────────────────────


def test_refresh_gets_the_queue_and_the_observation(caplog):
    pol = Recorder()
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        out = _run(_spec(pol))
    assert out.goal_reached
    assert pol.calls, "refresh() was never called"
    assert any("quick" in types for types, _ in pol.calls)
    assert any({"budget", "queue_snapshot", "allocations"} <= keys for _, keys in pol.calls)
    assert "refresh=ok" in caplog.text


def test_refresh_that_raises_does_not_stop_the_campaign(caplog):
    pol = Recorder(mode="raise")
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        out = _run(_spec(pol))
    assert out.goal_reached
    assert "refresh() raised RuntimeError: model down" in caplog.text
    assert "refresh=error" in caplog.text


def test_slow_refresh_is_cancelled_after_its_timeout(caplog):
    pol = Recorder(mode="slow")
    pol.refresh_timeout_s = 0.1
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        out = _run(_spec(pol, n=8))
    assert out.goal_reached
    assert "took longer than 0.1 s" in caplog.text


def test_policies_without_refresh_are_not_refreshed(caplog):
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        _run(_spec(SchedulingPolicy()))
    assert "refresh=" not in caplog.text


# ── decide_async() ─────────────────────────────────────────────────────────────


class AsyncCtl(CampaignController):
    """Launches 'quick' from decide_async(); the given cycles are slow or raise."""

    def __init__(self, faults: dict[int, str] | None = None, goal_n: int = 4) -> None:
        super().__init__(launch=["quick"], goal_type="quick", goal_n=goal_n)
        self.faults = faults or {}
        self.calls = 0

    async def decide_async(self, obs: dict) -> PolicyResult:
        self.calls += 1
        fault = self.faults.get(self.calls)
        if fault == "raise":
            raise RuntimeError("adr down")
        if fault == "slow":
            await asyncio.sleep(0.2)
        await asyncio.sleep(0)
        res = PolicyResult(terminate=self.is_goal_reached())
        if obs["queue_snapshot"].by_type.get("quick", 0) == 0:
            res.launch_requests.append(LaunchRequest.make("quick"))
        return res


def test_decide_async_is_awaited(caplog):
    ctl = AsyncCtl()
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        out = _run(_spec(controller=ctl))
    assert out.goal_reached and out.exit_reason == "goals_satisfied"
    assert ctl.calls > 0
    assert "decide=controller" in caplog.text
    assert out.controller_errors == 0


def test_decide_async_errors_and_timeouts_fall_back_to_the_rules(caplog):
    ctl = AsyncCtl(faults={1: "raise", 2: "slow"}, goal_n=30)
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        out = _run(_spec(controller=ctl, decide_timeout_s=0.1))
    assert out.goal_reached
    assert "decide=fallback(error)" in caplog.text
    assert "decide=fallback(timeout)" in caplog.text
    assert "decide=controller" in caplog.text
    assert out.controller_errors >= 2


# ── take_usage() ───────────────────────────────────────────────────────────────


def test_model_usage_is_counted_but_not_charged_by_default():
    out = _run(_spec(Recorder(tokens=100)))
    assert out.model_usage.get("llm_tokens", 0) >= 100
    assert "llm_tokens" not in out.spent


def test_model_usage_is_charged_when_the_campaign_limits_it():
    out = _run(_spec(Recorder(tokens=100), spending={"llm_tokens": 1e9}))
    assert out.model_usage["llm_tokens"] >= 100
    assert out.spent["llm_tokens"] == out.model_usage["llm_tokens"]


def test_model_usage_is_charged_with_charge_usage():
    pol = Recorder(tokens=50)
    pol.charge_usage = True
    out = _run(_spec(pol))
    assert out.spent.get("llm_tokens") == out.model_usage["llm_tokens"] >= 50
