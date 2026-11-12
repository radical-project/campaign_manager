"""C1 — controller fallback: a decide() that raises or is too slow does not end
the campaign; AuCaM uses the built-in rules for that cycle and logs which
decide made each cycle's decisions."""

from __future__ import annotations

import asyncio
import logging
import time

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    PolicyResult,
    WorkflowConfig,
    WorkflowReporter,
)


async def quick(reporter: WorkflowReporter) -> None:
    await asyncio.sleep(0.05)
    await reporter.emit(score=1.0)


class Faulty(CampaignController):
    """Rules: keep launching 'quick'; goal: goal_n completions.  decide() misbehaves
    in the given cycles ('raise', 'slow' or 'wrong'), otherwise uses the rules."""

    def __init__(self, faults: dict[int, str], slow_s: float = 1.0, goal_n: int = 3) -> None:
        super().__init__(launch=["quick"], goal_type="quick", goal_n=goal_n)
        self._faults = faults
        self._slow_s = slow_s
        self.calls = 0

    def decide(self, obs: dict) -> PolicyResult:
        self.calls += 1
        fault = self._faults.get(self.calls)
        if fault == "raise":
            raise RuntimeError("controller bug")
        if fault == "slow":
            time.sleep(self._slow_s)
        if fault == "wrong":
            return None  # type: ignore[return-value]
        return super().decide(obs)


def _run(ctl, decide_timeout_s=None):
    spec = CampaignSpec(
        name="fallback",
        budget=CampaignBudget(max_concurrent_workflows=1, max_concurrency_per_workflow=1),
        controller=ctl,
        workflows=[WorkflowConfig(name="quick", block=quick)],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
        decide_timeout_s=decide_timeout_s,
    )
    return asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))


def test_decide_raising_uses_fallback_and_campaign_reaches_goal(caplog):
    ctl = Faulty({1: "raise", 2: "raise", 3: "wrong"}, goal_n=8)
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        out = _run(ctl)
    assert out.goal_reached
    assert out.controller_errors == 3
    text = caplog.text
    assert "decide() raised RuntimeError: controller bug" in text
    assert "decide=fallback(error)" in text
    assert "decide=controller" in text
    assert "controller_errors=3" in text  # SUMMARY line


def test_slow_decide_times_out_and_later_cycles_fall_back_until_it_returns(caplog):
    ctl = Faulty({1: "slow"}, slow_s=0.3, goal_n=12)
    t0 = time.monotonic()
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        out = _run(ctl, decide_timeout_s=0.1)
    assert out.goal_reached
    assert out.controller_errors == 1  # the timeout; "busy" cycles are not errors
    text = caplog.text
    assert "decide() took longer than 0.1 s" in text
    assert "decide=fallback(timeout)" in text
    assert "decide=fallback(busy)" in text
    assert "decide=controller" in text
    assert time.monotonic() - t0 < 20


def test_stuck_decide_does_not_block_campaign_end():
    ctl = Faulty({n: "slow" for n in range(1, 100)}, slow_s=60.0)
    t0 = time.monotonic()
    out = _run(ctl, decide_timeout_s=0.1)
    assert out.goal_reached  # the built-in rules carried the campaign
    assert time.monotonic() - t0 < 20


def test_no_errors_when_decide_behaves():
    out = _run(Faulty({}))
    assert out.goal_reached and out.controller_errors == 0


class BadObserve(Faulty):
    """observe() raises in its first two cycles."""

    def __init__(self) -> None:
        super().__init__({}, goal_n=3)
        self.observed = 0

    def observe(self, **kw):
        self.observed += 1
        if self.observed <= 2:
            raise ValueError("observe bug")
        return super().observe(**kw)


def test_observe_raising_uses_base_observe(caplog):
    ctl = BadObserve()
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        out = _run(ctl)
    assert out.goal_reached
    assert out.controller_errors == 2
    assert "observe() raised ValueError: observe bug" in caplog.text
