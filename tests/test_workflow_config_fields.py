"""
Tests for WorkflowConfig fields: supports_pause, stale_timeout_s defaults.

Scenarios
---------
  1. Field defaults — all new fields default correctly; existing tests unaffected.
  4. supports_pause=False (default) — controller issues PAUSE; CM logs warning
     and the workflow continues running; no crash.
  5. supports_pause=True — controller issues PAUSE; CM logs info (no delivery yet);
     workflow continues running; no crash.
"""
from __future__ import annotations

import asyncio
import logging

import pytest

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignDecision,
    CampaignManager,
    CampaignSpec,
    LaunchRequest,
    PolicyResult,
    WorkflowConfig,
    WorkflowReporter,
    WorkflowResult,
)


# ── Workers ───────────────────────────────────────────────────────────────────

async def simple_worker(reporter: WorkflowReporter) -> None:
    for i in range(3):
        await asyncio.sleep(0.05)
        await reporter.emit(score=0.8, metrics={"iter": i})


async def background_worker() -> None:
    await asyncio.sleep(0.3)


# ── Unit tests (no CM needed) ─────────────────────────────────────────────────

def test_new_field_defaults() -> None:
    wfc = WorkflowConfig(name="w", block=simple_worker)
    assert wfc.supports_pause is False
    assert wfc.stale_timeout_s is None


def test_supports_pause_flag() -> None:
    wfc_no  = WorkflowConfig(name="w", block=simple_worker, supports_pause=False)
    wfc_yes = WorkflowConfig(name="w", block=simple_worker, supports_pause=True)
    assert wfc_no.supports_pause is False
    assert wfc_yes.supports_pause is True


# ── Integration: PAUSE guard ──────────────────────────────────────────────────

class PauseIssuingController(CampaignController):
    """Issues PAUSE on first active workflow each cycle, then terminates."""

    def __init__(self) -> None:
        self.completions = 0
        self._pause_issued = False

    def decide(self, obs: dict) -> PolicyResult:
        running = obs.get("running", {})
        live    = obs.get("live_reports", {})
        q       = obs["queue_snapshot"]

        decisions: dict[str, CampaignDecision] = {}

        # Issue PAUSE once to the first active workflow we can see.
        if not self._pause_issued and live:
            wf_id = next(iter(live))
            decisions[wf_id] = CampaignDecision.PAUSE
            self._pause_issued = True

        terminate = (
            self.completions >= 1
            and not running
            and q.total_queued == 0
        )
        return PolicyResult(decisions=decisions, terminate=terminate)


def _run_pause_scenario(supports_pause: bool) -> tuple[int, list[str]]:
    """
    Run a campaign where the controller issues PAUSE.
    Returns (completions, warning_messages).
    """
    ctrl = PauseIssuingController()
    warnings: list[str] = []

    class CapturingHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            if record.levelno == logging.WARNING and "PAUSE" in record.getMessage():
                warnings.append(record.getMessage())

    handler = CapturingHandler()
    log = logging.getLogger("src.aucam.campaign")
    log.addHandler(handler)

    def _on_done(r: WorkflowResult) -> None:
        if r.returncode == 0:
            ctrl.completions += 1

    try:
        spec = CampaignSpec(
            name="pause_guard_test",
            budget=CampaignBudget(max_concurrent_workflows=2, max_concurrency_per_workflow=2),
            controller=ctrl,
            workflows=[
                WorkflowConfig(
                    name="simple_worker",
                    block=simple_worker,
                    supports_pause=supports_pause,
                    launch_on_start=True,
                    on_completion=_on_done,
                ),
                WorkflowConfig(
                    name="background_worker",
                    block=background_worker,
                    supports_pause=supports_pause,
                    launch_on_start=True,
                ),
            ],
            policy_interval_s=0.05,
            shutdown_timeout_s=5.0,
        )
        cm = CampaignManager()
        cm.submit_campaign(spec)
        asyncio.run(asyncio.wait_for(cm.run(), timeout=15.0))
    finally:
        log.removeHandler(handler)

    return ctrl.completions, warnings


def test_pause_ignored_when_supports_pause_false() -> None:
    """
    PAUSE on a supports_pause=False workflow logs a warning and the
    workflow keeps running until it completes normally.
    """
    completions, warnings = _run_pause_scenario(supports_pause=False)
    assert completions >= 1, "simple_worker should complete despite PAUSE attempt"
    assert any("PAUSE ignored" in w for w in warnings), (
        f"Expected 'PAUSE ignored' warning; got: {warnings}"
    )


def test_pause_accepted_when_supports_pause_true(caplog: pytest.LogCaptureFixture) -> None:
    """
    PAUSE on a supports_pause=True workflow logs an info (no delivery yet)
    and the workflow continues running until signal delivery is implemented.
    """
    with caplog.at_level(logging.INFO, logger="src.aucam.campaign"):
        completions, _ = _run_pause_scenario(supports_pause=True)

    assert completions >= 1, "simple_worker should complete even after receiving PAUSE"
    pause_logs = [r.message for r in caplog.records if "PAUSE" in r.message]
    assert any("signal sent" in m or "PAUSE" in m for m in pause_logs), (
        f"Expected PAUSE signal log; got: {pause_logs}"
    )
