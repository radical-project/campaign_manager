"""
Observability chunk tests.

1. final_metrics  — WorkflowResult.final_metrics populated from last emit().
2. summary log    — campaign SUMMARY line appears at shutdown with correct counters.
3. max_queue_depth soft warning — queue overflow logs WARNING but does NOT drop.
4. event-driven wakeup — policy cycle fires immediately on workflow completion,
                          not only after policy_interval_s.
"""
from __future__ import annotations

import asyncio
import logging
import time

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignDecision,
    CampaignManager,
    CampaignSpec,
    LaunchPriority,
    LaunchRequest,
    PolicyResult,
    WorkflowConfig,
    WorkflowReporter,
    WorkflowResult,
)


# ── Workers ───────────────────────────────────────────────────────────────────

async def reporting_worker(reporter: WorkflowReporter) -> None:
    for i in range(3):
        await asyncio.sleep(0.02)
        await reporter.emit(score=float(i), metrics={"iteration": i, "loss": 1.0 / (i + 1)})


async def silent_worker() -> None:
    await asyncio.sleep(0.05)


async def quick_worker() -> None:
    await asyncio.sleep(0.05)


# ── Helper: run a minimal campaign and capture log output ─────────────────────

def _run_and_capture(
    spec: CampaignSpec,
) -> tuple[list[WorkflowResult], list[str]]:
    """Run campaign; return (results, info_log_messages)."""
    results: list[WorkflowResult] = []
    log_msgs: list[str] = []

    class Handler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            log_msgs.append(record.getMessage())

    handler = Handler()
    log = logging.getLogger("src.aucam.campaign")
    log.addHandler(handler)

    try:
        ctrl = _SimpleCtrl()
        orig_on_completion = spec.workflows[0].on_completion

        for wfc in spec.workflows:
            def _cap(r: WorkflowResult, _wfc=wfc) -> None:
                results.append(r)
                if _wfc.on_completion is not None:
                    _wfc.on_completion(r)

            object.__setattr__(wfc, "on_completion", _cap)

        asyncio.run(asyncio.wait_for(_run(spec), timeout=30.0))
    finally:
        log.removeHandler(handler)

    return results, log_msgs


async def _run(spec: CampaignSpec) -> None:
    cm = CampaignManager()
    cm.submit_campaign(spec)
    await cm.run()


# ── Simple controller ─────────────────────────────────────────────────────────

class _SimpleCtrl(CampaignController):
    def __init__(self, target: int = 1) -> None:
        self.completions = 0
        self.target = target

    def decide(self, obs: dict) -> PolicyResult:
        running = obs.get("running", {})
        q = obs["queue_snapshot"]
        return PolicyResult(
            terminate=(
                self.completions >= self.target
                and not running
                and q.total_queued == 0
            )
        )


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_final_metrics_populated() -> None:
    """WorkflowResult.final_metrics contains the last emitted metrics dict."""
    results: list[WorkflowResult] = []
    ctrl = _SimpleCtrl(target=1)

    def _on(r: WorkflowResult) -> None:
        results.append(r)
        if r.returncode == 0:
            ctrl.completions += 1

    spec = CampaignSpec(
        name="final_metrics_test",
        budget=CampaignBudget(max_concurrent_workflows=2, max_concurrency_per_workflow=1),
        controller=ctrl,
        workflows=[
            WorkflowConfig(
                name="reporting_worker",
                block=reporting_worker,
                launch_on_start=True,
                on_completion=_on,
            ),
        ],
        policy_interval_s=0.5,
        shutdown_timeout_s=5.0,
    )

    asyncio.run(asyncio.wait_for(_run(spec), timeout=15.0))

    reporting = [r for r in results if "reporting_worker" in r.workflow_type]
    assert reporting, "no results captured"
    r = reporting[0]
    assert r.returncode == 0
    assert "iteration" in r.final_metrics, (
        f"expected 'iteration' in final_metrics; got {r.final_metrics}"
    )
    assert r.final_metrics["iteration"] == 2, (
        f"expected last iteration=2; got {r.final_metrics}"
    )


def test_final_metrics_empty_for_no_reporter() -> None:
    """Workflows without a reporter get final_metrics={}."""
    results: list[WorkflowResult] = []
    ctrl = _SimpleCtrl(target=1)

    def _on(r: WorkflowResult) -> None:
        results.append(r)
        if r.returncode == 0:
            ctrl.completions += 1

    spec = CampaignSpec(
        name="no_reporter_test",
        budget=CampaignBudget(max_concurrent_workflows=1, max_concurrency_per_workflow=1),
        controller=ctrl,
        workflows=[
            WorkflowConfig(
                name="silent_worker",
                block=silent_worker,
                launch_on_start=True,
                on_completion=_on,
            ),
        ],
        policy_interval_s=0.5,
        shutdown_timeout_s=5.0,
    )

    asyncio.run(asyncio.wait_for(_run(spec), timeout=15.0))

    assert results and results[0].final_metrics == {}, (
        f"expected empty final_metrics; got {results[0].final_metrics}"
    )


def test_summary_log_emitted(caplog: pytest.LogCaptureFixture) -> None:
    """SUMMARY log line appears at shutdown with non-zero run/cycle counts."""
    import pytest  # noqa: PLC0415

    ctrl = _SimpleCtrl(target=1)

    def _on(r: WorkflowResult) -> None:
        if r.returncode == 0:
            ctrl.completions += 1

    spec = CampaignSpec(
        name="summary_test",
        budget=CampaignBudget(max_concurrent_workflows=2, max_concurrency_per_workflow=1),
        controller=ctrl,
        workflows=[
            WorkflowConfig(
                name="quick_worker",
                block=quick_worker,
                launch_on_start=True,
                on_completion=_on,
            ),
        ],
        policy_interval_s=0.1,
        shutdown_timeout_s=5.0,
    )

    with caplog.at_level(logging.INFO, logger="src.aucam.campaign"):
        asyncio.run(asyncio.wait_for(_run(spec), timeout=15.0))

    summary_lines = [r.message for r in caplog.records if "SUMMARY" in r.message]
    assert summary_lines, "No SUMMARY log line found"
    summary = summary_lines[0]
    assert "exit=controller_terminate" in summary, summary
    assert "run=1" in summary, summary
    assert "ok=1" in summary, summary


def test_max_queue_depth_warning_not_drop(caplog: pytest.LogCaptureFixture) -> None:
    """Queue overflow at max_queue_depth logs WARNING but all requests are enqueued."""
    import pytest  # noqa: PLC0415

    enqueued_count = [0]
    ctrl = _SimpleCtrl(target=3)

    def _on(r: WorkflowResult) -> None:
        if r.returncode == 0:
            ctrl.completions += 1
            enqueued_count[0] += 1

    # max_queue_depth=2 but we'll enqueue 3 from the policy; all should run.
    class _LaunchingCtrl(_SimpleCtrl):
        def __init__(self) -> None:
            super().__init__(target=3)
            self._launched = False

        def decide(self, obs: dict) -> PolicyResult:
            reqs = []
            if not self._launched:
                self._launched = True
                for _ in range(3):
                    reqs.append(
                        LaunchRequest.make("quick_worker", LaunchPriority.NORMAL)
                    )
            return PolicyResult(
                launch_requests=reqs,
                terminate=(
                    self.completions >= self.target
                    and not obs.get("running", {})
                    and obs["queue_snapshot"].total_queued == 0
                ),
            )

    ctrl2 = _LaunchingCtrl()

    def _on2(r: WorkflowResult) -> None:
        if r.returncode == 0:
            ctrl2.completions += 1
            enqueued_count[0] += 1

    spec = CampaignSpec(
        name="queue_depth_test",
        budget=CampaignBudget(max_concurrent_workflows=5, max_concurrency_per_workflow=5),
        controller=ctrl2,
        workflows=[
            WorkflowConfig(
                name="quick_worker",
                block=quick_worker,
                launch_on_start=False,
                on_completion=_on2,
            ),
        ],
        policy_interval_s=0.1,
        max_queue_depth=2,        # limit set to 2; controller enqueues 3
        shutdown_timeout_s=5.0,
    )

    with caplog.at_level(logging.WARNING, logger="src.aucam.campaign"):
        asyncio.run(asyncio.wait_for(_run(spec), timeout=15.0))

    # Warning was logged
    overflow_warns = [
        r.message for r in caplog.records
        if "max_queue_depth" in r.message and r.levelno == logging.WARNING
    ]
    assert overflow_warns, "Expected max_queue_depth WARNING; none found"

    # But all 3 still completed (not dropped)
    assert enqueued_count[0] == 3, (
        f"Expected 3 completions (soft warning, not drop); got {enqueued_count[0]}"
    )


def test_event_driven_policy_wakeup() -> None:
    """
    Policy wakeup on completion fires significantly before policy_interval_s.

    We set policy_interval_s=10.0 s so if the event-driven wakeup doesn't work
    the test would take >10 s.  With the wakeup it should finish in ~0.1 s.
    """
    ctrl = _SimpleCtrl(target=1)
    completion_times: list[float] = []
    t0 = time.time()

    def _on(r: WorkflowResult) -> None:
        if r.returncode == 0:
            ctrl.completions += 1
            completion_times.append(time.time() - t0)

    spec = CampaignSpec(
        name="event_wake_test",
        budget=CampaignBudget(max_concurrent_workflows=2, max_concurrency_per_workflow=1),
        controller=ctrl,
        workflows=[
            WorkflowConfig(
                name="quick_worker",
                block=quick_worker,
                launch_on_start=True,
                on_completion=_on,
            ),
        ],
        policy_interval_s=10.0,    # very long — only event-driven wake makes this fast
        shutdown_timeout_s=5.0,
    )

    elapsed = time.time()
    asyncio.run(asyncio.wait_for(_run(spec), timeout=15.0))
    elapsed = time.time() - elapsed

    assert elapsed < 3.0, (
        f"Campaign took {elapsed:.2f}s — event-driven wake not firing "
        f"(policy_interval_s=10.0 would require >10 s without it)"
    )
