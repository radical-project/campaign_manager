"""
End-to-end PAUSE/RESUME signal delivery test.

The campaign has one pausable_worker that calls reporter.poll() at the start
of every iteration and reporter.wait_for_resume() when it sees PAUSE.

A PauseResumeController issues PAUSE at iteration 3 (≈ 0.3 s) then waits two
more cycles before issuing RESUME.  The workflow tracks events in a shared list
so the test can assert the full PAUSE → RESUME → completion lifecycle.

A second worker (no reporter, short-lived) runs alongside to exercise the
multi-workflow path.

Also tests CM_DATA_ENDPOINT injection: LocalDictBackend.endpoint() returns None
so the env var should NOT be set.  When a non-None backend is used (future HPC
tests), it would be "host:port" or a descriptor string.
"""

from __future__ import annotations

import asyncio
import os

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignDecision,
    CampaignManager,
    CampaignSpec,
    PolicyResult,
    WorkflowConfig,
    WorkflowReporter,
    WorkflowResult,
)

# ── Shared state ──────────────────────────────────────────────────────────────

events: list[str] = []  # lifecycle log — written by pausable_worker
endpoint_env: list[str | None] = []  # what CM_DATA_ENDPOINT was set to


# ── Workers ───────────────────────────────────────────────────────────────────


async def pausable_worker(reporter: WorkflowReporter) -> None:
    endpoint_env.append(os.environ.get("CM_DATA_ENDPOINT"))
    for i in range(8):
        sig = await reporter.poll()
        if sig == CampaignDecision.PAUSE:
            events.append(f"PAUSED@{i}")
            await reporter.wait_for_resume()
            events.append(f"RESUMED@{i}")
        await asyncio.sleep(0.1)
        await reporter.emit(score=0.5 + i * 0.05, metrics={"iter": i})
    events.append("COMPLETED")


async def short_worker() -> None:
    await asyncio.sleep(0.15)


# ── Controller ────────────────────────────────────────────────────────────────


class PauseResumeController(CampaignController):
    """Issues PAUSE once, then RESUME after 3 more cycles.  Terminates on completion."""

    def __init__(self) -> None:
        self.completions = 0
        self._cycle = 0
        self._paused_id: str | None = None
        self._pause_cycle: int | None = None

    def decide(self, obs: dict) -> PolicyResult:
        self._cycle += 1
        live = obs.get("live_reports", {})
        running = obs.get("running", {})
        q = obs["queue_snapshot"]
        decisions: dict[str, CampaignDecision] = {}

        # Issue PAUSE to the first live worker at cycle 3.
        if self._cycle == 3 and live and self._paused_id is None:
            self._paused_id = next(iter(live))
            self._pause_cycle = self._cycle
            decisions[self._paused_id] = CampaignDecision.PAUSE

        # Issue RESUME 3 cycles after PAUSE.
        if (
            self._paused_id is not None
            and self._pause_cycle is not None
            and self._cycle == self._pause_cycle + 3
        ):
            decisions[self._paused_id] = CampaignDecision.RESUME
            self._paused_id = None
            self._pause_cycle = None

        terminate = self.completions >= 1 and not running and q.total_queued == 0
        return PolicyResult(decisions=decisions, terminate=terminate)


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_pause_resume_lifecycle() -> None:
    """
    Full PAUSE → RESUME lifecycle through LocalDictBackend signal channel.
    """
    events.clear()
    endpoint_env.clear()

    ctrl = PauseResumeController()

    def _on_done(r: WorkflowResult) -> None:
        if r.returncode == 0 and "pausable_worker" in r.workflow_type:
            ctrl.completions += 1

    spec = CampaignSpec(
        name="pause_resume_test",
        budget=CampaignBudget(max_concurrent_workflows=3, max_concurrency_per_workflow=2),
        controller=ctrl,
        workflows=[
            WorkflowConfig(
                name="pausable_worker",
                block=pausable_worker,
                supports_pause=True,
                launch_on_start=True,
                on_completion=_on_done,
            ),
            WorkflowConfig(
                name="short_worker",
                block=short_worker,
                launch_on_start=True,
            ),
        ],
        policy_interval_s=0.1,
        shutdown_timeout_s=10.0,
    )

    cm = CampaignManager()
    cm.submit_campaign(spec)
    asyncio.run(asyncio.wait_for(cm.run(), timeout=30.0))

    # Workflow completed
    assert "COMPLETED" in events, f"Worker never completed; events={events}"

    # A PAUSE was observed
    paused = [e for e in events if e.startswith("PAUSED@")]
    assert paused, f"No PAUSE observed; events={events}"

    # A RESUME followed the PAUSE
    resumed = [e for e in events if e.startswith("RESUMED@")]
    assert resumed, f"No RESUME observed; events={events}"

    # PAUSE came before RESUME in the lifecycle
    assert events.index(paused[0]) < events.index(resumed[0]), (
        f"RESUME appeared before PAUSE; events={events}"
    )

    # LocalDictBackend.endpoint() == None → CM_DATA_ENDPOINT not injected
    assert endpoint_env and endpoint_env[0] is None, (
        f"Expected CM_DATA_ENDPOINT=None for LocalDictBackend; got {endpoint_env}"
    )


def test_local_dict_backend_endpoint_is_none() -> None:
    """LocalDictBackend.endpoint() returns None (in-process, no external connection)."""
    from src.aucam.data_backend import LocalDictBackend

    assert LocalDictBackend().endpoint() is None


def test_local_dict_signal_roundtrip() -> None:
    """write_signal / poll_signal / clear_signal round-trip on LocalDictBackend."""
    from src.aucam.data_backend import LocalDictBackend

    async def _run():
        backend = LocalDictBackend()
        wf_id = "test_wf"

        assert await backend.poll_signal(wf_id) is None

        await backend.write_signal(wf_id, CampaignDecision.PAUSE)
        assert await backend.poll_signal(wf_id) == CampaignDecision.PAUSE

        await backend.clear_signal(wf_id)
        assert await backend.poll_signal(wf_id) is None

        await backend.write_signal(wf_id, CampaignDecision.RESUME)
        reporter = backend.make_reporter(wf_id)
        sig = await reporter.poll()
        assert sig == CampaignDecision.RESUME
        # Consumed — next poll returns None
        assert await reporter.poll() is None

    asyncio.run(_run())
