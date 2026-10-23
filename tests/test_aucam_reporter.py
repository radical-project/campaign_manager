"""
Reporter integration test for AuCaM.

Two campaigns run concurrently:

  scalar  — good_worker emits score=0.9 (float); bad_worker emits score=0.1.
            Controller cancels workers with score < THRESHOLD.

  dict    — sim_worker emits score={"loss": 0.05, "converged": True} (dict).
            Controller reads score["loss"] and score["converged"] to decide.
            A diverging_worker emits score={"loss": 99.0, "converged": False}
            and is cancelled.

Assertions:
  - Scalar campaign: ≥ GOAL_COMPLETIONS good completions; ≥ 1 bad cancellation.
  - Dict campaign: ≥ 1 sim_worker completion; ≥ 1 diverging_worker cancellation.
"""

import asyncio

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignDecision,
    CampaignManager,
    CampaignSpec,
    LaunchRequest,
    PolicyResult,
    WorkflowConfig,
    WorkflowReport,
    WorkflowReporter,
    WorkflowResult,
)

THRESHOLD = 0.5
GOAL_COMPLETIONS = 2
LOSS_THRESHOLD = 1.0  # cancel if loss > this


# ── Workflow coroutines ───────────────────────────────────────────────────────

# -- scalar-score workers --


async def good_worker(reporter: WorkflowReporter) -> None:
    for i in range(5):
        await asyncio.sleep(0.05)
        await reporter.emit(score=0.9, metrics={"iter": i})


async def bad_worker(reporter: WorkflowReporter) -> None:
    i = 0
    while True:  # runs until cancelled
        await asyncio.sleep(0.05)
        await reporter.emit(score=0.1, metrics={"iter": i})
        i += 1


# -- dict-score workers --


async def sim_worker(reporter: WorkflowReporter) -> None:
    """Converging simulation — emits a dict score."""
    for i in range(5):
        await asyncio.sleep(0.05)
        await reporter.emit(
            score={"loss": max(0.01, 0.5 - i * 0.1), "converged": i >= 4},
            metrics={"iter": i},
        )


async def diverging_worker(reporter: WorkflowReporter) -> None:
    """Diverging simulation — should be cancelled."""
    i = 0
    while True:
        await asyncio.sleep(0.05)
        await reporter.emit(
            score={"loss": 99.0 + i, "converged": False},
            metrics={"iter": i},
        )
        i += 1


# ── Controllers ───────────────────────────────────────────────────────────────


class ScoreController(CampaignController):
    """Scalar-score controller: cancel if score (float) < THRESHOLD."""

    def __init__(self) -> None:
        self.completions: int = 0
        self.cancelled: list[str] = []
        self._bad_launched: bool = False

    def decide(self, obs: dict) -> PolicyResult:
        running = obs.get("running", {})
        live: dict[str, WorkflowReport] = obs.get("live_reports", {})
        q = obs["queue_snapshot"]

        decisions: dict[str, CampaignDecision] = {}
        for wf_id, report in live.items():
            if isinstance(report.score, float) and report.score < THRESHOLD:
                decisions[wf_id] = CampaignDecision.CANCEL
                if wf_id not in self.cancelled:
                    self.cancelled.append(wf_id)

        requests: list[LaunchRequest] = []

        good_inflight = (
            self.completions + running.get("good_worker", 0) + q.by_type.get("good_worker", 0)
        )
        if good_inflight < GOAL_COMPLETIONS:
            requests.append(LaunchRequest.make("good_worker", requester="score-ctrl"))

        bad_inflight = running.get("bad_worker", 0) + q.by_type.get("bad_worker", 0)
        if bad_inflight == 0 and not decisions and not self._bad_launched:
            requests.append(LaunchRequest.make("bad_worker", requester="score-ctrl"))
            self._bad_launched = True

        terminate = self.completions >= GOAL_COMPLETIONS and not running and q.total_queued == 0
        return PolicyResult(terminate=terminate, decisions=decisions, launch_requests=requests)


class DictScoreController(CampaignController):
    """Dict-score controller: cancel if score["loss"] > LOSS_THRESHOLD."""

    def __init__(self) -> None:
        self.completions: int = 0
        self.cancelled: list[str] = []
        self._div_launched: bool = False

    def decide(self, obs: dict) -> PolicyResult:
        running = obs.get("running", {})
        live: dict[str, WorkflowReport] = obs.get("live_reports", {})
        q = obs["queue_snapshot"]

        decisions: dict[str, CampaignDecision] = {}
        for wf_id, report in live.items():
            if isinstance(report.score, dict):
                if report.score.get("loss", 0) > LOSS_THRESHOLD:
                    decisions[wf_id] = CampaignDecision.CANCEL
                    if wf_id not in self.cancelled:
                        self.cancelled.append(wf_id)

        requests: list[LaunchRequest] = []

        sim_inflight = (
            self.completions + running.get("sim_worker", 0) + q.by_type.get("sim_worker", 0)
        )
        if sim_inflight < 1:
            requests.append(LaunchRequest.make("sim_worker", requester="dict-ctrl"))

        div_inflight = running.get("diverging_worker", 0) + q.by_type.get("diverging_worker", 0)
        if div_inflight == 0 and not decisions and not self._div_launched:
            requests.append(LaunchRequest.make("diverging_worker", requester="dict-ctrl"))
            self._div_launched = True

        terminate = self.completions >= 1 and not running and q.total_queued == 0
        return PolicyResult(terminate=terminate, decisions=decisions, launch_requests=requests)


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_scalar_score_cancellation() -> None:
    """Float score: cancel workers below threshold, complete the rest."""
    asyncio.run(_run_scalar())


def test_dict_score_cancellation() -> None:
    """Dict score: cancel workers whose score['loss'] exceeds threshold."""
    asyncio.run(_run_dict())


def test_both_campaigns_concurrently() -> None:
    """Scalar and dict campaigns run in the same CampaignManager."""
    asyncio.run(_run_both())


async def _run_scalar() -> None:
    ctrl = ScoreController()

    def good_hook(r: WorkflowResult) -> None:
        if r.returncode == 0:
            ctrl.completions += 1

    spec = CampaignSpec(
        name="scalar",
        budget=CampaignBudget(max_concurrent_workflows=3, max_concurrency_per_workflow=3),
        controller=ctrl,
        workflows=[
            WorkflowConfig(
                name="good_worker",
                block=good_worker,
                launch_on_start=False,
                on_completion=good_hook,
            ),
            WorkflowConfig(name="bad_worker", block=bad_worker, launch_on_start=False),
        ],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )

    cm = CampaignManager()
    cm.submit_campaign(spec)
    await asyncio.wait_for(cm.run(), timeout=15.0)

    assert ctrl.completions >= GOAL_COMPLETIONS, (
        f"Expected {GOAL_COMPLETIONS} good completions, got {ctrl.completions}"
    )
    assert len(ctrl.cancelled) >= 1, "Expected ≥1 bad_worker cancellation"


async def _run_dict() -> None:
    ctrl = DictScoreController()

    def sim_hook(r: WorkflowResult) -> None:
        if r.returncode == 0:
            ctrl.completions += 1

    spec = CampaignSpec(
        name="dict",
        budget=CampaignBudget(max_concurrent_workflows=3, max_concurrency_per_workflow=3),
        controller=ctrl,
        workflows=[
            WorkflowConfig(
                name="sim_worker", block=sim_worker, launch_on_start=False, on_completion=sim_hook
            ),
            WorkflowConfig(name="diverging_worker", block=diverging_worker, launch_on_start=False),
        ],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )

    cm = CampaignManager()
    cm.submit_campaign(spec)
    await asyncio.wait_for(cm.run(), timeout=15.0)

    assert ctrl.completions >= 1, "Expected ≥1 sim_worker completion"
    assert len(ctrl.cancelled) >= 1, "Expected ≥1 diverging_worker cancellation"


async def _run_both() -> None:
    scalar_ctrl = ScoreController()
    dict_ctrl = DictScoreController()

    def good_hook(r: WorkflowResult) -> None:
        if r.returncode == 0:
            scalar_ctrl.completions += 1

    def sim_hook(r: WorkflowResult) -> None:
        if r.returncode == 0:
            dict_ctrl.completions += 1

    scalar_spec = CampaignSpec(
        name="scalar",
        budget=CampaignBudget(max_concurrent_workflows=3, max_concurrency_per_workflow=3),
        controller=scalar_ctrl,
        workflows=[
            WorkflowConfig(
                name="good_worker",
                block=good_worker,
                launch_on_start=False,
                on_completion=good_hook,
            ),
            WorkflowConfig(name="bad_worker", block=bad_worker, launch_on_start=False),
        ],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )
    dict_spec = CampaignSpec(
        name="dict",
        budget=CampaignBudget(max_concurrent_workflows=3, max_concurrency_per_workflow=3),
        controller=dict_ctrl,
        workflows=[
            WorkflowConfig(
                name="sim_worker", block=sim_worker, launch_on_start=False, on_completion=sim_hook
            ),
            WorkflowConfig(name="diverging_worker", block=diverging_worker, launch_on_start=False),
        ],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )

    cm = CampaignManager()
    cm.submit_campaign(scalar_spec)
    cm.submit_campaign(dict_spec)
    await asyncio.wait_for(cm.run(), timeout=15.0)

    assert scalar_ctrl.completions >= GOAL_COMPLETIONS
    assert len(scalar_ctrl.cancelled) >= 1
    assert dict_ctrl.completions >= 1
    assert len(dict_ctrl.cancelled) >= 1
