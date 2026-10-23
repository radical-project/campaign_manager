"""
Multi-campaign integration test.

Three campaigns run concurrently through a single CampaignManager:

  Alpha  — Runs 3 parallel short jobs concurrently; controller re-enqueues
           up to 3 workers total, terminates after all 3 complete.

  Beta   — DAG pipeline: prep → [compute_a, compute_b] → report.
           Each compute is auto-enqueued by DAG when prep finishes.
           Report is enqueued by controller after both computes complete.
           Exercises multi-source DAG and controller-driven launch.

  Gamma  — Adaptive serial: controller re-enqueues gamma_task until
           4 total completions (sequential, max_concurrent=1).

Collects per-workflow timing, per-policy-cycle observations, and
per-campaign start/end times, writing JSON to a results file.
"""

import asyncio
import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    LaunchRequest,
    PolicyResult,
    WorkflowConfig,
    WorkflowDependency,
    WorkflowResult,
)

RESULTS_FILE = Path(__file__).parent / "multi_campaign_results.json"
T0: float = 0.0


# ── Shared timing recorder ────────────────────────────────────────────────────


@dataclass
class WFEvent:
    campaign: str
    workflow_type: str
    workflow_id: str
    launch_t: float
    end_t: float
    returncode: int


@dataclass
class PolicyEvent:
    campaign: str
    t: float
    running: dict
    queue_depth: int
    terminate: bool


@dataclass
class Results:
    campaign_starts: dict = field(default_factory=dict)
    campaign_ends: dict = field(default_factory=dict)
    workflows: list = field(default_factory=list)
    policy_events: list = field(default_factory=list)


RESULTS = Results()


def record_wf(campaign: str, r: WorkflowResult) -> None:
    end_t = time.monotonic() - T0
    launch_t = end_t - r.wall_time_s
    RESULTS.workflows.append(
        asdict(
            WFEvent(
                campaign=campaign,
                workflow_type=r.workflow_type,
                workflow_id=r.workflow_id,
                launch_t=launch_t,
                end_t=end_t,
                returncode=r.returncode,
            )
        )
    )


def record_policy(campaign: str, running: dict, queue_depth: int, terminate: bool) -> None:
    RESULTS.policy_events.append(
        asdict(
            PolicyEvent(
                campaign=campaign,
                t=time.monotonic() - T0,
                running=running,
                queue_depth=queue_depth,
                terminate=terminate,
            )
        )
    )


# ── Workflow coroutines ───────────────────────────────────────────────────────


async def alpha_worker():
    await asyncio.sleep(0.25)


async def beta_prep():
    await asyncio.sleep(0.15)


async def beta_compute_a():
    await asyncio.sleep(0.30)


async def beta_compute_b():
    await asyncio.sleep(0.20)


async def beta_report():
    await asyncio.sleep(0.10)


async def gamma_task():
    await asyncio.sleep(0.18)


# ── Alpha ─────────────────────────────────────────────────────────────────────


class AlphaController(CampaignController):
    """Launch 3 parallel workers; terminate after all 3 complete."""

    TARGET = 3

    def __init__(self):
        self.done = 0

    def decide(self, obs: dict) -> PolicyResult:
        running = obs.get("running", {})
        q = obs["queue_snapshot"]

        in_flight = self.done + running.get("alpha_worker", 0) + q.by_type.get("alpha_worker", 0)
        requests = [
            LaunchRequest.make("alpha_worker", requester="alpha-ctrl")
            for _ in range(self.TARGET - in_flight)
            if in_flight < self.TARGET
        ]

        terminate = self.done >= self.TARGET and not running and q.total_queued == 0
        record_policy("alpha", running, q.total_queued, terminate)
        return PolicyResult(terminate=terminate, launch_requests=requests)


def alpha_spec(ctrl: AlphaController) -> CampaignSpec:
    def hook(r: WorkflowResult):
        ctrl.done += 1
        record_wf("alpha", r)

    return CampaignSpec(
        name="alpha",
        budget=CampaignBudget(max_concurrent_workflows=3, max_concurrency_per_workflow=3),
        controller=ctrl,
        workflows=[
            WorkflowConfig(
                name="alpha_worker", block=alpha_worker, launch_on_start=False, on_completion=hook
            ),
        ],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )


# ── Beta ──────────────────────────────────────────────────────────────────────


class BetaController(CampaignController):
    """
    DAG pipeline: prep → [compute_a, compute_b] in parallel → report.

    prep is launched at start.  compute_a and compute_b are auto-enqueued
    by the DAG when prep completes.  report is launched by the controller
    once both computes have finished.
    """

    def __init__(self):
        self.done: dict[str, int] = {}

    def decide(self, obs: dict) -> PolicyResult:
        running = obs.get("running", {})
        q = obs["queue_snapshot"]

        requests = []
        ca_done = self.done.get("beta_compute_a", 0)
        cb_done = self.done.get("beta_compute_b", 0)
        report_done = self.done.get("beta_report", 0)
        report_queued = q.by_type.get("beta_report", 0)
        report_running = running.get("beta_report", 0)

        # Launch report once both computes are done and report hasn't been requested yet.
        if (
            ca_done >= 1
            and cb_done >= 1
            and report_done == 0
            and report_queued == 0
            and report_running == 0
        ):
            requests.append(LaunchRequest.make("beta_report", requester="beta-ctrl"))

        terminate = report_done >= 1 and not running and q.total_queued == 0
        record_policy("beta", running, q.total_queued, terminate)
        return PolicyResult(terminate=terminate, launch_requests=requests)


def beta_spec(ctrl: BetaController) -> CampaignSpec:
    def mk_hook(name: str):
        def h(r: WorkflowResult):
            ctrl.done[name] = ctrl.done.get(name, 0) + 1
            record_wf("beta", r)

        return h

    return CampaignSpec(
        name="beta",
        budget=CampaignBudget(max_concurrent_workflows=3, max_concurrency_per_workflow=1),
        controller=ctrl,
        workflows=[
            WorkflowConfig(
                name="beta_prep",
                block=beta_prep,
                launch_on_start=True,
                on_completion=mk_hook("beta_prep"),
            ),
            WorkflowConfig(
                name="beta_compute_a",
                block=beta_compute_a,
                launch_on_start=False,
                on_completion=mk_hook("beta_compute_a"),
            ),
            WorkflowConfig(
                name="beta_compute_b",
                block=beta_compute_b,
                launch_on_start=False,
                on_completion=mk_hook("beta_compute_b"),
            ),
            # report is launched by controller, not by DAG
            WorkflowConfig(
                name="beta_report",
                block=beta_report,
                launch_on_start=False,
                on_completion=mk_hook("beta_report"),
            ),
        ],
        dependencies=[
            WorkflowDependency(source="beta_prep", target="beta_compute_a"),
            WorkflowDependency(source="beta_prep", target="beta_compute_b"),
        ],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )


# ── Gamma ─────────────────────────────────────────────────────────────────────


class GammaController(CampaignController):
    """Serial adaptive: re-enqueue gamma_task until 4 total completions."""

    TARGET = 4

    def __init__(self):
        self.total_done = 0

    def decide(self, obs: dict) -> PolicyResult:
        running = obs.get("running", {})
        q = obs["queue_snapshot"]

        in_flight = self.total_done + running.get("gamma_task", 0) + q.by_type.get("gamma_task", 0)
        requests = []
        if in_flight < self.TARGET:
            requests.append(LaunchRequest.make("gamma_task", requester="gamma-ctrl"))

        terminate = self.total_done >= self.TARGET and not running and q.total_queued == 0
        record_policy("gamma", running, q.total_queued, terminate)
        return PolicyResult(terminate=terminate, launch_requests=requests)


def gamma_spec(ctrl: GammaController) -> CampaignSpec:
    def hook(r: WorkflowResult):
        ctrl.total_done += 1
        record_wf("gamma", r)

    return CampaignSpec(
        name="gamma",
        budget=CampaignBudget(max_concurrent_workflows=1, max_concurrency_per_workflow=1),
        controller=ctrl,
        workflows=[
            WorkflowConfig(
                name="gamma_task", block=gamma_task, launch_on_start=False, on_completion=hook
            ),
        ],
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )


# ── Test ──────────────────────────────────────────────────────────────────────


def test_multi_campaign() -> None:
    asyncio.run(_run())


async def _run() -> None:
    global T0
    T0 = time.monotonic()

    alpha_ctrl = AlphaController()
    beta_ctrl = BetaController()
    gamma_ctrl = GammaController()

    cm = CampaignManager()
    cm.submit_campaign(alpha_spec(alpha_ctrl))
    cm.submit_campaign(beta_spec(beta_ctrl))
    cm.submit_campaign(gamma_spec(gamma_ctrl))

    await asyncio.wait_for(cm.run(), timeout=15.0)

    end_t = time.monotonic() - T0

    # Assertions
    assert alpha_ctrl.done >= AlphaController.TARGET, (
        f"alpha: expected {AlphaController.TARGET} completions, got {alpha_ctrl.done}"
    )
    assert beta_ctrl.done.get("beta_prep", 0) >= 1, "beta: prep missing"
    assert beta_ctrl.done.get("beta_compute_a", 0) >= 1, "beta: compute_a missing"
    assert beta_ctrl.done.get("beta_compute_b", 0) >= 1, "beta: compute_b missing"
    assert beta_ctrl.done.get("beta_report", 0) >= 1, (
        f"beta: report did not complete: {beta_ctrl.done}"
    )
    assert gamma_ctrl.total_done >= GammaController.TARGET, (
        f"gamma: expected {GammaController.TARGET} completions, got {gamma_ctrl.total_done}"
    )

    # Derive per-campaign timings from workflow events
    for campaign in ("alpha", "beta", "gamma"):
        wfs = [w for w in RESULTS.workflows if w["campaign"] == campaign]
        if wfs:
            RESULTS.campaign_starts[campaign] = min(w["launch_t"] for w in wfs)
            RESULTS.campaign_ends[campaign] = max(w["end_t"] for w in wfs)

    RESULTS_FILE.write_text(
        json.dumps(
            {
                "campaign_starts": RESULTS.campaign_starts,
                "campaign_ends": RESULTS.campaign_ends,
                "workflows": RESULTS.workflows,
                "policy_events": RESULTS.policy_events,
                "total_elapsed_s": end_t,
            },
            indent=2,
        )
    )

    print(f"\nResults written to {RESULTS_FILE}")
    print(f"Total elapsed: {end_t:.2f}s")
    print(f"Alpha done: {alpha_ctrl.done}")
    print(f"Beta done: {beta_ctrl.done}")
    print(f"Gamma done: {gamma_ctrl.total_done}")
    print(f"Workflows recorded: {len(RESULTS.workflows)}")
