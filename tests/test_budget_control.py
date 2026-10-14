"""
Budget-control integration test — three tiers of workflow cancellation.

  Tier 1 — Score-based:         bad_worker   emits score=0.1  → cancelled
  Tier 2 — Per-workflow budget: hog_worker   over-consumes GPU → cancelled
  Tier 3 — Hard campaign limit: campaign terminates once total GPU-hours ≥ limit

Workers
-------
  good_worker  2 GPU  5×0.05 s  score=0.9   completes normally; relaunched
  bad_worker   2 GPU  infinite  score=0.1   killed by score threshold
  hog_worker   8 GPU  infinite  score=0.8   killed by per-wf GPU-hours limit

Budget constants (chosen so all three tiers fire within ~1 s)
-------------------------------------------------------------
  SCORE_THRESHOLD  = 0.5        cancel if score < this
  PER_WF_GPU_LIMIT = 3e-4       GPU-h; cancel a single wf if it exceeds this
  CAMPAIGN_BUDGET  = 1e-3       GPU-h; terminate campaign when total ≥ this

Assertions
----------
  - ≥1 cancellation via score
  - ≥1 cancellation via per-wf resource budget
  - campaign terminated by hard budget, not by natural completion
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

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

# ── Constants ─────────────────────────────────────────────────────────────────

SCORE_THRESHOLD  = 0.5
PER_WF_GPU_LIMIT = 3e-4   # GPU-hours; ~0.135 s at 8 GPU
CAMPAIGN_BUDGET  = 1e-3   # GPU-hours total

# ── Workers ───────────────────────────────────────────────────────────────────

async def good_worker(reporter: WorkflowReporter) -> None:
    for i in range(5):
        await asyncio.sleep(0.05)
        await reporter.emit(score=0.9, metrics={"iter": i})


async def bad_worker(reporter: WorkflowReporter) -> None:
    i = 0
    while True:
        await asyncio.sleep(0.05)
        await reporter.emit(score=0.1, metrics={"iter": i})
        i += 1


async def hog_worker(reporter: WorkflowReporter) -> None:
    i = 0
    while True:
        await asyncio.sleep(0.05)
        await reporter.emit(score=0.8, metrics={"iter": i, "note": "high-gpu"})
        i += 1


# ── Log entry ─────────────────────────────────────────────────────────────────

@dataclass
class CycleLog:
    cycle: int
    t: float                                  # seconds since campaign start
    spent_gpu_h: float
    running: dict[str, int]
    consumption: dict[str, float]             # wf_id → gpu_hours
    decisions: list[dict]                     # [{"wf_id", "tier", "reason"}]
    launches: list[str]
    terminate: bool
    terminate_reason: str = ""


# ── Controller ────────────────────────────────────────────────────────────────

class BudgetController(CampaignController):
    """
    Three-tier controller.

    Tier 1: cancel if score < SCORE_THRESHOLD
    Tier 2: cancel if running_consumption["gpu_hours"] > PER_WF_GPU_LIMIT
    Tier 3: terminate if telemetry.spent["gpu_hours"] >= CAMPAIGN_BUDGET
    """

    def __init__(self) -> None:
        self.good_completions = 0
        self.cancel_score:    list[tuple[str, float]] = []   # (wf_id, score)
        self.cancel_resource: list[tuple[str, float]] = []   # (wf_id, gpu_h)
        self.terminated_by_budget = False
        self.log: list[CycleLog] = []
        self._start_t: float = time.time()
        self._launched_types: set[str] = set()

    def decide(self, obs: dict) -> PolicyResult:
        running: dict[str, int]         = obs.get("running", {})
        live:    dict[str, WorkflowReport] = obs.get("live_reports", {})
        telemetry                        = obs.get("telemetry")
        q                                = obs["queue_snapshot"]

        spent       = getattr(telemetry, "spent",               {}) if telemetry else {}
        consumption = getattr(telemetry, "running_consumption", {}) if telemetry else {}

        decisions:  dict[str, CampaignDecision] = {}
        log_decs:   list[dict] = []

        # ── Tier 1: score ─────────────────────────────────────────────────────
        for wf_id, report in live.items():
            score = report.score
            if isinstance(score, (int, float)) and score < SCORE_THRESHOLD:
                decisions[wf_id] = CampaignDecision.CANCEL
                if not any(x[0] == wf_id for x in self.cancel_score):
                    self.cancel_score.append((wf_id, float(score)))
                log_decs.append({"wf_id": wf_id, "tier": 1, "reason": f"score={score:.2f} < {SCORE_THRESHOLD}"})

        # ── Tier 2: per-workflow GPU budget ───────────────────────────────────
        for wf_id, cons in consumption.items():
            if wf_id in decisions:
                continue
            gpu_h = cons.get("gpu_hours", 0.0)
            if gpu_h > PER_WF_GPU_LIMIT:
                decisions[wf_id] = CampaignDecision.CANCEL
                if not any(x[0] == wf_id for x in self.cancel_resource):
                    self.cancel_resource.append((wf_id, gpu_h))
                log_decs.append({"wf_id": wf_id, "tier": 2, "reason": f"gpu_hours={gpu_h:.5f} > {PER_WF_GPU_LIMIT:.2e}"})

        # ── Tier 3: hard campaign budget ─────────────────────────────────────
        total_gpu_h = spent.get("gpu_hours", 0.0)
        terminate = total_gpu_h >= CAMPAIGN_BUDGET
        terminate_reason = ""
        if terminate:
            self.terminated_by_budget = True
            terminate_reason = f"spent={total_gpu_h:.5f} >= {CAMPAIGN_BUDGET:.2e}"

        # ── Launch requests ───────────────────────────────────────────────────
        requests: list[LaunchRequest] = []
        launches: list[str] = []
        if not terminate:
            for wtype in ("good_worker", "bad_worker", "hog_worker"):
                if wtype not in self._launched_types:
                    requests.append(LaunchRequest.make(wtype, requester="budget-ctrl"))
                    self._launched_types.add(wtype)
                    launches.append(wtype)

            # Relaunch good_worker after each completion.
            g_inflight = running.get("good_worker", 0) + q.by_type.get("good_worker", 0)
            if "good_worker" in self._launched_types and g_inflight == 0:
                requests.append(LaunchRequest.make("good_worker", requester="budget-ctrl"))
                launches.append("good_worker(relaunch)")

        # Natural completion fallback (budget never triggered in edge cases).
        if not terminate and not running and q.total_queued == 0 and self.good_completions >= 5:
            terminate = True
            terminate_reason = "natural completion"

        # ── Log ──────────────────────────────────────────────────────────────
        self.log.append(CycleLog(
            cycle=len(self.log) + 1,
            t=round(time.time() - self._start_t, 3),
            spent_gpu_h=total_gpu_h,
            running=dict(running),
            consumption={wf_id: round(c.get("gpu_hours", 0), 6) for wf_id, c in consumption.items()},
            decisions=log_decs,
            launches=launches,
            terminate=terminate,
            terminate_reason=terminate_reason,
        ))

        return PolicyResult(
            decisions=decisions,
            launch_requests=requests,
            terminate=terminate,
        )


# ── Test ──────────────────────────────────────────────────────────────────────

def test_three_tier_budget_control() -> None:
    ctrl = BudgetController()

    def _on_good_complete(r: WorkflowResult) -> None:
        if r.returncode == 0:
            ctrl.good_completions += 1

    spec = CampaignSpec(
        name="budget_demo",
        budget=CampaignBudget(
            max_concurrent_workflows=3,
            max_concurrency_per_workflow=1,
            budget={"gpu": 14.0},            # concurrency ceiling: 2+2+8+2 reserve
            spending={"gpu_hours": CAMPAIGN_BUDGET},
        ),
        controller=ctrl,
        workflows=[
            WorkflowConfig(name="good_worker", block=good_worker,
                           resources={"gpu": 4.0},
                           declared_spending={"wall_hours": 1.0/3600, "gpu_hours": 4.0/3600},
                           launch_on_start=False, on_completion=_on_good_complete),
            WorkflowConfig(name="bad_worker",  block=bad_worker,
                           resources={"gpu": 2.0},
                           declared_spending={"wall_hours": 1.0/3600, "gpu_hours": 2.0/3600},
                           launch_on_start=False),
            WorkflowConfig(name="hog_worker",  block=hog_worker,
                           resources={"gpu": 8.0},
                           declared_spending={"wall_hours": 1.0/3600, "gpu_hours": 8.0/3600},
                           launch_on_start=False),
        ],
        policy_interval_s=0.05,
        shutdown_timeout_s=10.0,
    )

    cm = CampaignManager()
    cm.submit_campaign(spec)
    asyncio.run(asyncio.wait_for(cm.run(), timeout=30.0))

    _print_summary(ctrl)

    # ── Assertions ────────────────────────────────────────────────────────────
    assert len(ctrl.cancel_score) >= 1, (
        f"Expected ≥1 score-based cancellation; got {ctrl.cancel_score}"
    )
    assert len(ctrl.cancel_resource) >= 1, (
        f"Expected ≥1 resource-based cancellation; got {ctrl.cancel_resource}"
    )
    assert ctrl.terminated_by_budget, (
        "Expected campaign to terminate due to hard GPU budget; it did not"
    )


# ── Summary printer ───────────────────────────────────────────────────────────

def _print_summary(ctrl: BudgetController) -> None:
    w = 62
    print("\n" + "═" * w)
    print("  BUDGET CONTROL TEST SUMMARY")
    print("═" * w)
    print(f"  Hard GPU budget : {CAMPAIGN_BUDGET:.2e} GPU-hours")
    print(f"  Per-wf limit    : {PER_WF_GPU_LIMIT:.2e} GPU-hours")
    print(f"  Score threshold : {SCORE_THRESHOLD}")
    print("─" * w)
    print(f"  Policy cycles   : {len(ctrl.log)}")
    print(f"  Good completions: {ctrl.good_completions}")
    print()

    if ctrl.cancel_score:
        print("  [TIER 1] Score cancellations:")
        for wf_id, score in ctrl.cancel_score:
            print(f"    {wf_id}  score={score:.2f}")

    if ctrl.cancel_resource:
        print("  [TIER 2] Resource cancellations:")
        for wf_id, gpu_h in ctrl.cancel_resource:
            print(f"    {wf_id}  gpu_hours={gpu_h:.5f}")

    term_entry = next((e for e in ctrl.log if e.terminate), None)
    if term_entry:
        print("  [TIER 3] Hard budget triggered:")
        print(f"    cycle={term_entry.cycle}  t={term_entry.t}s  {term_entry.terminate_reason}")

    print()
    print(f"  {'CYC':>3}  {'T(s)':>6}  {'GPU-h spent':>12}  {'DECISIONS'}")
    print("  " + "─" * 56)
    for e in ctrl.log:
        decs = ", ".join(
            f"[T{d['tier']}] {d['wf_id'].split('.')[-1][:12]} {d['reason']}"
            for d in e.decisions
        )
        launches = "+".join(e.launches) if e.launches else ""
        term_mark = " ← TERMINATE" if e.terminate else ""
        print(f"  {e.cycle:>3}  {e.t:>6.3f}  {e.spent_gpu_h:>12.6f}  {decs or launches or '—'}{term_mark}")

    print("═" * w + "\n")
