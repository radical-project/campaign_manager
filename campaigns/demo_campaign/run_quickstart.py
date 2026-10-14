#!/usr/bin/env python3
"""
AuCaM quickstart — demonstrates three core campaign-management features
running locally (no HPC, no asyncflow, no config files needed):

  1. Backpressure   — budget pool limits concurrent expensive workflows
  2. Bad-performer  — CancelRule kills slow-trending runs mid-flight
     cancellation
  3. Bandit learning — arm selection shifts toward high-value workflows
                       across two sequential campaigns

Run with:
    cd /work/nvme/bdyk/mgoliyad1/campaign_manager
    python campaigns/demo_campaign/run_quickstart.py

Add -v for verbose CM/scheduler logs.

Three workflow types
--------------------
  fast_sim   2 s wall, score 0.60–0.95 (reliable), cheap  (cpu pool = 1)
  slow_sim   8 s wall, score 0.45–0.80 (moderate),  expensive (cpu pool = 4)
  bad_sim    3 s wall, score trends downward          → cancelled when score < CANCEL_FLOOR

Budget pool: cpu=4.0 → at most 4 fast_sims, OR 1 slow_sim, OR any mix totalling ≤4.
This creates visible backpressure: a slow_sim saturates the pool and queues all new work
until it finishes.

Two campaigns run sequentially (driver mode) so the shared BanditSchedulingPolicy
can shift arm weights based on completion rates and scores.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    GlobalBudget,
    ScoreDirection,
    SchedulingPolicy,
    SubmitResult,
    WorkflowConfig,
)
from src.aucam.controller import CancelRule, PolicyResult
from src.aucam.reporter import WorkflowReporter
from src.aucam.types import WorkflowResult
from src.aucam import BanditSchedulingPolicy
from campaigns.demo_campaign.scheduler import print_policy_summary


# ── Tunable constants ──────────────────────────────────────────────────────────

GOAL_N       = 6        # campaign done after this many fast/slow completions
CANCEL_FLOOR = 0.30     # bad_sim cancelled when score drops below this
MAX_CONCURRENT = 5      # hard cap: no more than N workflows running at once
CPU_POOL     = 4.0      # budget pool size in "cpu units"

# Per-workflow cpu pool cost (charged on launch, returned on finish)
FAST_CPU = 1.0          # fast_sim costs 1 cpu unit
SLOW_CPU = 4.0          # slow_sim costs 4 cpu units  ← causes backpressure
BAD_CPU  = 1.0          # bad_sim costs 1 cpu unit

# Declared wall times for budget math (actual wall ≈ these values)
FAST_WALL_S  = 2.0
SLOW_WALL_S  = 4.0
BAD_WALL_S   = 3.0


# ── Workflow functions ─────────────────────────────────────────────────────────

async def fast_sim(reporter: WorkflowReporter) -> None:
    """Quick simulation, reliably scores 0.60–0.95."""
    score = random.uniform(0.60, 0.75)
    for i in range(4):
        await asyncio.sleep(FAST_WALL_S / 4)
        score = min(1.0, score + random.gauss(0.05, 0.03))
        await reporter.emit(score=score, metrics={"score": score}, iteration=i)
    await reporter.emit(score=score, metrics={"score": score, "status": "complete"}, iteration=4)


async def slow_sim(reporter: WorkflowReporter) -> None:
    """Long simulation, moderate scores, expensive — stresses the budget pool."""
    score = random.uniform(0.45, 0.65)
    for i in range(4):
        await asyncio.sleep(SLOW_WALL_S / 4)
        score = max(0.0, min(1.0, score + random.gauss(0.03, 0.04)))
        await reporter.emit(score=score, metrics={"score": score}, iteration=i)
    await reporter.emit(score=score, metrics={"score": score, "status": "complete"}, iteration=4)


async def bad_sim(reporter: WorkflowReporter) -> None:
    """Starts in the middle, scores trend downward — designed to trigger CancelRule."""
    score = random.uniform(0.35, 0.55)
    for i in range(4):
        await asyncio.sleep(BAD_WALL_S / 4)
        score = max(0.0, score + random.gauss(-0.08, 0.04))
        await reporter.emit(score=score, metrics={"score": score}, iteration=i)
    await reporter.emit(score=score, metrics={"score": score, "status": "complete"}, iteration=4)


# ── Quickstart controller ─────────────────────────────────────────────────────

class QuickstartController(CampaignController):
    """
    Extends CampaignController to add:
      - per-cycle console table showing what AUCAM is doing and why
      - cancel_floor for bad_sim (and optionally all types)
      - goal: GOAL_N completions of fast_sim or slow_sim
    """

    def __init__(self, goal_n: int = GOAL_N, cancel_floor: float = CANCEL_FLOOR,
                 campaign_name: str = "") -> None:
        self._campaign_name = campaign_name
        rules = [
            CancelRule(
                workflow_type="bad_sim",
                # Only cancel after ≥ 2 reports so we don't fire on the first noisy sample.
                when=lambda r: r.iteration >= 2 and r.score is not None and r.score < cancel_floor,
            ),
        ]
        super().__init__(
            launch=["fast_sim", "slow_sim", "bad_sim"],
            goal_type="fast_sim",   # count fast_sim completions toward goal
            goal_n=goal_n,
            rules=rules,
        )
        self._cancel_floor  = cancel_floor
        self._cycle         = 0
        self._scores: list[float] = []
        self._cancelled_total = 0
        self.cycle_log: list[dict] = []  # structured per-cycle data for visualization

    def on_complete(self, result: WorkflowResult) -> None:
        super().on_complete(result)
        if result.returncode == -1:
            self._cancelled_total += 1
        score = result.final_metrics.get("score")
        if score is not None and result.returncode == 0:
            self._scores.append(float(score))

    def is_goal_reached(self) -> bool:
        # Goal: GOAL_N combined completions of fast_sim + slow_sim
        done = (
            self.completed.get("fast_sim", 0)
            + self.completed.get("slow_sim", 0)
        )
        return done >= self._goal_n

    def decide(self, obs: dict) -> PolicyResult:
        self._cycle += 1
        result = super().decide(obs)
        self._print_cycle(obs, result)
        return result

    def get_summary(self) -> dict:
        if not self._scores:
            return {}
        return {
            "best_score":        max(self._scores),
            "avg_score":         sum(self._scores) / len(self._scores),
            "n_scores":          len(self._scores),
            "total_cancelled":   self._cancelled_total,
            "fast_sim_done":     self.completed.get("fast_sim", 0),
            "slow_sim_done":     self.completed.get("slow_sim", 0),
            "bad_sim_cancelled": self.cancelled.get("bad_sim", 0),
        }

    def _print_cycle(self, obs: dict, result: PolicyResult) -> None:
        live   = obs.get("live_reports", {})
        types  = obs.get("wf_types", {})
        q      = obs.get("queue_snapshot")
        budget = obs.get("budget", {})

        q_by_type = getattr(q, "by_type", {}) if q else {}
        q_total   = sum(q_by_type.values())

        # in_use is {campaign_id: {dim: value}} — sum across all campaigns
        in_use_map = budget.get("in_use", {}) if isinstance(budget, dict) else {}
        cpu_used   = sum(v.get("cpu", 0.0) for v in in_use_map.values())
        cpu_cap    = CPU_POOL

        cancelling = set(result.decisions.keys())

        # structured log entry for post-run visualization
        active_by_type: dict[str, int] = {}
        for wf_id, rpt in live.items():
            wtype = types.get(wf_id, "?")
            active_by_type[wtype] = active_by_type.get(wtype, 0) + 1
        self.cycle_log.append({
            "campaign": self._campaign_name,
            "cycle":    self._cycle,
            "active":   dict(active_by_type),
            "queued":   dict(q_by_type),
            "cpu_used": min(cpu_used, cpu_cap),
            "goal":     self.completed.get("fast_sim", 0) + self.completed.get("slow_sim", 0),
            "cancelling": len(cancelling),
            "scores": {
                types.get(wf_id, "?"): round(rpt.score, 3)
                for wf_id, rpt in live.items()
                if rpt.score is not None
            },
        })

        # cpu_used can transiently exceed the cap during launch batching;
        # clamp to cap so the display stays in range.
        cpu_display = min(cpu_used, cpu_cap)
        print(f"\n── Cycle {self._cycle} ─────────────────────────────────────────────")
        print(f"   active={len(live)}  queued={q_total}  "
              f"cpu_pool={cpu_display:.1f}/{cpu_cap:.1f}  "
              f"goal={self.completed.get('fast_sim',0)+self.completed.get('slow_sim',0)}/{self._goal_n}")

        if live:
            print("   Running:")
            for wf_id, rpt in sorted(live.items()):
                wtype = types.get(wf_id, "?")
                age   = time.time() - rpt.emitted_at
                score_s = f"score={rpt.score:.3f}" if rpt.score is not None else "score=—"
                tag   = "  ← CANCELLING (below floor)" if wf_id in cancelling else ""
                print(f"     {wtype:12s}  {wf_id[-8:]}  iter={rpt.iteration}  "
                      f"{score_s}  age={age:.1f}s{tag}")

        if q_by_type:
            queued_str = "  ".join(f"{t}×{n}" for t, n in sorted(q_by_type.items()))
            print(f"   Queued: {queued_str}")

        if result.launch_requests:
            lr_str = "  ".join(lr.workflow_type for lr in result.launch_requests)
            print(f"   Requesting: {lr_str}")

        if result.terminate:
            print("   *** GOAL REACHED — terminating ***")


# ── WorkflowConfig builder ────────────────────────────────────────────────────

def _declared(cpu_units: float) -> dict[str, float]:
    """Produce declared_spending that gives pool.cpu = cpu_units.

    _derive_resources computes: pool.cpu = cpu_hours × (ref_wall_h / declared_wall_h).
    Normalising both wall fields to 1.0 h makes the ratio 1.0, so pool.cpu = cpu_hours.
    """
    return {"wall_hours": 1.0, "cpu_hours": cpu_units}


def build_spec(name: str, policy: SchedulingPolicy, goal_n: int) -> tuple[CampaignSpec, QuickstartController]:
    ctrl = QuickstartController(goal_n=goal_n, campaign_name=name)
    return CampaignSpec(
        name=name,
        budget=CampaignBudget(
            max_concurrent_workflows=MAX_CONCURRENT,
            max_concurrency_per_workflow=2,
            budget={"cpu": CPU_POOL},
            spending={},
        ),
        controller=ctrl,
        workflows=[
            WorkflowConfig(
                name="fast_sim",
                block=fast_sim,
                score_directions={"score": ScoreDirection.HIGHER},
                launch_on_start=True,
                declared_spending=_declared(FAST_CPU),
            ),
            WorkflowConfig(
                name="slow_sim",
                block=slow_sim,
                score_directions={"score": ScoreDirection.HIGHER},
                launch_on_start=True,
                declared_spending=_declared(SLOW_CPU),
            ),
            WorkflowConfig(
                name="bad_sim",
                block=bad_sim,
                score_directions={"score": ScoreDirection.HIGHER},
                launch_on_start=True,
                declared_spending=_declared(BAD_CPU),
            ),
        ],
        policy_interval_s=0.5,
        shutdown_timeout_s=15.0,
        scheduling_policy=policy,
    ), ctrl


# ── Driver ────────────────────────────────────────────────────────────────────

async def _driver(
    cm: CampaignManager,
    policy: SchedulingPolicy,
    n_campaigns: int,
    all_cycles: list,
    campaign_summaries: list,
) -> None:
    for i in range(n_campaigns):
        name = f"quickstart-{chr(ord('a') + i)}"
        print(f"\n{'='*60}")
        print(f"  CAMPAIGN {i+1}/{n_campaigns}: {name}")
        print(f"{'='*60}")

        spec, ctrl = build_spec(name, policy, goal_n=GOAL_N)
        result: SubmitResult = await cm.submit(spec)
        if not result.accepted:
            logging.warning("[DRIVER] %s rejected: %s", name, result.reason)
            continue

        outcome = await result.wait()
        summary = outcome.summary or {}
        all_cycles.extend(ctrl.cycle_log)
        campaign_summaries.append({
            "name":      name,
            "elapsed_s": round(outcome.elapsed_s, 1),
            "completed": dict(outcome.completed),
            "cancelled": dict(outcome.cancelled),
            "best_score": summary.get("best_score"),
            "avg_score":  summary.get("avg_score"),
        })

        print(f"\n── {name} complete ──────────────────────────────────────────────")
        print(f"   goal_reached={outcome.goal_reached}  elapsed={outcome.elapsed_s:.1f}s")
        print(f"   completed: fast_sim={outcome.completed.get('fast_sim',0)}  "
              f"slow_sim={outcome.completed.get('slow_sim',0)}  "
              f"bad_sim={outcome.completed.get('bad_sim',0)}")
        print(f"   cancelled: bad_sim={outcome.cancelled.get('bad_sim',0)}  "
              f"fast_sim={outcome.cancelled.get('fast_sim',0)}  "
              f"slow_sim={outcome.cancelled.get('slow_sim',0)}")
        if summary:
            print(f"   best_score={summary.get('best_score',0):.3f}  "
                  f"avg_score={summary.get('avg_score',0):.3f}  "
                  f"n_scores={summary.get('n_scores',0)}")

        if i < n_campaigns - 1:
            print("\n[DRIVER] Pausing 2s before next campaign...")
            await asyncio.sleep(2.0)

    cm.driver_done()


# ── Main ──────────────────────────────────────────────────────────────────────

def _make_policy(policy_name: str) -> SchedulingPolicy:
    if policy_name == "bandit":
        def _score_reward(result):
            if result.returncode != 0:
                return 0.0
            score = result.final_metrics.get("score")
            return float(score) if score is not None else 0.5
        return BanditSchedulingPolicy(reward_fn=_score_reward)
    from campaigns.demo_campaign.scheduler import make_policy
    return make_policy(policy_name)


async def run(n_campaigns: int, policy_name: str, verbose: bool, json_out: str | None) -> None:
    policy = _make_policy(policy_name)
    cm     = CampaignManager(
        global_budget=GlobalBudget(max_concurrent_campaigns=1),
    )

    all_cycles: list[dict] = []
    campaign_summaries: list[dict] = []

    cm.expect_driver()
    driver_task = asyncio.ensure_future(
        _driver(cm, policy, n_campaigns, all_cycles, campaign_summaries)
    )

    async def _closer():
        try:
            await driver_task
        except Exception:
            logging.exception("[DRIVER] raised")

    asyncio.ensure_future(_closer())

    t0 = time.monotonic()
    await cm.run()
    elapsed = time.monotonic() - t0

    print(f"\n{'='*60}")
    print(f"  QUICKSTART COMPLETE — total {elapsed:.1f}s")
    print(f"{'='*60}")
    print_policy_summary(policy, policy_name)

    if json_out:
        arm_stats = policy.arm_stats() if hasattr(policy, "arm_stats") else {}
        data = {
            "campaigns":    campaign_summaries,
            "cycles":       all_cycles,
            "arm_stats":    arm_stats,
            "cpu_pool_cap": CPU_POOL,
            "cancel_floor": CANCEL_FLOOR,
            "elapsed_s":    round(elapsed, 1),
        }
        Path(json_out).write_text(json.dumps(data, indent=2))
        print(f"\n  JSON log written → {json_out}")

    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for t in pending:
        t.cancel()
    await asyncio.gather(*pending, return_exceptions=True)


def main() -> None:
    p = argparse.ArgumentParser(description="AuCaM quickstart")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("--campaigns", type=int, default=2,
                   help="number of sequential campaigns (default: 2)")
    p.add_argument("--policy", choices=["bandit", "fifo"], default="bandit")
    p.add_argument("--json-out", default=None, metavar="FILE",
                   help="write structured run data to JSON file for visualization")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)-8s %(name)-30s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

    raise SystemExit(asyncio.run(run(args.campaigns, args.policy, args.verbose, args.json_out)))


if __name__ == "__main__":
    main()
