#!/usr/bin/env python3
"""
AuCaM capability demo — what AuCaM adds over launching workflows by hand.

One driver runs two campaigns in sequence:

  Campaign 1 "explore" — two workflow types compete for one workflow slot:
      fast_good  1 CPU · 1.5 s · score 0.75–0.95, completes
      slow_poor  4 CPU · 5 s   · score drifts down → cancelled at ~3 s
    Shows:
      1. Cancellation — the controller's CancelRule stops slow_poor runs whose
         score falls below a floor, so they never use their full cost.
      2. Bandit scheduling — the campaign's UCB1 bandit tries both types,
         learns that slow_poor never completes, and launches it less and less.
      3. Spending accounting — every run is charged in AuCaM's ledger
         (prorated cpu_hours), reported back in CampaignOutcome.spent.

  Campaign 2 "follow-up" — the driver reads campaign 1's CampaignOutcome
    (completions, cancellations, spending, the controller's summary), drops
    workflow types that never completed, and submits a new campaign.
      4. Driver feedback loop — results flow back to the driver, which plans
         the next campaign.

Run:
    cd $WORK_DIR/campaign_manager
    python campaigns/demo_campaign/run_impact.py --json-out /tmp/impact_run.json
    python campaigns/demo_campaign/gen_impact_fig.py --json-in /tmp/impact_run.json
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

from src.aucam import (  # noqa: E402
    BanditSchedulingPolicy,
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    WorkflowConfig,
)
from src.aucam.controller import CancelRule, PolicyResult  # noqa: E402
from src.aucam.reporter import WorkflowReporter  # noqa: E402
from src.aucam.types import CampaignOutcome, WorkflowResult  # noqa: E402

# ── Parameters ─────────────────────────────────────────────────────────────────
FAST_WALL_S       = 1.5
SLOW_WALL_S       = 5.0
FAST_CPU          = 1.0
SLOW_CPU          = 4.0
CANCEL_FLOOR      = 0.30
GOAL_N            = 20      # fast_good completions per campaign
POLICY_INTERVAL_S = 0.5
EXPLORATION       = 2.0     # UCB1 exploration weight
SEED              = 7


def _declared(cpu: float, wall_s: float) -> dict[str, float]:
    """Expected cost of one run: cpu CPUs for wall_s seconds."""
    return {"wall_hours": wall_s / 3600.0, "cpu_hours": cpu * wall_s / 3600.0}


# ── Workflows ──────────────────────────────────────────────────────────────────

async def fast_good(reporter: WorkflowReporter) -> None:
    score = random.uniform(0.75, 0.95)
    n_iter = 3
    for i in range(n_iter):
        await asyncio.sleep(FAST_WALL_S / n_iter)
        score = min(1.0, score + random.gauss(0.02, 0.04))
        await reporter.emit(score=score, iteration=i)


async def slow_poor(reporter: WorkflowReporter) -> None:
    # Score drifts down; expected cancel at iteration 2 (t≈3 s).
    score = random.uniform(0.30, 0.45)
    n_iter = 5
    for i in range(n_iter):
        await asyncio.sleep(SLOW_WALL_S / n_iter)
        score = max(0.0, score + random.gauss(-0.07, 0.03))
        await reporter.emit(score=score, iteration=i)


WORKFLOWS = {
    "fast_good": WorkflowConfig(name="fast_good", block=fast_good,
                                declared_spending=_declared(FAST_CPU, FAST_WALL_S)),
    "slow_poor": WorkflowConfig(name="slow_poor", block=slow_poor,
                                declared_spending=_declared(SLOW_CPU, SLOW_WALL_S)),
}


# ── Controller ─────────────────────────────────────────────────────────────────

class DemoController(CampaignController):
    """Keeps one request of each type queued, cancels low-scoring slow_poor
    runs, and stops after GOAL_N fast_good completions.  Records every finished
    workflow for the figure and returns per-type statistics to the driver."""

    def __init__(self, types: list[str]) -> None:
        super().__init__(
            launch=types,
            goal_type="fast_good",
            goal_n=GOAL_N,
            rules=[CancelRule(
                workflow_type="slow_poor",
                when=lambda r: (r.iteration >= 2 and r.score is not None
                                and r.score < CANCEL_FLOOR),
            )],
        )
        self.t0 = time.monotonic()
        self.runs: list[dict] = []

    def decide(self, obs: dict) -> PolicyResult:
        return super().decide(obs)

    def on_complete(self, result: WorkflowResult) -> None:
        super().on_complete(result)
        self.runs.append({
            "type":     result.workflow_type,
            "rc":       result.returncode,
            "wall_s":   round(result.wall_time_s, 2),
            "end_s":    round(time.monotonic() - self.t0, 2),
            "cpu_s":    round(result.spent.get("cpu_hours", 0.0) * 3600.0, 2),
            "score":    result.final_score,
        })

    def get_summary(self) -> dict:
        """Returned to the driver in CampaignOutcome.summary."""
        by_type: dict[str, dict] = {}
        for r in self.runs:
            s = by_type.setdefault(r["type"], {"runs": 0, "completed": 0,
                                               "cancelled": 0, "cpu_s": 0.0})
            s["runs"] += 1
            s["completed"] += r["rc"] == 0
            s["cancelled"] += r["rc"] == -1
            s["cpu_s"] = round(s["cpu_s"] + r["cpu_s"], 2)
        return {"by_type": by_type, "runs": self.runs}


def _reward(result: WorkflowResult) -> float:
    """Bandit reward: 1 for a completed run, 0 for a cancelled or failed one."""
    return 1.0 if result.returncode == 0 else 0.0


def _spec(name: str, types: list[str]) -> tuple[CampaignSpec, DemoController]:
    ctrl = DemoController(types)
    spec = CampaignSpec(
        name=name,
        workflows=[WORKFLOWS[t] for t in types],
        controller=ctrl,
        budget=CampaignBudget(max_concurrent_workflows=1, max_concurrency_per_workflow=1),
        scheduling_policy=BanditSchedulingPolicy(reward_fn=_reward, exploration=EXPLORATION),
        policy_interval_s=POLICY_INTERVAL_S,
    )
    return spec, ctrl


# ── Driver ─────────────────────────────────────────────────────────────────────

def plan_next(outcome: CampaignOutcome) -> list[str]:
    """Driver logic: keep the workflow types that completed at least once."""
    by_type = outcome.summary.get("by_type", {})
    return [t for t, s in by_type.items() if s["completed"] > 0]


def _record(outcome: CampaignOutcome, types: list[str]) -> dict:
    by_type = outcome.summary.get("by_type", {})
    return {
        "name":       outcome.campaign_name,
        "types":      types,
        "elapsed_s":  round(outcome.elapsed_s, 2),
        "goal_reached": outcome.goal_reached,
        "completed":  dict(outcome.completed),
        "cancelled":  dict(outcome.cancelled),
        "cpu_s":      round(outcome.spent.get("cpu_hours", 0.0) * 3600.0, 2),
        "by_type":    by_type,
        "runs":       outcome.summary.get("runs", []),
    }


def _print(rec: dict) -> None:
    print(f"  elapsed={rec['elapsed_s']:.1f}s  goal_reached={rec['goal_reached']}  "
          f"cpu_spent={rec['cpu_s']:.1f} CPU·s")
    for t, s in sorted(rec["by_type"].items()):
        print(f"    {t:10s} runs={s['runs']:2d}  completed={s['completed']:2d}  "
              f"cancelled={s['cancelled']:2d}  cpu={s['cpu_s']:.1f} CPU·s")


async def driver(cm: CampaignManager, results: list) -> None:
    try:
        types = ["fast_good", "slow_poor"]
        for name in ("explore", "follow-up"):
            print(f"\n── Campaign {name}: types={types}")
            spec, _ = _spec(name, types)
            sub = await cm.submit(spec)
            if not sub.accepted:
                print(f"  rejected: {sub.reason}")
                return
            outcome = await sub.wait()
            rec = _record(outcome, types)
            results.append(rec)
            _print(rec)
            types = plan_next(outcome)
            dropped = sorted(set(rec["types"]) - set(types))
            if name == "explore":
                print(f"  driver: dropping {dropped or 'nothing'} for the next campaign")
    finally:
        cm.driver_done()


async def run(json_out: str | None) -> None:
    random.seed(SEED)
    cm = CampaignManager()
    cm.expect_driver()
    results: list = []
    t0 = time.monotonic()
    task = asyncio.ensure_future(driver(cm, results))
    await cm.run()
    await task
    print(f"\nTotal {time.monotonic() - t0:.1f}s")

    if json_out:
        payload = {
            "campaigns": results,
            "meta": {
                "fast_wall_s": FAST_WALL_S, "slow_wall_s": SLOW_WALL_S,
                "fast_cpu": FAST_CPU, "slow_cpu": SLOW_CPU,
                "cancel_floor": CANCEL_FLOOR, "goal_n": GOAL_N,
                "exploration": EXPLORATION,
            },
        }
        Path(json_out).write_text(json.dumps(payload, indent=2))
        print(f"JSON → {json_out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="AuCaM capability demo")
    ap.add_argument("--json-out", default=None, metavar="FILE",
                    help="write run data as JSON for gen_impact_fig.py")
    ap.add_argument("-v", "--verbose", action="store_true",
                    help="enable INFO-level AuCaM logging")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(levelname)s  %(name)s  %(message)s")
    asyncio.run(run(args.json_out))
