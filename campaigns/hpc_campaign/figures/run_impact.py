#!/usr/bin/env python3
"""
AuCaM capability demo — what AuCaM adds over launching workflows by hand.

One campaign, "explore": two workflow types compete for one workflow slot:
    fast_good  1 CPU · 1.5 s · score 0.75–0.95, completes
    slow_poor  4 CPU · 5 s   · score drifts down → cancelled at ~3 s
  Shows:
    1. Cancellation — the controller's CancelRule stops slow_poor runs whose
       score falls below a floor, so they never use their full cost.
    2. Bandit scheduling — the campaign's UCB1 bandit tries both types,
       learns that slow_poor never completes, and launches it less and less.
    3. Spending accounting — every run is charged in AuCaM's ledger
       (prorated cpu_hours), reported back in CampaignOutcome.spent.

Run:
    cd $WORK_DIR/campaign_manager
    python campaigns/hpc_campaign/figures/run_impact.py --json-out /tmp/impact_run.json
    python campaigns/hpc_campaign/figures/gen_impact_fig.py --json-in /tmp/impact_run.json
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

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

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
FAST_WALL_S = 1.5
SLOW_WALL_S = 5.0
FAST_CPU = 1.0
SLOW_CPU = 4.0
CANCEL_FLOOR = 0.30
GOAL_N = 20  # fast_good completions per campaign
POLICY_INTERVAL_S = 0.5
EXPLORATION = 2.0  # UCB1 exploration weight
SEED = 7


def _config(name: str, block, cpus: float, wall_s: float) -> WorkflowConfig:
    """cpus CPUs; a starting limit of 1.25 × its expected cost (cpus × wall_s).

    The workflows report no spending: AuCaM charges each its CPUs × run time.
    """
    return WorkflowConfig(
        name=name,
        block=block,
        resources={"cpus": cpus},
        spending_limit={"cpu_hours": 1.25 * cpus * wall_s / 3600.0},
    )


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
    "fast_good": _config("fast_good", fast_good, FAST_CPU, FAST_WALL_S),
    "slow_poor": _config("slow_poor", slow_poor, SLOW_CPU, SLOW_WALL_S),
}


# ── Controller ─────────────────────────────────────────────────────────────────


class DemoController(CampaignController):
    """Keeps one request of each type queued, cancels low-scoring slow_poor
    runs, and stops after GOAL_N fast_good completions.  Records every finished
    workflow for the figure and returns per-type statistics in the outcome."""

    def __init__(self, types: list[str]) -> None:
        super().__init__(
            launch=types,
            goal_type="fast_good",
            goal_n=GOAL_N,
            rules=[
                CancelRule(
                    workflow_type="slow_poor",
                    when=lambda r: (
                        r.iteration >= 2 and r.score is not None and r.score < CANCEL_FLOOR
                    ),
                )
            ],
        )
        self.t0 = time.monotonic()
        self.runs: list[dict] = []

    def decide(self, obs: dict) -> PolicyResult:
        return super().decide(obs)

    def on_complete(self, result: WorkflowResult) -> None:
        super().on_complete(result)
        self.runs.append(
            {
                "type": result.workflow_type,
                "rc": result.returncode,
                "wall_s": round(result.wall_time_s, 2),
                "end_s": round(time.monotonic() - self.t0, 2),
                "cpu_s": round(result.spent.get("cpu_hours", 0.0) * 3600.0, 2),
                "score": result.final_score,
            }
        )

    def get_summary(self) -> dict:
        """Returned in CampaignOutcome.summary."""
        by_type: dict[str, dict] = {}
        for r in self.runs:
            s = by_type.setdefault(
                r["type"], {"runs": 0, "completed": 0, "cancelled": 0, "cpu_s": 0.0}
            )
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
        budget=CampaignBudget(
            max_concurrent_workflows=1, max_concurrency_per_workflow=1, capacity={"cpus": 4}
        ),
        scheduling_policy=BanditSchedulingPolicy(reward_fn=_reward, exploration=EXPLORATION),
        policy_interval_s=POLICY_INTERVAL_S,
    )
    return spec, ctrl


# ── Run ────────────────────────────────────────────────────────────────────────


def _record(outcome: CampaignOutcome, types: list[str]) -> dict:
    by_type = outcome.summary.get("by_type", {})
    return {
        "name": outcome.campaign_name,
        "types": types,
        "elapsed_s": round(outcome.elapsed_s, 2),
        "goal_reached": outcome.goal_reached,
        "completed": dict(outcome.completed),
        "cancelled": dict(outcome.cancelled),
        "cpu_s": round(outcome.spent.get("cpu_hours", 0.0) * 3600.0, 2),
        "by_type": by_type,
        "runs": outcome.summary.get("runs", []),
    }


def _print(rec: dict) -> None:
    print(
        f"  elapsed={rec['elapsed_s']:.1f}s  goal_reached={rec['goal_reached']}  "
        f"cpu_spent={rec['cpu_s']:.1f} CPU·s"
    )
    for t, s in sorted(rec["by_type"].items()):
        print(
            f"    {t:10s} runs={s['runs']:2d}  completed={s['completed']:2d}  "
            f"cancelled={s['cancelled']:2d}  cpu={s['cpu_s']:.1f} CPU·s"
        )


async def run(json_out: str | None) -> None:
    random.seed(SEED)
    types = ["fast_good", "slow_poor"]
    print(f"── Campaign explore: types={types}")
    spec, _ = _spec("explore", types)
    outcome = await CampaignManager().run(spec)
    rec = _record(outcome, types)
    _print(rec)

    if json_out:
        payload = {
            "campaign": rec,
            "meta": {
                "fast_wall_s": FAST_WALL_S,
                "slow_wall_s": SLOW_WALL_S,
                "fast_cpu": FAST_CPU,
                "slow_cpu": SLOW_CPU,
                "cancel_floor": CANCEL_FLOOR,
                "goal_n": GOAL_N,
                "exploration": EXPLORATION,
            },
        }
        Path(json_out).write_text(json.dumps(payload, indent=2))
        print(f"JSON → {json_out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="AuCaM capability demo")
    ap.add_argument(
        "--json-out",
        default=None,
        metavar="FILE",
        help="write run data as JSON for gen_impact_fig.py",
    )
    ap.add_argument("-v", "--verbose", action="store_true", help="enable INFO-level AuCaM logging")
    args = ap.parse_args()
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s  %(name)s  %(message)s",
    )
    asyncio.run(run(args.json_out))
