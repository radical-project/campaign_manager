#!/usr/bin/env python3
"""
AuCaM feature impact demo — three features in one run, visible across 6 campaigns.

  1. Cancellation  — slow_poor killed mid-run, saving CPU-seconds
  2. Bandit        — arm selection shifts from explore (50/50) to exploit (fast_good)
  3. Backpressure  — slow_poor fills the 4-CPU pool, blocking others;
                     fades as bandit learns to avoid it

Workflow types
  fast_good  1 CPU unit · 1.5 s wall · score 0.75–0.95 (reliable)
  slow_poor  4 CPU units · 5 s wall  · score trends down → cancelled at ~3 s

Budget pool = 4 CPU units.  6 sequential campaigns share one bandit so beliefs
build across campaigns.  Per-campaign metrics are written to JSON for plotting.

Run:
    cd /work/nvme/bdyk/mgoliyad1/campaign_manager
    /work/nvme/bdyk/mgoliyad1/ve/miniapps/bin/python3 \\
        campaigns/demo_campaign/run_impact.py --json-out /tmp/impact_run.json
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
    SubmitResult,
    WorkflowConfig,
)
from src.aucam.controller import CancelRule, PolicyResult
from src.aucam.reporter import WorkflowReporter
from src.aucam.types import CampaignOutcome, WorkflowResult
from src.aucam import BanditSchedulingPolicy

# ── Parameters ─────────────────────────────────────────────────────────────────
FAST_WALL_S       = 1.5
SLOW_WALL_S       = 5.0
FAST_CPU          = 1.0
SLOW_CPU          = 4.0
CPU_POOL          = 4.0
CANCEL_FLOOR      = 0.30
GOAL_N            = 5       # fast_good completions per campaign
N_CAMPAIGNS       = 8
POLICY_INTERVAL_S = 0.5
MAX_CONCURRENT    = 1


def _declared(cpu_units: float) -> dict[str, float]:
    return {"wall_hours": 1.0, "cpu_hours": cpu_units}


# ── Workflow blocks ────────────────────────────────────────────────────────────

async def fast_good(reporter: WorkflowReporter) -> None:
    score = random.uniform(0.75, 0.95)
    n_iter = 3
    for i in range(n_iter):
        await asyncio.sleep(FAST_WALL_S / n_iter)
        score = min(1.0, score + random.gauss(0.02, 0.04))
        await reporter.emit(score=score, metrics={"score": round(score, 3)}, iteration=i)


async def slow_poor(reporter: WorkflowReporter) -> None:
    # Score starts 0.30–0.45 and drifts down ~0.07 per step.
    # Expected cancel at iteration 2 (t≈3 s): score ~0.16–0.31 → below floor.
    score = random.uniform(0.30, 0.45)
    n_iter = 5
    for i in range(n_iter):
        await asyncio.sleep(SLOW_WALL_S / n_iter)
        score = max(0.0, score + random.gauss(-0.07, 0.03))
        await reporter.emit(score=score, metrics={"score": round(score, 3)}, iteration=i)


# ── Controller ─────────────────────────────────────────────────────────────────

class ImpactController(CampaignController):
    """
    Tracks three per-campaign metrics:
      launches      — how many of each type were actually requested (enqueued)
      cancel_events — wall time and CPU saved for each cancelled slow_poor run
      bp_cycles     — scheduler cycles where only slow_poor active + queue > 0
    """

    def __init__(self, name: str = "") -> None:
        super().__init__(
            launch=["fast_good", "slow_poor"],
            goal_type="fast_good",
            goal_n=GOAL_N,
            rules=[
                CancelRule(
                    workflow_type="slow_poor",
                    when=lambda r: (
                        r.iteration >= 2
                        and r.score is not None
                        and r.score < CANCEL_FLOOR
                    ),
                ),
            ],
        )
        self._name      = name
        self.launches:      dict[str, int] = {}
        self.cancel_events: list[dict]     = []
        self.bp_cycles:     int            = 0
        self.cycle_log:     list[dict]     = []
        self._cycle:        int            = 0

    def on_complete(self, result: WorkflowResult) -> None:
        super().on_complete(result)
        if result.workflow_type == "slow_poor" and result.returncode == -1:
            saved = max(0.0, SLOW_WALL_S - result.wall_time_s)
            self.cancel_events.append({
                "actual_s":    round(result.wall_time_s, 2),
                "expected_s":  SLOW_WALL_S,
                "saved_cpu_s": round(saved * SLOW_CPU, 2),
            })

    def decide(self, obs: dict) -> PolicyResult:
        self._cycle += 1
        result = super().decide(obs)

        for req in result.launch_requests:
            t = req.workflow_type
            self.launches[t] = self.launches.get(t, 0) + 1

        live   = obs.get("live_reports", {})
        types  = obs.get("wf_types", {})
        q      = obs.get("queue_snapshot")
        budget = obs.get("budget", {})

        active_types = {types.get(wid) for wid in live}
        q_total      = sum(q.by_type.values()) if q else 0
        in_use       = budget.get("in_use", {}) if isinstance(budget, dict) else {}
        cpu_used     = sum(v.get("cpu", 0.0) for v in in_use.values())

        only_slow = active_types == {"slow_poor"}
        bp        = only_slow and q_total > 0
        if bp:
            self.bp_cycles += 1

        self.cycle_log.append({
            "campaign":    self._name,
            "cycle":       self._cycle,
            "fast_active": sum(1 for wid in live if types.get(wid) == "fast_good"),
            "slow_active": sum(1 for wid in live if types.get(wid) == "slow_poor"),
            "queued":      q_total,
            "cpu_used":    round(min(cpu_used, CPU_POOL), 2),
            "backpressure": bp,
        })
        return result

    def get_summary(self) -> dict:
        return {
            "launches":      dict(self.launches),
            "cancel_events": self.cancel_events,
            "bp_cycles":     self.bp_cycles,
            "bp_s":          round(self.bp_cycles * POLICY_INTERVAL_S, 2),
            "cpu_saved":     round(sum(e["saved_cpu_s"] for e in self.cancel_events), 2),
        }


# ── Bandit reward ──────────────────────────────────────────────────────────────

def _score_reward(result: WorkflowResult) -> float:
    """Binary: 1.0 for any completion, 0.0 for cancellation/failure.
    Using binary (not score-based) here because: fast_good always completes
    (reward=1.0), slow_poor always gets cancelled (reward=0.0).  The gap is
    maximum and deterministic, giving a clean explore→exploit transition.
    """
    return 1.0 if result.returncode == 0 else 0.0


# ── Spec builder ───────────────────────────────────────────────────────────────

def _build_spec(
    name: str,
    policy: BanditSchedulingPolicy,
) -> tuple[CampaignSpec, ImpactController]:
    ctrl = ImpactController(name=name)
    spec = CampaignSpec(
        name=name,
        workflows=[
            WorkflowConfig(
                name="fast_good",
                block=fast_good,
                declared_spending=_declared(FAST_CPU),
            ),
            WorkflowConfig(
                name="slow_poor",
                block=slow_poor,
                declared_spending=_declared(SLOW_CPU),
            ),
        ],
        controller=ctrl,
        budget=CampaignBudget(
            max_concurrent_workflows=MAX_CONCURRENT,
            max_concurrency_per_workflow=1,
            budget={"cpu": CPU_POOL},
            spending={},
        ),
        scheduling_policy=policy,
        policy_interval_s=POLICY_INTERVAL_S,
    )
    return spec, ctrl


# ── Driver ─────────────────────────────────────────────────────────────────────

async def _driver(
    cm: CampaignManager,
    n_campaigns: int,
    all_campaigns: list,
    all_cycles: list,
    policy: BanditSchedulingPolicy,
) -> None:
    for i in range(n_campaigns):
        name = f"impact-{chr(ord('a') + i)}"
        print(f"\n{'─'*56}\n  Campaign {i+1}/{n_campaigns}: {name}\n{'─'*56}")

        spec, ctrl = _build_spec(name, policy)
        sub: SubmitResult = await cm.submit(spec)
        if not sub.accepted:
            print(f"  REJECTED: {sub.reason}")
            continue

        outcome: CampaignOutcome = await sub.wait()
        summary  = ctrl.get_summary()
        arm_stats = policy.arm_stats()

        all_cycles.extend(ctrl.cycle_log)
        all_campaigns.append({
            "name":          name,
            "elapsed_s":     round(outcome.elapsed_s, 2),
            "completed":     dict(outcome.completed),
            "cancelled":     dict(outcome.cancelled),
            "launches":      summary["launches"],
            "cancel_events": summary["cancel_events"],
            "bp_cycles":     summary["bp_cycles"],
            "bp_s":          summary["bp_s"],
            "cpu_saved":     summary["cpu_saved"],
            "arm_stats":     {k: dict(v) for k, v in arm_stats.items()},
        })

        fast_done  = outcome.completed.get("fast_good", 0)
        slow_cancel = outcome.cancelled.get("slow_poor", 0)
        slow_done   = outcome.completed.get("slow_poor", 0)
        print(
            f"  elapsed={outcome.elapsed_s:.1f}s  "
            f"fast_done={fast_done}  "
            f"slow_cancelled={slow_cancel}  slow_done={slow_done}  "
            f"cpu_saved={summary['cpu_saved']:.1f}  bp={summary['bp_s']:.1f}s"
        )
        arms_s = "  ".join(
            f"{k}→{v['mean']:.3f}(n={int(v['n'])})"
            for k, v in sorted(arm_stats.items())
        )
        print(f"  bandit: {arms_s}")

        if i < n_campaigns - 1:
            await asyncio.sleep(0.3)

    cm.driver_done()


# ── Entry point ────────────────────────────────────────────────────────────────

async def run(n_campaigns: int, json_out: str | None) -> None:
    policy = BanditSchedulingPolicy(reward_fn=_score_reward, scope="global",
                                    exploration=2.0)
    cm = CampaignManager(global_budget=GlobalBudget(max_concurrent_campaigns=1))
    cm.expect_driver()

    all_campaigns: list = []
    all_cycles:    list = []

    driver_task = asyncio.ensure_future(
        _driver(cm, n_campaigns, all_campaigns, all_cycles, policy)
    )

    async def _closer() -> None:
        try:
            await driver_task
        except Exception:
            logging.exception("[DRIVER] raised")

    asyncio.ensure_future(_closer())

    t0 = time.monotonic()
    await cm.run()
    elapsed = time.monotonic() - t0

    print(f"\n{'═'*56}")
    print(f"  IMPACT DEMO — {n_campaigns} campaigns · {elapsed:.1f}s total")
    print(f"{'═'*56}\n")

    if json_out:
        payload = {
            "campaigns": all_campaigns,
            "cycles":    all_cycles,
            "meta": {
                "cpu_pool":       CPU_POOL,
                "cancel_floor":   CANCEL_FLOOR,
                "fast_wall_s":    FAST_WALL_S,
                "slow_wall_s":    SLOW_WALL_S,
                "fast_cpu":       FAST_CPU,
                "slow_cpu":       SLOW_CPU,
                "goal_n":         GOAL_N,
                "n_campaigns":    n_campaigns,
                "elapsed_s":      round(elapsed, 1),
            },
        }
        Path(json_out).write_text(json.dumps(payload, indent=2))
        print(f"  JSON → {json_out}\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="AuCaM feature impact demo")
    ap.add_argument("--campaigns", type=int, default=N_CAMPAIGNS,
                    help=f"number of sequential campaigns (default {N_CAMPAIGNS})")
    ap.add_argument("--json-out", default=None, metavar="FILE",
                    help="write structured run data as JSON for re-plotting")
    ap.add_argument("-v", "--verbose", action="store_true",
                    help="enable INFO-level CM and bandit logging")
    args = ap.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO,
                            format="%(levelname)s  %(name)s  %(message)s")
    else:
        logging.basicConfig(level=logging.WARNING)

    asyncio.run(run(args.campaigns, args.json_out))
