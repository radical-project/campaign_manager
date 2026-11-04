#!/usr/bin/env python3
"""
AuCaM Demo — all capabilities in one file, runs anywhere.

Two simulation arms compete.  The bandit learns which arm finds better scores;
the controller cancels low-scoring runs early; a dependency triggers an analysis
worker after every successful sim.

Capabilities shown:
  Workflow types       fast_sim (quick, 70% converge) and precise_sim (slower, 90% converge)
  Scoring              Each sim emits a score every step; AuCaM evaluates it each policy cycle
  CancelRule           Sims scoring below 0.3 are cancelled automatically
  WorkflowDependency   Each successful sim triggers one analysis run
  Scheduling policies  bandit (out of box), science (custom reward_fn), fifo, bandit_tight, custom
  CampaignBudget       Max concurrent workflows + cpu_hours spending cap
  Custom controller    Tracks best score; ends the campaign after N analyses
  Self-reported budget Workflows report cpu_hours in emit() for accurate charging

Scheduling policies (see scheduler.py):
  bandit        — UCB1, reward = 1 for any completion.  Learns which arm succeeds more often.
  science       — UCB1, reward = final_score.  Learns which arm finds BETTER scores.
  bandit_tight  — UCB1 with low exploration (c=0.5); faster exploitation.
  custom        — recency-bias subclass of SchedulingPolicy.
  fifo          — no learning; queue order.

Usage:
  python run_campaign.py                    # single campaign, bandit policy
  python run_campaign.py --policy science   # science-adapted bandit
  python run_campaign.py --policy custom    # custom SchedulingPolicy subclass
  python run_campaign.py -v                 # verbose: every launch and cycle
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scheduler import make_policy

from src.aucam import (
    BanditSchedulingPolicy,
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    CancelRule,
    Goal,
    LaunchPriority,
    LaunchRequest,
    LaunchRule,
    PolicyResult,
    WorkflowConfig,
    WorkflowDependency,
    WorkflowReporter,
    WorkflowResult,
)

# ── Workflows ──────────────────────────────────────────────────────────────────


async def fast_sim(reporter: WorkflowReporter) -> None:
    """Quick simulation — 6 steps, ~70 % convergence rate."""
    t0 = time.monotonic()
    score = 0.5
    converging = random.random() > 0.30
    for step in range(6):
        await asyncio.sleep(0.05)
        score += random.gauss(0.06 if converging else -0.10, 0.04)
        score = max(0.0, min(1.0, score))
        await reporter.emit(
            score=score,
            metrics={"step": step, "arm": "fast", "cpu_hours": (time.monotonic() - t0) / 3600.0},
            iteration=step,
        )


async def precise_sim(reporter: WorkflowReporter) -> None:
    """Careful simulation — 10 steps, ~90 % convergence rate.  Bandit should prefer this arm."""
    t0 = time.monotonic()
    score = 0.5
    converging = random.random() > 0.10
    for step in range(10):
        await asyncio.sleep(0.05)
        score += random.gauss(0.04 if converging else -0.08, 0.03)
        score = max(0.0, min(1.0, score))
        await reporter.emit(
            score=score,
            metrics={"step": step, "arm": "precise", "cpu_hours": (time.monotonic() - t0) / 3600.0},
            iteration=step,
        )


async def analysis(reporter: WorkflowReporter) -> None:
    """Post-processing step — triggered by WorkflowDependency after each successful sim."""
    t0 = time.monotonic()
    await asyncio.sleep(0.08)
    await reporter.emit(
        score=1.0, metrics={"status": "ok", "cpu_hours": (time.monotonic() - t0) / 3600.0}
    )


# ── Controller ─────────────────────────────────────────────────────────────────


class ShowcaseController(CampaignController):
    """Showcase controller — every CampaignController hook demonstrated.

    fast_sim  : kept queued by a LaunchRule in rules= (declarative).
    precise_sim: launched by decide() with dynamic priority (imperative).
    Both sims : cancelled by CancelRule when score drops below threshold.
    Campaign  : terminates after goal_n analyses OR on a perfect score.
    """

    def __init__(self, goal_analyses: int = 4, cancel_threshold: float = 0.3) -> None:
        super().__init__(
            # LaunchRule inside rules= is the explicit form of the launch= shorthand.
            # fast_sim is kept to exactly one queued request by the rule engine.
            # precise_sim is launched imperatively in decide() to show priority control.
            rules=[
                LaunchRule("fast_sim"),
                CancelRule(
                    "fast_sim", when=lambda r: r.score is not None and r.score < cancel_threshold
                ),
                CancelRule(
                    "precise_sim", when=lambda r: r.score is not None and r.score < cancel_threshold
                ),
            ],
            goal_type="analysis",
            goal_n=goal_analyses,
        )
        self._best: float | None = None
        self._cancel_threshold = cancel_threshold

    # ── observe: augment the snapshot passed to decide() ─────────────────────

    def observe(self, queue_snapshot, budget, running=None, **kwargs) -> dict:
        """Inject a custom field so decide() doesn't recompute it."""
        obs = super().observe(queue_snapshot, budget, running, **kwargs)
        total = sum(
            self.completed.get(t, 0) + self.cancelled.get(t, 0) for t in ("fast_sim", "precise_sim")
        )
        cancelled = sum(self.cancelled.get(t, 0) for t in ("fast_sim", "precise_sim"))
        obs["cancel_rate"] = cancelled / max(1, total)
        return obs

    # ── decide: custom launch logic + full PolicyResult fields ───────────────

    def decide(self, obs: dict) -> PolicyResult:
        """Delegate cancel/LaunchRule to _apply_rules; manage precise_sim manually."""
        base = self._apply_rules(self._rules, obs)
        q = obs["queue_snapshot"]

        # Keep one precise_sim queued; promote to HIGH when scores are trending up.
        extra: list[LaunchRequest] = []
        if not self.is_goal_reached() and q.by_type.get("precise_sim", 0) == 0:
            priority = (
                LaunchPriority.HIGH
                if self._best is not None and self._best > 0.7
                else LaunchPriority.NORMAL
            )
            extra.append(LaunchRequest.make("precise_sim", priority=priority))

        # High cancel rate: drop queued fast_sims to reclaim slots for analysis.
        cancel_queued = {"fast_sim": 2} if obs["cancel_rate"] > 0.6 else None

        return PolicyResult(
            decisions=base.decisions,
            launch_requests=base.launch_requests + extra,
            terminate=base.terminate,
            cancel_queued=cancel_queued,
        )

    # ── is_goal_reached: custom early-stop on top of the default counter ─────

    def is_goal_reached(self) -> bool:
        """Also stop when a perfect score is found, regardless of analysis count."""
        if self._best is not None and self._best >= 0.99:
            return True
        return super().is_goal_reached()

    # ── goals: declare a tracked quality target for the CM dashboard ─────────

    def goals(self) -> list[Goal]:
        return [Goal(name="quality", metric="score", threshold=0.8, direction="maximize")]

    # ── on_complete: accumulate domain state after each workflow exits ────────

    def on_complete(self, result: WorkflowResult) -> None:
        super().on_complete(result)  # keeps completed/cancelled/failed counters accurate
        if result.returncode == 0 and result.final_score is not None:
            if self._best is None or result.final_score > self._best:
                self._best = result.final_score

    # ── get_summary: returned in CampaignOutcome.summary ─────────────────────

    def get_summary(self) -> dict:
        sims_done = sum(self.completed.get(t, 0) for t in ("fast_sim", "precise_sim"))
        sims_cancelled = sum(self.cancelled.get(t, 0) for t in ("fast_sim", "precise_sim"))
        return {
            "best_score": self._best,
            "analyses_done": self.completed.get("analysis", 0),
            "sims_done": sims_done,
            "sims_cancelled": sims_cancelled,
            "cancel_rate": sims_cancelled / max(1, sims_done + sims_cancelled),
        }


# ── Campaign spec ──────────────────────────────────────────────────────────────


def build_spec(
    name: str,
    *,
    goal_analyses: int = 4,
    max_concurrent: int = 4,
    cancel_threshold: float = 0.3,
    policy=None,
) -> CampaignSpec:
    """Wire workflows, dependencies, controller, budget, and policy into one spec."""
    return CampaignSpec(
        name=name,
        budget=CampaignBudget(
            max_concurrent_workflows=max_concurrent,
            max_concurrency_per_workflow=2,
            spending={"cpu_hours": 0.05},  # per-campaign spending cap
        ),
        controller=ShowcaseController(
            goal_analyses=goal_analyses, cancel_threshold=cancel_threshold
        ),
        workflows=[
            WorkflowConfig(
                name="fast_sim",
                block=fast_sim,
                launch_on_start=True,
                stale_timeout_s=30.0,
                declared_spending={"cpu_hours": 0.001},  # in-flight reservation
            ),
            WorkflowConfig(
                name="precise_sim",
                block=precise_sim,
                launch_on_start=True,
                stale_timeout_s=60.0,
                declared_spending={"cpu_hours": 0.002},  # in-flight reservation
            ),
            WorkflowConfig(
                name="analysis",
                block=analysis,
                launch_on_start=False,  # only triggered by dependency below
                declared_spending={"cpu_hours": 0.0005},  # in-flight reservation
            ),
        ],
        dependencies=[
            # Each successful sim triggers one analysis run.
            WorkflowDependency(source="fast_sim", target="analysis"),
            WorkflowDependency(source="precise_sim", target="analysis"),
        ],
        scheduling_policy=policy,  # None → new BanditSchedulingPolicy per campaign
        policy_interval_s=0.5,
        shutdown_timeout_s=10.0,
    )


# ── CM lifecycle ───────────────────────────────────────────────────────────────


async def run(args: argparse.Namespace) -> None:
    """Run a single campaign, then print the summary."""
    policy = make_policy(args.policy)
    spec = build_spec(
        "demo",
        goal_analyses=args.goal_analyses,
        max_concurrent=args.max_concurrent,
        cancel_threshold=0.3,
        policy=policy,
    )

    cm = CampaignManager()
    result = cm.submit_campaign(spec)
    if not result.accepted:
        logging.warning("campaign rejected: %s", result.reason)
        return

    t0 = time.monotonic()
    await cm.run()  # returns when the campaign finishes
    cm.close()

    s = spec.controller.get_summary()
    logging.info(
        "done — best=%.3f  analyses=%d  cancel_rate=%.0f%%",
        s.get("best_score") or 0.0,
        s.get("analyses_done") or 0,
        (s.get("cancel_rate") or 0.0) * 100,
    )
    if isinstance(policy, BanditSchedulingPolicy):
        logging.info("arm stats: %s", policy.arm_stats())

    print(f"\nElapsed: {time.monotonic() - t0:.1f}s", flush=True)


# ── CLI ────────────────────────────────────────────────────────────────────────


def main() -> None:
    p = argparse.ArgumentParser(
        description="AuCaM demo — all capabilities in one file",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--goal-analyses",
        type=int,
        default=4,
        help="analyses needed before the campaign terminates",
    )
    p.add_argument("--max-concurrent", type=int, default=4, help="max concurrent workflows")
    p.add_argument(
        "--policy",
        choices=["bandit", "science", "fifo", "bandit_tight", "custom"],
        default="bandit",
        help="scheduling policy (see scheduler.py)",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
