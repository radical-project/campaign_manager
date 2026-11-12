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
  Per-workflow limits  Each workflow gets a spending_limit and resources from AuCaM
                       (context["spending_limit"], context["resources"]) and stays within
                       them; --overspend makes precise_sim ignore its spending limit and
                       --overcpu makes it use twice its CPUs, and AuCaM cancels it

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
  python run_campaign.py --overspend        # precise_sim ignores its budget → cancelled
  python run_campaign.py --overcpu          # precise_sim uses 2× its CPUs → cancelled
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import math
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
    CampaignDecision,
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
#
# Each step of a synthetic sim costs a fixed amount of CPU time, reported as a
# running cpu_hours total in every emit().  AuCaM passes each run its spending
# limit in context["spending_limit"]; a well-behaved workflow sizes its work to
# fit and stops before it would exceed it.  A workflow that ignores its limit is
# cancelled by AuCaM once its reported spending passes the limit.

H = 3600.0  # seconds per hour
STEP_S = 0.05  # wall time of one sim step, seconds
FAST_STEP_CPU_H = 0.0002  # CPU-hours per step
PRECISE_STEP_CPU_H = 0.0003
ANALYSIS_S = 0.08
ANALYSIS_CPU_H = 0.0001


def _cpu_limit(context: dict | None) -> float:
    """This run's cpu_hours limit from AuCaM (inf when it has none)."""
    return float((context or {}).get("spending_limit", {}).get("cpu_hours", math.inf))


def make_sim(
    arm: str,
    steps: int,
    step_cpu_h: float,
    p_converge: float,
    respect_limit: bool = True,
    overuse_cpus: bool = False,
):
    """A synthetic sim: ``steps`` steps of ``step_cpu_h`` each, ~``p_converge`` convergence.

    It uses the CPUs AuCaM gives it (context["resources"]["cpus"]), or twice
    that with ``overuse_cpus``, and reports them as cpus_in_use.
    """
    up, down = (0.06, -0.10) if arm == "fast" else (0.04, -0.08)

    async def sim(reporter: WorkflowReporter, context: dict) -> None:
        limit = _cpu_limit(context)
        cpus = float(context.get("resources", {}).get("cpus", 1.0))
        if overuse_cpus:
            cpus *= 2
        score = 0.5
        converging = random.random() < p_converge
        spent = 0.0
        for step in range(steps):
            # AuCaM sends CANCEL before cancelling (cancel_grace_s below): stop
            # cleanly, reporting what was spent.
            if await reporter.poll() == CampaignDecision.CANCEL:
                logging.info("[%s] CANCEL received at step %d — stopping", arm, step)
                return
            if respect_limit and spent + step_cpu_h > limit + 1e-12:  # float tolerance
                logging.info(
                    "[%s] stopping at step %d to stay within %.4f cpu_hours", arm, step, limit
                )
                break
            await asyncio.sleep(STEP_S)
            spent += step_cpu_h
            score += random.gauss(up if converging else down, 0.04 if arm == "fast" else 0.03)
            score = max(0.0, min(1.0, score))
            await reporter.emit(
                score=score,
                metrics={"step": step, "arm": arm, "cpu_hours": spent, "cpus_in_use": cpus},
                iteration=step,
            )

    sim.__name__ = sim.__qualname__ = f"{arm}_sim"
    return sim


# Quick simulation — 6 steps, ~70 % convergence rate.
fast_sim = make_sim("fast", steps=6, step_cpu_h=FAST_STEP_CPU_H, p_converge=0.70)
# Careful simulation — 10 steps, ~90 % convergence rate.  Bandit should prefer this arm.
precise_sim = make_sim("precise", steps=10, step_cpu_h=PRECISE_STEP_CPU_H, p_converge=0.90)
# --overspend: a precise_sim that runs 3x longer and ignores its spending limit.
runaway_sim = make_sim(
    "precise", steps=30, step_cpu_h=PRECISE_STEP_CPU_H, p_converge=0.90, respect_limit=False
)
# --overcpu: a precise_sim that uses twice the CPUs AuCaM gives it.
greedy_sim = make_sim(
    "precise", steps=10, step_cpu_h=PRECISE_STEP_CPU_H, p_converge=0.90, overuse_cpus=True
)


async def analysis(reporter: WorkflowReporter, context: dict) -> None:
    """Post-processing step — triggered by WorkflowDependency after each successful sim."""
    await asyncio.sleep(ANALYSIS_S)
    await reporter.emit(score=1.0, metrics={"status": "ok", "cpu_hours": ANALYSIS_CPU_H})


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
        # Best simulation score (analysis runs always report 1.0).
        is_sim = result.workflow_type in ("fast_sim", "precise_sim")
        if is_sim and result.returncode == 0 and result.final_score is not None:
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
    overspend: bool = False,
    overcpu: bool = False,
    analysis_threshold: float | None = None,
) -> CampaignSpec:
    """Wire workflows, dependencies, controller, budget, and policy into one spec."""

    def _dep(r: WorkflowResult) -> bool:
        if r.returncode != 0:
            return False
        return analysis_threshold is None or (r.final_score or 0.0) >= analysis_threshold

    return CampaignSpec(
        name=name,
        budget=CampaignBudget(
            max_concurrent_workflows=max_concurrent,
            max_concurrency_per_workflow=2,
            # The CPUs the demo may use at once; a sim starts only if its CPUs
            # (fast_sim 1, precise_sim 2) fit what is free.
            capacity={"cpus": 2 * max_concurrent},
            spending={"cpu_hours": 0.05},  # per-campaign spending cap
        ),
        controller=ShowcaseController(
            goal_analyses=goal_analyses, cancel_threshold=cancel_threshold
        ),
        workflows=[
            # declared_spending: what one workflow is expected to cost, with its
            #   expected wall time.  AuCaM uses it only when the workflow reports
            #   no spending (it then charges it prorated by actual wall time) or
            #   has no spending_limit.  These sims report cpu_hours, so it is a fallback.
            # spending_limit: the most one workflow may spend, here 1.5x expected.
            #   AuCaM holds it against the campaign while the workflow runs,
            #   passes it to the workflow, and cancels the workflow if it spends more.
            # bounds (precise_sim): a fixed range for its spending limit — one full
            #   run's cost to 1.5x that.  The scheduler picks each workflow's limit
            #   (its estimate + 25 %, then its learned cost + 25 %) and keeps it
            #   inside the range.
            WorkflowConfig(
                name="fast_sim",
                block=fast_sim,
                launch_on_start=True,
                stale_timeout_s=30.0,
                cancel_grace_s=2.0,  # it stops on CANCEL; AuCaM waits up to 2 s
                declared_spending={"wall_hours": 6 * STEP_S / H, "cpu_hours": 6 * FAST_STEP_CPU_H},
                spending_limit={"cpu_hours": round(9 * FAST_STEP_CPU_H, 6)},
                resources={"cpus": 1},  # passed in context["resources"]; AuCaM
                # cancels a workflow that reports more cpus_in_use
            ),
            WorkflowConfig(
                name="precise_sim",
                block=runaway_sim if overspend else greedy_sim if overcpu else precise_sim,
                launch_on_start=True,
                stale_timeout_s=60.0,
                cancel_grace_s=2.0,
                declared_spending={
                    "wall_hours": 10 * STEP_S / H,
                    "cpu_hours": 10 * PRECISE_STEP_CPU_H,
                },
                bounds={"cpu_hours": (10 * PRECISE_STEP_CPU_H, round(15 * PRECISE_STEP_CPU_H, 6))},
                resources={"cpus": 2},
            ),
            WorkflowConfig(
                name="analysis",
                block=analysis,
                launch_on_start=False,  # only triggered by dependency below
                declared_spending={"wall_hours": ANALYSIS_S / H, "cpu_hours": ANALYSIS_CPU_H},
                spending_limit={"cpu_hours": 2 * ANALYSIS_CPU_H},
            ),
        ],
        dependencies=[
            # Each successful sim triggers one analysis run.
            # Each successful sim triggers an analysis — with --policy learn,
            # only one whose final score reaches analysis_threshold, so a sim
            # given too little budget to converge does not count toward the goal.
            WorkflowDependency(source="fast_sim", target="analysis", condition=_dep),
            WorkflowDependency(source="precise_sim", target="analysis", condition=_dep),
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
        overspend=args.overspend,
        overcpu=args.overcpu,
        # learn: an analysis only for a sim that reached 0.75, so how much a sim
        # is given matters for reaching the goal.
        analysis_threshold=0.75 if args.policy == "learn" else None,
    )

    t0 = time.monotonic()
    outcome = await CampaignManager().run(spec)  # returns when the campaign ends

    s = outcome.summary  # the controller's get_summary()
    logging.info(
        "done (%s) — best=%.3f  analyses=%d  cancel_rate=%.0f%%",
        outcome.exit_reason,
        s.get("best_score") or 0.0,
        s.get("analyses_done") or 0,
        (s.get("cancel_rate") or 0.0) * 100,
    )
    logging.info(
        "spent %.4f cpu_hours; workflows cancelled for exceeding their own limit: %d, "
        "for using more CPUs than given: %d",
        outcome.spent.get("cpu_hours", 0.0),
        outcome.budget_cancels,
        outcome.resource_cancels,
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
        choices=["bandit", "science", "fifo", "bandit_tight", "custom", "learn"],
        default="bandit",
        help="scheduling policy (see scheduler.py)",
    )
    p.add_argument(
        "--overspend",
        action="store_true",
        help="precise_sim ignores its spending limit; AuCaM cancels it once it overspends",
    )
    p.add_argument(
        "--overcpu",
        action="store_true",
        help="precise_sim uses twice its CPUs; AuCaM cancels it once it reports that",
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
