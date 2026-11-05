"""
Dummy campaign scheduling policy.

Mirrors controller.py: campaign-specific scheduling logic lives here,
not in the CM or the driver script.

The ADR variant wires in a backpressure rule: when the recent sim
cancellation rate exceeds 60%, concurrent sim launches are capped at 1
to stop wasting slots on diverging runs.  DAG-triggered analysis workers
arrive as CRITICAL priority and bypass this rule entirely.
"""

from __future__ import annotations

from src.aucam import (
    ADRSchedulingPolicy,
    BanditSchedulingPolicy,
    FIFOSchedulingPolicy,
    LLMSchedulingPolicy,
    SchedulingPolicy,
)

_SIM_THROTTLE_WINDOW = 8  # how many recent sim results to inspect
_SIM_CANCEL_RATE_THRESHOLD = 0.6  # throttle when >60% of recent sims were cancelled


def _adr_rule(candidates, ctx):
    """
    Backpressure rule: throttle concurrent sim launches when the recent
    sim cancellation rate exceeds a threshold.

    When >60% of the last 8 sim results were cancelled (score below floor),
    the campaign is wasting slots on diverging runs.  This rule caps concurrent
    sims at 1 until the cancellation rate drops, freeing slots for analysis
    workers and reducing queue buildup.

    DAG-triggered analysis workers arrive as CRITICAL priority and are
    drained by the scheduler before this rule is consulted — no slot
    reservation is needed here.

    Returns a filtered launch list when the rule fires, or None to defer
    to the bandit.
    """
    recent_sims = [
        r
        for r in ctx.history[-_SIM_THROTTLE_WINDOW:]
        if r.workflow_type in ("sim_worker", "sim_worker_b")
    ]
    if len(recent_sims) < 3:
        return None  # not enough data — defer to bandit

    cancel_rate = sum(1 for r in recent_sims if r.returncode != 0) / len(recent_sims)
    if cancel_rate <= _SIM_CANCEL_RATE_THRESHOLD:
        return None  # healthy — defer to bandit

    # High cancellation: cap at 1 concurrent sim to reduce slot waste.
    running_sims = ctx.running_by_type.get("sim_worker", 0) + ctx.running_by_type.get(
        "sim_worker_b", 0
    )
    if running_sims >= 1:
        non_sims = [r for r in candidates if r.workflow_type not in ("sim_worker", "sim_worker_b")]
        return non_sims  # hold all new sims; only launch non-sim types
    return None  # one sim slot free — let bandit pick normally


def make_policy(name: str) -> SchedulingPolicy:
    """Instantiate the named scheduling policy for this campaign."""
    if name == "bandit":
        return BanditSchedulingPolicy()
    if name == "adr":
        return ADRSchedulingPolicy(rule_fn=_adr_rule)
    if name == "llm":
        return LLMSchedulingPolicy(model="claude-haiku-4-5-20251001")
    return FIFOSchedulingPolicy()  # "fifo"


def print_policy_summary(policy: SchedulingPolicy, name: str) -> None:
    """Print a policy-specific summary after the campaign finishes."""
    print(f"\n  scheduling policy : {name}")
    if isinstance(policy, BanditSchedulingPolicy):
        arms = policy.arm_stats()
        if arms:
            print(f"  bandit arms ({len(arms)} learned):")
            for arm_key, stats in sorted(arms.items()):
                print(f"    {arm_key:<40}  n={int(stats['n']):>3}  mean={float(stats['mean']):.3f}")
        else:
            print("  bandit arms: none yet (no completed workflows)")
    elif isinstance(policy, ADRSchedulingPolicy):
        print("  adr rule_fn : _adr_rule (throttle sims when cancel rate > 60%)")
    elif isinstance(policy, LLMSchedulingPolicy):
        print("  llm model   : stub — fell back to FIFO (not yet implemented)")
