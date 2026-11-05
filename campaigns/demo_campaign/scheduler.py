"""
Scheduling policies for the demo campaign.

  bandit        BanditSchedulingPolicy()
                Throughput reward (1=success, 0=cancel/fail). Learns which arm
                succeeds more often.

  science       BanditSchedulingPolicy(reward_fn=_score_reward)
                reward = final_score. Learns which arm finds BETTER scores.

  bandit_tight  BanditSchedulingPolicy(exploration=0.5)
                Low exploration constant — exploits faster, explores less.
                Useful when arms are well-characterised from a previous campaign.

  adr           ADRSchedulingPolicy(rule_fn=_backpressure)
                Backpressure rule: throttles sims when cancel rate > 60%.

  llm           LLMSchedulingPolicy(model="llama3:8b")
                LLM-guided ranking (STUB — falls back to FIFO today).

  custom        _RecencyBiasPolicy — subclass of SchedulingPolicy.
                Shows how to implement select() + on_outcome() from scratch.
                Prefers the workflow type that succeeded most recently.

  fifo          FIFOSchedulingPolicy() — queue order, no learning.

Reading order: _score_reward → science_bandit → _backpressure → adr_policy
               → _RecencyBiasPolicy → recency_policy → make_policy
"""

from __future__ import annotations

from src.aucam import (
    ADRSchedulingPolicy,
    BanditSchedulingPolicy,
    FIFOSchedulingPolicy,
    LLMSchedulingPolicy,
    SchedulingPolicy,
    WorkflowResult,
)
from src.aucam.scheduler import SchedulingContext

# ── Science-adapted bandit reward ──────────────────────────────────────────────


def _score_reward(result: WorkflowResult) -> float:
    """Reward = final score for successful runs; 0 for cancellation or failure.

    Unlike the default reward (1 for any completion), this ties the bandit's
    arm preference to outcome quality.  Arms that produce higher scores are
    pulled more often — the bandit acts as a science-driven optimizer, not just
    a success-rate optimizer.
    """
    if result.returncode != 0:
        return 0.0
    score = result.final_score
    return float(score) if score is not None else 0.5  # 0.5 if no score reported


def science_bandit() -> BanditSchedulingPolicy:
    """UCB1 bandit adapted for scientific campaigns.

    Drop-in replacement for BanditSchedulingPolicy(); only the reward changes.
    """
    return BanditSchedulingPolicy(reward_fn=_score_reward)


# ── ADR backpressure rule ──────────────────────────────────────────────────────

_WINDOW = 8  # inspect the last N sim results
_CANCEL_THRESHOLD = 0.6  # throttle when >60% of recent sims were cancelled


def _backpressure(candidates, ctx) -> list | None:
    """Throttle concurrent sims when the recent cancel rate is too high.

    When >60% of the last 8 sim results were cancelled (score below floor),
    slots are being wasted on diverging runs.  This rule caps concurrent sims
    at 1 until the rate drops, freeing slots for analysis workers.

    Returns a filtered candidate list when the rule fires; None defers to the
    bandit's normal selection.
    """
    recent_sims = [
        r for r in ctx.history[-_WINDOW:] if r.workflow_type in ("fast_sim", "precise_sim")
    ]
    if len(recent_sims) < 3:
        return None  # not enough data — let bandit decide

    cancel_rate = sum(1 for r in recent_sims if r.returncode != 0) / len(recent_sims)
    if cancel_rate <= _CANCEL_THRESHOLD:
        return None  # healthy — let bandit decide

    # High cancellation: hold new sims; let analysis workers through.
    running_sims = ctx.running_by_type.get("fast_sim", 0) + ctx.running_by_type.get(
        "precise_sim", 0
    )
    if running_sims >= 1:
        return [c for c in candidates if c.workflow_type not in ("fast_sim", "precise_sim")]
    return None  # one sim slot free — let bandit pick normally


def adr_policy() -> ADRSchedulingPolicy:
    """Bandit + backpressure rule: throttles sims when cancel rate > 60%."""
    return ADRSchedulingPolicy(rule_fn=_backpressure)


# ── Custom SchedulingPolicy subclass ──────────────────────────────────────────


class _RecencyBiasPolicy(SchedulingPolicy):
    """Prefer the workflow type that succeeded most recently.

    Shows how to subclass SchedulingPolicy:
      select()     — pick from candidates using ctx.history
      on_outcome() — update local state after each completion
    """

    def __init__(self) -> None:
        self._last_success: str | None = None

    def select(
        self,
        candidates: list,
        ctx: SchedulingContext,
    ) -> list:
        slots = ctx.max_concurrency - sum(ctx.running_by_type.values())
        if slots <= 0:
            return []
        # Promote the most recently successful type to the front; fall back to FIFO.
        if self._last_success:
            preferred = [r for r in candidates if r.workflow_type == self._last_success]
            others = [r for r in candidates if r.workflow_type != self._last_success]
            ordered = preferred + others
        else:
            ordered = candidates
        running = dict(ctx.running_by_type)
        selected = []
        for req in ordered:
            if len(selected) >= slots:
                break
            if running.get(req.workflow_type, 0) < ctx.max_concurrency_per_type:
                selected.append(req)
                running[req.workflow_type] = running.get(req.workflow_type, 0) + 1
        return selected

    def on_outcome(self, request, result: WorkflowResult) -> None:
        if result.returncode == 0:
            self._last_success = request.workflow_type


def recency_policy() -> _RecencyBiasPolicy:
    """Custom policy — prefers the workflow type that succeeded most recently."""
    return _RecencyBiasPolicy()


# ── Factory ────────────────────────────────────────────────────────────────────


def make_policy(name: str) -> SchedulingPolicy:
    """Return a fresh policy instance for the given name.

    Each campaign needs its own instance — AuCaM rejects one already in use.
    """
    if name == "bandit":
        return BanditSchedulingPolicy()
    if name == "science":
        return science_bandit()
    if name == "adr":
        return adr_policy()
    if name == "bandit_tight":
        # Low exploration — exploits faster once arms are characterised.
        return BanditSchedulingPolicy(exploration=0.5)
    if name == "llm":
        # Stub: falls back to FIFO today; shows the LLMSchedulingPolicy API.
        return LLMSchedulingPolicy(model="llama3:8b")
    if name == "custom":
        return recency_policy()
    return FIFOSchedulingPolicy()  # "fifo"
