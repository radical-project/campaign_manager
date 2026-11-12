"""Scheduling policies for AuCaM campaigns.

A SchedulingPolicy decides *which* queued LaunchRequests to launch on each
scheduler tick.  It knows nothing about the campaign budget — that is enforced
by the campaign (workflow-count caps, spending limit) before any launch.

Built-in policies
-----------------
FIFOSchedulingPolicy    — FIFO respecting concurrency caps
BanditSchedulingPolicy  — UCB1 bandit; learns per-workflow-type arm scores
                          from WorkflowResult rewards (default)
ADRSchedulingPolicy     — RADICAL Autonomous Decision Runtime; rule-override hook +
                          ADR engine integration (STUB — falls back to FIFO)
LLMSchedulingPolicy     — LLM-guided ranking (model-agnostic)
                          (STUB — falls back to FIFO)

A policy instance keeps what it learned: passing the same instance to the
next campaign's spec carries that knowledge over; pass a new instance to start
fresh.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .types import (
    RESOURCE_KEYS,
    Allocation,
    LaunchRequest,
    TelemetrySnapshot,
    WorkflowResult,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SchedulingContext — what the policy sees each scheduling tick
# ---------------------------------------------------------------------------


@dataclass
class SchedulingContext:
    """
    Snapshot passed to SchedulingPolicy.select() on each scheduling tick.

    running_by_type       — {workflow_type: count} currently active in this campaign.
    max_concurrency       — campaign-level concurrent-workflow cap.
    max_concurrency_per_type — default per-workflow-type cap
                           (CampaignBudget.max_concurrency_per_workflow).
    max_running_by_type   — caps of types that set their own
                           (WorkflowConfig.max_running); use ``cap(type)``.
    telemetry             — node resources + live wall times (last policy-cycle snapshot).
    history               — recent WorkflowResults from this campaign, newest last.
    policy_cycles         — how many policy cycles have elapsed.
    campaign_name         — name of the campaign making the selection.
    """

    running_by_type: dict[str, int]
    max_concurrency: int
    max_concurrency_per_type: int
    telemetry: TelemetrySnapshot
    history: list[WorkflowResult]
    policy_cycles: int
    campaign_name: str
    budget_remaining_fraction: float = 1.0
    max_running_by_type: dict[str, int] = field(default_factory=dict)

    def cap(self, workflow_type: str) -> int:
        """How many workflows of this type may run at once (W1)."""
        return self.max_running_by_type.get(workflow_type, self.max_concurrency_per_type)


@dataclass
class SizingContext:
    """
    What SchedulingPolicy.size() sees when one request is about to launch.

    proposal     — the request's own amounts, else the workflow type's defaults
                   (WorkflowConfig.spending_limit / resources).
    learned_cost — mean actual spending of successful runs of this type, per
                   dimension, once at least 3 have finished (empty before).
    remaining    — campaign spending limit − spent − in flight, per limited
                   dimension (absent: no campaign limit).
    queued       — requests waiting in the queue, including this one.
    learned_resources — mean peak CPUs / GPUs in use at once of successful runs
                   of this type, once at least 3 have finished (empty before).
    by_type      — per workflow type: finished counts, spending, cost per
                   completed workflow, mean score (as obs["budget"]["by_type"]).
    """

    proposal: Allocation
    learned_cost: dict[str, float]
    remaining: dict[str, float]
    queued: int
    learned_resources: dict[str, float] = field(default_factory=dict)
    by_type: dict[str, dict] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Base policy — FIFO (concrete, not abstract)
# ---------------------------------------------------------------------------


class SchedulingPolicy:
    """
    Base scheduling policy.  Default behaviour is FIFO — launch candidates
    in queue order up to the concurrency caps.  Override select() and/or
    on_outcome() for learned or rule-based policies.
    """

    def select(
        self,
        candidates: list[LaunchRequest],
        ctx: SchedulingContext,
    ) -> list[LaunchRequest]:
        """Return the subset of candidates to launch right now.

        Candidates are already in priority order (from WorkflowQueue.candidates()).
        The returned list may be empty (nothing to launch yet).
        Budget fits-check is applied by the caller after select() returns —
        the policy must not duplicate that check.
        """
        slots = ctx.max_concurrency - sum(ctx.running_by_type.values())
        if slots <= 0:
            return []
        running = dict(ctx.running_by_type)
        selected = []
        for req in candidates:
            if len(selected) >= slots:
                break
            if running.get(req.workflow_type, 0) < ctx.cap(req.workflow_type):
                selected.append(req)
                running[req.workflow_type] = running.get(req.workflow_type, 0) + 1
        return selected

    def on_outcome(self, request: LaunchRequest, result: WorkflowResult) -> None:
        """Called after every workflow completes.  Override to update learned state."""
        pass

    def state(self) -> Any:
        """What this policy has learned, saved in the campaign checkpoint (R1).

        Return something JSON-serializable, or None (default: nothing to save).
        """
        return None

    def restore(self, state: Any) -> None:
        """Load what ``state()`` returned, when a campaign restarts from a checkpoint."""

    # Sizing settings, used by the default size(); set them on an instance.
    size_margin: float = 0.25  # learned cost × (1 + margin) when a range is given
    fair_share: bool = False  # cap each run at remaining budget / queued requests

    def value(self, workflow_type: str, ctx: SizingContext) -> float:
        """How much budget to give this type relative to its learned cost (default 1.0).

        Placeholder for value-based sizing (U2): size() multiplies a spending
        range's learned-cost amount by this factor, then keeps it within the
        range and the remaining budget.  Override it to give more budget to
        types with a better result per unit spent, e.g. from
        ``ctx.by_type[workflow_type]["cost_per_completed"]`` and ``["mean_score"]``.
        """
        return 1.0

    def size(self, request: LaunchRequest, ctx: SizingContext) -> Allocation | None:
        """Decide what one run gets: its spending limit and its resources.

        Called when the request is about to launch.  Return None to drop the
        request because it does not fit the remaining campaign budget.

        Default:
          - an explicit amount (the request's, else the type's default) is kept;
          - a key with only a range (LaunchRequest.bounds or WorkflowConfig.bounds)
            gets, for spending, the learned cost × (1 + size_margin) × value(),
            or the range's maximum before costs are learned; for a resource, the
            learned peak use × (1 + size_margin) rounded up, or the range's
            maximum before it is learned;
          - every amount is kept within its range;
          - spending is capped at what is left in the campaign budget (and at
            remaining / queued requests with fair_share); a cap below the
            range's minimum drops the request.
        """
        spend = dict(ctx.proposal.spending_limit)
        res = dict(ctx.proposal.resources)
        bounds = request.bounds or {}
        for key, (lo, hi) in bounds.items():
            target = res if key in RESOURCE_KEYS else spend
            if key in target:
                value = target[key]
            elif key not in RESOURCE_KEYS and key in ctx.learned_cost:
                value = (
                    ctx.learned_cost[key]
                    * (1.0 + self.size_margin)
                    * self.value(request.workflow_type, ctx)
                )
            elif key in RESOURCE_KEYS and key in ctx.learned_resources:
                value = math.ceil(ctx.learned_resources[key] * (1.0 + self.size_margin) - 1e-9)
            else:
                value = hi
            target[key] = min(max(float(value), float(lo)), float(hi))
        capped = False
        for key, value in list(spend.items()):
            if key not in ctx.remaining:
                continue
            cap = ctx.remaining[key]
            if self.fair_share:
                cap = cap / max(1, ctx.queued)
            if value > cap:
                lo = float(bounds[key][0]) if key in bounds else 0.0
                if cap <= 0.0 or cap < lo:
                    return None
                spend[key] = cap
                capped = True
        return Allocation(spending_limit=spend, resources=res, capped=capped)


class FIFOSchedulingPolicy(SchedulingPolicy):
    """FIFO policy — same as the SchedulingPolicy base; provided as an explicit name."""

    pass


# ---------------------------------------------------------------------------
# Bandit policy — UCB1
# ---------------------------------------------------------------------------


def _default_reward(result: WorkflowResult) -> float:
    """Throughput reward: 1.0 for success, 0.0 for cancellation or failure.

    The scheduler cares about slot efficiency, not science quality.
    Science-domain scores belong to the CampaignController.
    """
    return 1.0 if result.returncode == 0 else 0.0


class BanditSchedulingPolicy(SchedulingPolicy):
    """
    UCB1 bandit scheduler.

    Arm = workflow_type (the policy belongs to one campaign).  Context is
    intentionally excluded: DAG routing
    fields (triggered_by, etc.) encode urgency, which the priority system
    already handles — they are not scheduling-relevant parameters.

    Reward is throughput-based (1.0 = slot produced a result, 0.0 = wasted).
    Science-domain scores belong to the CampaignController, not here.

    Arms with no pull history get UCB = ∞ so every type is explored before
    exploitation begins.  After exploration:
        UCB = mean_reward + C × √(ln(N) / n)

    Parameters
    ----------
    exploration : float
        UCB exploration constant C.  Default √2 is theoretically optimal for
        rewards in [0, 1].
    reward_fn : Callable[[WorkflowResult], float] | None
        Override the default throughput reward.  Must return a value in [0, 1].
    cost_aware : bool
        Weigh the reward by cost (U2): reward × 1 / (1 + cost_weight × relative
        cost), where relative cost is the workflow's spending ÷ the mean spending
        of all finished workflows of the campaign, averaged over its dimensions.
        A type that succeeds as often but costs twice the average then earns a
        third of the reward with cost_weight=1.  Default False: today's reward.
    cost_weight : float
        How strongly cost lowers the reward when cost_aware (default 1.0).
    """

    def __init__(
        self,
        *,
        exploration: float = math.sqrt(2),
        reward_fn: Callable[[WorkflowResult], float] | None = None,
        cost_aware: bool = False,
        cost_weight: float = 1.0,
    ) -> None:
        self._exploration = exploration
        self._reward_fn = reward_fn or _default_reward
        self.cost_aware = cost_aware
        self.cost_weight = cost_weight
        # Mean spending per dimension over all finished workflows (cost_aware).
        self._cost_mean: dict[str, dict[str, float]] = {}
        # arm_key → {"n": int, "mean": float}
        self._arms: dict[str, dict[str, float | int]] = {}
        self._total_pulls: int = 0

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _arm_key(self, req: LaunchRequest) -> str:
        return req.workflow_type

    def _ucb(self, arm_key: str, c: float | None = None) -> float:
        stats = self._arms.get(arm_key)
        if stats is None or stats["n"] == 0 or self._total_pulls == 0:
            return float("inf")  # unvisited — always explore first
        n = int(stats["n"])
        mean = float(stats["mean"])
        c_eff = self._exploration if c is None else c
        return mean + c_eff * math.sqrt(math.log(self._total_pulls) / n)

    def state(self) -> dict:
        """Arm statistics and cost means, for the checkpoint (R1)."""
        return {
            "arms": {k: dict(v) for k, v in self._arms.items()},
            "total_pulls": self._total_pulls,
            "cost_mean": {k: dict(v) for k, v in self._cost_mean.items()},
        }

    def restore(self, state: dict) -> None:
        if not isinstance(state, dict):
            return
        self._arms = {k: dict(v) for k, v in (state.get("arms") or {}).items()}
        self._total_pulls = int(state.get("total_pulls", 0))
        self._cost_mean = {k: dict(v) for k, v in (state.get("cost_mean") or {}).items()}

    def _relative_cost(self, spent: dict[str, float]) -> float:
        """The workflow's spending ÷ the campaign's mean so far, averaged over dimensions.

        Updates the means with this workflow first; 1.0 when it reports no spending.
        """
        ratios = []
        for key, val in (spent or {}).items():
            if key == "wall_hours":
                continue
            m = self._cost_mean.setdefault(key, {"n": 0, "mean": 0.0})
            m["mean"] = (m["mean"] * m["n"] + float(val)) / (m["n"] + 1)
            m["n"] += 1
            if m["mean"] > 0:
                ratios.append(float(val) / m["mean"])
        return sum(ratios) / len(ratios) if ratios else 1.0

    def arm_stats(self) -> dict[str, dict]:
        """Return a copy of the current arm belief table (for inspection / logging)."""
        return {k: {**v, "ucb": self._ucb(k)} for k, v in self._arms.items()}

    # ── SchedulingPolicy interface ────────────────────────────────────────────

    def select(
        self,
        candidates: list[LaunchRequest],
        ctx: SchedulingContext,
    ) -> list[LaunchRequest]:
        slots = ctx.max_concurrency - sum(ctx.running_by_type.values())
        if slots <= 0:
            return []

        # Scale exploration by sqrt(budget_remaining) so the bandit shifts
        # toward pure exploitation as the budget runs out.
        c_eff = self._exploration * math.sqrt(max(0.0, ctx.budget_remaining_fraction))

        running = dict(ctx.running_by_type)
        eligible: list[tuple[float, int, LaunchRequest]] = []
        for idx, req in enumerate(candidates):
            if running.get(req.workflow_type, 0) < ctx.cap(req.workflow_type):
                ucb = self._ucb(self._arm_key(req), c=c_eff)
                # idx as tiebreaker preserves original queue order for equal UCB
                eligible.append((ucb, idx, req))

        # Descending UCB; ties broken by original position (ascending idx)
        eligible.sort(key=lambda x: (-x[0], x[1]))

        logger.info(
            "[BANDIT] %s  budget_rem=%.1f%%  c_eff=%.3f  arms=%s",
            ctx.campaign_name,
            ctx.budget_remaining_fraction * 100,
            c_eff,
            {
                self._arm_key(
                    r
                ): f"μ={self._arms.get(self._arm_key(r), {}).get('mean', float('nan')):.3f}"
                for _, _, r in eligible
            },
        )

        selected: list[LaunchRequest] = []
        for ucb, _, req in eligible:
            if len(selected) >= slots:
                break
            t = running.get(req.workflow_type, 0)
            if t < ctx.cap(req.workflow_type):
                selected.append(req)
                running[req.workflow_type] = t + 1
                if ucb < float("inf"):
                    logger.info(
                        "[BANDIT] %s  → %s  ucb=%.3f  c_eff=%.3f",
                        ctx.campaign_name,
                        self._arm_key(req),
                        ucb,
                        c_eff,
                    )

        return selected

    def on_outcome(self, request: LaunchRequest, result: WorkflowResult) -> None:
        arm = self._arm_key(request)
        reward = self._reward_fn(result)
        if self.cost_aware:
            reward *= 1.0 / (1.0 + self.cost_weight * self._relative_cost(result.spent))
        stats = self._arms.setdefault(arm, {"n": 0, "mean": 0.0})
        n = int(stats["n"])
        stats["mean"] = (float(stats["mean"]) * n + reward) / (n + 1)
        stats["n"] = n + 1
        self._total_pulls += 1
        logger.debug(
            "[BANDIT] outcome arm=%s reward=%.3f mean=%.3f n=%d",
            arm,
            reward,
            float(stats["mean"]),
            n + 1,
        )


# ---------------------------------------------------------------------------
# ADR policy — RADICAL Autonomous Decision Runtime (stub)
# ---------------------------------------------------------------------------


class ADRSchedulingPolicy(SchedulingPolicy):
    """
    RADICAL Autonomous Decision Runtime (ADR) scheduling policy.

    Designed to integrate with the RADICAL-ADR policy engine: it maintains
    a running observation window and supports an optional rule-override hook
    that fires before the learned policy, letting the campaign inject hard
    constraints (e.g. "never launch hog_worker while GPU utilisation > 80%").

    NOT YET IMPLEMENTED — currently falls back to FIFO with a one-time WARNING.
    Use BanditSchedulingPolicy for learned scheduling today.

    Parameters
    ----------
    rule_fn : Callable[[list[LaunchRequest], SchedulingContext], list[LaunchRequest] | None]
        Optional override hook.  Called before ADR logic on every tick.
        Return a non-None list to short-circuit ADR; return None to defer.
    """

    def __init__(
        self,
        *,
        rule_fn: Callable[[list[LaunchRequest], SchedulingContext], list[LaunchRequest] | None]
        | None = None,
    ) -> None:
        self._rule_fn = rule_fn
        self._warned = False

    def select(
        self,
        candidates: list[LaunchRequest],
        ctx: SchedulingContext,
    ) -> list[LaunchRequest]:
        if not self._warned:
            logger.warning(
                "[ADR] ADRSchedulingPolicy is a stub — falling back to FIFO. "
                "Use BanditSchedulingPolicy for learned scheduling."
            )
            self._warned = True
        if self._rule_fn is not None:
            override = self._rule_fn(candidates, ctx)
            if override is not None:
                return override
        return super().select(candidates, ctx)


# ---------------------------------------------------------------------------
# LLM policy — LLM-guided scheduling (stub)
# ---------------------------------------------------------------------------


class LLMSchedulingPolicy(SchedulingPolicy):
    """
    LLM-guided scheduling policy.

    On each scheduling tick, serializes the candidates and SchedulingContext
    into a prompt and asks an LLM to rank and select which workflows to launch
    next.  Model-agnostic: any OpenAI-compatible endpoint is supported.

    NOT YET IMPLEMENTED — currently falls back to FIFO with a one-time WARNING.

    Parameters
    ----------
    model : str
        Model identifier passed to the endpoint (required; e.g. ``"llama3:8b"``).
    endpoint : str | None
        Base URL of an OpenAI-compatible chat-completions API.
        ``None`` must be set before use; there is no built-in default provider.
    system_prompt : str | None
        Custom system prompt prepended to the per-tick observation.  The
        built-in default describes AuCaM scheduling semantics.
    max_tokens : int
        Maximum tokens to request from the model per scheduling tick.
    """

    def __init__(
        self,
        *,
        model: str,
        endpoint: str | None = None,
        system_prompt: str | None = None,
        max_tokens: int = 512,
    ) -> None:
        self._model = model
        self._endpoint = endpoint
        self._system_prompt = system_prompt
        self._max_tokens = max_tokens
        self._warned = False

    def select(
        self,
        candidates: list[LaunchRequest],
        ctx: SchedulingContext,
    ) -> list[LaunchRequest]:
        if not self._warned:
            logger.warning(
                "[LLM] LLMSchedulingPolicy is a stub (model=%s) — falling back to FIFO.",
                self._model,
            )
            self._warned = True
        return super().select(candidates, ctx)
