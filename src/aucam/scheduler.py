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

import asyncio
import json
import logging
import math
import os
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .types import (
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
    requested    — the request's own exact amounts (the controller's intent for
                   this workflow); None: treat ``proposal`` as requested.
    start        — the type's starting values (WorkflowConfig.spending_limit /
                   resources), used until it has learned values.
    declared     — the type's declared_spending (no wall_hours); starting value
                   × (1 + size_margin) where the type gives none.
    raised       — spending / resources raised after a workflow of this type was
                   cancelled at its own limit, until learned values take over.
    allocations  — the allocation history: one AllocationRecord per finished
                   workflow (allocation, run time, spending, outcome, source,
                   progress, credit, pending).
    learn        — False: the type has a fixed amount (WorkflowConfig.learn);
                   learned and raised amounts are empty, and a learner
                   overriding size() should keep the default amount.
    """

    proposal: Allocation
    learned_cost: dict[str, float]
    remaining: dict[str, float]
    queued: int
    learned_resources: dict[str, float] = field(default_factory=dict)
    by_type: dict[str, dict] = field(default_factory=dict)
    requested: Allocation | None = None
    start: Allocation | None = None
    declared: dict[str, float] = field(default_factory=dict)
    raised: dict[str, float] = field(default_factory=dict)
    raised_resources: dict[str, float] = field(default_factory=dict)
    # Every finished workflow so far, oldest first (AllocationRecord): what it
    # got, how it ended, its progress and credit.  For learners overriding size().
    allocations: list = field(default_factory=list)
    # Resources free now (capacity minus what running workflows were given) and
    # the campaign's total capacity; a resource it does not name counts as 0.
    free: dict[str, float] = field(default_factory=dict)
    capacity: dict[str, float] = field(default_factory=dict)
    learn: bool = True


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

    # ── AI policies (S1): decide ahead, apply fast ──────────────────────────
    # select() and size() run every time a slot frees up, so they must be fast.
    # A policy that needs a slow call (an LLM, ADR) makes it in refresh(), which
    # AuCaM runs in the background once per policy cycle, and when a workflow
    # type it has not seen is queued; select() / size() then use its result.

    refresh_timeout_s: float = 60.0  # a refresh() still running after this is cancelled
    # Model usage (take_usage()) is charged to the campaign's ledger only if the
    # campaign limits that dimension, or with charge_usage = True.
    charge_usage: bool = False

    async def refresh(self, ctx: SchedulingContext, queued: list[LaunchRequest], obs: dict) -> None:
        """Optional: compute decisions ahead for select() / size() (default: nothing).

        ``queued`` are the waiting requests, ``obs`` the controller's last
        observation (budget, by_type, learned costs, …).  An exception or a
        timeout only means select() keeps using what it had, or its fallback.
        """

    def take_usage(self) -> dict[str, float]:
        """Model usage since the last call, e.g. {"llm_tokens": 1200} (default: none)."""
        return {}

    # Sizing settings, used by the default size(); set them on an instance.
    size_margin: float = 0.25  # learned cost (and declared) × (1 + margin)

    def value(self, workflow_type: str, ctx: SizingContext) -> float:
        """How much budget to give this type relative to its learned cost (default 1.0).

        Placeholder for value-based sizing (U2): size() multiplies the
        learned-cost amount by this factor.  Override it to give more budget to
        types with a better result per unit spent, e.g. from
        ``ctx.by_type[workflow_type]["cost_per_completed"]`` and ``["mean_score"]``.
        """
        return 1.0

    def size(self, request: LaunchRequest, ctx: SizingContext) -> Allocation | None:
        """Decide what one workflow gets: its spending limit and its resources.

        Called when the request is about to launch.  Default rule, per spending
        dimension, the first that applies:
          1. the request's own amount (the controller's intent) — as given;
          2. the type's learned cost × (1 + size_margin) × value(), once it has
             enough finished workflows (min_cost_samples);
          3. the raised amount, after a workflow of the type was cancelled at
             its own limit (that limit × (1 + size_margin));
          4. the type's starting value (WorkflowConfig.spending_limit) — as given;
          5. the type's declared_spending × (1 + size_margin).
        Resources follow the same order with learned peak use (rounded up) and
        no declared step.  Returns None if the spending does not fit what is
        left in the campaign budget: the request waits while other workflows
        run, and is dropped once nothing else runs.
        """
        margin = 1.0 + self.size_margin
        requested = ctx.requested if ctx.requested is not None else ctx.proposal
        start = ctx.start if ctx.start is not None else Allocation()

        spend: dict[str, float] = {}
        s_keys = (
            set(requested.spending_limit)
            | set(ctx.learned_cost)
            | set(ctx.raised)
            | set(start.spending_limit)
            | set(ctx.declared)
        ) - {"wall_hours"}
        for key in s_keys:
            if key in requested.spending_limit:
                value = requested.spending_limit[key]
            elif key in ctx.learned_cost:
                value = ctx.learned_cost[key] * margin * self.value(request.workflow_type, ctx)
            elif key in ctx.raised:
                value = ctx.raised[key]
            elif key in start.spending_limit:
                value = start.spending_limit[key]
            else:
                value = ctx.declared[key] * margin
            spend[key] = float(value)

        res: dict[str, float] = {}
        r_keys = (
            set(requested.resources)
            | set(ctx.learned_resources)
            | set(ctx.raised_resources)
            | set(start.resources)
        )
        for key in r_keys:
            if key in requested.resources:
                value = requested.resources[key]
            elif key in ctx.learned_resources:
                value = math.ceil(ctx.learned_resources[key] * margin - 1e-9)
            elif key in ctx.raised_resources:
                value = ctx.raised_resources[key]
            else:
                value = start.resources[key]
            res[key] = float(value)

        for key, value in spend.items():
            if key in ctx.remaining and value > ctx.remaining[key] + 1e-12:
                return None
        return Allocation(spending_limit=spend, resources=res)


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
# AI policies (S1) — a ranking of workflow types, computed in refresh()
# ---------------------------------------------------------------------------


def parse_ranking(data: Any, known: set[str] | None = None) -> tuple[list[str], dict[str, float]]:
    """Read ``{"rank": [type, ...], "value": {type: factor}}`` from a model or ADR.

    Types not in ``known`` (when given) are dropped; factors are clamped to
    0.1–10.  Raises ValueError if ``rank`` is missing or not a list.
    """
    if not isinstance(data, dict) or not isinstance(data.get("rank"), list):
        raise ValueError(f"expected {{'rank': [...]}}, got {str(data)[:200]!r}")
    rank: list[str] = []
    for t in data["rank"]:
        t = str(t)
        if (known is None or t in known) and t not in rank:
            rank.append(t)
    factors: dict[str, float] = {}
    for t, v in (data.get("value") or {}).items():
        if known is None or t in known:
            try:
                factors[str(t)] = min(10.0, max(0.1, float(v)))
            except (TypeError, ValueError):
                continue
    return rank, factors


class RankingSchedulingPolicy(SchedulingPolicy):
    """Base for AI scheduling policies: refresh() ranks workflow types, select() applies it.

    A subclass computes the ranking in ``refresh()`` (an LLM, ADR, …) and
    calls ``set_ranking()``.  While the ranking is at most ``max_age_cycles``
    policy cycles old, select() starts queued requests in ranked order (types
    it does not name last, in queue order), within the free slots and per-type
    caps, and value() returns the factors given with it (size() multiplies a
    type's learned-cost amount by them).  Without a fresh ranking — before
    the first, or after errors — the ``fallback`` policy decides (default: a
    new BanditSchedulingPolicy), and it always learns from outcomes.
    """

    def __init__(
        self,
        *,
        fallback: SchedulingPolicy | None = None,
        max_age_cycles: int = 3,
        refresh_timeout_s: float = 60.0,
    ) -> None:
        self.fallback = fallback if fallback is not None else BanditSchedulingPolicy()
        self.max_age_cycles = max_age_cycles
        self.refresh_timeout_s = refresh_timeout_s
        self.ranking: list[str] | None = None
        self.factors: dict[str, float] = {}
        self._ranked_cycle = -(10**9)
        self._cycle = 0  # latest policy cycle seen

    def set_ranking(self, ranking: list[str], factors: dict[str, float] | None, cycle: int) -> None:
        self.ranking = list(ranking)
        self.factors = dict(factors or {})
        self._ranked_cycle = self._cycle = cycle

    def fresh(self) -> bool:
        """True while the ranking is at most max_age_cycles policy cycles old."""
        return self.ranking is not None and self._cycle - self._ranked_cycle <= self.max_age_cycles

    def select(
        self, candidates: list[LaunchRequest], ctx: SchedulingContext
    ) -> list[LaunchRequest]:
        self._cycle = max(self._cycle, ctx.policy_cycles)
        if not self.fresh():
            return self.fallback.select(candidates, ctx)
        order = {t: i for i, t in enumerate(self.ranking or [])}
        ranked = sorted(candidates, key=lambda r: order.get(r.workflow_type, len(order)))
        return SchedulingPolicy.select(self, ranked, ctx)

    def value(self, workflow_type: str, ctx: SizingContext) -> float:
        if self.fresh():
            return self.factors.get(workflow_type, 1.0)
        return self.fallback.value(workflow_type, ctx)

    def on_outcome(self, request: LaunchRequest, result: WorkflowResult) -> None:
        self.fallback.on_outcome(request, result)

    def state(self) -> Any:
        return {"fallback": self.fallback.state()}

    def restore(self, state: Any) -> None:
        if isinstance(state, dict) and state.get("fallback") is not None:
            self.fallback.restore(state["fallback"])


_LLM_SYSTEM_PROMPT = """\
You help schedule an HPC science campaign. Workflows of several types wait in a
queue; each time a slot frees up, the queued requests start in the order of
your ranking of their types. Rank the types so the campaign reaches its goal
sooner within its budget: favour types that complete often, score well and
cost little, but keep trying a type that has little history. You may also
give a factor per type (0.1-10, default 1) to scale the budget a workflow of
that type gets.
Reply with JSON only: {"rank": ["best_type", ...], "value": {"type": 1.0}}"""


def _openai_chat(endpoint: str, api_key: str | None) -> Callable:
    """A client for an OpenAI-compatible chat-completions endpoint (stdlib only)."""
    url = endpoint.rstrip("/") + "/chat/completions"

    def _post(body: dict, timeout: float) -> dict:
        req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST")
        req.add_header("Content-Type", "application/json")
        if api_key:
            req.add_header("Authorization", f"Bearer {api_key}")
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
            return json.loads(resp.read())

    async def chat(messages, *, model, max_tokens, temperature, timeout=60.0):
        body = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        data = await asyncio.to_thread(_post, body, timeout)
        return data["choices"][0]["message"]["content"], data.get("usage") or {}

    return chat


def _json_from(text: str) -> Any:
    """The first JSON object in a model's reply (it may add text or code fences)."""
    start = text.find("{")
    while start != -1:
        depth = 0
        for k in range(start, len(text)):
            depth += {"{": 1, "}": -1}.get(text[k], 0)
            if depth == 0:
                try:
                    return json.loads(text[start : k + 1])
                except ValueError:
                    break
        start = text.find("{", start + 1)
    raise ValueError(f"no JSON object in the model's reply: {text[:200]!r}")


class LLMSchedulingPolicy(RankingSchedulingPolicy):
    """Ranks workflow types with a language model, once per policy cycle (S1).

    In refresh() it sends the model a short summary of the campaign — queued
    and running types, caps, budget, per-type results and learned costs
    (``render()``) — and reads back ``{"rank": [...], "value": {...}}``.
    select() / size() apply it while it is fresh; otherwise, and if the model
    fails, times out or answers something unusable, ``fallback`` decides.

    Parameters
    ----------
    model : str
        Model name passed to the endpoint, e.g. ``"llama3.1:8b"``.
    endpoint : str | None
        Base URL of an OpenAI-compatible API, e.g. ``"http://localhost:11434/v1"``
        (Ollama) or a hosted one.  Default: ``$AUCAM_LLM_ENDPOINT`` or
        ``$OPENAI_BASE_URL``.  Without one (and no ``client``) the policy only
        uses its fallback, with a warning.
    api_key : str | None
        Default: ``$AUCAM_LLM_API_KEY`` or ``$OPENAI_API_KEY``.
    client : async callable, optional
        ``await client(messages, model=, max_tokens=, temperature=)`` →
        ``(text, usage)``, instead of the built-in HTTP client (e.g. for tests
        or another SDK).
    system_prompt, max_tokens, temperature
        Sent with every request.
    fallback, max_age_cycles, refresh_timeout_s
        See RankingSchedulingPolicy.
    charge_usage : bool
        Charge the tokens used (``llm_tokens``) to the campaign's ledger even
        if the campaign does not limit them.
    """

    def __init__(
        self,
        *,
        model: str,
        endpoint: str | None = None,
        api_key: str | None = None,
        client: Callable | None = None,
        system_prompt: str | None = None,
        max_tokens: int = 512,
        temperature: float = 0.0,
        fallback: SchedulingPolicy | None = None,
        max_age_cycles: int = 3,
        refresh_timeout_s: float = 60.0,
        charge_usage: bool = False,
    ) -> None:
        super().__init__(
            fallback=fallback, max_age_cycles=max_age_cycles, refresh_timeout_s=refresh_timeout_s
        )
        self.model = model
        self.endpoint = (
            endpoint or os.environ.get("AUCAM_LLM_ENDPOINT") or os.environ.get("OPENAI_BASE_URL")
        )
        key = api_key or os.environ.get("AUCAM_LLM_API_KEY") or os.environ.get("OPENAI_API_KEY")
        self._client = client or (_openai_chat(self.endpoint, key) if self.endpoint else None)
        self.system_prompt = system_prompt or _LLM_SYSTEM_PROMPT
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.charge_usage = charge_usage
        self._tokens = 0.0
        self._warned = False

    def render(self, ctx: SchedulingContext, queued: list[LaunchRequest], obs: dict) -> str:
        """The campaign summary sent to the model; override to add your own data."""
        budget = obs.get("budget") or {}
        types = sorted(
            {r.workflow_type for r in queued}
            | set(ctx.running_by_type)
            | set(budget.get("by_type") or {})
        )
        queued_n: dict[str, int] = {}
        for r in queued:
            queued_n[r.workflow_type] = queued_n.get(r.workflow_type, 0) + 1
        summary = {
            "types": types,
            "queued": queued_n,
            "running": dict(ctx.running_by_type),
            "max_running": {t: ctx.cap(t) for t in types},
            "budget_remaining_fraction": round(ctx.budget_remaining_fraction, 3),
            "budget": {k: budget.get(k) for k in ("limits", "spent", "burn") if budget.get(k)},
            "by_type": budget.get("by_type") or {},
            "learned_cost": obs.get("learned_cost") or {},
            "policy_cycle": ctx.policy_cycles,
        }
        return json.dumps(summary, default=str)

    async def refresh(self, ctx: SchedulingContext, queued: list[LaunchRequest], obs: dict) -> None:
        self._cycle = max(self._cycle, ctx.policy_cycles)
        if self._client is None:
            if not self._warned:
                logger.warning(
                    "[LLM] no endpoint (endpoint=, $AUCAM_LLM_ENDPOINT or $OPENAI_BASE_URL)"
                    " — using the fallback policy"
                )
                self._warned = True
            return
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": self.render(ctx, queued, obs)},
        ]
        text, usage = await self._client(
            messages, model=self.model, max_tokens=self.max_tokens, temperature=self.temperature
        )
        usage = usage or {}
        self._tokens += float(
            usage.get("total_tokens")
            or (usage.get("prompt_tokens") or 0) + (usage.get("completion_tokens") or 0)
        )
        budget = obs.get("budget") or {}
        known = (
            {r.workflow_type for r in queued}
            | set(ctx.running_by_type)
            | set(budget.get("by_type") or {})
        )
        rank, factors = parse_ranking(_json_from(text), known)
        self.set_ranking(rank, factors, ctx.policy_cycles)
        logger.info("[LLM] rank=%s value=%s", rank, factors)

    def take_usage(self) -> dict[str, float]:
        tokens, self._tokens = self._tokens, 0.0
        return {"llm_tokens": tokens} if tokens else {}
