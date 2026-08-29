"""Wrapper policies and the make_scheduling_policy factory."""

from __future__ import annotations

import logging
from typing import Optional

from radical.adr import Decision, Policy, decide

from .bandit import BanditSchedulingPolicy
from .ensemble import BlendPolicy
from .llm import LLMSchedulingPolicy
from .rule import DownstreamFirstPolicy

log = logging.getLogger(__name__)


class NullSchedulingPolicy(Policy):
    """No-op policy for goal-only supervision.

    Makes no scheduling decisions but satisfies the Policy interface so that
    ``operator.run()`` can proceed and evaluate ``@goals`` each cycle.
    Use when ``policy: none`` is set in config but campaign goals are still
    declared (``cm.adr.goals.n_target``), so early stopping still fires.
    """

    @decide
    async def run(self, obs: dict) -> Decision:
        return Decision(actions=[])


class LoggingPolicy(Policy):
    """Wraps any policy and prints a one-line ADR decision summary each cycle.

    Extracts priority and batch-size assignments from ``Decision.actions`` and
    shows only what changed vs. the previous cycle, so the log stays readable
    over many cycles.  Reads ``inner._regime`` when present to surface which
    decision branch was taken (any policy may expose ``self._regime``).

    Example output::

        [ADR c=  4]  n_hits=3  [NORMAL]  | stage_a(r=4/4 p=1 H)  stage_b(r=3/4 p=7 H)  | priorities: stage_b=101→102
        [ADR c=  7]  n_hits=5  [STARVED] | stage_a(r=0/4 p=0 H)  stage_b(r=3/4 p=4 H)  | priorities: stage_a=101→105  | STOP
    """

    def __init__(self, inner: Policy, *, log_every: int = 1) -> None:
        super().__init__()
        self._inner = inner
        self._log_every = log_every
        self._prev_priorities: dict[str, int] = {}

    @decide
    async def run(self, obs: dict) -> Decision:
        d = await self._inner.decide(obs)
        cycle = obs.get("cycle", 0)
        if cycle % self._log_every != 0 and not d.stop:
            return d

        # Parse decision actions into readable dicts.
        new_priorities: dict[str, int] = {}
        new_batch: dict[str, int] = {}
        for a in d.actions:
            name = getattr(a, "task_name", None)
            kw   = getattr(a, "task_kwargs", {})
            if name == "set_priority":
                new_priorities[kw["stage"]] = kw["priority"]
            elif name == "set_batch_size":
                new_batch[kw["stage"]] = kw["size"]

        # Stage status — compact: name(r=running/cap p=pending BP_initial)
        stages = obs.get("stages", {})
        stage_str = "  ".join(
            f"{name}(r={s.get('running', 0)}/{s.get('cap', '?')}"
            f" p={s.get('pending', 0)}"
            f" {s.get('bp_state', 'HOLD')})"
            for name, s in stages.items()
        )

        # Priority diff — only show stages whose priority changed.
        prio_parts = []
        for stage in sorted(new_priorities):
            new_p = new_priorities[stage]
            old_p = self._prev_priorities.get(stage)
            if old_p is None:
                prio_parts.append(f"{stage}={new_p}")
            elif old_p != new_p:
                prio_parts.append(f"{stage}={old_p}→{new_p}")
        prio_str = "  ".join(prio_parts) if prio_parts else "(unchanged)"

        # Batch size changes.
        batch_str = "  ".join(f"{s}={v}" for s, v in sorted(new_batch.items()))

        # Optional regime label set by the inner policy (e.g. DdSimRulePolicy).
        regime = getattr(self._inner, "_regime", None)

        n_hits = obs.get("n_hits", 0)
        parts = [f"[ADR c={cycle:>3}]  n_hits={n_hits}"]
        if regime:
            parts.append(f"[{regime}]")
        parts.append(f"|  {stage_str}")
        parts.append(f"|  priorities: {prio_str}")
        if batch_str:
            parts.append(f"|  batch: {batch_str}")
        if d.stop:
            parts.append("|  STOP")

        print("  ".join(parts))
        self._prev_priorities.update(new_priorities)
        return d

    @property
    def summary(self) -> dict:
        summ = getattr(self._inner, "summary", None)
        return summ if isinstance(summ, dict) else {}


class RuleCorrectionsPolicy(Policy):
    """Post-hoc priority corrections applied on top of rule or bandit inner policies.

    Applied per stage each cycle (corrections are relative to the inner policy's
    this-cycle assignment, or the live obs priority for stages inner didn't touch):

      stalls  (correct_stalls=True):
          +2 per 3 consecutive scheduler stall cycles, capped at +6.
          A stalling stage is losing slot contention — boost it to compete.

      frozen  (correct_budget=True):
          -1 when BudgetController for this stage is frozen (surrogate accuracy
          drifted). Frozen stages waste GPU hours on poor candidates.

      budget_burn  (correct_budget=True):
          -1 when Monitor has an active budget_burn alert for this stage.
          Overspending stages get a mild brake without hard-stopping them.

    Corrections stack freely; no net cap is applied.  A compact [corrections]
    line is printed only when at least one correction fires.
    Forwards ``_regime`` from the inner policy so LoggingPolicy can read it.
    """

    def __init__(
        self,
        inner: Policy,
        op,
        *,
        correct_stalls: bool = True,
        correct_budget: bool = True,
    ) -> None:
        super().__init__()
        self._inner = inner
        self._act = op.get_actions()
        self._correct_stalls = correct_stalls
        self._correct_budget = correct_budget
        # Cycles where at least one correction fired — readable by benchmarks.
        self.correction_cycles: int = 0

    @property
    def _regime(self):
        return getattr(self._inner, "_regime", None)

    @property
    def starved_cycles(self) -> int:
        return getattr(self._inner, "starved_cycles", 0)

    @property
    def mean_boost(self) -> float:
        return getattr(self._inner, "mean_boost", 0.0)

    @decide
    async def run(self, obs: dict) -> Decision:
        d = await self._inner.decide(obs)
        stages = obs.get("stages", {})
        if not stages:
            return d

        # Separate set_priority actions from everything else (batch sizes, triggers).
        inner_priorities: dict[str, int] = {}
        other_actions = []
        for a in d.actions:
            name = getattr(a, "task_name", None)
            kw = getattr(a, "task_kwargs", {})
            if name == "set_priority" and "stage" in kw and "priority" in kw:
                inner_priorities[kw["stage"]] = int(kw["priority"])
            else:
                other_actions.append(a)

        bc_map = obs.get("budget_controllers", {})
        monitor_alerts = obs.get("monitor_alerts", {})

        # Compute per-stage correction deltas.
        deltas: dict[str, int] = {}
        for stage, info in stages.items():
            delta = 0

            if self._correct_stalls:
                # Skip stall correction for source stages (is_source=True).
                # Source stages (e.g. inference) accumulate stall counts when a
                # downstream stage (e.g. miniapps) wins a contested slot — that is
                # the CORRECT outcome of downstream-first scheduling, not a stall to
                # fix.  Boosting a source stage here inverts the depth ordering and
                # can hand GPU slots back to inference when miniapps is starved.
                if not info.get("is_source", True):
                    stalls = int(info.get("stalls", 0))
                    # Fire on the first stall cycle (+2), ramp to cap (+6) by stall 3.
                    delta += min(2 * stalls, 6)

            if self._correct_budget:
                if bc_map.get(stage, {}).get("frozen", False):
                    delta -= 1
                if "budget_burn" in monitor_alerts.get(stage, []):
                    delta -= 1

            if delta != 0:
                deltas[stage] = delta

        if not deltas:
            return d

        self.correction_cycles += 1
        print(f"[corrections] {' '.join(f'{s}{v:+d}' for s, v in sorted(deltas.items()))}")

        # Rebuild priority actions with corrections; keep all non-priority actions.
        corrected_actions = list(other_actions)
        remaining = set(deltas)
        for stage, base in inner_priorities.items():
            adj = deltas.get(stage, 0)
            corrected_actions.append(self._act.set_priority(stage=stage, priority=base + adj))
            remaining.discard(stage)
        # Stages not touched by inner policy but needing correction: use live obs priority.
        for stage in remaining:
            base = int(stages[stage].get("priority", 0))
            corrected_actions.append(
                self._act.set_priority(stage=stage, priority=base + deltas[stage])
            )

        return Decision(actions=corrected_actions, stop=d.stop)


def make_scheduling_policy(
    op,
    kind: str = "rule",
    llm_api_key: Optional[str] = None,
    model: str = "openai/gpt-4o-mini",
    **kw,
) -> Policy:
    """Build a scheduling policy for a CampaignOperator.

    kind:
      - ``"null"``            → NullSchedulingPolicy (goals only, no scheduling)
      - ``"rule"``            → op.rule_policy() if defined, else DownstreamFirstPolicy
      - ``"downstream_first"``→ DownstreamFirstPolicy unconditionally (explicit generic)
      - ``"bandit"``          → BanditSchedulingPolicy (Thompson-sampling; A/B compare)
      - ``"llm"``             → LLMSchedulingPolicy with embedded rule fallback;
                                 requires ``llm_api_key``

    Extra kwargs are forwarded to the chosen policy's constructor.
    Common kwargs:
      rule/downstream_first: base_priority, batch_base, batch_lo, batch_hi
      bandit:                warmstart, seed, base_priority, long_task_threshold_s
      llm:                   min_call_interval_s, timeout_s, system_prompt
    """
    if kind in ("null", "none"):
        return NullSchedulingPolicy()
    if kind == "rule":
        # Baseline inference-first policy: no priority overrides, preserving the
        # campaign's config priorities (inference > miniapps).  NullSchedulingPolicy
        # emits no set_priority actions so the CM uses its configured group priorities.
        # This creates genuine GPU starvation for downstream stages, which rule_telemetry
        # then resolves with the telemetry boost.  RuleCorrections are also disabled so
        # stall boosts don't inadvertently elevate the starved miniapps without telemetry.
        custom = op.rule_policy() if hasattr(op, "rule_policy") else None
        if custom is not None:
            return RuleCorrectionsPolicy(custom, op, correct_stalls=True, correct_budget=True)
        return NullSchedulingPolicy()
    if kind == "rule_telemetry":
        # Telemetry-aware variant: source-first base ordering (inference > miniapps) plus
        # starvation priority boost scaled by live cpu_util when a downstream stage is
        # starved.  The boost magnitude flips the ordering (miniapps > inference) only
        # when miniapps has pending replicas that can't get a GPU — exactly the case
        # where telemetry provides actionable information the rule baseline lacks.
        inner = DownstreamFirstPolicy(
            op, source_first=True, use_telemetry_boost=True, **kw
        )
        return RuleCorrectionsPolicy(inner, op, correct_stalls=False, correct_budget=True)
    if kind == "downstream_first":
        # Explicit alias: always use the generic rule regardless of campaign.
        inner = DownstreamFirstPolicy(op, **kw)
        return RuleCorrectionsPolicy(inner, op, correct_stalls=True, correct_budget=True)
    if kind == "bandit":
        # Bandit handles budget-freeze signals poorly (it would keep updating
        # on frozen-stage outcomes); stall boost is still useful for contention.
        inner = BanditSchedulingPolicy(op, **kw)
        return RuleCorrectionsPolicy(inner, op, correct_stalls=True, correct_budget=False)
    if kind == "llm":
        if not llm_api_key:
            raise ValueError("kind='llm' requires llm_api_key")
        # LLMSchedulingPolicy embeds its own silent rule fallback and receives
        # the full observation including budget/monitor fields — no outer
        # corrections wrapper needed.
        return LLMSchedulingPolicy(llm_api_key, op, model=model, **kw)
    if kind == "blend":
        if not llm_api_key:
            raise ValueError("kind='blend' requires llm_api_key (for the LLM sub-policy)")
        # Extract bandit-specific kwargs before forwarding the rest to LLMSchedulingPolicy.
        seed      = kw.pop("seed",      0)
        warmstart = kw.pop("warmstart", False)
        bandit = BanditSchedulingPolicy(op, seed=seed, warmstart=warmstart)
        llm    = LLMSchedulingPolicy(llm_api_key, op, model=model, **kw)
        # Wrap the blend in RuleCorrectionsPolicy so budget/stall overrides still fire
        # on the final averaged decision.
        return RuleCorrectionsPolicy(
            BlendPolicy([bandit, llm]), op,
            correct_stalls=True, correct_budget=False,
        )
    raise ValueError(
        f"unknown policy kind {kind!r} "
        "(null | rule | downstream_first | bandit | llm | blend)"
    )
