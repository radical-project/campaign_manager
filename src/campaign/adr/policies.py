"""Scheduling policies for the CampaignOperator.

Two interchangeable policies, both returning the same ``Decision`` shape:

  - ``DownstreamFirstPolicy`` — deterministic rule policy.  Encodes the
    downstream-first heuristic the SchedulingBandit had to *learn*: feed the
    deepest stage that has work and a free slot, hold the screening stage so it
    doesn't starve the pipeline, and size batches by backpressure state.
    No API key, fully testable.

  - ``LLMSchedulingPolicy`` — LLM-driven policy (OpenRouter / OpenAI-compatible
    via ``instructor``).  Reasons over the full observation each cycle instead
    of a scalar reward.  ``openai`` + ``instructor`` are imported lazily so this
    module imports without them.

``make_scheduling_policy`` composes them with ADR's primary/fallback contract:
the LLM steers, the rule policy catches failures.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

from radical.adr import Decision, LLMPolicy, Policy, decide  # noqa: E402
from radical.adr.decision import Action, ActionKind  # noqa: E402

from ..bandit import SchedulingBandit  # noqa: E402


class BlendPolicy(Policy):
    """Blend two policies' numeric lever actions by averaging.

    When both policies emit the same (task_name, stage) pair for a blendable
    lever, a single averaged action is emitted instead of two conflicting writes.

    Blendable levers: set_priority→priority, set_batch_size→size,
    set_score_cutoff→value. Non-blendable actions pass through deduplicated.
    stop uses AND semantics; goals/remove_goals/directives are unioned.
    """

    _BLENDABLE: dict[str, str] = {
        "set_priority":     "priority",
        "set_batch_size":   "size",
        "set_score_cutoff": "value",
    }

    def __init__(self, policies: list[Policy]) -> None:
        if len(policies) != 2:
            raise ValueError(f"BlendPolicy requires exactly 2 policies, got {len(policies)}")
        super().__init__()
        self._policies = policies

    async def decide(self, obs: dict) -> Decision:
        d0 = await self._policies[0].decide(obs)
        d1 = await self._policies[1].decide(obs)

        def _index(actions: list) -> tuple[dict, list]:
            indexed: dict[tuple, Action] = {}
            passthrough: list[Action] = []
            for a in actions:
                if (a.kind == ActionKind.SPAWN_TASK
                        and a.task_name in BlendPolicy._BLENDABLE
                        and "stage" in a.task_kwargs):
                    indexed[(a.task_name, a.task_kwargs["stage"])] = a
                else:
                    passthrough.append(a)
            return indexed, passthrough

        idx0, pass0 = _index(d0.actions)
        idx1, pass1 = _index(d1.actions)
        blended: list[Action] = []

        for key in set(idx0) | set(idx1):
            task_name, stage = key
            val_key = BlendPolicy._BLENDABLE[task_name]
            if key in idx0 and key in idx1:
                v0 = idx0[key].task_kwargs[val_key]
                v1 = idx1[key].task_kwargs[val_key]
                avg = (v0 + v1) / 2
                merged_val = int(round(avg)) if val_key in ("priority", "size") else avg
                merged_kwargs = {**idx0[key].task_kwargs, val_key: merged_val}
                blended.append(Action(kind=ActionKind.SPAWN_TASK, task_name=task_name,
                                      task_kwargs=merged_kwargs))
                log.debug("BlendPolicy: %s(stage=%s)  p0=%g  p1=%g  → blended=%g",
                          task_name, stage, v0, v1, merged_val)
            else:
                blended.append(idx0.get(key) or idx1[key])

        seen: set = set()
        for a in pass0 + pass1:
            dedup_key = (a.kind, a.task_name, a.param_key)
            if dedup_key not in seen:
                seen.add(dedup_key)
                blended.append(a)

        return Decision(
            actions=blended,
            stop=d0.stop and d1.stop,
            goals=d0.goals + d1.goals,
            remove_goals=list(set(d0.remove_goals) | set(d1.remove_goals)),
            directives={**d0.directives, **d1.directives},
        )

# ── Shared helpers ─────────────────────────────────────────────────────────


def _stage_depth(stages: dict) -> dict[str, int]:
    """Dependency-chain depth per stage (roots = 0). Downstream = larger depth."""
    depth: dict[str, int] = {}

    def _d(name: str, seen: frozenset) -> int:
        if name in depth:
            return depth[name]
        deps = stages.get(name, {}).get("deps", [])
        deps = [d for d in deps if d in stages and d not in seen]
        val = 0 if not deps else 1 + max(_d(d, seen | {name}) for d in deps)
        depth[name] = val
        return val

    for s in stages:
        _d(s, frozenset())
    return depth


def _batch_for_bp(bp_state: str, current: int, lo: int = 10, hi: int = 200) -> int:
    """Shrink under THROTTLE, grow under WIDEN, hold otherwise."""
    if bp_state == "THROTTLE":
        return max(lo, current // 2)
    if bp_state == "WIDEN":
        return min(hi, current * 2)
    return current


# ── Rule policy ─────────────────────────────────────────────────────────────


class DownstreamFirstPolicy(Policy):
    """Deterministic downstream-first scheduling — the rule the bandit learns.

    Every cycle it assigns descending priorities by dependency depth (deepest =
    highest), so the CM's two-pass scheduler always feeds the most-downstream
    stage first and holds the screening root lowest.  This is the fixed schedule
    the bandit converges to — making it the natural deterministic baseline for
    a learned vs. hand-coded comparison.

    Note: this is *proactive* (it ranks every cycle regardless of visible queue
    depth).  An earlier reactive variant gated boosts on ``queue_depth > 0``,
    but the emulation's sharder buffers drain between ticks, so that gate
    effectively never fired — the policy did nothing and the campaign stalled.
    """

    def __init__(
        self,
        op,
        base_priority: int = 100,
        batch_base: int = 50,
        batch_lo: int = 1,
        batch_hi: int = 1000,
    ) -> None:
        super().__init__()
        self._act = op.get_actions()
        self._base = base_priority
        self._batch_base = batch_base
        self._batch_lo = batch_lo
        self._batch_hi = batch_hi

    @decide
    async def run(self, obs: dict) -> Decision:
        stages = obs.get("stages", {})
        if not stages:
            return Decision()

        # Real hardware utilisation from TelemetrySubscriber (0.0 when absent).
        gpu_util = float(obs.get("gpu_util", 0.0))
        cpu_util = float(obs.get("cpu_util", 0.0))

        # Run in a thread so Lustre cold-page-fault stalls don't block the event loop.
        depth = await asyncio.to_thread(_stage_depth, stages)
        # Deepest-first: highest priority to the most downstream stage.
        order = sorted(stages, key=lambda s: depth[s], reverse=True)
        n = len(order)
        actions = [
            self._act.set_priority(stage=s, priority=self._base + (n - i))
            for i, s in enumerate(order)
        ]

        # Batch sizing: BP state is the primary signal; hardware utilisation
        # clamps or stretches it when real telemetry is flowing (gpu_util > 0).
        #   GPU > 85% → clamp to base (don't over-dispatch to saturated GPUs)
        #   GPU < 40% → double the BP-derived size (fill idle hardware)
        #   CPU > 90% → halve the size (CPU is the bottleneck, ease pressure)
        # GPU-util adjustments apply ONLY to stages that actually use GPU slots;
        # CPU-only stages (requires_gpu=False) are unaffected by GPU saturation.
        for s, info in stages.items():
            new_batch = _batch_for_bp(
                info["bp_state"],
                self._batch_base,
                lo=self._batch_lo,
                hi=self._batch_hi,
            )
            if info.get("requires_gpu", True):
                if gpu_util > 85.0:
                    new_batch = min(new_batch, self._batch_base)
                elif gpu_util > 0.0 and gpu_util < 40.0:
                    new_batch = min(new_batch * 2, info.get("cap", new_batch) * 4)
            if cpu_util > 90.0:
                new_batch = max(1, new_batch // 2)
            if new_batch != self._batch_base:
                actions.append(self._act.set_batch_size(stage=s, size=new_batch))

        return Decision(actions=actions)


# ── Bandit policy (the in-CM SchedulingBandit, wrapped as an ADR Policy) ──────

# BP state → reward, matching SchedulingBandit's documented signal.
_BP_REWARD = {"WIDEN": 0.8, "HOLD": 0.7, "THROTTLE": 0.2}
_TERMINAL_REWARD = 0.5  # terminal stage / no downstream BP → neutral


def _downstream_bp(stage: str, stages: dict) -> str | None:
    """BP state of the stage that *stage* feeds (its first downstream consumer)."""
    for _other, info in stages.items():
        if stage in info.get("deps", []):
            return info.get("bp_state", "HOLD")
    return None  # no downstream → terminal


class BanditSchedulingPolicy(Policy):
    """Thompson-sampling SchedulingBandit exposed as an ADR Policy.

    Reproduces the in-CM bandit's behaviour through the operator's levers so it
    can be A/B-compared against the rule and LLM policies on equal footing:

      1. Reward: each new replica completion feeds the bandit a reward derived
         from that stage's *downstream* backpressure (WIDEN 0.8 / HOLD 0.7 /
         THROTTLE 0.2; terminal stage 0.5) — the same signal the CM uses.
      2. Decide: rank all stages by a fresh Thompson sample and emit descending
         ``set_priority`` actions, so the CM's two-pass scheduler tries the
         bandit's most-promising stage first.

    ``warmstart=True`` seeds depth-based priors (Beta(depth+1, 1)), matching the
    CM's ``bandit_warmstart`` flag.
    """

    def __init__(
        self,
        op,
        seed: int | None = 0,
        warmstart: bool = False,
        base_priority: int = 100,
        long_task_threshold_s: float | None = None,
    ) -> None:
        super().__init__()
        self._act = op.get_actions()
        self._seed = seed
        self._warmstart = warmstart
        self._base = base_priority
        # When set, rewards are discounted 20% for stages whose mean task duration
        # exceeds this threshold (signals stall / resource starvation).
        # Default None = disabled.  Set campaign-side when typical replica runtime
        # is known (e.g. long_task_threshold_s = 5 × expected_mean_s).
        self._long_task_threshold_s = long_task_threshold_s
        self._bandit: SchedulingBandit | None = None
        self._prev_finished: dict[str, int] = {}

    def _ensure_bandit(self, stages: dict) -> SchedulingBandit:
        if self._bandit is None:
            priors = None
            if self._warmstart:
                depth = _stage_depth(stages)
                priors = {s: (float(depth[s] + 1), 1.0) for s in stages}
            self._bandit = SchedulingBandit(list(stages), seed=self._seed, stage_priors=priors)
        return self._bandit

    @decide
    async def run(self, obs: dict) -> Decision:
        stages = obs.get("stages", {})
        if not stages:
            return Decision()
        bandit = self._ensure_bandit(stages)

        # Real hardware signals from TelemetrySubscriber (0.0 / None when absent).
        fail_rate = float(obs.get("task_fail_rate", 0.0))
        avg_dur = obs.get("avg_task_duration_s")  # None when no completions yet

        # 1. Reward: feed the bandit for each new completion since last cycle.
        #    Base reward: downstream BP state (same as the in-CM bandit).
        #    Telemetry modulation:
        #      - High fail_rate discounts the reward (hardware/config problem).
        #      - avg_task_duration unusually long (> 5× nominal) also discounts,
        #        modelling a stalled or resource-starved stage.
        for s, info in stages.items():
            delta = info["finished"] - self._prev_finished.get(s, 0)
            if delta > 0:
                ds_bp = _downstream_bp(s, stages)
                reward = _TERMINAL_REWARD if ds_bp is None else _BP_REWARD.get(ds_bp, 0.5)
                # Discount by failure rate (capped at 50% discount so bandit
                # doesn't collapse when transient errors spike).
                if fail_rate > 0.0:
                    reward *= 1.0 - min(fail_rate, 0.5)
                # Discount when average task duration is pathologically long
                # relative to the campaign's expected baseline (campaign-configured).
                if (
                    avg_dur is not None
                    and self._long_task_threshold_s is not None
                    and avg_dur > self._long_task_threshold_s
                ):
                    reward *= 0.8
                for _ in range(min(delta, 64)):  # cap pathological catch-up
                    bandit.update(s, reward)
            self._prev_finished[s] = info["finished"]

        # 2. Decide: rank by Thompson sample, emit descending priorities.
        ranked = bandit.rank([_NamedStage(s) for s in stages])
        n = len(ranked)
        actions = [
            self._act.set_priority(stage=g.name, priority=self._base + (n - i))
            for i, g in enumerate(ranked)
        ]
        return Decision(actions=actions)

    @property
    def summary(self) -> dict:
        """Posterior mean per stage — for comparison logging."""
        return self._bandit.summary() if self._bandit is not None else {}


class _NamedStage:
    """Minimal object with a ``.name`` for SchedulingBandit.rank()."""

    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name


# ── LLM policy ──────────────────────────────────────────────────────────────


class ScheduleDecision(BaseModel):
    """Structured output the LLM must return each cycle.

    Set a priority for EVERY stage in the pipeline — not just one boost and one
    deprioritize.  Downstream (deepest) stages should get the highest numbers;
    the root screening stage should be lowest.  The CM's two-pass scheduler uses
    these numbers to decide who gets the next free GPU/CPU slot.
    """

    priorities: dict[str, int] = Field(
        default_factory=dict,
        description=(
            "Priority for every stage: {stage_id: priority_value}. "
            "Higher number = scheduled first. Default to downstream-first — deepest "
            "(terminal) stage highest (e.g. 105), source stage lowest (e.g. 101) — "
            "and nudge only on clear evidence. Must cover ALL stages in the observation."
        ),
    )
    batch_sizes: Optional[dict[str, int]] = Field(
        default=None,
        description=(
            "Optional batch-size overrides: {stage_id: target_size}. "
            "Shrink for stages with bp_state=THROTTLE; grow for WIDEN. "
            "Omit stages that need no change. May be omitted entirely."
        ),
    )
    stop: bool = Field(
        False,
        description=(
            "Set to True to signal the operator to stop the campaign. "
            "The condition for stopping is defined in your system prompt."
        ),
    )


def resolve_system_prompt(adr_cfg: dict, config_dir=None) -> Optional[str]:
    """Resolve the LLM system prompt from a ``cm.adr`` config block.

    Precedence:
      1. ``system_prompt``      — inline string in the config (wins)
      2. ``system_prompt_file`` — path to a text file (relative to ``config_dir``)
      3. None                   — no prompt configured; LLMSchedulingPolicy will raise

    The shared observation schema (``src/campaign/adr/prompts/observation_schema.txt``)
    is automatically appended when a campaign-specific prompt is found, so campaign
    prompts can focus on topology and goals without duplicating field documentation.

    Returns the prompt string, or None if neither key is set.
    """
    from pathlib import Path

    inline = adr_cfg.get("system_prompt")
    if inline:
        prompt = str(inline)
    else:
        path = adr_cfg.get("system_prompt_file")
        if not path:
            return None
        p = Path(path)
        if config_dir is not None and not p.is_absolute():
            p = Path(config_dir) / p
        prompt = p.read_text()

    schema_path = Path(__file__).parent / "prompts" / "observation_schema.txt"
    if schema_path.exists():
        prompt = prompt.rstrip() + "\n\n" + schema_path.read_text()

    return prompt


class LLMSchedulingPolicy(LLMPolicy):
    """LLM-driven scheduling policy with a built-in silent rule fallback.

    When the LLM is unavailable (connection error, timeout, rate-limit) the
    policy returns the rule policy's decision directly — without raising.
    This is critical: radical.adr's @decide machinery prints a traceback for
    every exception that escapes run(), so the only way to avoid log spam on a
    down endpoint is to never raise from run() at all.

    ``system_prompt`` is required: set ``cm.adr.system_prompt`` (inline) or
    ``cm.adr.system_prompt_file`` (path) in the campaign config.  Every campaign
    has different pipeline semantics and stopping criteria, so a single built-in
    prompt cannot be correct for all of them.
    """

    def __init__(
        self,
        api_key: str,
        op,
        model: str = "openai/gpt-4o-mini",
        base_url: str = "https://openrouter.ai/api/v1",
        timeout_s: float = 20.0,
        max_retries: int = 0,
        instructor_retries: int = 1,
        system_prompt: Optional[str] = None,
        min_call_interval_s: float = 10.0,
    ) -> None:
        super().__init__()
        try:
            import instructor
            from openai import AsyncOpenAI
        except ImportError as e:  # pragma: no cover - exercised only without deps
            raise ImportError(
                "LLMSchedulingPolicy requires 'openai' and 'instructor'. "
                "Install: pip install openai instructor"
            ) from e
        if not system_prompt:
            raise ValueError(
                "LLMSchedulingPolicy requires a system_prompt. "
                "Set cm.adr.system_prompt (inline) or cm.adr.system_prompt_file "
                "(path to a .txt file) in your campaign config."
            )
        self._act = op.get_actions()
        self._model = model
        self.system_prompt = system_prompt
        self._timeout_s = timeout_s
        # instructor re-prompts on schema-validation failure; each retry is a
        # full inference.  Default to 1 so a bad response fails fast to the
        # rule fallback; raise for flaky-but-fast endpoints.
        self._instructor_retries = max(1, int(instructor_retries))
        # Per-call timeout prevents a hung LLM from blocking the operator loop.
        # max_retries=0 so the HTTP client fails fast as well.
        self.client = instructor.from_openai(
            AsyncOpenAI(
                api_key=api_key, base_url=base_url, timeout=timeout_s, max_retries=max_retries
            )
        )
        # Silence instructor's per-attempt verbose logging ("API call failed on
        # attempt N", "Max retries exceeded").  When the endpoint is simply down
        # these fill the campaign log on every cycle; we emit our own single-line
        # warning on the first failure instead.
        import logging as _logging

        _logging.getLogger("instructor.v2.retry").setLevel(_logging.CRITICAL)
        # Silent rule fallback — called directly (not via @decide composition)
        # so no exception escapes run() and radical.adr never prints a traceback.
        self._rule = DownstreamFirstPolicy(op)
        # Rate-limit: don't attempt the API more often than min_call_interval_s.
        # Ticks that arrive before the interval silently use the rule policy.
        self._min_call_interval_s = float(min_call_interval_s)
        self._last_call_time: float = 0.0
        # Log only the first failure per outage; go silent until the endpoint
        # recovers, then warn once again on the next failure.
        self._endpoint_down: bool = False

    @decide
    async def run(self, obs: dict) -> Decision:
        import time as _time

        now = _time.monotonic()

        # Rate-limit: operator may tick every 1 s while llm_tick_s is 10 s.
        if now - self._last_call_time < self._min_call_interval_s:
            remaining = self._min_call_interval_s - (now - self._last_call_time)
            log.debug(
                "LLMSchedulingPolicy: rate-limited (%.1fs until next call) — rule fallback",
                remaining,
            )
            return await self._rule.decide(obs)

        self._last_call_time = now
        try:
            sd: ScheduleDecision = await asyncio.wait_for(
                self.client.chat.completions.create(
                    model=self._model,
                    response_model=ScheduleDecision,
                    max_retries=self._instructor_retries,
                    messages=[
                        {"role": "system", "content": self.system_prompt},
                        {"role": "user", "content": self.render_observation(obs)},
                    ],
                ),
                timeout=self._timeout_s + 5.0,
            )
        except Exception as exc:
            reason = f"{type(exc).__name__}: {exc}"
            if not self._endpoint_down:
                self._endpoint_down = True
                log.warning(
                    "LLMSchedulingPolicy: endpoint unavailable (%s) — rule fallback until reconnected",
                    reason,
                )
            else:
                log.debug(
                    "LLMSchedulingPolicy: endpoint still unavailable (%s) — rule fallback",
                    reason,
                )
            return await self._rule.decide(obs)

        # Successful call — clear the outage flag so the next failure is logged.
        if self._endpoint_down:
            log.info("LLMSchedulingPolicy: endpoint recovered — resuming LLM decisions")
            self._endpoint_down = False
        log.info(
            "LLMSchedulingPolicy: LLM decision (model=%s) — priorities=%s%s%s",
            self._model,
            sd.priorities,
            f", batch_sizes={sd.batch_sizes}" if sd.batch_sizes else "",
            " [stop=True]" if sd.stop else "",
        )
        return self._to_decision(sd)

    def _to_decision(self, sd: ScheduleDecision) -> Decision:
        actions = []
        for stage, priority in (sd.priorities or {}).items():
            actions.append(self._act.set_priority(stage=stage, priority=int(priority)))
        for stage, size in (sd.batch_sizes or {}).items():
            actions.append(self._act.set_batch_size(stage=stage, size=int(size)))
        return Decision(actions=actions, stop=sd.stop)


# ── Null policy (goal-only supervision, no scheduling changes) ──────────────


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


# ── Logging wrapper ─────────────────────────────────────────────────────────


class LoggingPolicy(Policy):
    """Wraps any policy and prints a one-line ADR decision summary each cycle.

    Extracts priority and batch-size assignments from ``Decision.actions`` and
    shows only what changed vs. the previous cycle, so the log stays readable
    over many cycles.  Reads ``inner._regime`` when present (e.g. DdSimRulePolicy)
    to surface which decision branch was taken.

    Example output::

        [ADR c=  4]  n_hits=3  [NORMAL]  | analysis(r=4/4 p=1 H)  ddsim_a(r=3/4 p=7 H)  ddsim_b(r=1/4 p=5 H)  | priorities: ddsim_b=101→102
        [ADR c=  7]  n_hits=5  [STARVED] | analysis(r=0/4 p=0 H)  ddsim_a(r=3/4 p=4 H)  ddsim_b(r=2/4 p=3 H)  | priorities: ddsim_a=101→105  | STOP
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


# ── Rule-corrections wrapper ────────────────────────────────────────────────


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
                stalls = int(info.get("stalls", 0))
                delta += min(2 * (stalls // 3), 6)

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


# ── Composition factory ─────────────────────────────────────────────────────


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
        # Delegate to the campaign's own rule first; fall back to the generic
        # depth-first heuristic so existing campaigns keep working unchanged.
        custom = op.rule_policy() if hasattr(op, "rule_policy") else None
        inner = custom if custom is not None else DownstreamFirstPolicy(op, **kw)
        return RuleCorrectionsPolicy(inner, op, correct_stalls=True, correct_budget=True)
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
