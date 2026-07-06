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

from radical.adr import Decision, LLMPolicy, Policy, decide

from ..bandit import SchedulingBandit


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

        depth = _stage_depth(stages)
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
                info["bp_state"], self._batch_base,
                lo=self._batch_lo, hi=self._batch_hi,
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
_TERMINAL_REWARD = 0.5   # terminal stage / no downstream BP → neutral


def _downstream_bp(stage: str, stages: dict) -> str | None:
    """BP state of the stage that *stage* feeds (its first downstream consumer)."""
    for other, info in stages.items():
        if stage in info.get("deps", []):
            return info.get("bp_state", "HOLD")
    return None   # no downstream → terminal


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
            self._bandit = SchedulingBandit(
                list(stages), seed=self._seed, stage_priors=priors)
        return self._bandit

    @decide
    async def run(self, obs: dict) -> Decision:
        stages = obs.get("stages", {})
        if not stages:
            return Decision()
        bandit = self._ensure_bandit(stages)

        # Real hardware signals from TelemetrySubscriber (0.0 / None when absent).
        fail_rate    = float(obs.get("task_fail_rate", 0.0))
        avg_dur      = obs.get("avg_task_duration_s")   # None when no completions yet

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
                    reward *= (1.0 - min(fail_rate, 0.5))
                # Discount when average task duration is pathologically long
                # relative to the campaign's expected baseline (campaign-configured).
                if (
                    avg_dur is not None
                    and self._long_task_threshold_s is not None
                    and avg_dur > self._long_task_threshold_s
                ):
                    reward *= 0.8
                for _ in range(min(delta, 64)):   # cap pathological catch-up
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
    priorities:  dict[str, int] = Field(
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
    stop: bool = Field(False, description="True only when the campaign target is already reached.")


_SYSTEM_PROMPT = (
    "You schedule a multi-stage scientific pipeline to produce as many terminal "
    "'hits' as possible. The pipeline is a cascade: each stage feeds the next, and "
    "only the deepest (terminal) stage produces hits. Resources are scarce and "
    "oversubscribed — only a few stages can run at once.\n\n"
    "Each cycle you receive the live state of every stage:\n"
    "  running    — replicas currently executing\n"
    "  cap        — max replicas this stage can run at once\n"
    "  pending    — replicas WAITING to start (blocked on resources)\n"
    "  starved    — true when the stage has pending work but is running BELOW cap\n"
    "  is_source  — true for the SOURCE stage (no upstream); its pending is the raw "
    "input library, NOT a bottleneck\n"
    "  bp_state   — backpressure: HOLD | THROTTLE (overloaded) | WIDEN (room for more)\n\n"
    "You also receive live hardware telemetry (present on HPC runs, 0 when unavailable):\n"
    "  gpu_util             — EWA GPU utilisation % averaged across devices (0–100)\n"
    "  cpu_util             — EWA node CPU utilisation % (0–100)\n"
    "  mem_util             — EWA node memory utilisation % (0–100)\n"
    "  gpu_utils_per_device — latest GPU % per device id\n"
    "  task_fail_rate       — fraction of completed tasks that failed (0–1)\n"
    "  avg_task_duration_s  — rolling mean of task wall-clock seconds (None = no data)\n\n"
    "USE telemetry to modulate your decisions:\n"
    "  gpu_util > 85%  → shrink batch sizes; avoid over-dispatching to saturated GPUs\n"
    "  gpu_util < 40%  → grow batch sizes; fill idle GPU capacity\n"
    "  task_fail_rate > 0.2 → reduce priorities of failing stages; investigate stalls\n"
    "  avg_task_duration unusually long → discount that stage's throughput estimate\n\n"
    "STRATEGY — start from the proven default, then make small evidence-based nudges:\n\n"
    "DEFAULT (use this unless you have a clear reason not to): DOWNSTREAM-FIRST. "
    "Rank stages by depth — the deepest (terminal) stage highest, the source stage "
    "lowest. This keeps the leading edge of work flowing all the way to hits and is "
    "near-optimal for a balanced cascade. Concretely for a 5-stage line: "
    "s5 > s4 > s3 > s2 > s1.\n\n"
    "WHY this default is strong and hard to beat: hits only come out of the terminal "
    "stage, so keeping the terminal stages high ensures finished work converts to hits "
    "immediately instead of piling up. Cheap downstream stages need only a few slots; "
    "giving them priority does NOT waste resources (when they have no work they simply "
    "don't run, and the slots flow upstream automatically).\n\n"
    "CONSERVATIVE NUDGES (only when the evidence is clear):\n"
    "  * Never put the is_source stage above a downstream stage — its huge pending is "
    "just the raw library; running it faster only enlarges downstream backlogs.\n"
    "  * If a non-source stage is starved=true with a LARGE and GROWING pending while "
    "the deeper stages are idle (pending=0, low running), raise that starved stage a "
    "little — but keep the terminal stages high enough to keep draining its output. "
    "Do NOT give a shallow stage the single highest priority; that starves the drain "
    "path and hits stop coming.\n"
    "  * Otherwise keep the downstream-first order.\n\n"
    "BATCH SIZES (optional): THROTTLE → shrink; WIDEN → grow; HOLD → omit.\n\n"
    "Return a priority for EVERY stage shown (higher = scheduled first; only relative "
    "order matters). Set stop=true only when hits >= target."
)

# Public alias — the built-in default used when cm.adr.system_prompt is unset.
DEFAULT_SCHEDULING_PROMPT = _SYSTEM_PROMPT


def resolve_system_prompt(adr_cfg: dict, config_dir=None) -> Optional[str]:
    """Resolve the LLM system prompt from a ``cm.adr`` config block.

    Precedence:
      1. ``system_prompt``      — inline string in the config (wins)
      2. ``system_prompt_file`` — path to a text file (relative to ``config_dir``)
      3. None                   — caller falls back to DEFAULT_SCHEDULING_PROMPT

    Returns the prompt string, or None if neither key is set.
    """
    inline = adr_cfg.get("system_prompt")
    if inline:
        return str(inline)
    path = adr_cfg.get("system_prompt_file")
    if path:
        from pathlib import Path
        p = Path(path)
        if config_dir is not None and not p.is_absolute():
            p = Path(config_dir) / p
        return p.read_text()
    return None


class LLMSchedulingPolicy(LLMPolicy):
    """LLM-driven scheduling policy with a built-in silent rule fallback.

    When the LLM is unavailable (connection error, timeout, rate-limit) the
    policy returns the rule policy's decision directly — without raising.
    This is critical: radical.adr's @decide machinery prints a traceback for
    every exception that escapes run(), so the only way to avoid log spam on a
    down endpoint is to never raise from run() at all.

    The system prompt is configurable: set ``cm.adr.system_prompt`` (inline) or
    ``cm.adr.system_prompt_file`` (path) in the campaign config.
    """

    system_prompt = _SYSTEM_PROMPT

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
        self._act = op.get_actions()
        self._model = model
        if system_prompt:
            self.system_prompt = system_prompt
        self._timeout_s = timeout_s
        # instructor re-prompts on schema-validation failure; each retry is a
        # full inference.  Default to 1 so a bad response fails fast to the
        # rule fallback; raise for flaky-but-fast endpoints.
        self._instructor_retries = max(1, int(instructor_retries))
        # Per-call timeout prevents a hung LLM from blocking the operator loop.
        # max_retries=0 so the HTTP client fails fast as well.
        self.client = instructor.from_openai(
            AsyncOpenAI(api_key=api_key, base_url=base_url,
                        timeout=timeout_s, max_retries=max_retries))
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
                        {"role": "user",   "content": self.render_observation(obs)},
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
      - ``"rule"``   → DownstreamFirstPolicy (deterministic; default)
      - ``"bandit"`` → BanditSchedulingPolicy (Thompson-sampling; A/B compare)
      - ``"llm"``    → LLMSchedulingPolicy with embedded rule fallback;
                       requires ``llm_api_key``

    Extra kwargs are forwarded to the chosen policy's constructor.
    Common kwargs:
      rule:   base_priority, batch_base, batch_lo, batch_hi
      bandit: warmstart, seed, base_priority, long_task_threshold_s
      llm:    min_call_interval_s, timeout_s, system_prompt
    """
    if kind == "rule":
        return DownstreamFirstPolicy(op, **kw)
    if kind == "bandit":
        return BanditSchedulingPolicy(op, **kw)
    if kind == "llm":
        if not llm_api_key:
            raise ValueError("kind='llm' requires llm_api_key")
        # LLMSchedulingPolicy embeds its own silent rule fallback; no outer
        # Policy(primary, fallback) wrapping is needed.
        return LLMSchedulingPolicy(llm_api_key, op, model=model, **kw)
    raise ValueError(f"unknown policy kind {kind!r} (rule | bandit | llm)")
