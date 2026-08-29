"""LLM-driven scheduling policies and supporting types."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from pydantic import BaseModel, Field
from radical.adr import Decision, LLMPolicy, decide

from .rule import DownstreamFirstPolicy

log = logging.getLogger(__name__)


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


class ModeDecision(BaseModel):
    """Mode classification returned by ModeLLMSchedulingPolicy.

    The LLM picks one named scheduling mode; code maps the choice to fixed
    campaign-specific priorities via the ``mode_priorities`` table supplied at
    construction.  Valid mode names are defined by the campaign's system prompt
    and must match keys in that table.
    """

    mode: str = Field(
        description=(
            "Current pipeline scheduling mode. "
            "Valid values and their meanings are defined in the system prompt. "
            "Return exactly one of the mode names listed there."
        )
    )
    stop: bool = Field(
        default=False,
        description="Set true only when the campaign's stopping condition is met.",
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

    schema_path = Path(__file__).parent.parent / "prompts" / "observation_schema.txt"
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
        # Prefer the campaign's own rule policy; fall back to the generic depth-first.
        _custom = op.rule_policy() if hasattr(op, "rule_policy") else None
        self._rule = _custom if _custom is not None else DownstreamFirstPolicy(op)
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


class ModeLLMSchedulingPolicy(LLMPolicy):
    """LLM classifies the pipeline mode; code maps mode → fixed proven priorities.

    The LLM produces a ``ModeDecision`` (a named scheduling regime) rather than
    raw priority numbers, eliminating the main failure mode of
    ``LLMSchedulingPolicy``: hallucinated priority values that can lock critical
    stages out of CPU/GPU contention for many cycles.

    Campaigns supply the full mode→priority table via ``mode_priorities`` at
    construction.  The system prompt must define the valid mode names and their
    triggering conditions; ``mode_priorities`` maps each name to the proven
    per-stage priority assignments for that regime.

    Call ``await policy.pre_warm()`` before ``cm.start()`` to prime the
    endpoint and eliminate cold-start latency on the first real campaign cycle.
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
        provider: str = "openai",
        mode_priorities: dict | None = None,
    ) -> None:
        super().__init__()
        if not system_prompt:
            raise ValueError(
                "ModeLLMSchedulingPolicy requires a system_prompt. "
                "Set cm.adr.system_prompt (inline) or cm.adr.system_prompt_file "
                "(path to a .txt file) in your campaign config."
            )
        if not mode_priorities:
            raise ValueError(
                "ModeLLMSchedulingPolicy requires a mode_priorities dict mapping "
                "each mode name to its per-stage priority assignments. "
                "Example: {'NORMAL': {'stage_a': 108, 'stage_b': 101}, ...}"
            )
        for mode_name, stage_pris in mode_priorities.items():
            vals = list(stage_pris.values())
            if len(vals) != len(set(vals)):
                dupes = sorted({v for v in vals if vals.count(v) > 1})
                raise ValueError(
                    f"ModeLLMSchedulingPolicy: mode {mode_name!r} has duplicate priority "
                    f"values {dupes} — all values must be distinct so ranking is deterministic. "
                    f"Got: {stage_pris}"
                )
        stage_sets = {name: frozenset(pris) for name, pris in mode_priorities.items()}
        unique_sets = set(stage_sets.values())
        if len(unique_sets) > 1:
            raise ValueError(
                f"ModeLLMSchedulingPolicy: all modes must cover identical stage sets. "
                f"Got: { {name: sorted(stages) for name, stages in stage_sets.items()} }"
            )
        self._provider = provider
        self._act = op.get_actions()
        self._model = model
        self.system_prompt = system_prompt
        self._mode_priorities: dict = mode_priorities
        self._timeout_s = timeout_s
        self._instructor_retries = max(1, int(instructor_retries))
        if provider == "anthropic":
            # Defer ALL client creation to pre_warm(), which runs it in a thread.
            # instructor.from_anthropic(AsyncAnthropic(...)) calls C extensions on
            # Lustre-backed files; running it between asyncio ticks causes a sync
            # stall that freezes the event loop for 3-7 s. The thread absorbs it.
            self._anthropic_init = dict(api_key=api_key, timeout=timeout_s, max_retries=max_retries)
            self.client = None  # set by pre_warm()
        else:
            try:
                import instructor
                from openai import AsyncOpenAI
            except ImportError as e:  # pragma: no cover
                raise ImportError(
                    "ModeLLMSchedulingPolicy with provider='openai' requires 'openai' "
                    "and 'instructor'. Install: pip install openai instructor"
                ) from e
            import logging as _logging
            _logging.getLogger("instructor.v2.retry").setLevel(_logging.CRITICAL)
            self.client = instructor.from_openai(
                AsyncOpenAI(
                    api_key=api_key, base_url=base_url, timeout=timeout_s, max_retries=max_retries
                )
            )
        # Fallback: campaign rule policy if available, else generic depth-first.
        self._rule = op.rule_policy() if hasattr(op, "rule_policy") else DownstreamFirstPolicy(op)
        self._min_call_interval_s = float(min_call_interval_s)
        import time as _time_init
        self._last_call_time: float = _time_init.monotonic()
        self._endpoint_down: bool = False
        self._pending: Optional[asyncio.Task] = None   # in-flight LLM call
        self._last_md: Optional[ModeDecision] = None   # most recent successful result

    def _make_llm_task(self, obs: dict) -> asyncio.Task[ModeDecision]:
        """Create a background asyncio task for the LLM call (never blocks the loop)."""
        if self._provider == "anthropic":
            # AsyncAnthropic is anyio-based — truly async, yields at every I/O point.
            coro = self.client.messages.create(
                model=self._model,
                response_model=ModeDecision,
                max_retries=self._instructor_retries,
                max_tokens=256,
                system=self.system_prompt,
                messages=[{"role": "user", "content": self.render_observation(obs)}],
            )
        else:
            coro = self.client.chat.completions.create(
                model=self._model,
                response_model=ModeDecision,
                max_retries=self._instructor_retries,
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": self.render_observation(obs)},
                ],
            )
        return asyncio.create_task(coro)

    @decide
    async def run(self, obs: dict) -> Decision:
        import time as _time
        now = _time.monotonic()

        # --- Collect completed LLM result if available ---
        if self._pending is not None and self._pending.done():
            try:
                md = self._pending.result()
                self._last_md = md
                if self._endpoint_down:
                    log.info("ModeLLMSchedulingPolicy: endpoint recovered")
                    self._endpoint_down = False
                log.info(
                    "ModeLLMSchedulingPolicy: mode=%s → priorities=%s%s",
                    md.mode,
                    self._mode_priorities.get(md.mode, next(iter(self._mode_priorities.values()))),
                    " [stop=True]" if md.stop else "",
                )
            except Exception as exc:
                if not self._endpoint_down:
                    self._endpoint_down = True
                    log.warning(
                        "ModeLLMSchedulingPolicy: endpoint unavailable (%s) — rule fallback",
                        f"{type(exc).__name__}: {exc}",
                    )
            # Reset the interval clock from collection time, not fire time.
            # Without this, the fire-new-call check below runs in the same cycle
            # immediately after collection and sets _pending again, so the cached
            # result is never applied (the _pending is None guard is always False).
            self._last_call_time = now
            self._pending = None

        # --- Cancel stale in-flight call ---
        if (self._pending is not None and not self._pending.done()
                and now - self._last_call_time > self._timeout_s + 5.0):
            self._pending.cancel()
            self._pending = None
            log.warning("ModeLLMSchedulingPolicy: LLM call timed out, cancelled")

        # --- Fire a new call if interval elapsed and nothing in flight ---
        if (self._pending is None
                and now - self._last_call_time >= self._min_call_interval_s):
            self._last_call_time = now
            self._pending = self._make_llm_task(obs)

        # --- Decide ---
        if self._last_md is not None:
            # Serve the cached LLM result even while a new call is in flight.
            # Waiting for the in-flight response before applying the cache causes
            # pure rule-fallback for the entire API round-trip (3-5 s on HPC
            # networks), which defeats the purpose of having an LLM policy at all.
            priorities = self._mode_priorities.get(
                self._last_md.mode, next(iter(self._mode_priorities.values()))
            )
            actions = [
                self._act.set_priority(stage=s, priority=p)
                for s, p in priorities.items()
            ]
            return Decision(actions=actions, stop=self._last_md.stop)

        # No cached result yet — pure rule fallback until first response arrives.
        log.debug("ModeLLMSchedulingPolicy: no LLM result yet — rule fallback")
        return await self._rule.decide(obs)

    async def pre_warm(self) -> bool:
        """Fire a trivial LLM call to establish the HTTP connection before campaign start.

        benchmark.py calls _preload_llm_modules() before asyncio.run() which warms all
        Lustre-backed library pages; __init__ already created the AsyncAnthropic client
        so by the time this coroutine runs no cold-page-fault stalls remain.

        Returns True on success.
        """
        # Use the first mode in the table as the neutral pre-warm response so the
        # prompt is valid for any campaign's mode vocabulary, not just ddsim's "NORMAL".
        neutral_mode = next(iter(self._mode_priorities))
        try:
            if self._provider == "anthropic":
                init_kwargs = self._anthropic_init
                model = self._model

                def _create_client():
                    # All imports and C-extension init happen in this thread so
                    # Lustre cold-page-fault stalls never block the event loop.
                    import logging as _logging

                    import instructor as _instructor
                    _logging.getLogger("instructor.v2.retry").setLevel(_logging.CRITICAL)
                    from anthropic import AsyncAnthropic as _AsyncAnthropic
                    return _instructor.from_anthropic(_AsyncAnthropic(**init_kwargs))

                self.client = await asyncio.wait_for(
                    asyncio.to_thread(_create_client),
                    timeout=15.0,
                )
                # Client is ready; now fire the warm-up API call (fully async).
                md = await asyncio.wait_for(
                    self.client.messages.create(
                        model=model,
                        response_model=ModeDecision,
                        max_retries=1,
                        max_tokens=64,
                        system=f"Return mode={neutral_mode} stop=false.",
                        messages=[{"role": "user", "content": "{}"}],
                    ),
                    timeout=self._timeout_s + 15.0,
                )
            else:
                md = await asyncio.wait_for(
                    self.client.chat.completions.create(
                        model=self._model,
                        response_model=ModeDecision,
                        max_retries=1,
                        messages=[
                            {"role": "system", "content": f"Return mode={neutral_mode} stop=false."},
                            {"role": "user", "content": "{}"},
                        ],
                    ),
                    timeout=self._timeout_s + 10.0,
                )
            # Cache the pre-warm result so the campaign starts with a valid LLM
            # decision immediately, without waiting for the first real API round-trip.
            # Also reset _last_call_time so the first real call fires after
            # min_call_interval_s from campaign start rather than immediately (which
            # would block the cached result from being used while the call is in flight).
            import time as _time_prewarm
            self._last_md = md
            self._last_call_time = _time_prewarm.monotonic()
            log.info("ModeLLMSchedulingPolicy: endpoint pre-warmed (mode=%s cached)", md.mode)
            return True
        except Exception as exc:
            log.warning(
                "ModeLLMSchedulingPolicy: pre-warm failed (%s) — first call may be slow", exc
            )
            return False
