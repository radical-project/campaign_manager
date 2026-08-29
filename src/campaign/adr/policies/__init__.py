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

from ._helpers import _batch_for_bp, _stage_depth
from .bandit import BanditSchedulingPolicy
from .ensemble import BlendPolicy, ConsensusPolicy
from .llm import (
    LLMSchedulingPolicy,
    ModeDecision,
    ModeLLMSchedulingPolicy,
    ScheduleDecision,
    resolve_system_prompt,
)
from .rule import DownstreamFirstPolicy
from .wrappers import (
    LoggingPolicy,
    NullSchedulingPolicy,
    RuleCorrectionsPolicy,
    make_scheduling_policy,
)

__all__ = [
    "BlendPolicy",
    "ConsensusPolicy",
    "DownstreamFirstPolicy",
    "BanditSchedulingPolicy",
    "LLMSchedulingPolicy",
    "ModeDecision",
    "ModeLLMSchedulingPolicy",
    "ScheduleDecision",
    "resolve_system_prompt",
    "NullSchedulingPolicy",
    "LoggingPolicy",
    "RuleCorrectionsPolicy",
    "make_scheduling_policy",
    "_stage_depth",
    "_batch_for_bp",
]
