"""ADR bridge — drive an AsyncCampaignManager from a radical.adr Policy.

This subpackage lets a radical.adr ``Policy`` (rule-based or LLM-driven) make
the campaign's adaptive scheduling decisions instead of the in-CM bandits.
The CM keeps owning scheduling, execution lifecycle, and resources; the ADR
``CampaignOperator`` only observes the campaign and advises it (the ADR
"sacred boundary").

Quick start (supervised alongside a live CM)::

    from src.campaign.adr import (
        CampaignView, CampaignOperator, run_supervised, make_scheduling_policy,
    )

    view = CampaignView(cm, target=5)
    op   = CampaignOperator(view, engine=cm._asyncflow)
    op.policy = make_scheduling_policy(op, llm_api_key=API_KEY)  # rule + LLM
    await cm.start()
    await run_supervised(cm, op)

Requires ``radical.adr`` (pip install -e ../radical.adr). The LLM policy
additionally needs ``openai`` + ``instructor`` (imported lazily).
"""

from .view import CampaignView, CampaignViewProtocol
from .operator import CampaignOperator, run_supervised
from .recorder import PolicyRecorder
from .telemetry import TelemetrySubscriber
from .policies import (
    DEFAULT_SCHEDULING_PROMPT,
    BanditSchedulingPolicy,
    DownstreamFirstPolicy,
    LLMSchedulingPolicy,
    ScheduleDecision,
    make_scheduling_policy,
    resolve_system_prompt,
)

__all__ = [
    "CampaignView",
    "CampaignViewProtocol",
    "CampaignOperator",
    "run_supervised",
    "PolicyRecorder",
    "TelemetrySubscriber",
    "DEFAULT_SCHEDULING_PROMPT",
    "DownstreamFirstPolicy",
    "BanditSchedulingPolicy",
    "LLMSchedulingPolicy",
    "ScheduleDecision",
    "make_scheduling_policy",
    "resolve_system_prompt",
]
