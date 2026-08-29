"""ADR bridge — drive an AsyncCampaignManager from a radical.adr Policy.

This subpackage lets a radical.adr ``Policy`` (rule-based or LLM-driven) make
the campaign's adaptive scheduling decisions.  The CM keeps owning scheduling,
execution lifecycle, and resources; the ADR ``CampaignOperator`` only observes
and advises (the ADR "sacred boundary").

Campaigns subclass ``CampaignOperator`` in ``campaigns/<name>/operator.py`` to
declare their specific stopping goals and observation extensions::

    from src.campaign.adr import CampaignView, run_supervised, make_scheduling_policy
    from campaigns.my_campaign.operator import MyCampaignOperator

    view = CampaignView(cm)
    op   = MyCampaignOperator(view, engine=asyncflow, n_target=10)
    op.policy = make_scheduling_policy(op, kind="rule")
    await cm.start()
    await run_supervised(cm, op)

Requires ``radical.adr`` (pip install -e ../radical.adr). The LLM policy
additionally needs ``openai`` + ``instructor`` (imported lazily).
"""

from .supervisor import CampaignAbortedError, run_supervised

try:
    from .operator import CampaignOperator
    from .policies import (
        BanditSchedulingPolicy,
        DownstreamFirstPolicy,
        LLMSchedulingPolicy,
        LoggingPolicy,
        ModeDecision,
        ModeLLMSchedulingPolicy,
        NullSchedulingPolicy,
        RuleCorrectionsPolicy,
        ScheduleDecision,
        make_scheduling_policy,
        resolve_system_prompt,
    )
    from .recorder import PolicyRecorder
    from .telemetry import TelemetrySubscriber
    from .view import CampaignView, CampaignViewProtocol
except ImportError:
    pass

__all__ = [
    "CampaignAbortedError",
    "CampaignView",
    "CampaignViewProtocol",
    "CampaignOperator",
    "run_supervised",
    "PolicyRecorder",
    "TelemetrySubscriber",
    "DownstreamFirstPolicy",
    "BanditSchedulingPolicy",
    "LLMSchedulingPolicy",
    "LoggingPolicy",
    "ModeDecision",
    "ModeLLMSchedulingPolicy",
    "NullSchedulingPolicy",
    "RuleCorrectionsPolicy",
    "ScheduleDecision",
    "make_scheduling_policy",
    "resolve_system_prompt",
]
