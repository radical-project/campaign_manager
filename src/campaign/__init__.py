"""Campaign management for multi-workflow orchestration."""

from .campaign_manager import AsyncCampaignManager
from .base_workflow import BaseWorkflow
from .sync_wrapper import CampaignManager
from .types import CampaignState, ResourcePool, WorkflowStats
from .backpressure import BackpressureNegotiator, BPState
from .budget_controller import BudgetController, BudgetEvent
from .candidate_log import CandidateLog, CandidateHistory, StageResult
from .monitor import Monitor, DriftEvent, DriftKind
from .plan import (
    BackpressureEdge, CampaignPlan, EdgeSpec, PilotSpec,
    ReplanThresholds, RetryPolicy, StageSpec, SurrogateSpec,
    load_plan, plan_to_workflows_dict,
)
from .profiles import ProfileWeights, PROFILES, get_profile
from .replanning import ReplanningController, ReplanningState, ReplanRequest
from .surrogate import (
    Surrogate, NullSurrogate, RandomSurrogate, CorrelatedSurrogate,
    RecallTracker, build_default_surrogate,
)
from .sharder import Sharder, ShardingSpec
from .triage import Triage, TriageDecision
from .bandit import BanditArm, SchedulingBandit

__all__ = [
    "AsyncCampaignManager",
    "CampaignManager",
    "BaseWorkflow",
    "CampaignState",
    "ResourcePool",
    "WorkflowStats",
    "BackpressureNegotiator",
    "BPState",
    "BudgetController",
    "BudgetEvent",
    "CandidateLog",
    "CandidateHistory",
    "StageResult",
    "Monitor",
    "DriftEvent",
    "DriftKind",
    # Plan schema
    "BackpressureEdge",
    "CampaignPlan",
    "EdgeSpec",
    "PilotSpec",
    "ReplanThresholds",
    "RetryPolicy",
    "StageSpec",
    "SurrogateSpec",
    "load_plan",
    "plan_to_workflows_dict",
    # Profiles / Sharder / Triage / Bandit
    "ProfileWeights",
    "PROFILES",
    "get_profile",
    "ReplanningController",
    "ReplanningState",
    "ReplanRequest",
    "Sharder",
    "ShardingSpec",
    "Surrogate",
    "NullSurrogate",
    "RandomSurrogate",
    "CorrelatedSurrogate",
    "RecallTracker",
    "build_default_surrogate",
    "Triage",
    "TriageDecision",
    "BanditArm",
    "SchedulingBandit",
]
