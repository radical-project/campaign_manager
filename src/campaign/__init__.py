"""Campaign management for multi-workflow orchestration."""

from .artifacts import ArtifactManifest
from .backpressure import BackpressureNegotiator, BPState
from .bandit import BanditArm, SchedulingBandit
from .base_workflow import BaseWorkflow
from .budget_controller import BudgetController, BudgetEvent
from .campaign_manager import AsyncCampaignManager
from .candidate_log import CandidateHistory, CandidateLog, StageResult
from .logging_setup import enable_logging
from .monitor import DriftEvent, DriftKind, Monitor
from .plan import (
    BackpressureEdge,
    CampaignPlan,
    EdgeSpec,
    PilotSpec,
    ReplanThresholds,
    RetryPolicy,
    StageSpec,
    SurrogateSpec,
    load_plan,
    plan_to_workflows_dict,
)
from .profiles import PROFILES, ProfileWeights, get_profile
from .replanning import ReplanningController, ReplanningState, ReplanRequest
from .sharder import Sharder, ShardingSpec
from .surrogate import (
    CorrelatedSurrogate,
    NullSurrogate,
    RandomSurrogate,
    RecallTracker,
    Surrogate,
    build_default_surrogate,
)
from .sync_wrapper import CampaignManager
from .triage import Triage, TriageDecision
from .types import CampaignState, ResourcePool, WorkflowStats

__all__ = [
    "enable_logging",
    "ArtifactManifest",
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
