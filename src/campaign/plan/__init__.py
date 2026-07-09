"""Structured campaign plan (Planner → CM contract).

The Plan is the read-only specification that the upstream Planner emits and
the CM executes within.  This subpackage provides:

  schema.py  — dataclass-based plan model (CampaignPlan + nested specs)
  loader.py  — YAML loading with back-compat for legacy flat configs

When pydantic is added to the project, schema.py can be ported to BaseModel
without changing public APIs.
"""

from .loader import load_plan, plan_to_workflows_dict
from .schema import (
    BackpressureEdge,
    CampaignPlan,
    EdgeSpec,
    PilotSpec,
    ReplanThresholds,
    RetryPolicy,
    StageSpec,
    SurrogateSpec,
)

__all__ = [
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
]
