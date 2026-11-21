"""Configuration dataclasses for the IMPRESS AuCaM campaign."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import yaml

# ── Global (engine / telemetry / scheduling) ──────────────────────────────────


@dataclass
class TelemetryCfg:
    enabled: bool = False
    dir: str = ""


@dataclass
class SchedulingCfg:
    policy: str = "bandit"


@dataclass
class GlobalConfig:
    engine: str = "concurrent"
    verbose: bool = False
    telemetry: TelemetryCfg = field(default_factory=TelemetryCfg)
    scheduling: SchedulingCfg = field(default_factory=SchedulingCfg)
    redis_server_path: str | None = None


# ── Campaign ──────────────────────────────────────────────────────────────────


@dataclass
class ControllerCfg:
    goal_n: int = 4  # results needed before the campaign stops
    budget_warn_fraction: float = 0.80  # log a warning when this fraction is used


@dataclass
class BudgetCfg:
    max_concurrent: int = 2
    max_concurrency_per_workflow: int = 1
    capacity: dict[str, Any] = field(default_factory=lambda: {"cpus": 8, "gpus": 2})
    spending: dict[str, float] = field(default_factory=dict)
    #: The allocation's wall time.  Set it and AuCaM works the spending limit
    #: out as capacity x wall_hours, so the budget matches the job.
    wall_hours: float | None = None
    #: What happens at the limit: "cancel" the running workflows, or "finish".
    on_limit: str = "cancel"
    #: Load-aware launching (needs telemetry): how many workflows may run
    #: follows how busy the nodes are, up to max_concurrent.
    target_utilization: float | None = 0.85
    max_waiting_tasks: int | None = None
    overcommit: float = 1.0


@dataclass
class WorkflowCfg:
    stale_timeout_s: float = 3600.0
    spending_limit: dict[str, float] = field(
        default_factory=lambda: {"cpu_hours": 2.0, "gpu_hours": 0.5}
    )
    budget_floor: dict[str, float] = field(default_factory=dict)
    resources: dict[str, Any] = field(default_factory=lambda: {"cpus": 4, "gpus": 1})
    respect_limit: bool = True
    score_floor: float = 0.0  # cancel mid-run if progress drops below this (0 = off)
    #: Seconds a workflow gets to stop itself after CANCEL before AuCaM cancels
    #: the block (None: the campaign's default).
    cancel_grace_s: float | None = None
    #: AuCaM cancels a workflow still running after this long (None: no bound).
    max_wall_time_s: float | None = None
    #: Instances of this type that may run at once (None: the budget's cap).
    max_running: int | None = None


@dataclass
class ImpressWorkflowsCfg:
    impress: WorkflowCfg = field(default_factory=WorkflowCfg)
    # The three workflow types, steered by the scheduling policy
    binding_tractable: WorkflowCfg = field(
        default_factory=lambda: WorkflowCfg(
            spending_limit={"cpu_hours": 0.5, "gpu_hours": 0.5},
            resources={"cpus": 4, "gpus": 1},
        )
    )
    binding_borderline: WorkflowCfg = field(
        default_factory=lambda: WorkflowCfg(
            spending_limit={"cpu_hours": 0.5, "gpu_hours": 0.5},
            resources={"cpus": 4, "gpus": 1},
        )
    )
    binding_refractory: WorkflowCfg = field(
        default_factory=lambda: WorkflowCfg(
            spending_limit={"cpu_hours": 0.5, "gpu_hours": 0.5},
            resources={"cpus": 4, "gpus": 1},
        )
    )


@dataclass
class CampaignConfig:
    controller: ControllerCfg = field(default_factory=ControllerCfg)
    budget: BudgetCfg = field(default_factory=BudgetCfg)
    workflows: ImpressWorkflowsCfg = field(default_factory=ImpressWorkflowsCfg)
    policy_interval_s: float = 5.0
    shutdown_timeout_s: float = 60.0
    checkpoint_dir: str | None = None
    #: Run decide() in a thread and fall back to the built-in rules when it has
    #: not returned in time (None: wait for it).
    #: Policy cycles that may fail in a row before the campaign ends as
    #: controller_failed (None: never).


# ── Loaders ───────────────────────────────────────────────────────────────────


def _merge(base, overrides: dict) -> None:
    """Shallow-merge overrides into base dataclass."""
    for k, v in overrides.items():
        if hasattr(base, k):
            attr = getattr(base, k)
            if hasattr(attr, "__dataclass_fields__") and isinstance(v, dict):
                _merge(attr, v)
            else:
                setattr(base, k, v)


def load_global(path: str) -> GlobalConfig:
    cfg = GlobalConfig()
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    _merge(cfg, raw)
    if raw.get("telemetry") and isinstance(raw["telemetry"], dict):
        _merge(cfg.telemetry, raw["telemetry"])
    return cfg


def load_campaign(path: str) -> CampaignConfig:
    cfg = CampaignConfig()
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    _merge(cfg, raw)
    return cfg
