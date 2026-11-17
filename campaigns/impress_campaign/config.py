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
    goal_n: int = 4  # completions before campaign terminates
    budget_warn_fraction: float = 0.80  # log a warning when this fraction is used


@dataclass
class BudgetCfg:
    max_concurrent: int = 2
    max_concurrency_per_workflow: int = 1
    capacity: dict[str, Any] = field(default_factory=lambda: {"cpus": 8, "gpus": 2})
    spending: dict[str, float] = field(default_factory=dict)


@dataclass
class WorkflowCfg:
    stale_timeout_s: float = 3600.0
    spending_limit: dict[str, float] = field(
        default_factory=lambda: {"cpu_hours": 2.0, "gpu_hours": 0.5}
    )
    resources: dict[str, Any] = field(default_factory=lambda: {"cpus": 4, "gpus": 1})
    respect_limit: bool = True
    score_floor: float = 0.0  # cancel mid-run if progress drops below this (0 = off)


@dataclass
class ImpressWorkflowsCfg:
    impress: WorkflowCfg = field(default_factory=WorkflowCfg)


@dataclass
class CampaignConfig:
    controller: ControllerCfg = field(default_factory=ControllerCfg)
    budget: BudgetCfg = field(default_factory=BudgetCfg)
    workflows: ImpressWorkflowsCfg = field(default_factory=ImpressWorkflowsCfg)
    policy_interval_s: float = 5.0
    shutdown_timeout_s: float = 60.0
    checkpoint_dir: str | None = None


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
