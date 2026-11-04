"""
Config dataclasses and YAML loaders for the dummy campaign.

Two config objects are used at runtime:

  GlobalConfig     — CM-level settings: engine, scheduling policy, global
                     budget, telemetry, and driver behaviour.
                     Loaded from  campaign_configs/global.yaml  (or global_hpc.yaml).

  CampaignConfig   — Campaign template: controller specs, per-campaign budget,
                     and workflow timing.  The driver instantiates one
                     CampaignSpec from this template for each run.
                     Loaded from  campaign_configs/campaign.yaml  (or campaign_hpc.yaml).

Usage::

    gcfg = load_global("campaign_configs/global.yaml")
    ccfg = load_campaign("campaign_configs/campaign.yaml")
    # CLI flags can override individual fields before use.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

# ── Global config ─────────────────────────────────────────────────────────────


@dataclass
class SchedulingConfig:
    policy: str = "bandit"  # bandit | fifo | adr | llm


@dataclass
class GlobalBudgetConfig:
    max_concurrent_campaigns: int | None = None
    max_total_concurrent_workflows: int | None = None
    spending: dict[str, float] = field(default_factory=dict)
    n_campaigns: int | None = None
    max_rejections: int | None = None


@dataclass
class TelemetryConfig:
    enabled: bool = False
    dir: str | None = None


@dataclass
class DriverConfig:
    enabled: bool = False  # use cm.submit()/cm.close() driver API
    delay_s: float = 2.0  # pause between consecutive submissions


@dataclass
class GlobalConfig:
    engine: str = "concurrent"  # concurrent | dragon
    data_backend: str = "auto"  # auto | dragon | redis | local
    redis_server_path: str | None = None  # path to redis-server binary (redis backend only)
    verbose: bool = False
    num_campaigns: int = 1
    scheduling: SchedulingConfig = field(default_factory=SchedulingConfig)
    global_budget: GlobalBudgetConfig = field(default_factory=GlobalBudgetConfig)
    telemetry: TelemetryConfig = field(default_factory=TelemetryConfig)
    driver: DriverConfig = field(default_factory=DriverConfig)


# ── Campaign config ───────────────────────────────────────────────────────────


@dataclass
class ControllerConfig:
    goal_analyses: int = 3  # terminate after this many analysis_worker completions
    score_floor: float = 0.4  # simulations below this score are cancelled


@dataclass
class BudgetConfig:
    max_concurrent: int = 4
    max_concurrency_per_workflow: int = 2
    cpu_hours: float | None = None  # soft spending cap; None = unlimited
    gpu_hours: float | None = None


@dataclass
class SimWorkerConfig:
    n_iters: int = 10
    iter_sleep_s: float = 0.08
    stale_timeout_s: float = 120.0
    declared_spending: dict = field(default_factory=dict)


@dataclass
class SimWorkerBConfig:
    # n_iters_b is auto-derived as max(3, n_iters // 3) — not stored here
    stale_timeout_s: float = 60.0
    declared_spending: dict = field(default_factory=dict)


@dataclass
class AnalysisWorkerConfig:
    steps: int = 2
    declared_spending: dict = field(default_factory=dict)


@dataclass
class WorkflowsConfig:
    sim_worker: SimWorkerConfig = field(default_factory=SimWorkerConfig)
    sim_worker_b: SimWorkerBConfig = field(default_factory=SimWorkerBConfig)
    analysis_worker: AnalysisWorkerConfig = field(default_factory=AnalysisWorkerConfig)


@dataclass
class CampaignConfig:
    controller: ControllerConfig = field(default_factory=ControllerConfig)
    budget: BudgetConfig = field(default_factory=BudgetConfig)
    workflows: WorkflowsConfig = field(default_factory=WorkflowsConfig)
    policy_interval_s: float = 0.5
    shutdown_timeout_s: float = 10.0
    checkpoint_dir: str | None = None


# ── Loaders ───────────────────────────────────────────────────────────────────


def load_global(path: str | Path) -> GlobalConfig:
    """Load a :class:`GlobalConfig` from a YAML file."""
    d = _load_yaml(path)
    sched = d.get("scheduling") or {}
    gb = d.get("global_budget") or {}
    tel = d.get("telemetry") or {}
    drv = d.get("driver") or {}
    return GlobalConfig(
        engine=d.get("engine", "concurrent"),
        data_backend=d.get("data_backend", "auto"),
        redis_server_path=d.get("redis_server_path") or None,
        verbose=bool(d.get("verbose", False)),
        num_campaigns=int(d.get("num_campaigns", 1)),
        scheduling=SchedulingConfig(
            policy=sched.get("policy", "bandit"),
        ),
        global_budget=GlobalBudgetConfig(
            max_concurrent_campaigns=gb.get("max_concurrent_campaigns"),
            max_total_concurrent_workflows=gb.get("max_total_concurrent_workflows"),
            spending={k: float(v) for k, v in (gb.get("spending") or {}).items() if v is not None},
            n_campaigns=_optional_int(gb.get("n_campaigns")),
            max_rejections=_optional_int(gb.get("max_rejections")),
        ),
        telemetry=TelemetryConfig(
            enabled=bool(tel.get("enabled", False)),
            dir=tel.get("dir"),
        ),
        driver=DriverConfig(
            enabled=bool(drv.get("enabled", False)),
            delay_s=float(drv.get("delay_s", 2.0)),
        ),
    )


def load_campaign(path: str | Path) -> CampaignConfig:
    """Load a :class:`CampaignConfig` from a YAML file."""
    d = _load_yaml(path)
    ctrl = d.get("controller") or {}
    bud = d.get("budget") or {}
    sp = bud.get("spending") or {}
    wf = d.get("workflows") or {}
    sim = wf.get("sim_worker") or {}
    simb = wf.get("sim_worker_b") or {}
    ana = wf.get("analysis_worker") or {}
    return CampaignConfig(
        controller=ControllerConfig(
            goal_analyses=int(ctrl.get("goal_analyses", 3)),
            score_floor=float(ctrl.get("score_floor", 0.4)),
        ),
        budget=BudgetConfig(
            max_concurrent=int(bud.get("max_concurrent", 4)),
            max_concurrency_per_workflow=int(bud.get("max_concurrency_per_workflow", 2)),
            cpu_hours=_optional_float(sp.get("cpu_hours")),
            gpu_hours=_optional_float(sp.get("gpu_hours")),
        ),
        workflows=WorkflowsConfig(
            sim_worker=SimWorkerConfig(
                n_iters=int(sim.get("n_iters", 10)),
                iter_sleep_s=float(sim.get("iter_sleep_s", 0.08)),
                stale_timeout_s=float(sim.get("stale_timeout_s", 120.0)),
                declared_spending={
                    k: float(v) for k, v in (sim.get("declared_spending") or {}).items()
                },
            ),
            sim_worker_b=SimWorkerBConfig(
                stale_timeout_s=float(simb.get("stale_timeout_s", 60.0)),
                declared_spending={
                    k: float(v) for k, v in (simb.get("declared_spending") or {}).items()
                },
            ),
            analysis_worker=AnalysisWorkerConfig(
                steps=int(ana.get("steps", 2)),
                declared_spending={
                    k: float(v) for k, v in (ana.get("declared_spending") or {}).items()
                },
            ),
        ),
        policy_interval_s=float(d.get("policy_interval_s", 0.5)),
        shutdown_timeout_s=float(d.get("shutdown_timeout_s", 10.0)),
        checkpoint_dir=d.get("checkpoint_dir"),
    )


# ── Helpers ───────────────────────────────────────────────────────────────────


def _load_yaml(path: str | Path) -> dict:
    try:
        import yaml
    except ImportError as e:
        raise ImportError("pyyaml is required to load config files: pip install pyyaml") from e
    with open(path) as f:
        return yaml.safe_load(f) or {}


def _optional_float(v: object) -> float | None:
    return float(v) if v is not None else None


def _optional_int(v: object) -> int | None:
    return int(v) if v is not None else None
