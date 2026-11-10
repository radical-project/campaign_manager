"""
Config dataclasses and YAML loaders for the MiniApps AuCaM campaign.

Two config objects:

  GlobalConfig    — CM-level: engine, scheduling policy, telemetry.
                    Loaded from campaign_configs/global.yaml.

  CampaignConfig  — The campaign: controller params, budget and MiniApps
                    workflow settings.  The campaign runs two arms
                    (miniapps_fast / miniapps_slow) via WorkflowsConfig.fast_config()
                    and slow_config().
                    Loaded from campaign_configs/campaign.yaml.

Usage::

    gcfg = load_global("campaign_configs/global.yaml")
    ccfg = load_campaign("campaign_configs/campaign.yaml")
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path

# ── Global config ─────────────────────────────────────────────────────────────


@dataclass
class SchedulingConfig:
    policy: str = "bandit"  # bandit | fifo | adr | llm


@dataclass
class TelemetryConfig:
    enabled: bool = False
    dir: str | None = None


@dataclass
class GlobalConfig:
    engine: str = "concurrent"
    verbose: bool = False
    scheduling: SchedulingConfig = field(default_factory=SchedulingConfig)
    telemetry: TelemetryConfig = field(default_factory=TelemetryConfig)


# ── Campaign config ───────────────────────────────────────────────────────────


@dataclass
class ControllerConfig:
    goal_n: int = 4  # stop after N miniapps workflow completions
    score_ceiling: float = 0.90  # cancel workflow if mean MSE exceeds this


@dataclass
class BudgetConfig:
    max_concurrent: int = 2  # parallel miniapps replicas per campaign
    max_concurrency_per_workflow: int = 1
    spending: dict[str, float | None] = field(default_factory=dict)


@dataclass
class MiniAppsConfig:
    """Parameters forwarded to MiniAppsWorkflow.__init__(config=...)."""

    total_num_sim: int = 10
    max_sim_batch: int = 4
    training_cores: int = 1
    miniapps_data_ready: int = 4
    num_step: int = 100
    num_epochs: int = 100
    stale_timeout_s: float = 600.0
    home_dir: str = ""  # empty → ~/MiniApps (expanded at runtime)
    debug: bool = False
    free_resources_for_train: bool = True
    clean_unregistered_sims: bool = False
    # Python executable for sim/train/predict/selection tasks.
    # Empty string → MiniAppsWorkflow uses sys.executable (the launcher Python).
    executable: str = ""

    def to_dict(self) -> dict:
        """Return a dict suitable for MiniAppsWorkflow(config=...)."""
        d = {
            "total_num_sim": self.total_num_sim,
            "max_sim_batch": self.max_sim_batch,
            "training_cores": self.training_cores,
            "miniapps_data_ready": self.miniapps_data_ready,
            "num_step": self.num_step,
            "num_epochs": self.num_epochs,
            "free_resources_for_train": self.free_resources_for_train,
            "clean_unregistered_sims": self.clean_unregistered_sims,
            "debug": self.debug,
        }
        if self.home_dir:
            d["home_dir"] = self.home_dir
        if self.executable:
            d["executable"] = self.executable
        return d


@dataclass
class WorkflowsConfig:
    miniapps: MiniAppsConfig = field(default_factory=MiniAppsConfig)
    slow_step_multiplier: int = 3  # miniapps_slow uses num_step × this
    # Declared spending per workflow arm — keys: wall_hours, cpu_hours, gpu_hours.
    # fast_spending/slow_spending drive budget accounting; slow defaults to
    # fast × slow_step_multiplier when absent.
    fast_spending: dict[str, float] = field(default_factory=dict)
    slow_spending: dict[str, float] = field(default_factory=dict)
    # Per-run spending limit for each arm (most one run may spend); AuCaM passes
    # it to the run and cancels a run that spends more.  slow defaults to
    # fast × slow_step_multiplier.  respect_limit[arm] = False makes that arm
    # ignore its limit (the overuse use case).
    fast_spending_limit: dict[str, float] = field(default_factory=dict)
    slow_spending_limit: dict[str, float] = field(default_factory=dict)
    respect_limit: dict[str, bool] = field(default_factory=lambda: {"fast": True, "slow": True})

    def fast_config(self) -> MiniAppsConfig:
        return self.miniapps

    def slow_config(self) -> MiniAppsConfig:
        cfg = deepcopy(self.miniapps)
        cfg.num_step = cfg.num_step * self.slow_step_multiplier
        return cfg

    def resolved_slow_spending(self) -> dict[str, float]:
        """Slow arm declared_spending: explicit YAML values, or fast × multiplier."""
        if self.slow_spending:
            return self.slow_spending
        return {k: v * self.slow_step_multiplier for k, v in self.fast_spending.items()}

    def resolved_slow_spending_limit(self) -> dict[str, float]:
        """Slow arm spending limit: explicit YAML values, or fast × multiplier."""
        if self.slow_spending_limit:
            return self.slow_spending_limit
        return {k: v * self.slow_step_multiplier for k, v in self.fast_spending_limit.items()}


@dataclass
class CampaignConfig:
    controller: ControllerConfig = field(default_factory=ControllerConfig)
    budget: BudgetConfig = field(default_factory=BudgetConfig)
    workflows: WorkflowsConfig = field(default_factory=WorkflowsConfig)
    policy_interval_s: float = 2.0
    shutdown_timeout_s: float = 60.0
    checkpoint_dir: str | None = None


# ── Loaders ───────────────────────────────────────────────────────────────────


def load_global(path: str | Path) -> GlobalConfig:
    d = _load_yaml(path)
    sched = d.get("scheduling") or {}
    tel = d.get("telemetry") or {}
    return GlobalConfig(
        engine=d.get("engine", "concurrent"),
        verbose=bool(d.get("verbose", False)),
        scheduling=SchedulingConfig(policy=sched.get("policy", "bandit")),
        telemetry=TelemetryConfig(
            enabled=bool(tel.get("enabled", False)),
            dir=tel.get("dir"),
        ),
    )


def load_campaign(path: str | Path) -> CampaignConfig:
    d = _load_yaml(path)
    ctrl = d.get("controller") or {}
    bud = d.get("budget") or {}
    sp = bud.get("spending") or {}
    ma = d.get("miniapps") or {}
    wf = d.get("workflows") or {}

    def _spending_dict(raw: dict | None) -> dict[str, float]:
        if not raw:
            return {}
        return {k: float(v) for k, v in raw.items() if v is not None}

    return CampaignConfig(
        controller=ControllerConfig(
            goal_n=int(ctrl.get("goal_n", 4)),
            score_ceiling=float(ctrl.get("score_ceiling", 0.90)),
        ),
        budget=BudgetConfig(
            max_concurrent=int(bud.get("max_concurrent", 2)),
            max_concurrency_per_workflow=int(bud.get("max_concurrency_per_workflow", 1)),
            spending={k: _optional_float(v) for k, v in sp.items()},
        ),
        workflows=WorkflowsConfig(
            slow_step_multiplier=int(d.get("slow_step_multiplier", 3)),
            fast_spending=_spending_dict(wf.get("fast_spending")),
            slow_spending=_spending_dict(wf.get("slow_spending")),
            fast_spending_limit=_spending_dict(wf.get("fast_spending_limit")),
            slow_spending_limit=_spending_dict(wf.get("slow_spending_limit")),
            respect_limit={
                "fast": bool((wf.get("respect_limit") or {}).get("fast", True)),
                "slow": bool((wf.get("respect_limit") or {}).get("slow", True)),
            },
            miniapps=MiniAppsConfig(
                total_num_sim=int(ma.get("total_num_sim", 10)),
                max_sim_batch=int(ma.get("max_sim_batch", 4)),
                training_cores=int(ma.get("training_cores", 1)),
                miniapps_data_ready=int(ma.get("miniapps_data_ready", 4)),
                num_step=int(ma.get("num_step", 100)),
                num_epochs=int(ma.get("num_epochs", 100)),
                stale_timeout_s=float(ma.get("stale_timeout_s", 600.0)),
                home_dir=str(ma.get("home_dir") or ""),
                debug=bool(ma.get("debug", False)),
                free_resources_for_train=bool(ma.get("free_resources_for_train", True)),
                clean_unregistered_sims=bool(ma.get("clean_unregistered_sims", False)),
                executable=str(ma.get("executable") or ""),
            ),
        ),
        policy_interval_s=float(d.get("policy_interval_s", 2.0)),
        shutdown_timeout_s=float(d.get("shutdown_timeout_s", 60.0)),
        checkpoint_dir=d.get("checkpoint_dir"),
    )


# ── Helpers ───────────────────────────────────────────────────────────────────


def _load_yaml(path: str | Path) -> dict:
    try:
        import yaml
    except ImportError as e:
        raise ImportError("pyyaml is required: pip install pyyaml") from e
    with open(path) as f:
        return yaml.safe_load(f) or {}


def _optional_float(v: object) -> float | None:
    return float(v) if v is not None else None
