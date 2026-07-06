#!/usr/bin/env python3
# Limit OpenBLAS/OMP threads before any numpy import to avoid pthread_create
# failures on login nodes where process counts are restricted.
import os

os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")

"""
ESM2 / DDSim campaign runner — starts async workflow replicas with optional
ADR supervision for adaptive scheduling on real HPC hardware (Dragon backend).

Usage
-----
    # HPC (Dragon backend, real GPUs) — run from this directory:
    dragon run_campaing.py --config config.yaml

    # Local testing (concurrent backend):
    python run_campaing.py --config config.yaml --engine concurrent

    # With ADR scheduling policy:
    python run_campaing.py --config config.yaml --policy rule
    python run_campaing.py --config config.yaml --policy llm --record

Config file structure
---------------------
    engine: dragon       # concurrent | dragon
    resources:
      total_cpus: 64
      total_gpus: 4

    # Optional ADR supervision (adaptive scheduling):
    cm:
      adr:
        policy: rule     # none | rule | bandit | llm
        tick_s: 2.0      # operator decision cadence
        record: false    # write per-cycle decisions to JSONL

    workflows:
      md:
        replicas: 2
        required_cpus: 4
        required_gpus: 1
        config_file: "${MD_HOME}/config.yaml"

      miniapps:
        dependencies: [md]
        required_cpus: 4
        required_gpus: 1
        config_file: "${MINAPPS_DIR}/config.yaml"
"""

import argparse  # noqa: E402
import asyncio  # noqa: E402
import sys  # noqa: E402
from pathlib import Path  # noqa: E402

import yaml  # noqa: E402

# campaign_manager root (for `src.campaign`, `src.utils`) and the script's own
# directory (so the workflow_registry can import dummy_workflow / ddmd_workflow /
# miniapps_workflow / inference_workflow regardless of the launch cwd — Dragon
# launches from a different directory than this file lives in).
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

from src.campaign import AsyncCampaignManager as CampaignManager  # noqa: E402
from src.utils.workflow import _expand_env, load_config  # noqa: E402


def _expand_workflow_configs(config: dict, config_dir: Path) -> dict:
    """Load external per-workflow YAML files referenced by 'config_file' keys."""
    for wf_cfg in config.get("workflows", {}).values():
        cfg_file = wf_cfg.pop("config_file", None)
        if not cfg_file:
            continue
        cfg_path = Path(os.path.expandvars(cfg_file))
        if not cfg_path.is_absolute():
            cfg_path = config_dir / cfg_path
        with open(cfg_path) as f:
            wf_specific = _expand_env(yaml.safe_load(f) or {})
        # Scheduling params in config.yaml win; workflow file fills the rest.
        wf_specific.update(wf_cfg)
        wf_cfg.clear()
        wf_cfg.update(wf_specific)
    return config


def _build_registry(config: dict) -> dict:
    """Dynamically import workflow classes from the 'workflow_registry' config section."""
    import importlib

    registry = {}
    for name, cls_path in config.get("workflow_registry", {}).items():
        module_name, cls_name = cls_path.rsplit(".", 1)
        module = importlib.import_module(module_name)
        registry[name] = getattr(module, cls_name)
    return registry


def _build_adr_operator(cm, asyncflow, adr_cfg: dict, policy_override=None,
                        telemetry=None):
    """Build a CampaignOperator + policy for ADR supervision, or (None, None).

    On HPC runs, pass ``telemetry`` (the TelemetryManager returned by
    ``asyncflow.start_telemetry()``) to feed real GPU/CPU/mem utilisation into
    the ADR observation so policies can react to hardware saturation.

    Policy source precedence: --policy CLI override > cm.adr.policy config.
    kind ∈ {none, rule, bandit, llm}; 'none' = no ADR supervision.
    """
    kind = (policy_override or adr_cfg.get("policy", "none") or "none").lower()
    if kind in ("none", "off", ""):
        return None, None

    from src.campaign.adr import (
        CampaignView, PolicyRecorder, TelemetrySubscriber,
        make_scheduling_policy, resolve_system_prompt,
    )
    from ddsim_operator import DDSimCampaignOperator

    # Wire telemetry into the view so observe() includes hardware metrics.
    # terminal: the stage whose finished-replica count is "hits" for the operator goal.
    # Required when routing uses _on_completion (no config dependencies).
    tel_sub  = TelemetrySubscriber(telemetry) if telemetry is not None else None
    terminal = adr_cfg.get("terminal") or None
    view     = CampaignView(cm, terminal=terminal, telemetry_subscriber=tel_sub)

    # Optional per-cycle decision recorder.
    recorder = None
    record_path = adr_cfg.get("record")
    if record_path in (True, "auto"):
        record_path = f"adr-decisions-{kind}.jsonl"
    if record_path:
        recorder = PolicyRecorder(record_path, policy_kind=kind)

    op = DDSimCampaignOperator(
        view, engine=asyncflow, observer=recorder,
        n_md_runs=int(adr_cfg.get("n_md_runs", 4)),
        max_fail_rate=float(adr_cfg.get("max_fail_rate", 0.05)),
    )

    kw, api_key = {}, None
    if kind == "bandit":
        kw["warmstart"] = bool(adr_cfg.get("warmstart", False))
        kw["seed"] = adr_cfg.get("seed", 0)
    elif kind == "llm":
        api_key = os.environ.get(
            adr_cfg.get("llm_api_key_env", "OPENROUTER_API_KEY"), "")
        if adr_cfg.get("base_url"):
            kw["base_url"] = adr_cfg["base_url"]
        if adr_cfg.get("llm_timeout_s") is not None:
            kw["timeout_s"] = float(adr_cfg["llm_timeout_s"])
        if adr_cfg.get("llm_max_retries") is not None:
            kw["instructor_retries"] = int(adr_cfg["llm_max_retries"])
        kw["min_call_interval_s"] = float(adr_cfg.get("llm_tick_s", 10.0))
        bu = adr_cfg.get("base_url", "") or ""
        if not api_key and ("localhost" in bu or "127.0.0.1" in bu):
            api_key = "sk-noauth"
        prompt = resolve_system_prompt(adr_cfg)   # cm.adr.system_prompt[_file]
        if prompt:
            kw["system_prompt"] = prompt

    op.policy = make_scheduling_policy(
        op, kind=kind, llm_api_key=api_key,
        model=adr_cfg.get("model", "openai/gpt-4o-mini"), **kw)

    if recorder is not None:
        recorder.bind(view=view, policy=op.policy)
        print(f"ADR decision recorder → {record_path}")

    default_tick = float(adr_cfg.get("tick_s", 2.0))
    tick = float(adr_cfg.get("llm_tick_s", default_tick)) if kind == "llm" else default_tick
    return op, tick


async def main(config_file: str, policy_override=None, record_override=None,
               engine_override=None) -> None:
    config_path = Path(config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_file}")
    config = load_config(config_file)
    config_dir = config_path.parent
    _expand_workflow_configs(config, config_dir)

    engine_type = engine_override or config.get("engine", "dragon")

    # ── Build backend and asyncflow ───────────────────────────────────────────
    engine_dragon = None
    asyncflow = None

    if engine_type == "dragon":
        try:
            from radical.asyncflow import WorkflowEngine
            from rhapsody.backends import DragonExecutionBackendV3

            engine_dragon = await DragonExecutionBackendV3()
            asyncflow = await WorkflowEngine.create(engine_dragon)
            print("Dragon backend started")
        except ImportError:
            engine_type = "concurrent"

    if engine_type == "concurrent":
        from radical.asyncflow import WorkflowEngine
        from rhapsody.backends import ConcurrentExecutionBackend

        backend = await ConcurrentExecutionBackend()
        asyncflow = await WorkflowEngine.create(backend)
        print("ConcurrentExecutionBackend started")

    # ── Telemetry (optional — needs opentelemetry SDK) ────────────────────────
    tel_cfg = config.get("telemetry", {})
    telemetry = None
    if tel_cfg.get("collect_telemetry", False):
        telemetry_dir = tel_cfg.get("telemetry_dir", "telemetry-results")
        if hasattr(asyncflow, "start_telemetry"):
            try:
                telemetry = await asyncflow.start_telemetry(
                    resource_poll_interval=tel_cfg.get("resource_poll_interval", 0.5),
                    checkpoint_path=telemetry_dir,
                )
                print(f"Asyncflow telemetry started → {telemetry_dir}")
            except ImportError as exc:
                print(f"Telemetry disabled (missing optional dep: {exc}). "
                      f"Install with: pip install opentelemetry-sdk")

    # ── Campaign ──────────────────────────────────────────────────────────────
    registry = _build_registry(config)
    cm = CampaignManager.from_config(
        config,
        registry,
        asyncflow=asyncflow,
        engine_dragon=engine_dragon,
    )

    groups = config.get("workflows", {})
    print(
        "Campaign: "
        + ", ".join(
            f"{name}: {cfg.get('replicas', 0)} replica(s) "
            f"cap={cfg.get('concurrency_cap', '—')} "
            f"deps={cfg.get('dependencies', [])}"
            for name, cfg in groups.items()
        )
    )

    # ── ADR supervision (optional) ────────────────────────────────────────────
    # The CM scheduler orders eligible groups purely by group.priority; the ADR
    # policy drives those priorities via set_priority() each tick.  On HPC runs
    # the TelemetrySubscriber feeds real GPU/CPU utilisation into the observation.
    adr_cfg = dict(config.get("cm", {}).get("adr", {}))
    if record_override is not None:
        adr_cfg["record"] = record_override
    operator, tick_s = _build_adr_operator(
        cm, asyncflow, adr_cfg, policy_override, telemetry=telemetry)

    try:
        await cm.start()
        if operator is not None:
            from src.campaign.adr import run_supervised
            kind = (policy_override or adr_cfg.get("policy", "?")).lower()
            print(f"ADR supervision active: policy={kind}  tick={tick_s}s")
            await run_supervised(cm, operator, tick_s=tick_s)
        else:
            await cm.wait()
    finally:
        await cm.close()
        if telemetry:
            await telemetry.stop()
            print("Asyncflow telemetry stopped")
        await asyncflow.shutdown()

    # ── Summary ────────────────────────────────────────────────────────────
    print("\n── Campaign complete ──")
    for name, info in cm.status()["groups"].items():
        print(
            f"  {name}: status={info['status']}  "
            f"replicas={info['replicas_finished']}/{info['replicas_total']}"
        )

    print("\n── Replica counts per workflow ──")
    for name, s in cm.stats().items():
        print(
            f"  {name}: "
            f"replicas_started={s.replicas_started}  "
            f"replicas_finished={s.replicas_finished}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SPHERICAL ESM2/DDSim campaign runner")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to YAML config file (default: config.yaml)",
    )
    parser.add_argument(
        "--policy", default=None,
        choices=["none", "rule", "bandit", "llm"],
        help="ADR scheduling policy (overrides cm.adr.policy). "
             "'none' = no ADR supervision (static priorities).",
    )
    parser.add_argument(
        "--record", nargs="?", const="auto", default=None,
        help="Record per-cycle ADR decisions to JSONL "
             "(bare flag → adr-decisions-<policy>.jsonl).",
    )
    parser.add_argument(
        "--engine", default=None,
        choices=["dragon", "concurrent"],
        help="Override engine type from config (useful for local testing).",
    )

    args = parser.parse_args()
    asyncio.run(main(args.config, policy_override=args.policy,
                     record_override=args.record, engine_override=args.engine))
