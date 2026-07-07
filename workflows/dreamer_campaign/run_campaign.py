#!/usr/bin/env python3
# Limit OpenBLAS/OMP threads before any numpy import to avoid pthread_create
# failures on login nodes where process counts are restricted.
import os

os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")

"""
Dreamer campaign runner — supports two config formats:

  Flat format (legacy):
    workflows:
      s1_ligand_filter: { replicas: 4, required_cpus: 16, ... }

  Plan format (cm-prototype):
    stages:
      - id: s1_ligand_filter
        upstream: library
        downstream: s2_ml_affinity
        concurrency_cap: 5000
        pilot: { partition: cpu, ... }
        dreamer: { num_cores: 128, ... }
    edges:
      - { upstream: s1_ligand_filter, downstream: s2_ml_affinity, profile: diverse_top }
    cm:
      engine: concurrent
      concurrency_scale: 0.002
      resources: { total_cpus: 64, total_gpus: 4 }
      workflow_registry: { s1_ligand_filter: dreamer_workflow.DreamerWorkflow }

  The plan format is auto-detected by the presence of a "stages" key.
  The translator maps:
    stage.upstream / downstream  → dependencies / trigger_downstream
    stage.pilot.partition        → required_cpus / required_gpus
    stage.concurrency_cap        → concurrency_cap  (× cm.concurrency_scale)
    edge.profile                 → schedule_strategy / early_binding
    edge.backpressure            → backpressure_high / backpressure_low (metadata)
    stage.dreamer.*              → dreamer emulation parameters

Usage:
    python run_campaign.py [--config config.yaml]
"""

import argparse
import asyncio
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.campaign import AsyncCampaignManager as CampaignManager  # noqa: E402
from src.utils.workflow import _expand_env, load_config  # noqa: E402


# ── Plan format translation tables ───────────────────────────────────────────

# pilot.partition → CM resource requirements
_PILOT_RESOURCES: dict[str, dict] = {
    "cpu":      {"required_cpus": 16, "required_gpus": 0},
    "gpu":      {"required_cpus":  4, "required_gpus": 1},
    "mpi+gpu":  {"required_cpus": 16, "required_gpus": 2},
    "largemem": {"required_cpus":  8, "required_gpus": 1},
}

# edge.profile → dreamer schedule_strategy
_PROFILE_STRATEGY: dict[str, str] = {
    "round_robin":     "random",
    "diverse_top":     "smallest_to_fastest",
    "explore_exploit": "largest_to_fastest",
    "pure_promise":    "largest_to_fastest",
    "active_learning": "largest_to_fastest",
}

# edge.profile → dreamer early_binding
_PROFILE_EARLY_BINDING: dict[str, bool] = {
    "round_robin":     True,   # diversity-focused: bind early
    "diverse_top":     True,
    "explore_exploit": False,  # score/uncertainty-focused: late binding
    "pure_promise":    False,
    "active_learning": False,
}


def _build_from_plan(config: dict) -> dict:
    """
    Translate cm-prototype plan (stages + edges) into the flat ``workflows``
    dict consumed by AsyncCampaignManager.from_config().

    Returns the translated workflows dict; does not mutate ``config``.
    """
    cm_cfg = config.get("cm", {})

    # debug section: SPHERICAL emulation overrides (not in prototype schema).
    # Must be read HERE before main() overwrites config["debug"] with a bool.
    debug_cfg = config.get("debug", {})
    debug_cfg = debug_cfg if isinstance(debug_cfg, dict) else {}
    stage_replicas_dbg = debug_cfg.get("stage_replicas",  {})
    trigger_fractions  = debug_cfg.get("trigger_fractions", {})

    stage_ids: set[str] = {s["id"] for s in config.get("stages", [])}

    # Outgoing edge per source stage (profile → schedule_strategy)
    edge_out: dict[str, dict] = {}
    # Incoming edge per destination stage (backpressure water marks for that stage's queue)
    edge_in:  dict[str, dict] = {}
    for edge in config.get("edges", []):
        src, dst = edge.get("upstream", ""), edge.get("downstream", "")
        if src in stage_ids:
            edge_out[src] = edge
        if dst in stage_ids:
            edge_in[dst] = edge

    workflows: dict[str, dict] = {}
    for stage in config.get("stages", []):
        sid        = stage["id"]
        upstream   = stage.get("upstream",   "")
        downstream = stage.get("downstream", "")

        # Only treat upstream as a CM dependency when it's a real stage
        deps = [upstream] if upstream in stage_ids else []

        # concurrency_cap drives the CM's concurrency_cap field directly for
        # local emulation.  Accept the legacy max_replicas key for back-compat.
        cap   = int(stage.get("concurrency_cap",
                              stage.get("max_replicas", 0)))

        # pilot.partition → required_cpus / required_gpus
        pilot     = stage.get("pilot", {})
        partition = pilot.get("partition", "cpu").lower()
        resources = _PILOT_RESOURCES.get(partition,
                                         {"required_cpus": 4, "required_gpus": 0})

        # Outgoing edge: profile → dreamer strategy + early_binding
        out_edge = edge_out.get(sid, {})
        profile  = out_edge.get("profile", "diverse_top")
        # Incoming edge: backpressure controls THIS stage's own queue depth
        in_bp    = edge_in.get(sid, {}).get("backpressure", {})
        strategy   = _PROFILE_STRATEGY.get(profile,       "smallest_to_fastest")
        early_bind = _PROFILE_EARLY_BINDING.get(profile,   True)

        # dreamer emulation block — may override strategy / early_binding
        dreamer = dict(stage.get("dreamer", {}))

        # trigger_downstream: only when downstream is a registered stage
        trigger = downstream if downstream in stage_ids else None

        # Replicas: debug.stage_replicas overrides stage.replicas (root stages only)
        replicas = int(stage_replicas_dbg.get(sid,
                       stage.get("replicas", 0 if deps else 1)))

        # Trigger fraction: debug.trigger_fractions → falls back to threshold_top_fraction
        trigger_fraction = float(trigger_fractions.get(
            sid, stage.get("threshold_top_fraction", 1.0) or 1.0))

        wf_cfg: dict = {
            # ── CM scheduling (consumed by from_config, not forwarded) ───
            "replicas":             replicas,
            "concurrency_floor":    int(stage.get("concurrency_floor",
                                                  stage.get("min_replicas", 0))),
            "concurrency_cap":      cap,
            "priority":             int(stage.get("priority", 0)),
            "dependencies":         deps,
            "dependency_threshold": int(stage.get("dependency_threshold", 1)),
            **resources,   # required_cpus, required_gpus

            # ── Workflow config (forwarded to DreamerWorkflow.config) ────
            "trigger_downstream":       trigger,
            "trigger_fraction":         trigger_fraction,   # from debug.trigger_fractions
            "threshold_top_fraction":   stage.get("threshold_top_fraction"),
            "budget_node_hours":        stage.get("budget_node_hours"),
            "downstream_input_target":  stage.get("downstream_input_target"),
            # campaign_target: early-stop trigger read by executor._on_replica_finished.
            # Must be forwarded into workflow_config (the executor does not see the
            # typed plan StageSpec; budget_kp/burn_rate_band reach BudgetController
            # via the plan path, but the early-stop check reads workflow_config).
            "campaign_target":          stage.get("campaign_target"),
            "pilot":                    pilot or None,
            "surrogate":                stage.get("surrogate"),
            "profile":                  profile,
            "schedule_strategy":        dreamer.pop("schedule_strategy", strategy),
            "early_binding":            dreamer.pop("early_binding", early_bind),
            # Backpressure for THIS stage's queue — only for dependent stages.
            # Root (independent) stages have a fixed initial queue size so BP
            # would immediately throttle them; skip it for those.
            "backpressure_high": (in_bp.get("high_water") or None) if deps else None,
            "backpressure_low":  (in_bp.get("low_water")  or None) if deps else None,
            # Sharding spec — only for dependent stages (root stages are not triggered).
            "sharding": stage.get("sharding") if deps else None,
            **dreamer,   # num_cores, perf_dist, num_tasks, ops_dist, profile_dir…
        }

        # Drop keys with None / falsy values that would clutter workflow config
        workflows[sid] = {k: v for k, v in wf_cfg.items() if v is not None}

    return workflows


# ── Legacy flat-format helpers ────────────────────────────────────────────────

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
        wf_specific.update(wf_cfg)
        wf_cfg.clear()
        wf_cfg.update(wf_specific)
    return config


def _build_registry(config: dict) -> dict:
    """Dynamically import workflow classes from 'workflow_registry'."""
    import importlib
    registry = {}
    for name, cls_path in config.get("workflow_registry", {}).items():
        module_name, cls_name = cls_path.rsplit(".", 1)
        registry[name] = getattr(importlib.import_module(module_name), cls_name)
    return registry


# ── Main ──────────────────────────────────────────────────────────────────────

def _build_adr_operator(cm, asyncflow, adr_cfg: dict, policy_override=None,
                        config_dir=None):
    """Build a CampaignOperator + policy for ADR supervision, or (None, None).

    Policy source precedence: --policy CLI override > cm.adr.policy config.
    kind ∈ {none, rule, bandit, llm}. 'none' = no ADR supervision (the scheduler
    uses static group priorities; the in-loop bandit was removed).
    """
    kind = (policy_override or adr_cfg.get("policy", "none") or "none").lower()
    if kind in ("none", "off", ""):
        return None, None

    from src.campaign.adr import (
        CampaignView, CampaignOperator, PolicyRecorder, make_scheduling_policy,
        resolve_system_prompt,
    )

    # The CM has no in-loop scheduling bandit; the scheduler orders eligible
    # groups purely by group.priority, which the ADR policy drives via its
    # set_priority lever.
    view = CampaignView(cm)

    # Optional per-cycle decision recorder (for plot_policy_comparison.py).
    recorder = None
    record_path = adr_cfg.get("record")
    if record_path in (True, "auto"):
        record_path = f"adr-decisions-{kind}.jsonl"
    if record_path:
        recorder = PolicyRecorder(record_path, policy_kind=kind)

    op = CampaignOperator(view, engine=asyncflow, observer=recorder)

    kw, api_key = {}, None
    if kind == "bandit":
        kw["warmstart"] = bool(adr_cfg.get("warmstart", False))
        kw["seed"] = adr_cfg.get("seed", 0)
    elif kind == "llm":
        api_key = os.environ.get(
            adr_cfg.get("llm_api_key_env", "OPENROUTER_API_KEY"), "")
        # Any OpenAI-compatible endpoint works (OpenRouter, HuggingFace router,
        # a local Ollama/llama.cpp server). Set cm.adr.base_url to switch.
        if adr_cfg.get("base_url"):
            kw["base_url"] = adr_cfg["base_url"]
        if adr_cfg.get("llm_timeout_s") is not None:
            kw["timeout_s"] = float(adr_cfg["llm_timeout_s"])
        if adr_cfg.get("llm_max_retries") is not None:
            kw["instructor_retries"] = int(adr_cfg["llm_max_retries"])
        # Local endpoints (Ollama/llama.cpp) need no real key; AsyncOpenAI still
        # requires a non-empty string, so supply a placeholder for localhost.
        # Remote endpoints keep the empty key so make_scheduling_policy raises a
        # clear "kind='llm' requires llm_api_key" instead of failing every call.
        bu = adr_cfg.get("base_url", "") or ""
        if not api_key and ("localhost" in bu or "127.0.0.1" in bu):
            api_key = "sk-noauth"
        # User-tweakable system prompt (cm.adr.system_prompt or system_prompt_file);
        # falls back to DEFAULT_SCHEDULING_PROMPT when unset.
        prompt = resolve_system_prompt(adr_cfg, config_dir)
        if prompt:
            kw["system_prompt"] = prompt
    op.policy = make_scheduling_policy(
        op, kind=kind, llm_api_key=api_key,
        model=adr_cfg.get("model", "openai/gpt-4o-mini"), **kw)
    if recorder is not None:
        recorder.bind(view=view, policy=op.policy)
        print(f"ADR decision recorder → {record_path}")
    # The LLM policy gets its own (slower) tick so free, rate-limited models
    # don't get throttled; falls back to tick_s when llm_tick_s isn't set.
    default_tick = float(adr_cfg.get("tick_s", 1.0))
    tick = float(adr_cfg.get("llm_tick_s", default_tick)) if kind == "llm" else default_tick
    return op, tick


async def main(config_file: str, policy_override=None, record_override=None) -> None:
    config_path = Path(config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_file}")
    config     = load_config(config_file)
    config_dir = config_path.parent

    # ── Detect and translate plan format ─────────────────────────────────────
    if "stages" in config:
        cm_cfg = config.get("cm", {})
        config["workflows"] = _build_from_plan(config)
        # Hoist cm: runtime keys to the top level where from_config expects them.
        # "debug" is intentionally excluded: our top-level debug: is a dict of
        # emulation overrides; we set the CM's debug bool explicitly below.
        for key in ("engine", "resources", "telemetry", "workflow_registry", "features"):
            if key in cm_cfg and key not in config:
                config[key] = cm_cfg[key]
        # Overwrite the debug dict with the CM boolean so from_config works correctly
        config["debug"] = bool(cm_cfg.get("debug", False))
        n_stages = len(config["stages"])
        n_edges  = len(config.get("edges", []))
        print(f"Plan format: {n_stages} stages, {n_edges} edges → "
              f"{len(config['workflows'])} workflow groups")
    else:
        _expand_workflow_configs(config, config_dir)

    engine_type = config.get("engine", "concurrent")

    # ── Build async backend ───────────────────────────────────────────────────
    engine_dragon = None
    asyncflow     = None

    if engine_type == "dragon":
        try:
            from radical.asyncflow import WorkflowEngine
            from rhapsody.backends import DragonExecutionBackendV3

            engine_dragon = await DragonExecutionBackendV3()
            asyncflow     = await WorkflowEngine.create(engine_dragon)
            print("Dragon backend started")
        except ImportError:
            engine_type = "concurrent"

    if engine_type == "concurrent":
        from radical.asyncflow import WorkflowEngine
        from rhapsody.backends import ConcurrentExecutionBackend

        backend   = await ConcurrentExecutionBackend()
        asyncflow = await WorkflowEngine.create(backend)
        print("ConcurrentExecutionBackend started")

    # ── Telemetry (optional) ────────────────────────────────────────────────────
    # Asyncflow telemetry needs the opentelemetry SDK; it's an optional extra and
    # the campaign (and ADR operator, which doesn't use it) runs fine without it.
    # Degrade gracefully if the dep is missing rather than crashing the run.
    tel_cfg   = config.get("telemetry", {})
    telemetry = None
    if tel_cfg.get("collect_telemetry", False):
        telemetry_dir = tel_cfg.get("telemetry_dir", "telemetry-results")
        if hasattr(asyncflow, "start_telemetry"):
            try:
                telemetry = await asyncflow.start_telemetry(
                    resource_poll_interval=0.5,
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
    adr_cfg = dict(config.get("cm", {}).get("adr", {}))
    if record_override is not None:
        adr_cfg["record"] = record_override
    operator, tick_s = _build_adr_operator(cm, asyncflow, adr_cfg, policy_override,
                                           config_dir=config_dir)

    try:
        await cm.start()
        if operator is not None:
            from src.campaign.adr import run_supervised
            kind = (policy_override or adr_cfg.get("policy", "?")).lower()
            print(f"ADR supervision active: policy={kind}  tick={tick_s}s "
                  f"(ADR policy drives scheduling priority)")
            await run_supervised(cm, operator, tick_s=tick_s)
        else:
            await cm.wait()
    finally:
        await cm.close()
        if telemetry:
            await telemetry.stop()
            print("Asyncflow telemetry stopped")
        await asyncflow.shutdown()

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n── Campaign complete ──")
    for name, info in cm.status()["groups"].items():
        print(f"  {name}: status={info['status']}  "
              f"replicas={info['replicas_finished']}/{info['replicas_total']}")

    print("\n── Replica counts per workflow ──")
    for name, s in cm.stats().items():
        print(f"  {name}: replicas_started={s.replicas_started}  "
              f"replicas_finished={s.replicas_finished}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SPHERICAL dreamer campaign runner")
    parser.add_argument("--config", default="config.yaml",
                        help="Path to YAML config (flat or plan format)")
    parser.add_argument("--policy", default=None,
                        choices=["none", "rule", "bandit", "llm"],
                        help="ADR scheduling policy (overrides cm.adr.policy). "
                             "'none' = no ADR supervision (static priorities).")
    parser.add_argument("--record", nargs="?", const="auto", default=None,
                        help="Record per-cycle ADR decisions to JSONL "
                             "(bare flag → adr-decisions-<policy>.jsonl).")
    args = parser.parse_args()
    asyncio.run(main(args.config, policy_override=args.policy,
                     record_override=args.record))
