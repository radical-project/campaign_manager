#!/usr/bin/env python3
"""
Dummy minimization campaign runner.

Runs a two-stage search → refine pipeline in which 50 search replicas each
generate a random score; candidates below ``refine_threshold`` trigger a refine
replica that improves the score.  The campaign stops when 10 refine replicas
finish (campaign_target) or all search replicas are exhausted.

Usage
-----
    # Local (concurrent backend):
    python run_campaign.py --config config.yaml

    # With ADR scheduling policy:
    python run_campaign.py --config config.yaml --policy rule
    python run_campaign.py --config config.yaml --policy bandit

Config file structure
---------------------
    engine: concurrent

    workflows:
      search:
        replicas: 50
        refine_threshold: 0.35
        trigger_refine: refine
      refine:
        campaign_target: 10
        score_decay: 0.6
        score_noise: 0.03

    cm:
      adr:
        policy: rule
        tick_s: 0.5
        terminal: refine
"""

import asyncio
import sys
from pathlib import Path

# Project root (for src.campaign, src.utils) and this directory (for workflow imports).
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

import argparse  # noqa: E402

from src.campaign import AsyncCampaignManager as CampaignManager  # noqa: E402
from src.utils.workflow import load_config  # noqa: E402


def _build_registry(config: dict) -> dict:
    """Dynamically import workflow classes from the 'workflow_registry' config section."""
    import importlib

    registry = {}
    for name, cls_path in config.get("workflow_registry", {}).items():
        module_name, cls_name = cls_path.rsplit(".", 1)
        module = importlib.import_module(module_name)
        registry[name] = getattr(module, cls_name)
    return registry


def _build_adr_operator(cm, asyncflow, adr_cfg: dict, policy_override=None):
    """Build a CampaignOperator + tick for ADR supervision, or return (None, None).

    policy_override wins over adr_cfg["policy"].
    kind ∈ {none, rule, bandit, llm}; 'none' = no ADR supervision.
    """
    import os

    kind = (policy_override or adr_cfg.get("policy", "none") or "none").lower()
    if kind in ("none", "off", ""):
        return None, None

    from src.campaign.adr import (
        CampaignOperator,
        CampaignView,
        PolicyRecorder,
        make_scheduling_policy,
        resolve_system_prompt,
    )

    terminal = adr_cfg.get("terminal") or None
    view = CampaignView(cm, terminal=terminal)

    record_path = adr_cfg.get("record")
    if record_path in (True, "auto"):
        record_path = f"adr-decisions-{kind}.jsonl"
    recorder = PolicyRecorder(record_path, policy_kind=kind) if record_path else None

    op = CampaignOperator(view, engine=asyncflow, observer=recorder)

    kw, api_key = {}, None
    if kind == "bandit":
        kw["warmstart"] = bool(adr_cfg.get("warmstart", False))
        kw["seed"] = adr_cfg.get("seed", 0)
    elif kind == "llm":
        api_key = os.environ.get(adr_cfg.get("llm_api_key_env", "OPENROUTER_API_KEY"), "")
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
        prompt = resolve_system_prompt(adr_cfg)
        if prompt:
            kw["system_prompt"] = prompt

    op.policy = make_scheduling_policy(
        op, kind=kind, llm_api_key=api_key, model=adr_cfg.get("model", "openai/gpt-4o-mini"), **kw
    )

    if recorder is not None:
        recorder.bind(view=view, policy=op.policy)
        print(f"ADR decision recorder → {record_path}")

    tick = float(adr_cfg.get("tick_s", 0.5))
    return op, tick


async def main(config_file: str, policy_override=None, record_override=None) -> None:
    config_path = Path(config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_file}")

    config = load_config(config_file)
    engine_type = config.get("engine", "concurrent")

    # ── Backend + asyncflow ───────────────────────────────────────────────────
    if engine_type == "dragon":
        from radical.asyncflow import WorkflowEngine
        from rhapsody.backends import DragonExecutionBackendV3

        engine_dragon = await DragonExecutionBackendV3()
        asyncflow = await WorkflowEngine.create(engine_dragon)
        print("Dragon backend started")
    else:
        from radical.asyncflow import WorkflowEngine
        from rhapsody.backends import ConcurrentExecutionBackend

        engine_dragon = None
        backend = await ConcurrentExecutionBackend()
        asyncflow = await WorkflowEngine.create(backend)
        print("ConcurrentExecutionBackend started")

    # ── Campaign ──────────────────────────────────────────────────────────────
    registry = _build_registry(config)
    cm = CampaignManager.from_config(
        config, registry, asyncflow=asyncflow, engine_dragon=engine_dragon
    )

    groups = config.get("workflows", {})
    print(
        "Campaign: "
        + ", ".join(
            f"{name}: {cfg.get('replicas', 0)} replica(s) "
            f"cap={cfg.get('concurrency_cap', '—')} "
            f"target={cfg.get('campaign_target', '—')}"
            for name, cfg in groups.items()
        )
    )

    # ── ADR supervision (optional) ────────────────────────────────────────────
    adr_cfg = dict(config.get("cm", {}).get("adr", {}))
    if record_override is not None:
        adr_cfg["record"] = record_override
    operator, tick_s = _build_adr_operator(cm, asyncflow, adr_cfg, policy_override)

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
        await asyncflow.shutdown()

    # ── Minimization summary ──────────────────────────────────────────────────
    from dummy_workflow import DummyWorkflow

    gs = cm.status()["groups"]
    search_done = gs.get("search", {}).get("replicas_finished", 0)
    refine_done = gs.get("refine", {}).get("replicas_finished", 0)

    best = DummyWorkflow._best_score
    n_ev = DummyWorkflow._n_evaluated
    print("\n── Minimization complete ──")
    print(f"  best_score   = {best:.4f}")
    print(f"  n_evaluated  = {n_ev}")
    print(f"  search done  = {search_done}")
    print(f"  refine done  = {refine_done}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dummy minimization campaign runner")
    parser.add_argument(
        "--config", default="config.yaml", help="Path to YAML config file (default: config.yaml)"
    )
    parser.add_argument(
        "--policy",
        default=None,
        choices=["none", "rule", "bandit", "llm"],
        help="ADR scheduling policy (overrides cm.adr.policy)",
    )
    parser.add_argument(
        "--record",
        nargs="?",
        const="auto",
        default=None,
        help="Record per-cycle ADR decisions to JSONL",
    )
    args = parser.parse_args()
    asyncio.run(main(args.config, policy_override=args.policy, record_override=args.record))
