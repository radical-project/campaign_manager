#!/usr/bin/env python3
"""
dummy_ddsim campaign runner.

Two independent DDSim pools (fast/coarse and long/fine) feed a shared
analysis stage.  The campaign stops when campaign_target analyses finish.

Usage
-----
    # Local (concurrent backend):
    python run_campaign.py --config config.yaml

    # With ADR scheduling policy:
    python run_campaign.py --config config.yaml --policy rule
    python run_campaign.py --config config.yaml --policy bandit
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

import argparse  # noqa: E402

from src.campaign import AsyncCampaignManager as CampaignManager  # noqa: E402
from src.utils.workflow import load_config  # noqa: E402


def _build_registry(config: dict) -> dict:
    import importlib

    registry = {}
    for name, cls_path in config.get("workflow_registry", {}).items():
        module_name, cls_name = cls_path.rsplit(".", 1)
        module = importlib.import_module(module_name)
        registry[name] = getattr(module, cls_name)
    return registry


def _build_adr_operator(cm, asyncflow, adr_cfg: dict, policy_override=None, config_dir=None):
    import os
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parent))
    from ddsim_operator import DdSimCampaignOperator

    from src.campaign.adr import (
        CampaignView,
        LoggingPolicy,
        NullSchedulingPolicy,
        PolicyRecorder,
        make_scheduling_policy,
        resolve_system_prompt,
    )

    # n_target / n_budget from adr.goals.
    goals_cfg = adr_cfg.get("goals", {})
    n_target = int(goals_cfg.get("n_target", 15))
    n_budget = int(goals_cfg.get("n_budget", 80))

    # policy_key=None → use operator.default_policy(); "none" → goal-only (no scheduling).
    policy_key = policy_override or adr_cfg.get("policy")

    terminal = adr_cfg.get("terminal") or None
    view = CampaignView(cm, terminal=terminal)

    effective_kind = (policy_key or "default").lower()
    record_path = adr_cfg.get("record")
    if record_path in (True, "auto"):
        record_path = f"adr-decisions-{effective_kind}.jsonl"
    recorder = PolicyRecorder(record_path, policy_kind=effective_kind) if record_path else None

    op = DdSimCampaignOperator(view, engine=asyncflow, n_target=n_target, n_budget=n_budget, observer=recorder)

    if policy_key is None:
        policy = op.default_policy()
        if policy is None:
            return None, None
        kind_label = "default"
    elif str(policy_key).lower() in ("none", "off", ""):
        # "none" suppresses scheduling decisions but goals still need checking.
        # NullSchedulingPolicy satisfies the Policy interface without changing
        # any priorities or batch sizes — the operator loop runs for early-stop only.
        policy = NullSchedulingPolicy()
        kind_label = "none"
    else:
        kind = str(policy_key).lower()
        kind_label = kind
        kw: dict = {}
        api_key = None
        if kind == "bandit":
            kw["warmstart"] = bool(adr_cfg.get("warmstart", False))
            kw["seed"] = adr_cfg.get("seed", 0)
        elif kind == "blend":
            # Blend = Bandit averaged with LLM; needs both sets of kwargs.
            kw["warmstart"] = bool(adr_cfg.get("warmstart", False))
            kw["seed"]      = adr_cfg.get("seed", 0)
            api_key = os.environ.get(adr_cfg.get("llm_api_key_env", "OPENROUTER_API_KEY"), "")
            bu = adr_cfg.get("base_url", "") or ""
            if not api_key and ("localhost" in bu or "127.0.0.1" in bu):
                api_key = "sk-noauth"
            if bu:
                kw["base_url"] = bu
            if adr_cfg.get("llm_timeout_s") is not None:
                kw["timeout_s"] = float(adr_cfg["llm_timeout_s"])
            if adr_cfg.get("llm_max_retries") is not None:
                kw["instructor_retries"] = int(adr_cfg["llm_max_retries"])
            prompt = resolve_system_prompt(adr_cfg, config_dir)
            if prompt:
                kw["system_prompt"] = prompt
        elif kind == "llm":
            api_key = os.environ.get(adr_cfg.get("llm_api_key_env", "OPENROUTER_API_KEY"), "")
            if adr_cfg.get("base_url"):
                kw["base_url"] = adr_cfg["base_url"]
            if adr_cfg.get("llm_timeout_s") is not None:
                kw["timeout_s"] = float(adr_cfg["llm_timeout_s"])
            if adr_cfg.get("llm_max_retries") is not None:
                kw["instructor_retries"] = int(adr_cfg["llm_max_retries"])
            bu = adr_cfg.get("base_url", "") or ""
            if not api_key and ("localhost" in bu or "127.0.0.1" in bu):
                api_key = "sk-noauth"
            prompt = resolve_system_prompt(adr_cfg, config_dir)
            if prompt:
                kw["system_prompt"] = prompt
        policy = make_scheduling_policy(
            op, kind=kind, llm_api_key=api_key,
            model=adr_cfg.get("model", "openai/gpt-4o-mini"), **kw
        )

    if policy is not None and not isinstance(policy, NullSchedulingPolicy):
        log_every = int(adr_cfg.get("log_every", 1))
        policy = LoggingPolicy(policy, log_every=log_every)
    op.policy = policy
    if recorder is not None:
        recorder.bind(view=view, policy=op.policy, metrics=cm.metrics())
        print(f"ADR decision recorder → {record_path}")

    tick = float(adr_cfg.get("tick_s", 0.5))
    return op, tick


async def main(config_file: str, policy_override=None, record_override=None) -> None:
    import logging as _logging

    from src.campaign import enable_logging
    enable_logging(_logging.INFO, configure_stack=True)

    config_path = Path(config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_file}")

    config = load_config(config_file)
    engine_type = config.get("engine", "concurrent")

    # ── Backend + asyncflow ───────────────────────────────────────────────────
    from radical.asyncflow import WorkflowEngine

    if engine_type == "dragon":
        from rhapsody.backends import DragonExecutionBackend

        engine_dragon = await DragonExecutionBackend()
        asyncflow = await WorkflowEngine.create(engine_dragon)
        print("Dragon backend started")
    else:
        from rhapsody.backends import ConcurrentExecutionBackend

        engine_dragon = None
        backend = await ConcurrentExecutionBackend()
        asyncflow = await WorkflowEngine.create(backend)
        print("ConcurrentExecutionBackend started")

    # ── Campaign ──────────────────────────────────────────────────────────────
    registry = _build_registry(config)
    cm = CampaignManager.from_config(
        config, registry, engine=asyncflow, engine_dragon=engine_dragon
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
    operator, tick_s = _build_adr_operator(cm, asyncflow, adr_cfg, policy_override, config_dir=config_path.parent)

    # ── Startup summary ───────────────────────────────────────────────────────
    adr_policy = (policy_override or adr_cfg.get("policy") or "default").lower()
    _goals_cfg = adr_cfg.get("goals", {})
    n_target   = _goals_cfg.get("n_target", "—")
    n_budget   = _goals_cfg.get("n_budget", "—")
    terminal   = adr_cfg.get("terminal") or "(inferred leaf)"
    n_groups   = len(config.get("workflows", {}))
    print("=" * 62)
    print(f"  Config  : {config_file}")
    print(f"  Engine  : {engine_type}  |  {n_groups} workflow groups")
    print(f"  ADR     : policy={adr_policy}  tick={tick_s}s")
    print(f"  Goals   : {n_target} '{terminal}' analyses  OR  {n_budget} total sims (budget)")
    if adr_policy == "llm":
        print(f"  Model   : {adr_cfg.get('model', '—')}")
        bu = adr_cfg.get("base_url") or "OpenRouter (default)"
        print(f"  Backend : {bu}")
        key_env = adr_cfg.get("llm_api_key_env", "OPENROUTER_API_KEY")
        import os
        key_set = "set" if os.environ.get(key_env) else "NOT SET"
        print(f"  Key env : {key_env} ({key_set})")
    print("=" * 62)

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

    # ── Summary ───────────────────────────────────────────────────────────────
    from ddsim_workflow import AnalysisWorkflow, DdSimWorkflow

    gs = cm.status()["groups"]
    a_done = gs.get("ddsim_a", {}).get("replicas_finished", 0)
    b_done = gs.get("ddsim_b", {}).get("replicas_finished", 0)
    an_done = gs.get("analysis", {}).get("replicas_finished", 0)

    na = DdSimWorkflow._n_sim.get("ddsim_a", 0)
    nb = DdSimWorkflow._n_sim.get("ddsim_b", 0)
    ana_a = AnalysisWorkflow._n_analyzed.get("ddsim_a", 0)
    ana_b = AnalysisWorkflow._n_analyzed.get("ddsim_b", 0)

    summary_lines = [
        f"  best raw score   = {DdSimWorkflow._best_raw:.4f}",
        f"  best analyzed    = {AnalysisWorkflow._best_analyzed:.4f}",
        f"  sims done        = {na} (fast/ddsim_a) + {nb} (long/ddsim_b) = {na + nb}",
        f"  analyses done    = {ana_a} (from fast) + {ana_b} (from long) = {ana_a + ana_b}",
        f"  CM groups        = ddsim_a:{a_done}  ddsim_b:{b_done}  analysis:{an_done}",
    ]

    print("\n── DDSim campaign complete ──")
    print("\n".join(summary_lines))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="dummy_ddsim campaign runner")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument(
        "--policy",
        default=None,
        choices=["none", "rule", "downstream_first", "bandit", "llm"],
        help="ADR scheduling policy (overrides cm.adr.policy). "
        "'rule' = campaign's rule_policy() or DownstreamFirstPolicy fallback. "
        "'downstream_first' = generic depth-first rule unconditionally.",
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
