#!/usr/bin/env python3
"""
Small Molecule Binding campaign runner.

Runs n explore + m exploit SmallMoleculeBindingPipeline replicas per ADR cycle
for max_cycles total cycles, supervised by SmMolBindingOperator.

Usage
-----
    # Mock mode (no GPU, test campaign structure):
    python run_campaign.py --config config.yaml

    # Real runs — set env vars first:
    export SM_BINDING_EXAMPLES_DIR=/path/to/IMPRESS/examples/small_molecule_binding
    export IMPRESS_SRC=/path/to/IMPRESS/src
    python run_campaign.py --config config.yaml
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

# ── sys.path setup ─────────────────────────────────────────────────────────────
# campaign_manager root (for src.campaign.*)
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
# This campaign dir (for sm_binding_workflow, sm_binding_operator)
sys.path.insert(0, str(Path(__file__).parent))

# IMPRESS repo root — set via IMPRESS_SRC env var (defaults set in delta_sbatch.sh).
# The importable `impress` package lives in {IMPRESS_SRC}/src/, so we append that.
_impress_src = os.environ.get("IMPRESS_SRC", "")
if _impress_src:
    sys.path.insert(0, os.path.join(_impress_src, "src"))

# IMPRESS small_molecule_binding examples dir (for `small_molecule_binding` and
# `run_small_molecule_binding` modules) — set via SM_BINDING_EXAMPLES_DIR env var
_sm_binding_dir = os.environ.get("SM_BINDING_EXAMPLES_DIR", "")
if _sm_binding_dir:
    sys.path.insert(0, _sm_binding_dir)

import argparse  # noqa: E402

from src.campaign import AsyncCampaignManager as CampaignManager  # noqa: E402
from src.campaign import enable_logging  # noqa: E402
from src.utils.workflow import load_config  # noqa: E402


def _build_registry(config: dict) -> dict:
    import importlib

    registry = {}
    for name, cls_path in config.get("workflow_registry", {}).items():
        module_name, cls_name = cls_path.rsplit(".", 1)
        module = importlib.import_module(module_name)
        registry[name] = getattr(module, cls_name)
    return registry


def _build_operator(cm, asyncflow, adr_cfg: dict):
    from sm_binding_operator import SmMolBindingOperator

    from src.campaign.adr import CampaignView

    goals_cfg = adr_cfg.get("goals", {})
    max_cycles = int(goals_cfg.get("max_cycles", 1))
    explore_n  = int(goals_cfg.get("explore_n",  2))
    exploit_m  = int(goals_cfg.get("exploit_m",  1))

    terminal = adr_cfg.get("terminal") or None
    view = CampaignView(cm, terminal=terminal)

    op = SmMolBindingOperator(
        view,
        engine=asyncflow,
        max_cycles=max_cycles,
        explore_n=explore_n,
        exploit_m=exploit_m,
    )
    # Use the campaign's rule policy (SmMolBindingPolicy — dummy cycle launcher).
    op.policy = op.rule_policy()

    tick = float(adr_cfg.get("tick_s", 1.0))
    return op, tick


async def main(config_file: str) -> None:
    import logging as _logging

    enable_logging(_logging.INFO, configure_stack=True)

    # Suppress asyncio subprocess trace logs (run shell command / process created /
    # exited / Executing <Task> took ...) — these repeat for every task invocation.
    for _noisy in ("asyncio", "asyncio.coroutines", "concurrent.futures"):
        _logging.getLogger(_noisy).setLevel(_logging.WARNING)

    # Suppress Dragon/rhapsody DEBUG messages that fire during backend shutdown
    # (queue creation, batch join/destroy traces). Keep INFO level so "backend ready"
    # and "shutdown complete" messages still appear.
    for _debug_noisy in ("rhapsody.backends.execution.dragon", "dragon.native.queue",
                         "dragon.native.process"):
        _logging.getLogger(_debug_noisy).setLevel(_logging.INFO)

    config_path = Path(config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_file}")

    config = load_config(config_file)
    engine_type = config.get("engine", "concurrent")

    # ── Backend ───────────────────────────────────────────────────────────────
    # One Dragon instance is started here and shared with every IMPRESS pipeline
    # replica via BaseWorkflow.engine_dragon.  This lets all pipelines submit
    # GPU tasks to the same Dragon-managed pool across nodes simultaneously.
    from radical.asyncflow import WorkflowEngine

    if engine_type == "dragon":
        from rhapsody.backends import DragonExecutionBackend

        engine_dragon = await DragonExecutionBackend()
        asyncflow = await WorkflowEngine.create(engine_dragon)
        print("DragonExecutionBackend started — shared across all pipeline replicas")
    else:
        from rhapsody.backends import ConcurrentExecutionBackend

        engine_dragon = None
        backend = await ConcurrentExecutionBackend()
        asyncflow = await WorkflowEngine.create(backend)
        print("ConcurrentExecutionBackend started (mock/local mode)")

    # ── Campaign ──────────────────────────────────────────────────────────────
    registry = _build_registry(config)
    cm = CampaignManager.from_config(
        config, registry, engine=asyncflow, engine_dragon=engine_dragon
    )

    # ── ADR operator ─────────────────────────────────────────────────────────
    adr_cfg = dict(config.get("cm", {}).get("adr", {}))
    operator, tick_s = _build_operator(cm, asyncflow, adr_cfg)

    goals_cfg = adr_cfg.get("goals", {})
    n_explore = goals_cfg.get("explore_n", 2)
    n_exploit = goals_cfg.get("exploit_m", 1)
    n_cycles  = goals_cfg.get("max_cycles", 1)

    print("=" * 62)
    print(f"  Config   : {config_file}")
    print(f"  Engine   : {engine_type}")
    print(f"  Cycles   : {n_cycles}")
    print(f"  Explore  : {n_explore} pipeline(s)/cycle")
    print(f"  Exploit  : {n_exploit} pipeline(s)/cycle")
    print(f"  Total    : {n_cycles * (n_explore + n_exploit)} pipeline runs")
    print(f"  ADR tick : {tick_s}s")
    print("=" * 62)

    try:
        await cm.start()

        # Kick-start cycle 0: both groups have replicas=0 so the CM has no
        # pending work at startup and would signal done before run_supervised
        # fires its first policy tick.  Inject cycle 0's replicas now, then
        # advance the policy's internal state so the first tick doesn't
        # double-trigger.
        if n_explore > 0:
            await cm.trigger_dependent("explore", replicas=n_explore)
        if n_exploit > 0:
            await cm.trigger_dependent("exploit", replicas=n_exploit)
        operator.policy._cycle_launched = True
        operator.policy._cumulative_total = n_explore + n_exploit
        print(f"Cycle 0 triggered: {n_explore} explore + {n_exploit} exploit")

        from src.campaign.adr import run_supervised

        await run_supervised(cm, operator, tick_s=tick_s)
    finally:
        await cm.close()
        await asyncflow.shutdown()

    # ── Summary ───────────────────────────────────────────────────────────────
    from sm_binding_workflow import SmMolBindingWorkflow

    gs = cm.status()["groups"]
    explore_done = gs.get("explore", {}).get("replicas_finished", 0)
    exploit_done = gs.get("exploit", {}).get("replicas_finished", 0)

    results = SmMolBindingWorkflow._results
    best_plddt = max(
        (r.get("best_plddt", 0) for r in results if r.get("status") == "done"),
        default=None,
    )

    print("\n── Small Molecule Binding campaign complete ──")
    print(f"  explore pipelines done = {explore_done}")
    print(f"  exploit pipelines done = {exploit_done}")
    print(f"  total results          = {len(results)}")
    if best_plddt is not None:
        print(f"  best pLDDT             = {best_plddt:.2f}")
    for r in results:
        rid = r.get("replica_id", "?")
        print(f"  result file            → {rid}_result.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Small Molecule Binding campaign runner")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()
    asyncio.run(main(args.config))
