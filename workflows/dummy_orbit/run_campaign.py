#!/usr/bin/env python3
"""
dummy_orbit campaign runner.

Runs a two-stage search → refine pipeline where tasks execute on a remote
HPC node via the ORBIT broker + rhapsody concurrent backend.

Usage
-----
    export RADICAL_ORBIT_BROKER_URL=wss://dt-login03.delta.ncsa.illinois.edu:8020
    python run_campaign.py --config config.yaml

Prerequisites
-------------
    # Terminal 1 (login node)
    ./bin/radical-orbit-broker.py --port 8020

    # Terminal 2 (compute node, inside allocation)
    ./bin/radical-orbit-endpoint.py -p rhapsody
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

import argparse  # noqa: E402

from src.campaign import AsyncCampaignManager as CampaignManager  # noqa: E402
from src.utils.workflow import load_config                         # noqa: E402


def _build_registry(config: dict) -> dict:
    import importlib
    registry = {}
    for name, cls_path in config.get("workflow_registry", {}).items():
        module_name, cls_name = cls_path.rsplit(".", 1)
        module = importlib.import_module(module_name)
        registry[name] = getattr(module, cls_name)
    return registry


async def main(config_file: str) -> None:
    config_path = Path(config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_file}")

    config = load_config(config_file)

    from radical.asyncflow import WorkflowEngine
    from rhapsody.backends import ConcurrentExecutionBackend

    backend   = await ConcurrentExecutionBackend()
    asyncflow = await WorkflowEngine.create(backend)
    print("ConcurrentExecutionBackend started (asyncflow)")

    registry = _build_registry(config)
    cm = CampaignManager.from_config(config, registry, asyncflow=asyncflow)

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

    try:
        await cm.start()
        await cm.wait()
    finally:
        await cm.close()
        # Shut down the shared orbit connection
        from orbit_workflow import OrbitWorkflow
        await OrbitWorkflow.close_connection()
        await asyncflow.shutdown()

    # ── Summary ───────────────────────────────────────────────────────────────
    from orbit_workflow import OrbitWorkflow

    gs          = cm.status()["groups"]
    search_done = gs.get("search", {}).get("replicas_finished", 0)
    refine_done = gs.get("refine", {}).get("replicas_finished", 0)

    print("\n── Campaign complete ──")
    print(f"  best_score   = {OrbitWorkflow._best_score:.4f}")
    print(f"  n_evaluated  = {OrbitWorkflow._n_evaluated}")
    print(f"  search done  = {search_done}")
    print(f"  refine done  = {refine_done}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="dummy_orbit campaign runner")
    parser.add_argument("--config", default="config.yaml",
                        help="Path to YAML config (default: config.yaml)")
    args = parser.parse_args()
    asyncio.run(main(args.config))
