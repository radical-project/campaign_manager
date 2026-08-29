#!/usr/bin/env python3
"""Two-phase hierarchical ddsim campaign runner.

Demonstrates the ADR hierarchical operator pattern (example 08) in the ddsim
campaign:

  Stage 1  DdSim-A screening:  Stage1Operator runs ddsim_a + analysis until
           all ddsim_a replicas finish.  Best score_p50 is reported to the
           parent each cycle via SNAPSHOT messaging.

  Stage 2  DdSim-B refinement: Parent reads Stage 1's best score, tightens
           the triage cutoff, triggers ddsim_b replicas, then launches
           Stage2Operator once Stage1 is confirmed DONE.

Usage
-----
    cd campaigns/ddsim_campaign
    python run_campaign_phased.py --config config_phased.yaml
"""

import asyncio
import logging
import sys
import time
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


async def main(config_file: str, resume: str | None = None) -> None:
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
        from rhapsody.backends import DragonExecutionBackendV3

        engine_dragon = await DragonExecutionBackendV3()
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

    # ── Phased operator setup ─────────────────────────────────────────────────
    import os

    from radical.adr import RecordingObserver

    from ddsim_operator_phased import ParentOperator, ParentPolicy
    from src.campaign.adr import CampaignView, LoggingPolicy

    adr_cfg   = config.get("cm", {}).get("adr", {})
    phased    = adr_cfg.get("phased", {})
    n_ddsim_a = int(phased.get("n_ddsim_a", 20))
    n_ddsim_b = int(phased.get("n_ddsim_b", 20))
    tick_s    = float(adr_cfg.get("tick_s", 0.5))
    terminal  = adr_cfg.get("terminal") or None
    log_every = int(adr_cfg.get("log_every", 1))

    slurm_job  = os.environ.get("SLURM_JOB_ID", "local")
    adr_logs   = config_path.parent / "adr-logs"
    adr_logs.mkdir(parents=True, exist_ok=True)
    trace_path = adr_logs / f"trace_{slurm_job}.jsonl"
    ckpt_path  = adr_logs / f"checkpoint_{slurm_job}.json"

    view = CampaignView(cm, terminal=terminal)

    parent = ParentOperator(
        view, engine=asyncflow,
        n_ddsim_a=n_ddsim_a,
        n_ddsim_b=n_ddsim_b,
        observer=RecordingObserver(str(trace_path)),
    )
    parent.policy = LoggingPolicy(ParentPolicy(parent), log_every=log_every)

    if resume:
        parent.load_checkpoint_full(resume)
        print(f"  Resumed from      : {resume}")

    # ── Startup summary ───────────────────────────────────────────────────────
    n_groups = len(config.get("workflows", {}))
    print("=" * 64)
    print(f"  Config  : {config_file}")
    print(f"  Engine  : {engine_type}  |  {n_groups} workflow groups")
    print(f"  ADR     : phased  tick={tick_s}s  log_every={log_every}")
    print(f"  Stage 1 : {n_ddsim_a} ddsim_a replicas → screen")
    print(f"  Stage 2 : {n_ddsim_b} ddsim_b replicas → refine (triggered by parent)")
    print("=" * 64)
    print(
        "  ADR concepts: child.start() | SNAPSHOT messaging | info passing"
    )
    print("=" * 64)

    _t0 = time.monotonic()
    _log = logging.getLogger(__name__)

    # ── Run ───────────────────────────────────────────────────────────────────
    # Custom phased driver — NOT run_supervised.
    #
    # run_supervised waits for `cm.wait()` to return, which fires as soon as
    # Stage 1 work (ddsim_a + analysis) completes.  At that point it cancels
    # the parent before Stage 2 is triggered.  Instead we drive the parent
    # loop directly and wait for the PARENT to stop (Decision(stop=True) after
    # Stage 2 finishes), then drain any remaining CM work.
    async def _drive_parent() -> None:
        async for _snapshot in parent.run():
            await asyncio.sleep(tick_s)

    try:
        await cm.start()
        print("ADR supervision active: staged hierarchical operator")
        await _drive_parent()
        # Parent stopped.  Wait up to 120 s for in-flight Stage 2 work to drain.
        try:
            await asyncio.wait_for(cm.wait(), timeout=120.0)
        except asyncio.TimeoutError:
            _log.warning(
                "CM did not drain within 120 s after parent stopped — forcing stop"
            )
            await cm.stop()
    finally:
        _t_shutdown = time.monotonic() - _t0
        _log.info("Shutdown t=%.2fs — saving checkpoint", _t_shutdown)
        parent.save_checkpoint_full(str(ckpt_path))

        _log.info("Shutdown t=%.2fs — parent.shutdown()", time.monotonic() - _t0)
        await parent.shutdown()

        _log.info("Shutdown t=%.2fs — cm.close()", time.monotonic() - _t0)
        await cm.close()

        _log.info("Shutdown t=%.2fs — asyncflow.shutdown()", time.monotonic() - _t0)
        await asyncflow.shutdown()

        _log.info("Shutdown t=%.2fs — done", time.monotonic() - _t0)

    # ── Summary ───────────────────────────────────────────────────────────────
    from ddsim_workflow import AnalysisWorkflow, DdSimWorkflow

    gs     = cm.status()["groups"]
    a_done = gs.get("ddsim_a",  {}).get("replicas_finished", 0)
    b_done = gs.get("ddsim_b",  {}).get("replicas_finished", 0)
    an_done = gs.get("analysis", {}).get("replicas_finished", 0)

    na    = DdSimWorkflow._n_sim.get("ddsim_a", 0)
    nb    = DdSimWorkflow._n_sim.get("ddsim_b", 0)
    ana_a = AnalysisWorkflow._n_analyzed.get("ddsim_a", 0)
    ana_b = AnalysisWorkflow._n_analyzed.get("ddsim_b", 0)

    # stage1_best_score is persisted in state.artifacts by watch() (survives clear_cycle).
    p1_score = parent.state.artifacts.get("stage1_best_score", None)

    summary_lines = [
        f"  Stage 1 best score   = {p1_score:.4f}" if p1_score else "  Stage 1 best score   = n/a",
        f"  best raw score       = {DdSimWorkflow._best_raw:.4f}",
        f"  best analyzed        = {AnalysisWorkflow._best_analyzed:.4f}" if AnalysisWorkflow._best_analyzed != float('inf') else "  best analyzed        = n/a",
        f"  sims done            = {na} (ddsim_a) + {nb} (ddsim_b) = {na + nb}",
        f"  analyses done        = {ana_a} (from ddsim_a) + {ana_b} (from ddsim_b) = {ana_a + ana_b}",
        f"  CM groups            = ddsim_a:{a_done}  ddsim_b:{b_done}  analysis:{an_done}",
    ]

    print("\n── Staged DDSim campaign complete ──")
    print("\n".join(summary_lines))
    print(f"(trace saved      → {trace_path})")
    print(f"(checkpoint saved → {ckpt_path})")


if __name__ == "__main__":
    # Pre-warm the key synchronous code paths before entering the event loop.
    #
    # On HPC systems with Lustre/GPFS shared filesystems, cold page faults on
    # Python bytecode pages can stall any code path for 90+ s on first execution.
    # The main fix is asyncio.to_thread() wrapping in operator.py and policies.py
    # so that stalls hit worker threads, not the event loop thread.
    #
    # This pre-warm is a complementary best-effort: it executes each hot path
    # once before asyncio starts, so worker threads also start warm.
    import asyncio as _asyncio
    from radical.adr.state import RuntimeState as _RuntimeState

    _st = _RuntimeState("prewarm")
    _loop = _asyncio.new_event_loop()
    # Exercise the isinstance(status, asyncio.Future) branch in both directions.
    _st.runtime["_prewarm_fut"] = _loop.create_future()
    _st.runtime["_prewarm_str"] = "DONE"
    _st.snapshot()
    _loop.close()

    del _st, _RuntimeState, _loop, _asyncio

    parser = argparse.ArgumentParser(description="Two-phase hierarchical ddsim campaign runner")
    parser.add_argument("--config", default="config_phased.yaml")
    parser.add_argument(
        "--resume", default=None, metavar="CKPT",
        help="checkpoint file to resume from (adr-logs/checkpoint_<job>.json)",
    )
    args = parser.parse_args()
    asyncio.run(main(args.config, args.resume))
