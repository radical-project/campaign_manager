#!/usr/bin/env python3
"""
Demo AuCaM campaign — synthetic sim → analysis pipeline.

Three workflow types:
  sim_worker      — synthetic simulation; ~65% convergence, N_ITERS steps.
                    Diverging runs (score < score_floor) are cancelled.
  sim_worker_b    — faster, more reliable sim; ~90% convergence, N_ITERS/3 steps.
                    The bandit learns to prefer this arm's higher throughput.
  analysis_worker — triggered by a successful sim via WorkflowDependency.

Campaign terminates when goal_analyses analyses have completed.

Reading order: build_spec → run

Usage:
------
    # Local — quick start (all defaults):
    python run_campaign.py --policy bandit

    # HPC with Dragon and telemetry:
    python run_campaign.py \\
        --global-config campaign_configs/global_hpc.yaml \\
        --engine dragon --telemetry-dir /path/to/tel
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import workflows as _workflows_mod
from config import (
    CampaignConfig,
    GlobalConfig,
    load_campaign,
    load_global,
)
from controller import ScoreController
from scheduler import make_policy, print_policy_summary
from workflows import analysis_worker, sim_worker, sim_worker_b

from src.aucam import (
    CampaignBudget,
    CampaignManager,
    CampaignOutcome,
    CampaignSpec,
    RadexDragonBackend,
    RhapsodyRedisBackend,
    SchedulingPolicy,
    WorkflowConfig,
    WorkflowDependency,
    WorkflowResult,
)


# ── Results writer ─────────────────────────────────────────────────────────────


class _ResultsWriter:
    """Appends one JSON line per completed WorkflowResult to a JSONL file."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._f = open(self._path, "a")

    def write(self, result: WorkflowResult) -> None:
        record = {
            "workflow_id": result.workflow_id,
            "workflow_type": result.workflow_type,
            "returncode": result.returncode,  # 0=ok  -1=cancelled  N=error
            "wall_time_s": result.wall_time_s,
            "spent": result.spent,
            "resource_profile": result.resource_profile,
            "final_metrics": result.final_metrics,
        }
        self._f.write(json.dumps(record) + "\n")
        self._f.flush()

    def close(self) -> None:
        self._f.close()


# ── Campaign spec ──────────────────────────────────────────────────────────────


def build_spec(
    *,
    name: str,
    ccfg: CampaignConfig,
    results_writer: _ResultsWriter | None = None,
    data_backend=None,
    scheduling_policy: SchedulingPolicy | None = None,
    use_cmd_worker: bool = False,
) -> CampaignSpec:
    """Describe one campaign: 3 workflows, DAG dependencies, controller, and budget.

    WorkflowDependency wires sim → analysis: each successful sim triggers one
    analysis_worker.  The controller cancels low-scoring sims and terminates
    the campaign once goal_analyses analyses are done.
    """
    ctrl = ScoreController(
        goal_analyses=ccfg.controller.goal_analyses,
        use_cmd_worker=use_cmd_worker,
        score_floor=ccfg.controller.score_floor,
    )

    def _on_complete(result: WorkflowResult) -> None:
        # CM already calls ctrl.on_complete; only write JSONL here.
        if results_writer is not None:
            results_writer.write(result)

    spending: dict[str, float] = {}
    if ccfg.budget.cpu_hours is not None:
        spending["cpu_hours"] = ccfg.budget.cpu_hours
    if ccfg.budget.gpu_hours is not None:
        spending["gpu_hours"] = ccfg.budget.gpu_hours

    workflows = [
        WorkflowConfig(
            name="sim_worker",
            block=sim_worker,
            stale_timeout_s=ccfg.workflows.sim_worker.stale_timeout_s,
            launch_on_start=True,
            on_completion=_on_complete,
            spending_limit=ccfg.workflows.sim_worker.spending_limit,
            resources=ccfg.workflows.sim_worker.resources,
        ),
        WorkflowConfig(
            name="sim_worker_b",
            block=sim_worker_b,
            stale_timeout_s=ccfg.workflows.sim_worker_b.stale_timeout_s,
            launch_on_start=True,
            on_completion=_on_complete,
            spending_limit=ccfg.workflows.sim_worker_b.spending_limit,
            resources=ccfg.workflows.sim_worker_b.resources,
        ),
        WorkflowConfig(
            name="analysis_worker",
            block=analysis_worker,
            launch_on_start=False,  # triggered by dependency, not on start
            on_completion=_on_complete,
            spending_limit=ccfg.workflows.analysis_worker.spending_limit,
            resources=ccfg.workflows.analysis_worker.resources,
        ),
    ]
    dependencies = [
        # Each successful sim triggers one analysis.
        WorkflowDependency(
            source="sim_worker", target="analysis_worker", condition=lambda r: r.returncode == 0
        ),
        WorkflowDependency(
            source="sim_worker_b", target="analysis_worker", condition=lambda r: r.returncode == 0
        ),
    ]

    if use_cmd_worker:
        workflows.append(
            WorkflowConfig(
                name="cmd_sim_worker",
                launch_cmd=[sys.executable, str(Path(__file__).parent / "cmd_sim_worker.py")],
                env={
                    "DEMO_N_ITERS": str(ccfg.workflows.sim_worker.n_iters),
                    "DEMO_SLEEP_S": str(ccfg.workflows.sim_worker.iter_sleep_s),
                    "DEMO_RESPECT_LIMIT": "1" if ccfg.workflows.sim_worker.respect_limit else "0",
                    "DEMO_RESPECT_RESOURCES": (
                        "1" if ccfg.workflows.sim_worker.respect_resources else "0"
                    ),
                },
                stale_timeout_s=ccfg.workflows.sim_worker.stale_timeout_s,
                launch_on_start=True,
                on_completion=_on_complete,
                spending_limit=ccfg.workflows.sim_worker.spending_limit,
                resources=ccfg.workflows.sim_worker.resources,
            )
        )
        dependencies.append(
            WorkflowDependency(
                source="cmd_sim_worker",
                target="analysis_worker",
                condition=lambda r: r.returncode == 0,
            )
        )

    return CampaignSpec(
        name=name,
        budget=CampaignBudget(
            max_concurrent_workflows=ccfg.budget.max_concurrent,
            max_concurrency_per_workflow=ccfg.budget.max_concurrency_per_workflow,
            capacity=ccfg.budget.capacity,
            spending=spending,
        ),
        controller=ctrl,
        workflows=workflows,
        dependencies=dependencies,
        policy_interval_s=ccfg.policy_interval_s,
        shutdown_timeout_s=ccfg.shutdown_timeout_s,
        checkpoint_dir=ccfg.checkpoint_dir,
        data_backend=data_backend,
        scheduling_policy=scheduling_policy,
    )


# ── CM lifecycle ───────────────────────────────────────────────────────────────


def _log_outcome(outcome: CampaignOutcome) -> None:
    """Log what a planner would use to decide the next campaign."""
    sims_ok = outcome.completed.get("sim_worker", 0) + outcome.completed.get("sim_worker_b", 0)
    sims_can = outcome.cancelled.get("sim_worker", 0) + outcome.cancelled.get("sim_worker_b", 0)
    logging.info(
        "[CM] %s: exit=%s goal_reached=%s cancel_rate=%.0f%% analyses=%d elapsed=%.1fs",
        outcome.campaign_name,
        outcome.exit_reason,
        outcome.goal_reached,
        100 * sims_can / max(1, sims_ok + sims_can),
        outcome.completed.get("analysis_worker", 0),
        outcome.elapsed_s,
    )
    if outcome.spent:
        spent_str = "  ".join(f"{k}={v:.5f}" for k, v in sorted(outcome.spent.items()))
        logging.info("[CM] %s: spent: %s", outcome.campaign_name, spent_str)


async def run(gcfg: GlobalConfig, ccfg: CampaignConfig, args: argparse.Namespace) -> None:
    """Create the CampaignManager, run the campaign, then print results."""
    backend = await _make_backend(gcfg.engine)

    # Dragon's runtime init re-hooks the root logger — re-apply after it returns.
    if gcfg.telemetry.dir:
        _reconfigure_logging(
            level=logging.DEBUG if gcfg.verbose else logging.INFO,
            telemetry_dir=gcfg.telemetry.dir,
        )

    data_backend, _rdb = await _make_data_backend(
        gcfg.engine, gcfg.data_backend, gcfg.redis_server_path
    )

    cm = CampaignManager(
        backend=backend,
        enable_telemetry=gcfg.telemetry.enabled,
        telemetry_checkpoint_dir=gcfg.telemetry.dir,
    )

    name = "demo-campaign"
    rw = _ResultsWriter(Path(gcfg.telemetry.dir) / "results.jsonl") if gcfg.telemetry.dir else None
    policy = make_policy(gcfg.scheduling.policy)
    spec = build_spec(
        name=name,
        ccfg=ccfg,
        results_writer=rw,
        data_backend=data_backend,
        scheduling_policy=policy,
        use_cmd_worker=data_backend is not None,
    )

    t0 = time.monotonic()
    try:
        outcome = await cm.run(spec)
    finally:
        if rw is not None:
            rw.close()
        if _rdb is not None:
            if hasattr(data_backend, "close"):
                data_backend.close()
            await _rdb.shutdown()
        if backend is not None and hasattr(backend, "shutdown"):
            await backend.shutdown()
    elapsed = time.monotonic() - t0
    _log_outcome(outcome)

    # Print after cm.run() returns — Dragon is fully shut down, stdout is safe.
    print(f"\n  campaign {name}:")
    print_policy_summary(policy, gcfg.scheduling.policy)
    print(f"\nTotal elapsed: {elapsed:.1f}s", flush=True)

    # Cancel lingering asyncio tasks left by Dragon/CM so the event loop can close.
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    if pending:
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


# ── Infrastructure ─────────────────────────────────────────────────────────────


def _reconfigure_logging(*, level: int, telemetry_dir: str) -> None:
    """Re-apply logging config after Dragon runtime init clears our handlers."""
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-8s %(name)-30s %(message)s", datefmt="%H:%M:%S"
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)
    Path(telemetry_dir).mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(Path(telemetry_dir) / "campaign.log")
    fh.setFormatter(fmt)
    root.addHandler(fh)


async def _make_backend(engine: str):
    if engine == "dragon":
        from rhapsody.backends.execution.dragon import DragonExecutionBackend

        return await DragonExecutionBackend()
    if engine == "concurrent":
        from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend

        return await ConcurrentExecutionBackend()
    return None


async def _make_data_backend(
    engine: str, data_backend_type: str = "auto", redis_server_path: str | None = None
):
    resolved = data_backend_type if data_backend_type != "auto" else engine

    if resolved == "dragon":
        from rhapsody.backends.data import DragonDataBackend as _RhapsodyDDB

        rdb = await _RhapsodyDDB(managers_per_node=1, n_nodes=1, total_mem=256 * 1024 * 1024)
        descriptor = rdb.endpoints[0].serialize()
        logging.info("Dragon data backend ready")
        return RadexDragonBackend(descriptor), rdb

    if resolved == "redis":
        from rhapsody.backends.data import RedisDataBackend as _RhapsodyRDB

        kwargs = {}
        if redis_server_path:
            kwargs["redis_server_path"] = redis_server_path
        rdb = await _RhapsodyRDB(**kwargs)
        descriptor = rdb.endpoints[0].serialize()
        logging.info("Redis data backend ready at %s", descriptor)
        return RhapsodyRedisBackend(descriptor), rdb

    return None, None


# ── CLI ─────────────────────────────────────────────────────────────────────────


def _build_parser(gcfg: GlobalConfig, ccfg: CampaignConfig) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Demo AuCaM campaign — synthetic sim → analysis pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    p.add_argument("--global-config", default=None)
    p.add_argument("--campaign-config", default=None)
    p.add_argument("-v", "--verbose", action="store_true", default=gcfg.verbose)
    p.add_argument("--engine", choices=["default", "concurrent", "dragon"], default=gcfg.engine)
    p.add_argument("--telemetry-dir", default=gcfg.telemetry.dir, help="enables telemetry JSONL")
    p.add_argument(
        "--policy", choices=["bandit", "fifo", "adr", "llm"], default=gcfg.scheduling.policy
    )

    # Campaign overrides
    p.add_argument(
        "--goal-analyses",
        type=int,
        default=ccfg.controller.goal_analyses,
        help="analyses before campaign terminates",
    )
    p.add_argument(
        "--max-concurrent",
        type=int,
        default=ccfg.budget.max_concurrent,
        help="max concurrent workflows",
    )
    p.add_argument("--checkpoint-dir", default=ccfg.checkpoint_dir)

    # Workflow timing
    p.add_argument(
        "--n-iters",
        type=int,
        default=ccfg.workflows.sim_worker.n_iters,
        help="iterations per sim_worker",
    )
    p.add_argument(
        "--iter-sleep",
        type=float,
        default=ccfg.workflows.sim_worker.iter_sleep_s,
        help="sleep (s) per iteration",
    )
    p.add_argument("--analysis-steps", type=int, default=ccfg.workflows.analysis_worker.steps)

    return p


def main() -> None:
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--global-config", default="campaign_configs/global.yaml")
    pre.add_argument("--campaign-config", default="campaign_configs/campaign.yaml")
    pre_args, _ = pre.parse_known_args()

    gcfg = (
        load_global(pre_args.global_config)
        if Path(pre_args.global_config).exists()
        else GlobalConfig()
    )
    ccfg = (
        load_campaign(pre_args.campaign_config)
        if Path(pre_args.campaign_config).exists()
        else CampaignConfig()
    )

    args = _build_parser(gcfg, ccfg).parse_args()

    gcfg.engine = args.engine if args.engine != "default" else gcfg.engine
    gcfg.verbose = args.verbose
    gcfg.scheduling.policy = args.policy
    gcfg.telemetry.dir = args.telemetry_dir
    if args.telemetry_dir:
        gcfg.telemetry.enabled = True

    ccfg.controller.goal_analyses = args.goal_analyses
    ccfg.budget.max_concurrent = args.max_concurrent
    ccfg.checkpoint_dir = args.checkpoint_dir
    ccfg.workflows.sim_worker.n_iters = args.n_iters
    ccfg.workflows.sim_worker.iter_sleep_s = args.iter_sleep
    ccfg.workflows.analysis_worker.steps = args.analysis_steps

    # Apply workflow timing so workers see the configured values.
    _workflows_mod.configure(
        n_iters=args.n_iters,
        iter_sleep_s=args.iter_sleep,
        analysis_steps=args.analysis_steps,
        respect_limit={
            "sim_worker": ccfg.workflows.sim_worker.respect_limit,
            "sim_worker_b": ccfg.workflows.sim_worker_b.respect_limit,
            "analysis_worker": ccfg.workflows.analysis_worker.respect_limit,
        },
        respect_resources={
            "sim_worker": ccfg.workflows.sim_worker.respect_resources,
            "sim_worker_b": ccfg.workflows.sim_worker_b.respect_resources,
            "analysis_worker": ccfg.workflows.analysis_worker.respect_resources,
        },
    )

    level = logging.DEBUG if gcfg.verbose else logging.INFO
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-8s %(name)-30s %(message)s", datefmt="%H:%M:%S"
    )
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if gcfg.telemetry.dir:
        Path(gcfg.telemetry.dir).mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(Path(gcfg.telemetry.dir) / "campaign.log")
        fh.setFormatter(fmt)
        handlers.append(fh)
    for h in handlers:
        h.setFormatter(fmt)
    logging.basicConfig(level=level, handlers=handlers, force=True)

    raise SystemExit(asyncio.run(run(gcfg, ccfg, args)))


if __name__ == "__main__":
    main()
