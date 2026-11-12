#!/usr/bin/env python3
"""
MiniApps AuCaM campaign — fast vs slow pipeline arms.

Two workflow arms compete in the campaign:
  miniapps_fast — baseline num_step
  miniapps_slow — num_step × slow_step_multiplier (default 3×)

The bandit learns which arm converges faster; the controller cancels high-MSE runs.
At the end a table shows per-arm best/avg MSE and the CPU spent.

Score semantics: lower MSE is better.

Reading order: build_spec → _make_policy → run

Usage:
------
    # Local dev — all defaults:
    python run_campaign.py

    # HPC with Dragon and telemetry (auto-enabled by --telemetry-dir):
    python run_campaign.py \\
        --campaign-config campaign_configs/campaign_hpc.yaml \\
        --engine dragon --verbose --telemetry-dir /path/to/tel
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import math
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import workflows as _wf_mod
from config import (
    CampaignConfig,
    GlobalConfig,
    load_campaign,
    load_global,
)
from controller import MiniAppsController
from reporting import _ResultsWriter, _log_comparison, _print_policy_summary

from src.aucam import (
    CampaignBudget,
    CampaignManager,
    CampaignSpec,
    SchedulingPolicy,
    WorkflowConfig,
    WorkflowResult,
)


# ── Campaign spec ──────────────────────────────────────────────────────────────


def build_spec(
    *,
    name: str,
    ccfg: CampaignConfig,
    results_writer: _ResultsWriter | None = None,
    data_backend=None,
    scheduling_policy: SchedulingPolicy | None = None,
    cpu_hours: float | None = None,
    gpu_hours: float | None = None,
    engine: str = "concurrent",
) -> CampaignSpec:
    """Describe one campaign: two arms, a controller, and a budget.

    The controller (MiniAppsController) holds the science logic — cancel threshold
    and goal count.  Each arm's worker is a closure over its own config.
    """
    ctrl = MiniAppsController(
        goal_n=ccfg.controller.goal_n,
        score_ceiling=ccfg.controller.score_ceiling,
    )

    def _on_complete(result: WorkflowResult) -> None:
        # CampaignManager already calls ctrl.on_complete; only write JSONL here.
        if results_writer is not None:
            results_writer.write(result)

    spending: dict[str, float] = {}
    if cpu_hours is not None:
        spending["cpu_hours"] = cpu_hours
    if gpu_hours is not None:
        spending["gpu_hours"] = gpu_hours
    for k, v in (ccfg.budget.spending or {}).items():
        if k not in spending and v is not None:
            spending[k] = v

    # Each arm gets its own closure-bound worker — no shared mutable state.
    fast_cfg = ccfg.workflows.fast_config()
    slow_cfg = ccfg.workflows.slow_config()
    wcfg = ccfg.workflows
    fast_worker = _wf_mod.make_miniapps_worker(
        fast_cfg.to_dict(),
        arm_label="fast",
        engine=engine,
        declared_spending=wcfg.fast_spending,
        respect_limit=wcfg.respect_limit["fast"],
    )
    slow_worker = _wf_mod.make_miniapps_worker(
        slow_cfg.to_dict(),
        arm_label="slow",
        engine=engine,
        declared_spending=wcfg.resolved_slow_spending(),
        respect_limit=wcfg.respect_limit["slow"],
    )

    stale = ccfg.workflows.miniapps.stale_timeout_s

    return CampaignSpec(
        name=name,
        budget=CampaignBudget(
            max_concurrent_workflows=ccfg.budget.max_concurrent,
            max_concurrency_per_workflow=ccfg.budget.max_concurrency_per_workflow,
            capacity=ccfg.budget.capacity,
            spending=spending,
        ),
        controller=ctrl,
        workflows=[
            WorkflowConfig(
                name="miniapps_fast",
                block=fast_worker,
                stale_timeout_s=stale,
                launch_on_start=True,
                on_completion=_on_complete,
                declared_spending=ccfg.workflows.fast_spending,
                spending_limit=ccfg.workflows.fast_spending_limit,
                resources=ccfg.workflows.fast_resources,
            ),
            WorkflowConfig(
                name="miniapps_slow",
                block=slow_worker,
                # slow arm gets proportionally more time before stale-cancel
                stale_timeout_s=stale * ccfg.workflows.slow_step_multiplier,
                launch_on_start=True,
                on_completion=_on_complete,
                declared_spending=ccfg.workflows.resolved_slow_spending(),
                spending_limit=ccfg.workflows.resolved_slow_spending_limit(),
                resources=ccfg.workflows.slow_resources or ccfg.workflows.fast_resources,
            ),
        ],
        policy_interval_s=ccfg.policy_interval_s,
        shutdown_timeout_s=ccfg.shutdown_timeout_s,
        checkpoint_dir=ccfg.checkpoint_dir,
        data_backend=data_backend,
        scheduling_policy=scheduling_policy,
    )


# ── Scheduling policy ──────────────────────────────────────────────────────────


def _make_policy(name: str, ccfg=None) -> SchedulingPolicy | None:
    """UCB1 bandit with a time-efficiency reward: exp(-wall_time / ref_s).

    ref_s = stale_timeout / slow_multiplier  — calibrated to the fast arm's
    expected duration, so the fast arm naturally scores higher than the slow arm.
    """
    from src.aucam import BanditSchedulingPolicy, FIFOSchedulingPolicy

    if name == "bandit":
        stale = ccfg.workflows.miniapps.stale_timeout_s if ccfg is not None else 1800.0
        mult = ccfg.workflows.slow_step_multiplier if ccfg is not None else 3
        ref_s = stale / mult  # fast arm completes near ref_s → reward ≈ 0.37

        def _reward(result) -> float:
            if result.returncode != 0:
                return 0.0  # cancelled or failed — no reward
            return math.exp(-result.wall_time_s / ref_s)

        # exploration=0.5: the mean gap overrides the UCB bonus after ~2 pulls per arm
        return BanditSchedulingPolicy(reward_fn=_reward, exploration=0.5)
    if name == "fifo":
        return FIFOSchedulingPolicy()
    return None


# ── Execution ──────────────────────────────────────────────────────────────────


async def run(gcfg: GlobalConfig, ccfg: CampaignConfig, args: argparse.Namespace) -> None:
    """Create the backend and the CampaignManager, run the campaign, print results."""
    if gcfg.engine == "dragon":
        from rhapsody.backends.execution.dragon import DragonExecutionBackend

        backend = await DragonExecutionBackend()
    elif gcfg.engine == "concurrent":
        from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend

        backend = await ConcurrentExecutionBackend()
    else:
        backend = None

    name = "miniapps-campaign"
    rw = None
    if gcfg.telemetry.dir:
        rw = _ResultsWriter(Path(gcfg.telemetry.dir) / "results.jsonl")
    policy = _make_policy(gcfg.scheduling.policy, ccfg)
    spec = build_spec(
        name=name,
        ccfg=ccfg,
        results_writer=rw,
        scheduling_policy=policy,
        cpu_hours=args.spending_cpu_hours,
        gpu_hours=args.spending_gpu_hours,
        engine=gcfg.engine,
    )
    logging.info(
        "[RUN] %s (num_step_fast=%d  num_step_slow=%d)",
        name,
        ccfg.workflows.fast_config().num_step,
        ccfg.workflows.slow_config().num_step,
    )

    cm = CampaignManager(
        backend=backend,
        enable_telemetry=gcfg.telemetry.enabled,
        telemetry_checkpoint_dir=gcfg.telemetry.dir,
    )
    t0 = time.monotonic()
    try:
        outcome = await cm.run(spec)  # returns when the campaign ends
    finally:
        if rw:
            rw.close()
        if backend is not None and hasattr(backend, "shutdown"):
            await backend.shutdown()
    elapsed = time.monotonic() - t0

    # Print after the backend is shut down — Dragon no longer intercepts stdout.
    logging.info("[RUN] %s ended: %s", name, outcome.exit_reason)
    _log_comparison([(name, outcome, ccfg)])
    print(f"\n[POLICY] {name}:")
    _print_policy_summary(policy)
    print(f"\nTotal elapsed: {elapsed:.1f}s", flush=True)


# ── CLI ────────────────────────────────────────────────────────────────────────


def _build_parser(gcfg: GlobalConfig, ccfg: CampaignConfig) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="MiniApps AuCaM campaign — fast vs slow arms",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    p.add_argument("--global-config", default=None, help="path to global YAML config")
    p.add_argument("--campaign-config", default=None, help="path to campaign YAML config")
    p.add_argument("-v", "--verbose", action="store_true", default=gcfg.verbose)
    p.add_argument("--engine", choices=["default", "concurrent", "dragon"], default=gcfg.engine)
    p.add_argument(
        "--telemetry-dir", default=gcfg.telemetry.dir, help="directory for telemetry JSONL"
    )
    p.add_argument("--policy", choices=["bandit", "fifo"], default=gcfg.scheduling.policy)

    # Controller / budget
    p.add_argument(
        "--goal-n",
        type=int,
        default=ccfg.controller.goal_n,
        help="completions to reach before campaign terminates",
    )
    p.add_argument(
        "--score-ceiling",
        type=float,
        default=ccfg.controller.score_ceiling,
        help="cancel workflow if mean MSE exceeds this",
    )
    p.add_argument(
        "--max-concurrent",
        type=int,
        default=ccfg.budget.max_concurrent,
        help="max concurrent replicas per campaign",
    )
    p.add_argument("--checkpoint-dir", default=ccfg.checkpoint_dir)

    # MiniApps workflow parameters
    p.add_argument("--total-num-sim", type=int, default=ccfg.workflows.miniapps.total_num_sim)
    p.add_argument(
        "--num-step",
        type=int,
        default=ccfg.workflows.miniapps.num_step,
        help="timesteps per run (fast arm baseline)",
    )
    p.add_argument("--num-epochs", type=int, default=ccfg.workflows.miniapps.num_epochs)

    # Spending limits (override budget.spending in the campaign YAML)
    p.add_argument("--spending-cpu-hours", type=float, default=None)
    p.add_argument("--spending-gpu-hours", type=float, default=None)

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

    ccfg.controller.goal_n = args.goal_n
    ccfg.controller.score_ceiling = args.score_ceiling
    ccfg.budget.max_concurrent = args.max_concurrent
    ccfg.checkpoint_dir = args.checkpoint_dir
    ccfg.workflows.miniapps.total_num_sim = args.total_num_sim
    ccfg.workflows.miniapps.num_step = args.num_step
    ccfg.workflows.miniapps.num_epochs = args.num_epochs

    log_level = logging.DEBUG if gcfg.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("src.aucam").setLevel(log_level)

    asyncio.run(run(gcfg, ccfg, args))


if __name__ == "__main__":
    main()
