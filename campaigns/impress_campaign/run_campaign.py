#!/usr/bin/env python3
"""
IMPRESS AuCaM campaign — protein-binding design with budget monitoring and live
score reports.

Each AuCaM workflow runs one full IMPRESS-A campaign (design → score → Pareto front).
AuCaM enforces a per-workflow budget (cpu_hours + gpu_hours), cancels overspending
runs, and the ImpressController logs a budget-status line each policy cycle.

Score: `progress` from CampaignDigest — fraction of the requested Pareto front that
is satisfied (0–1; higher is better).  Per-objective bests (complex_plddt,
ligand_iptm, …) travel in metrics and are logged on completion.

Usage
-----
    # Local dev (concurrent backend, all defaults):
    python run_campaign.py --spec path/to/spec.yaml

    # HPC with Dragon and telemetry:
    python run_campaign.py \\
        --spec path/to/spec.yaml \\
        --global-config campaign_configs/global_hpc.yaml \\
        --engine dragon --verbose \\
        --telemetry-dir /path/to/tel

Reading order: build_spec → run → main
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config import CampaignConfig, GlobalConfig, load_campaign, load_global
from controller import ImpressController

from src.aucam import (
    BanditSchedulingPolicy,
    CampaignBudget,
    CampaignManager,
    CampaignSpec,
    WorkflowConfig,
)


# ── Campaign spec ──────────────────────────────────────────────────────────────


def build_spec(
    *,
    name: str,
    spec_path: str,
    ccfg: CampaignConfig,
    scheduling_policy=None,
    model: str = "D",
    report_interval_s: float = 30.0,
    guard: bool = True,
) -> CampaignSpec:
    """Describe one IMPRESS campaign: one workflow type, budget, and controller.

    Uses `impress_a.integrations.aucam.workflow_config()` so that the spending
    limit and resource numbers come from the IMPRESS offer (the same numbers the
    IMPRESS campaign enforces internally), keeping them consistent.
    """
    try:
        from impress_a.integrations.aucam import workflow_config as impress_workflow_config
        from impress_a.integrations.aucam import reward_fn as impress_reward_fn
        wf_config_kwargs = impress_workflow_config(
            spec_path,
            name="impress",
            model=model,
            report_interval_s=report_interval_s,
            guard=guard,
        )
        wf_config = wf_config_kwargs  # already a WorkflowConfig
        reward_fn = impress_reward_fn
    except ImportError:
        # IMPRESS not installed: fall back to manual config (useful for testing)
        logging.warning(
            "impress_a not importable — falling back to manually configured WorkflowConfig. "
            "Install IMPRESS-A (`pip install -e /path/to/IMPRESS/agent`) for full integration."
        )
        from impress_a.integrations.aucam import make_impress_worker  # will also raise if absent
        wf_config = WorkflowConfig(
            name="impress",
            block=make_impress_worker(spec_path, model=model, guard=guard,
                                      report_interval_s=report_interval_s),
            stale_timeout_s=ccfg.workflows.impress.stale_timeout_s,
            launch_on_start=True,
            spending_limit=ccfg.workflows.impress.spending_limit,
            resources=ccfg.workflows.impress.resources,
        )
        reward_fn = None

    ctrl = ImpressController(
        goal_n=ccfg.controller.goal_n,
        score_floor=ccfg.workflows.impress.score_floor,
        budget_warn_fraction=ccfg.controller.budget_warn_fraction,
    )

    # Bandit with IMPRESS-aware reward (progress fraction) when available,
    # else default throughput reward.
    if scheduling_policy is None:
        policy_kwargs = {}
        if reward_fn is not None:
            policy_kwargs["reward_fn"] = reward_fn
        scheduling_policy = BanditSchedulingPolicy(**policy_kwargs)

    spending: dict[str, float] = {}
    for dim in ("cpu_hours", "gpu_hours"):
        v = ccfg.budget.spending.get(dim)
        if v is not None:
            spending[dim] = v

    return CampaignSpec(
        name=name,
        budget=CampaignBudget(
            max_concurrent_workflows=ccfg.budget.max_concurrent,
            max_concurrency_per_workflow=ccfg.budget.max_concurrency_per_workflow,
            capacity=ccfg.budget.capacity,
            spending=spending if spending else {},
        ),
        controller=ctrl,
        workflows=[wf_config],
        policy_interval_s=ccfg.policy_interval_s,
        shutdown_timeout_s=ccfg.shutdown_timeout_s,
        checkpoint_dir=ccfg.checkpoint_dir,
        scheduling_policy=scheduling_policy,
    )


# ── Execution ──────────────────────────────────────────────────────────────────


async def run(
    gcfg: GlobalConfig,
    ccfg: CampaignConfig,
    spec_path: str,
    args: argparse.Namespace,
) -> None:
    if gcfg.engine == "dragon":
        from rhapsody.backends.execution.dragon import DragonExecutionBackend
        backend = await DragonExecutionBackend()
    elif gcfg.engine == "concurrent":
        from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend
        backend = await ConcurrentExecutionBackend()
    else:
        backend = None

    name = f"impress-{Path(spec_path).stem}"
    spec = build_spec(
        name=name,
        spec_path=spec_path,
        ccfg=ccfg,
        model=args.model,
        report_interval_s=args.report_interval_s,
        guard=not args.no_guard,
    )

    cm = CampaignManager(
        backend=backend,
        enable_telemetry=gcfg.telemetry.enabled,
        telemetry_checkpoint_dir=gcfg.telemetry.dir or None,
    )

    t0 = time.monotonic()
    try:
        outcome = await cm.run(spec)
    finally:
        if backend is not None and hasattr(backend, "shutdown"):
            await backend.shutdown()
    elapsed = time.monotonic() - t0

    _log_outcome(name, outcome, elapsed)


def _log_outcome(name: str, outcome, elapsed: float) -> None:
    n_ok = outcome.completed.get("impress", 0)
    n_cancel = outcome.cancelled.get("impress", 0)
    n_fail = outcome.failed.get("impress", 0)
    n_run = n_ok + n_cancel + n_fail
    cancel_rate = n_cancel / max(1, n_run)

    lines = [
        "",
        "=" * 72,
        f"  CAMPAIGN SUMMARY  {name}",
        "=" * 72,
        f"  exit            : {outcome.exit_reason}",
        f"  goal reached    : {outcome.goal_reached}",
        f"  elapsed         : {elapsed:.1f}s",
        "",
        f"  workflows run   : {n_run}",
        f"    completed     : {n_ok}",
        f"    cancelled     : {n_cancel}  (cancel rate {100*cancel_rate:.0f}%)",
        f"    failed        : {n_fail}",
        f"    budget_cancels: {outcome.budget_cancels}",
        f"    resource_cancels: {outcome.resource_cancels}",
        f"    unfit_dropped : {outcome.unfit_dropped}",
        f"  controller_errors: {outcome.controller_errors}",
    ]

    if outcome.spent:
        lines.append("")
        lines.append("  spent:")
        for k, v in sorted(outcome.spent.items()):
            lines.append(f"    {k:<20}: {v:.6f}")

    if outcome.model_usage:
        lines.append("")
        lines.append("  model_usage:")
        for k, v in sorted(outcome.model_usage.items()):
            lines.append(f"    {k:<20}: {v}")

    s = (outcome.summary or {}).get("impress", {})
    if s:
        lines.append("")
        lines.append("  impress scores:")
        lines.append(f"    n               : {s.get('n', 0)}")
        if s.get("best_progress") is not None:
            lines.append(f"    best_progress   : {s['best_progress']:.3f}")
            lines.append(f"    avg_progress    : {s.get('avg_progress', 0):.3f}")

    lines.append("=" * 72)
    for line in lines:
        logging.info(line)


# ── CLI ────────────────────────────────────────────────────────────────────────


def _build_parser(gcfg: GlobalConfig, ccfg: CampaignConfig) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="IMPRESS AuCaM campaign",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--spec", required=True, help="path to IMPRESS campaign spec YAML")
    p.add_argument("--global-config", default=None)
    p.add_argument("--campaign-config", default=None)
    p.add_argument("-v", "--verbose", action="store_true", default=gcfg.verbose)
    p.add_argument("--engine", choices=["default", "concurrent", "dragon"],
                   default=gcfg.engine)
    p.add_argument("--telemetry-dir", default=gcfg.telemetry.dir,
                   help="enables telemetry JSONL output")
    p.add_argument("--model", default="D",
                   help="IMPRESS policy model letter (D, B, …)")
    p.add_argument("--no-guard", action="store_true", default=False,
                   help="disable IMPRESS gate guards (for testing)")
    p.add_argument("--report-interval-s", type=float, default=30.0,
                   help="how often the IMPRESS workflow reports to AuCaM (seconds)")

    # Controller
    p.add_argument("--goal-n", type=int, default=ccfg.controller.goal_n,
                   help="completions before campaign terminates")
    p.add_argument("--score-floor", type=float, default=ccfg.workflows.impress.score_floor,
                   help="cancel mid-run if progress drops below this (0 = disable)")
    p.add_argument("--max-concurrent", type=int, default=ccfg.budget.max_concurrent)
    p.add_argument("--checkpoint-dir", default=ccfg.checkpoint_dir)

    # Budget overrides
    p.add_argument("--spending-cpu-hours", type=float, default=None,
                   help="campaign-wide CPU-hour spending limit")
    p.add_argument("--spending-gpu-hours", type=float, default=None,
                   help="campaign-wide GPU-hour spending limit")
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
    gcfg.telemetry.dir = args.telemetry_dir
    if args.telemetry_dir:
        gcfg.telemetry.enabled = True

    ccfg.controller.goal_n = args.goal_n
    ccfg.workflows.impress.score_floor = args.score_floor
    ccfg.budget.max_concurrent = args.max_concurrent
    ccfg.checkpoint_dir = args.checkpoint_dir
    if args.spending_cpu_hours is not None:
        ccfg.budget.spending["cpu_hours"] = args.spending_cpu_hours
    if args.spending_gpu_hours is not None:
        ccfg.budget.spending["gpu_hours"] = args.spending_gpu_hours

    log_level = logging.DEBUG if gcfg.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s  %(levelname)-7s  %(name)-30s  %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("src.aucam").setLevel(log_level)
    logging.getLogger("impress_a").setLevel(log_level)

    asyncio.run(run(gcfg, ccfg, args.spec, args))


if __name__ == "__main__":
    main()
