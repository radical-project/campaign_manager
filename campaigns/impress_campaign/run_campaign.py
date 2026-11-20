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

Modes
-----
Single-arm (original):
    python run_campaign.py --spec path/to/spec.yaml

Multi-arm bandit demo (easy / medium / hard):
    python run_campaign.py --multi-arm [--specs-dir specs/]

Reading order: build_spec → build_multi_arm_spec → run → main
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.aucam import (
    BanditSchedulingPolicy,
    CampaignBudget,
    CampaignManager,
    CampaignSpec,
    WorkflowConfig,
)

from config import CampaignConfig, GlobalConfig, load_campaign, load_global
from controller import ImpressController, MultiArmController

logger = logging.getLogger(__name__)

# ── Single-arm spec ────────────────────────────────────────────────────────────


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
    """Describe one IMPRESS campaign: one workflow type, budget, and controller."""
    try:
        from impress_a.integrations.aucam import reward_fn as impress_reward_fn
        from impress_a.integrations.aucam import workflow_config as impress_workflow_config

        wf_config = impress_workflow_config(
            spec_path,
            name="impress",
            model=model,
            report_interval_s=report_interval_s,
            guard=guard,
        )
        reward_fn = impress_reward_fn
    except ImportError:
        logging.warning(
            "impress_a not importable — falling back to manually configured WorkflowConfig."
        )
        from impress_a.integrations.aucam import make_impress_worker

        wf_config = WorkflowConfig(
            name="impress",
            block=make_impress_worker(
                spec_path, model=model, guard=guard, report_interval_s=report_interval_s
            ),
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

    if scheduling_policy is None:
        policy_kwargs = {}
        if reward_fn is not None:
            policy_kwargs["reward_fn"] = reward_fn
        scheduling_policy = BanditSchedulingPolicy(**policy_kwargs)

    spending: dict[str, float] = {
        dim: v for dim in ("cpu_hours", "gpu_hours") if (v := ccfg.budget.spending.get(dim))
    }

    return CampaignSpec(
        name=name,
        budget=CampaignBudget(
            max_concurrent_workflows=ccfg.budget.max_concurrent,
            max_concurrency_per_workflow=ccfg.budget.max_concurrency_per_workflow,
            capacity=ccfg.budget.capacity,
            spending=spending,
        ),
        controller=ctrl,
        workflows=[wf_config],
        policy_interval_s=ccfg.policy_interval_s,
        shutdown_timeout_s=ccfg.shutdown_timeout_s,
        checkpoint_dir=ccfg.checkpoint_dir,
        scheduling_policy=scheduling_policy,
    )


# ── Multi-arm spec ─────────────────────────────────────────────────────────────

_ARM_SPECS = {
    "binding_easy": "mock-binding-easy.yaml",
    "binding_medium": "mock-binding-medium.yaml",
    "binding_hard": "mock-binding-hard.yaml",
}


def build_multi_arm_spec(
    *,
    name: str,
    specs_dir: str,
    ccfg: CampaignConfig,
    model: str = "D",
    report_interval_s: float = 1.0,
    guard: bool = True,
    telemetry_path: str | None = None,
) -> CampaignSpec:
    """Three IMPRESS arms (easy / medium / hard) driven by a shared bandit.

    Each arm is a separate WorkflowConfig backed by its own IMPRESS spec. The
    BanditSchedulingPolicy learns rewards from progress scores and shifts
    allocation toward higher-performing arms over time.
    """
    from impress_a.integrations.aucam import reward_fn as impress_reward_fn
    from impress_a.integrations.aucam import workflow_config as impress_workflow_config

    specs_path = Path(specs_dir)
    wf_configs = []
    for arm, spec_file in _ARM_SPECS.items():
        arm_spec_path = specs_path / spec_file
        arm_ccfg = getattr(ccfg.workflows, arm, None)
        # Pass gpu_hours spending_limit from campaign.yaml so AuCaM holds a small
        # per-launch reservation (0.5 vs 4.0 from spec ceiling) — 3×0.5=1.5 fits within
        # the campaign GPU budget, allowing all 3 arms to run concurrently.
        # Only gpu_hours: cpu_hours for mock tools is ~2µs/run and would trigger spurious
        # budget drops via cost learning (backlog G20).
        overrides: dict = {}
        if arm_ccfg is not None:
            gpu_sl = arm_ccfg.spending_limit.get("gpu_hours")
            if gpu_sl:
                overrides["spending_limit"] = {"gpu_hours": float(gpu_sl)}
            # budget_floor: drop threshold from IMPRESS offer (1 cycle = 0.006 gpu_h).
            # Without learn=False, AuCaM narrows spending_limit to 0.0075 after 3 runs,
            # which triggers the IMPRESS 10%-of-available interlock (0.006 > 10%×0.0075)
            # and then drops all subsequent launches (0.0075 < budget_floor=0.006).
            if arm_ccfg.budget_floor:
                overrides["budget_floor"] = dict(arm_ccfg.budget_floor)
        # Disable cost-learning for multi-arm demo: each arm's cycle count is fixed
        # (easy=1, medium/hard=40), so learned costs would only narrow spending_limit
        # to 0.0075 and cause DROP after 3 easy runs (backlog G20).
        overrides["learn"] = False
        wf_cfg = impress_workflow_config(
            arm_spec_path,
            name=arm,
            model=model,
            report_interval_s=report_interval_s,
            guard=guard,
            **overrides,
        )
        wf_configs.append(wf_cfg)

    ctrl = MultiArmController(
        arms=list(_ARM_SPECS.keys()),
        goal_n=ccfg.controller.goal_n,
        budget_warn_fraction=ccfg.controller.budget_warn_fraction,
        telemetry_path=telemetry_path,
    )

    spending: dict[str, float] = {
        dim: v for dim in ("cpu_hours", "gpu_hours") if (v := ccfg.budget.spending.get(dim))
    }

    return CampaignSpec(
        name=name,
        budget=CampaignBudget(
            max_concurrent_workflows=ccfg.budget.max_concurrent,
            max_concurrency_per_workflow=ccfg.budget.max_concurrency_per_workflow,
            capacity=ccfg.budget.capacity,
            spending=spending,
        ),
        controller=ctrl,
        workflows=wf_configs,
        policy_interval_s=ccfg.policy_interval_s,
        shutdown_timeout_s=ccfg.shutdown_timeout_s,
        checkpoint_dir=ccfg.checkpoint_dir,
        scheduling_policy=BanditSchedulingPolicy(reward_fn=impress_reward_fn),
    )


# ── Execution ──────────────────────────────────────────────────────────────────


async def run(
    gcfg: GlobalConfig,
    ccfg: CampaignConfig,
    args: argparse.Namespace,
) -> None:
    import os
    import tempfile

    from impress_a.integrations.aucam import impress_backends

    if args.multi_arm:
        # Each multi-arm run gets a fresh provenance root.  Without this, IMPRESS loads
        # results.jsonl from a previous run of the same campaign_id, finds metrics that
        # already satisfy the objectives, and stops medium/hard after exactly one cycle.
        fresh_root = tempfile.mkdtemp(prefix="impress_multi_arm_")
        os.environ["IMPRESS_A_RUN_ROOT"] = fresh_root
        logger.info("multi-arm run root: %s", fresh_root)

    if args.multi_arm:
        specs_dir = args.specs_dir or str(Path(__file__).parent / "specs")
        # All three arms use the same backend type; build routes from any spec.
        ref_spec = str(Path(specs_dir) / list(_ARM_SPECS.values())[0])
        name = "impress-multi-arm"
        spec = build_multi_arm_spec(
            name=name,
            specs_dir=specs_dir,
            ccfg=ccfg,
            model=args.model,
            report_interval_s=args.report_interval_s,
            guard=not args.no_guard,
            telemetry_path=args.telemetry_out,
        )
    else:
        ref_spec = args.spec
        name = f"impress-{Path(args.spec).stem}"
        spec = build_spec(
            name=name,
            spec_path=args.spec,
            ccfg=ccfg,
            model=args.model,
            report_interval_s=args.report_interval_s,
            guard=not args.no_guard,
        )

    backends = await impress_backends(ref_spec)

    cm = CampaignManager(
        backend=backends,
        enable_telemetry=gcfg.telemetry.enabled,
        telemetry_checkpoint_dir=gcfg.telemetry.dir or None,
    )

    t0 = time.monotonic()
    try:
        outcome = await cm.run(spec)
    finally:
        for b in backends:
            if hasattr(b, "shutdown"):
                await b.shutdown()
    elapsed = time.monotonic() - t0

    _log_outcome(name, outcome, elapsed, multi_arm=args.multi_arm)


def _log_outcome(name: str, outcome, elapsed: float, *, multi_arm: bool = False) -> None:
    arms = list(_ARM_SPECS.keys()) if multi_arm else ["impress"]

    total_ok = sum(outcome.completed.get(a, 0) for a in arms)
    total_cancel = sum(outcome.cancelled.get(a, 0) for a in arms)
    total_fail = sum(outcome.failed.get(a, 0) for a in arms)
    total_run = total_ok + total_cancel + total_fail
    cancel_rate = total_cancel / max(1, total_run)

    lines = [
        "",
        "=" * 72,
        f"  CAMPAIGN SUMMARY  {name}",
        "=" * 72,
        f"  exit            : {outcome.exit_reason}",
        f"  goal reached    : {outcome.goal_reached}",
        f"  elapsed         : {elapsed:.1f}s",
        "",
        f"  workflows run   : {total_run}",
        f"    completed     : {total_ok}",
        f"    cancelled     : {total_cancel}  (cancel rate {100 * cancel_rate:.0f}%)",
        f"    failed        : {total_fail}",
        f"    budget_cancels: {outcome.budget_cancels}",
        f"    resource_cancels: {outcome.resource_cancels}",
        f"    unfit_dropped : {outcome.unfit_dropped}",
        f"  controller_errors: {outcome.controller_errors}",
    ]

    if multi_arm:
        lines.append("")
        lines.append("  per-arm completions:")
        for arm in arms:
            ok = outcome.completed.get(arm, 0)
            lines.append(f"    {arm:<20}: {ok}")

    if outcome.spent:
        lines.append("")
        lines.append("  spent:")
        for k, v in sorted(outcome.spent.items()):
            lines.append(f"    {k:<20}: {v:.6f}")

    summary = outcome.summary or {}
    for arm in arms:
        s = summary.get(arm, {})
        if s:
            lines.append("")
            lines.append(f"  {arm} scores:")
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

    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--spec", help="single-arm: path to IMPRESS campaign spec YAML")
    mode.add_argument(
        "--multi-arm",
        action="store_true",
        default=False,
        help="multi-arm bandit demo: easy / medium / hard arms",
    )
    p.add_argument(
        "--specs-dir",
        default=None,
        help="directory containing mock-binding-*.yaml specs (default: ./specs/)",
    )
    p.add_argument(
        "--telemetry-out",
        default=None,
        help="write per-event telemetry JSON here (multi-arm mode; used by plot_results.py)",
    )

    p.add_argument("--global-config", default=None)
    p.add_argument("--campaign-config", default=None)
    p.add_argument("-v", "--verbose", action="store_true", default=gcfg.verbose)
    p.add_argument("--engine", choices=["default", "concurrent", "dragon"], default=gcfg.engine)
    p.add_argument(
        "--telemetry-dir", default=gcfg.telemetry.dir, help="enables AuCaM telemetry JSONL"
    )
    p.add_argument("--model", default="D", help="IMPRESS policy model letter (D, B, …)")
    p.add_argument(
        "--no-guard",
        action="store_true",
        default=False,
        help="disable IMPRESS gate guards (for testing)",
    )
    p.add_argument(
        "--report-interval-s",
        type=float,
        default=1.0,
        help="how often each IMPRESS workflow reports to AuCaM (seconds)",
    )

    # Controller
    p.add_argument(
        "--goal-n",
        type=int,
        default=ccfg.controller.goal_n,
        help="completions before campaign terminates",
    )
    p.add_argument(
        "--score-floor",
        type=float,
        default=ccfg.workflows.impress.score_floor,
        help="cancel mid-run if progress drops below this (0 = disable)",
    )
    p.add_argument("--max-concurrent", type=int, default=ccfg.budget.max_concurrent)
    p.add_argument("--checkpoint-dir", default=ccfg.checkpoint_dir)

    # Budget overrides
    p.add_argument(
        "--spending-cpu-hours",
        type=float,
        default=None,
        help="campaign-wide CPU-hour spending limit",
    )
    p.add_argument(
        "--spending-gpu-hours",
        type=float,
        default=None,
        help="campaign-wide GPU-hour spending limit",
    )
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

    asyncio.run(run(gcfg, ccfg, args))


if __name__ == "__main__":
    main()
