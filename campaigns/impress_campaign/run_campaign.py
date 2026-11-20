#!/usr/bin/env python3
"""IMPRESS AuCaM campaign — AuCaM steering IMPRESS-A protein-binding design.

Each AuCaM workflow is a complete IMPRESS-A design run (plan -> score ->
Pareto front).  AuCaM gives each run a budget and resources, scores it on the
`progress` IMPRESS reports (the fraction of the requested front that is
satisfied, 0-1), cancels runs that are weak or expensive, and decides which type
to launch next.

    python run_campaign.py --spec mock-binding-tractable.yaml   # one type
    python run_campaign.py --multi-arm                                # three, steered

Reading order: build_spec -> build_multi_arm_spec -> run -> main
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Before anything configures logging: rhapsody's backends import RADICAL-Pilot when
# it is installed, and radical.utils then makes logging's locks safe across fork().
# Handlers created before that keep unprotected locks, which is what it warns about.
import rhapsody.backends  # noqa: F401, E402

from src.aucam import (
    BanditSchedulingPolicy,
    CampaignBudget,
    CampaignManager,
    CampaignSpec,
)

from config import CampaignConfig, GlobalConfig, load_campaign, load_global
from controller import ImpressController, MultiArmController
from scheduler import RecordingPolicy, make_policy

logger = logging.getLogger(__name__)

# ── One workflow type ──────────────────────────────────────────────────────────


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
    """An AuCaM campaign of one workflow type: budget, controller, policy."""
    from impress_a.integrations.aucam import reward_fn, workflow_config

    wf_config = workflow_config(
        spec_path,
        name="impress",
        model=model,
        report_interval_s=report_interval_s,
        guard=guard,
    )

    ctrl = ImpressController(
        goal_n=ccfg.controller.goal_n,
        score_floor=ccfg.workflows.impress.score_floor,
        budget_warn_fraction=ccfg.controller.budget_warn_fraction,
    )

    if scheduling_policy is None:
        scheduling_policy = BanditSchedulingPolicy(reward_fn=reward_fn)

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


# ── Three workflow types ───────────────────────────────────────────────────────

_WF_SPECS = {
    "binding_tractable": "mock-binding-tractable.yaml",
    "binding_borderline": "mock-binding-borderline.yaml",
    "binding_refractory": "mock-binding-refractory.yaml",
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
    policy_name: str = "bandit",
    score_floor: float = 0.0,
    cost_cancel: float = 0.0,
) -> CampaignSpec:
    """Three IMPRESS workflow types (tractable / borderline / refractory).

    Each is a separate WorkflowConfig backed by its own IMPRESS spec. The
    scheduling policy learns rewards from the progress scores they report and
    shifts allocation toward the types that pay — which is why it calls them
    arms.
    """
    from impress_a.integrations.aucam import reward_fn as impress_reward_fn
    from impress_a.integrations.aucam import workflow_config as impress_workflow_config

    specs_path = Path(specs_dir)
    wf_configs = []
    for wf_type, spec_file in _WF_SPECS.items():
        type_cfg = getattr(ccfg.workflows, wf_type, None)
        overrides: dict = {}
        if type_cfg is not None:
            # A small per-launch reservation (0.5 gpu_hours) instead of the spec's own
            # 4.0 ceiling, so all three types fit in the campaign budget at once.
            if gpu_sl := type_cfg.spending_limit.get("gpu_hours"):
                overrides["spending_limit"] = {"gpu_hours": float(gpu_sl)}
            if type_cfg.budget_floor:
                overrides["budget_floor"] = dict(type_cfg.budget_floor)
        # Cost learning off: each type's cycle count is fixed, so a learned cost would
        # only narrow the limit until launches were dropped for being under the floor.
        overrides["learn"] = False
        wf_configs.append(
            impress_workflow_config(
                specs_path / spec_file,
                name=wf_type,
                model=model,
                report_interval_s=report_interval_s,
                guard=guard,
                **overrides,
            )
        )

    ctrl = MultiArmController(
        workflow_types=list(_WF_SPECS),
        goal_n=ccfg.controller.goal_n,
        budget_warn_fraction=ccfg.controller.budget_warn_fraction,
        telemetry_path=telemetry_path,
        in_flight_per_type=ccfg.budget.max_concurrency_per_workflow,
        score_floor=score_floor,
        cost_cancel={"gpu_hours": cost_cancel} if cost_cancel else None,
    )

    spending: dict[str, float] = {
        dim: v for dim in ("cpu_hours", "gpu_hours") if (v := ccfg.budget.spending.get(dim))
    }

    budget = ccfg.budget
    return CampaignSpec(
        name=name,
        budget=CampaignBudget(
            max_concurrent_workflows=budget.max_concurrent,
            max_concurrency_per_workflow=budget.max_concurrency_per_workflow,
            capacity=budget.capacity,
            # With wall_hours set, AuCaM works the limit out as capacity x
            # wall_hours and keeps whatever `spending` states (see the guide).
            spending=spending,
            wall_hours=budget.wall_hours,
            on_limit=budget.on_limit,
            target_utilization=budget.target_utilization,
            max_waiting_tasks=budget.max_waiting_tasks,
            overcommit=budget.overcommit,
        ),
        controller=ctrl,
        workflows=wf_configs,
        policy_interval_s=ccfg.policy_interval_s,
        shutdown_timeout_s=ccfg.shutdown_timeout_s,
        checkpoint_dir=ccfg.checkpoint_dir,
        # Wrapped so the figures can show WHICH type the scheduler picked and when —
        # the launch order and the exploration behind it live only in the scheduler.
        # The baseline policies are wrapped too, or an A/B would compare a recorded
        # run against an empty one.
        scheduling_policy=RecordingPolicy(
            make_policy(policy_name, reward_fn=impress_reward_fn),
            ctrl.record,
        ),
    )


def _resolve_spec(spec: str) -> str:
    """A path to a spec file, or the name of one impress_a ships.

    The specs live in the IMPRESS checkout, so a path to one is long and
    machine-dependent; a bare `mock-binding-tractable.yaml` is what a reader
    can type.
    """
    if Path(spec).is_file():
        return spec
    candidate = Path(_impress_specs_dir()) / Path(spec).name
    if candidate.is_file():
        return str(candidate)
    raise SystemExit(
        f"no spec at {spec!r}, and IMPRESS has none by that name. "
        f"It has: {', '.join(sorted(f.name for f in Path(_impress_specs_dir()).glob('*.yaml')))}"
    )


def _impress_specs_dir() -> str:
    """Where the specs live: `agent/campaigns/` in the IMPRESS checkout.

    They are IMPRESS's file format and IMPRESS owns what the knobs in them mean,
    so they are kept in that repository rather than copied here.  Found, in
    order, through `--specs-dir` (handled by the caller), `$IMPRESS_DIR`, and
    the checkout behind an editable `impress_a` install.
    """
    if impress_dir := os.environ.get("IMPRESS_DIR"):
        path = Path(impress_dir) / "agent" / "campaigns"
    else:
        import impress_a  # noqa: PLC0415 - only to locate an editable checkout

        # <checkout>/agent/src/impress_a/__init__.py -> <checkout>/agent/campaigns
        path = Path(impress_a.__file__).resolve().parents[2] / "campaigns"
    if not path.is_dir():
        raise SystemExit(
            f"no IMPRESS specs at {path}.\n"
            "Set IMPRESS_DIR to your IMPRESS checkout, or point --specs-dir at a "
            "directory holding " + ", ".join(_WF_SPECS.values())
        )
    return str(path)


# ── Execution ──────────────────────────────────────────────────────────────────


async def run(
    gcfg: GlobalConfig,
    ccfg: CampaignConfig,
    args: argparse.Namespace,
) -> None:
    import tempfile

    try:
        from impress_a.integrations.aucam import impress_backends
    except ModuleNotFoundError as exc:  # pragma: no cover - environment, not logic
        if exc.name != "impress_a":
            raise
        raise SystemExit(
            "impress_a is not installed in this environment.\n"
            "This campaign runs IMPRESS-A workflows, so both packages must be\n"
            "installed in the environment you launch it from:\n"
            "    pip install -e /path/to/campaign_manager   # aucam\n"
            "    pip install -e /path/to/IMPRESS/agent      # impress_a"
        ) from exc

    if args.multi_arm:
        # A fresh provenance root per run: otherwise IMPRESS reloads the previous
        # run's results.jsonl, finds the objectives already satisfied, and stops.
        os.environ["IMPRESS_A_RUN_ROOT"] = tempfile.mkdtemp(prefix="impress_multi_arm_")

        specs_dir = args.specs_dir or _impress_specs_dir()
        # All three types use the same backend; build routes from any spec.
        ref_spec = str(Path(specs_dir) / next(iter(_WF_SPECS.values())))
        name = "impress-multi-arm"
        spec = build_multi_arm_spec(
            name=name,
            specs_dir=specs_dir,
            ccfg=ccfg,
            model=args.model,
            report_interval_s=args.report_interval_s,
            guard=not args.no_guard,
            telemetry_path=args.telemetry_out,
            policy_name=args.policy,
            score_floor=args.score_floor,
            cost_cancel=args.cost_cancel,
        )
    else:
        ref_spec = _resolve_spec(args.spec)
        name = f"impress-{Path(ref_spec).stem}"
        spec = build_spec(
            name=name,
            spec_path=ref_spec,
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
    """One block per campaign: how it ended, what it ran, what it spent, per type."""
    wf_types = list(_WF_SPECS) if multi_arm else ["impress"]
    summary = outcome.summary or {}

    lines = [
        "",
        "=" * 72,
        f"  CAMPAIGN SUMMARY  {name}",
        "=" * 72,
        f"  exit         : {outcome.exit_reason}  (goal reached: {outcome.goal_reached})",
        f"  elapsed      : {elapsed:.1f}s",
        "  spent        : " + "  ".join(f"{k}={v:.4f}" for k, v in sorted(outcome.spent.items())),
        "",
    ]
    for wf_type in wf_types:
        done = outcome.completed.get(wf_type, 0)
        cancelled = outcome.cancelled.get(wf_type, 0)
        failed = outcome.failed.get(wf_type, 0)
        scores = summary.get(wf_type, {})
        best = scores.get("best_progress")
        avg = scores.get("avg_progress")
        score_text = (
            f"  best={best:.3f} avg={avg:.3f}" if best is not None and avg is not None else ""
        )
        lines.append(
            f"  {wf_type:<20} completed={done} cancelled={cancelled} failed={failed}{score_text}"
        )
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
    mode.add_argument(
        "--spec",
        help="one workflow type: a spec file, or the name of one impress_a ships",
    )
    mode.add_argument(
        "--multi-arm",
        action="store_true",
        default=False,
        help="three workflow types steered by the policy: tractable / borderline / refractory",
    )
    p.add_argument(
        "--specs-dir",
        default=None,
        help="directory holding the mock-binding-*.yaml specs (default: $IMPRESS_DIR/agent/campaigns)",
    )
    p.add_argument(
        "--telemetry-out",
        default=None,
        help="write per-event telemetry JSON here (--multi-arm; read by plot_results.py)",
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

    p.add_argument(
        "--policy",
        choices=["bandit", "throughput", "fifo", "even"],
        default=gcfg.scheduling.policy,
        help="how the queue is ordered and sized (see scheduler.py)",
    )
    p.add_argument(
        "--cost-cancel",
        type=float,
        default=0.0,
        help="cancel a run that spends this many gpu_hours without improving",
    )

    # Budget overrides
    p.add_argument(
        "--wall-hours",
        type=float,
        default=ccfg.budget.wall_hours,
        help="the allocation's wall time: the budget becomes capacity x wall_hours",
    )
    p.add_argument(
        "--on-limit",
        choices=["cancel", "finish"],
        default=ccfg.budget.on_limit,
        help="at the spending limit: cancel running workflows, or let them finish",
    )
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
    ccfg.budget.wall_hours = args.wall_hours
    if args.wall_hours:
        # Asked on the command line, the allocation IS the budget: drop the
        # figures campaign.yaml states, which AuCaM would otherwise keep (a
        # stated dimension caps that one tighter than the allocation).
        ccfg.budget.spending = {}
    ccfg.budget.on_limit = args.on_limit
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
