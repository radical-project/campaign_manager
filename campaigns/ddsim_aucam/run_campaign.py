#!/usr/bin/env python3
"""
MiniApps AuCaM campaign — parallel exploration of MiniApps pipeline configurations.

Each campaign runs two workflow arms concurrently:
  miniapps_fast — baseline num_step (e.g. 1 000 steps on HPC)
  miniapps_slow — num_step × slow_step_multiplier (e.g. 3 000 steps)

The bandit scheduling policy selects between arms based on observed MSE scores.
The campaign terminates when goal_n total completions across both arms are reached.
The cross-campaign comparison table shows per-arm and overall best/avg MSE.

Score semantics: lower is better (mean per-sim MSE from predictions.yaml).

Usage:
------
    # Local dev — 2 campaigns (fast + slow), 1 at a time:
    python run_campaign.py \\
        --global-config  campaign_configs/global.yaml \\
        --campaign-config campaign_configs/campaign.yaml

    # HPC (Dragon):
    python run_campaign.py \\
        --global-config campaign_configs/global_hpc.yaml \\
        --campaign-config campaign_configs/campaign_hpc.yaml \\
        --telemetry-dir /path/to/tel

    # Quick start (all defaults):
    python run_campaign.py --num-campaigns 2
"""

from __future__ import annotations

import argparse
import asyncio
import json
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
    vary_config,
)
from controller import MiniAppsController

from src.aucam import (
    CampaignBudget,
    CampaignManager,
    CampaignOutcome,
    CampaignSpec,
    GlobalBudget,
    SchedulingPolicy,
    SubmitResult,
    WorkflowConfig,
    WorkflowResult,
)

# ── Results writer ─────────────────────────────────────────────────────────────


class _ResultsWriter:
    """Appends each WorkflowResult to a JSONL file for post-hoc analysis."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._f = open(self._path, "a")

    def write(self, result: WorkflowResult) -> None:
        record = {
            "workflow_id": result.workflow_id,
            "workflow_type": result.workflow_type,
            "returncode": result.returncode,
            "wall_time_s": result.wall_time_s,
            "spent": result.spent,
            "resource_profile": result.resource_profile,
            "final_metrics": result.final_metrics,
        }
        self._f.write(json.dumps(record) + "\n")
        self._f.flush()

    def close(self) -> None:
        self._f.close()


# ── Spec builder ───────────────────────────────────────────────────────────────


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
) -> tuple[CampaignSpec, MiniAppsController]:
    """Build a CampaignSpec and MiniAppsController from a CampaignConfig template."""
    ctrl = MiniAppsController(
        goal_n=ccfg.controller.goal_n,
        score_ceiling=ccfg.controller.score_ceiling,
    )

    def _on_complete(result: WorkflowResult) -> None:
        # ctrl.on_complete is already called by CampaignManager; only do extras here.
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

    # Two arms: fast and slow.  Each gets its own closure-bound worker so
    # concurrent campaigns never share config state.
    fast_cfg = ccfg.workflows.fast_config()
    slow_cfg = ccfg.workflows.slow_config()
    fast_worker = _wf_mod.make_miniapps_worker(fast_cfg.to_dict(), arm_label="fast", engine=engine)
    slow_worker = _wf_mod.make_miniapps_worker(slow_cfg.to_dict(), arm_label="slow", engine=engine)

    stale = ccfg.workflows.miniapps.stale_timeout_s

    spec = CampaignSpec(
        name=name,
        budget=CampaignBudget(
            max_concurrent_workflows=ccfg.budget.max_concurrent,
            max_concurrency_per_workflow=ccfg.budget.max_concurrency_per_workflow,
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
            ),
            WorkflowConfig(
                name="miniapps_slow",
                block=slow_worker,
                stale_timeout_s=stale * ccfg.workflows.slow_step_multiplier,
                launch_on_start=True,
                on_completion=_on_complete,
                declared_spending=ccfg.workflows.resolved_slow_spending(),
            ),
        ],
        policy_interval_s=ccfg.policy_interval_s,
        shutdown_timeout_s=ccfg.shutdown_timeout_s,
        checkpoint_dir=ccfg.checkpoint_dir,
        data_backend=data_backend,
        scheduling_policy=scheduling_policy,
    )
    return spec, ctrl


# ── Backend factories ──────────────────────────────────────────────────────────


async def _make_backend(engine: str):
    if engine == "dragon":
        from rhapsody.backends.execution.dragon import DragonExecutionBackend

        return await DragonExecutionBackend()
    if engine == "concurrent":
        from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend

        return await ConcurrentExecutionBackend()
    return None


async def _make_data_backend(engine: str):
    return None, None


# ── Scheduling policy ──────────────────────────────────────────────────────────


def _make_policy(name: str, ccfg=None) -> SchedulingPolicy | None:
    from src.aucam import BanditSchedulingPolicy, FIFOSchedulingPolicy

    if name == "bandit":
        # Time-efficiency reward: exp(-t / ref_s).
        # ref_s = stale_timeout / slow_multiplier calibrates the decay to the
        # fast arm's actual wall time (stale_timeout is conservative; dividing
        # by the multiplier brings it close to fast's expected duration).
        # exploration=0.5 lets the mean gap overcome the UCB bonus after the
        # mandatory 2 initial pulls, enabling exploitation within goal_n=6.
        stale = ccfg.workflows.miniapps.stale_timeout_s if ccfg is not None else 1800.0
        mult = ccfg.workflows.slow_step_multiplier if ccfg is not None else 3
        ref_s = stale / mult

        def _reward(result) -> float:
            if result.returncode != 0:
                return 0.0
            return math.exp(-result.wall_time_s / ref_s)

        return BanditSchedulingPolicy(reward_fn=_reward, exploration=0.5)
    if name == "fifo":
        return FIFOSchedulingPolicy()
    return None


def _print_policy_summary(policy: SchedulingPolicy | None, name: str) -> None:
    if policy is None:
        return
    if hasattr(policy, "arm_stats"):
        stats = policy.arm_stats()
        if stats:
            print("\n[POLICY] bandit arm statistics:")
            for arm, s in sorted(stats.items()):
                ucb = s.get("ucb", 0)
                ucb_str = "  ∞  " if ucb == float("inf") else f"{ucb:.3f}"
                print(
                    f"   {arm:20s}  n={s.get('n', 0):4d}  μ={s.get('mean', 0):.3f}  ucb={ucb_str}"
                )


# ── Parallel driver ────────────────────────────────────────────────────────────


async def _run_one_campaign(
    sem: asyncio.Semaphore,
    cm: CampaignManager,
    *,
    name: str,
    ccfg: CampaignConfig,
    policy: SchedulingPolicy | None,
    data_backend,
    telemetry_dir: str | None,
    cpu_hours: float | None,
    gpu_hours: float | None,
    engine: str = "concurrent",
) -> tuple[str, CampaignOutcome, CampaignConfig] | None:
    async with sem:
        rw = None
        if telemetry_dir:
            fname = f"results_{name.split('-')[-1]}.jsonl"
            rw = _ResultsWriter(Path(telemetry_dir) / fname)

        spec, _ = build_spec(
            name=name,
            ccfg=ccfg,
            results_writer=rw,
            data_backend=data_backend,
            scheduling_policy=policy,
            cpu_hours=cpu_hours,
            gpu_hours=gpu_hours,
            engine=engine,
        )

        result: SubmitResult = await cm.submit(spec)
        if not result.accepted:
            logging.warning("[DRIVER] %s: rejected — %s", name, result.reason)
            if rw:
                rw.close()
            return None

        logging.info(
            "[DRIVER] %s: accepted (num_step_fast=%d  num_step_slow=%d)",
            name,
            ccfg.workflows.fast_config().num_step,
            ccfg.workflows.slow_config().num_step,
        )
        outcome = await result.wait()
        if rw:
            rw.close()
        return (name, outcome, ccfg)


async def _driver(
    cm: CampaignManager,
    gcfg: GlobalConfig,
    ccfg_template: CampaignConfig,
    *,
    n_campaigns: int,
    policy_name: str,
    policies: list,
    triples: list,
    data_backend,
    telemetry_dir: str | None,
    cpu_hours_list: list[float | None],
    gpu_hours_list: list[float | None],
) -> None:
    max_cc = gcfg.driver.max_concurrent or n_campaigns
    sem = asyncio.Semaphore(max_cc)
    labels = [chr(ord("a") + i) for i in range(n_campaigns)] if n_campaigns > 1 else [""]

    logging.info("[DRIVER] launching %d campaigns (max_concurrent=%d)", n_campaigns, max_cc)

    tasks = []
    for i, label in enumerate(labels):
        name = f"miniapps-campaign-{label}" if label else "miniapps-campaign"
        ccfg = vary_config(ccfg_template, i)
        # Each campaign gets its own scheduling policy (no cross-campaign learning).
        policy = _make_policy(policy_name, ccfg)
        policies.append((name, policy))
        tasks.append(
            _run_one_campaign(
                sem,
                cm,
                name=name,
                ccfg=ccfg,
                policy=policy,
                data_backend=data_backend,
                telemetry_dir=telemetry_dir,
                cpu_hours=cpu_hours_list[i],
                gpu_hours=gpu_hours_list[i],
                engine=gcfg.engine,
            )
        )

    raw = await asyncio.gather(*tasks)
    # Populate triples in-place so the caller can print after Dragon shuts down.
    triples.extend(t for t in raw if t is not None)
    if not triples:
        logging.warning("[DRIVER] all campaigns were rejected or failed")

    cm.close()


def _log_comparison(triples: list[tuple[str, CampaignOutcome, CampaignConfig]]) -> None:
    best_name = None
    best_score = float("inf")

    w = 97
    print("\n" + "─" * w)
    print(
        f"{'Campaign':<26}  {'goal':>5}  "
        f"{'fast n':>6}  {'fast best':>9}  {'fast avg':>9}  "
        f"{'slow n':>6}  {'slow best':>9}  {'slow avg':>9}  "
        f"{'cpu_h':>8}"
    )
    print("─" * w)

    for name, outcome, _ccfg in triples:
        s = outcome.summary or {}
        goal = "YES" if outcome.goal_reached else "no"
        cpu = outcome.spent.get("cpu_hours", 0.0)

        def _fmt(arm: str, field: str, s: dict = s) -> str:
            v = (s.get(arm) or {}).get(field)
            return f"{v:.4f}" if v is not None else "      –"

        fn = (s.get("miniapps_fast") or {}).get("n", 0)
        sn = (s.get("miniapps_slow") or {}).get("n", 0)

        print(
            f"{name:<26}  {goal:>5}  "
            f"{fn:>6}  {_fmt('miniapps_fast', 'best'):>9}  {_fmt('miniapps_fast', 'avg'):>9}  "
            f"{sn:>6}  {_fmt('miniapps_slow', 'best'):>9}  {_fmt('miniapps_slow', 'avg'):>9}  "
            f"{cpu:>8.5f}"
        )

        overall_best = (s.get("overall") or {}).get("best")
        if overall_best is not None and overall_best < best_score:
            best_score = overall_best
            best_name = name

    print("─" * w)
    if best_name:
        print(f"Best campaign: {best_name}  (overall best MSE={best_score:.4f})")
    print()


# ── CSV helper ────────────────────────────────────────────────────────────────


def _parse_csv_floats(val: str | None, n: int) -> list[float | None]:
    if val is None:
        return [None] * n
    parts = [float(x.strip()) if x.strip() else None for x in str(val).split(",")]
    if len(parts) == 1:
        return parts * n
    return (parts + [None] * n)[:n]


# ── Main run ──────────────────────────────────────────────────────────────────


async def run(gcfg: GlobalConfig, ccfg: CampaignConfig, args: argparse.Namespace) -> None:
    n = gcfg.driver.num_campaigns
    cpu_hours_list = _parse_csv_floats(args.spending_cpu_hours, n)
    gpu_hours_list = _parse_csv_floats(args.spending_gpu_hours, n)

    policies: list = []  # (campaign name, its own scheduling policy)
    backend = await _make_backend(gcfg.engine)
    data_backend, _rdb = await _make_data_backend(gcfg.engine)

    global_budget: GlobalBudget | None = None
    gb = gcfg.global_budget
    if (
        gb.max_total_concurrent_workflows is not None
        or gb.spending
        or gb.n_campaigns is not None
        or gb.max_rejections is not None
    ):
        global_budget = GlobalBudget(
            max_concurrent_campaigns=None,
            max_total_concurrent_workflows=gb.max_total_concurrent_workflows,
            spending=gb.spending,
            n_campaigns=gb.n_campaigns,
            max_rejections=gb.max_rejections,
        )

    cm = CampaignManager(
        backend=backend,
        global_budget=global_budget,
        enable_telemetry=gcfg.telemetry.enabled,
        telemetry_checkpoint_dir=gcfg.telemetry.dir,
    )

    t0 = time.monotonic()
    triples: list = []

    cm.expect_driver()
    asyncio.create_task(
        _driver(
            cm,
            gcfg,
            ccfg,
            n_campaigns=n,
            policy_name=gcfg.scheduling.policy,
            policies=policies,
            triples=triples,
            data_backend=data_backend,
            telemetry_dir=gcfg.telemetry.dir,
            cpu_hours_list=cpu_hours_list,
            gpu_hours_list=gpu_hours_list,
        )
    )

    try:
        await cm.run()
    finally:
        if _rdb is not None:
            await _rdb.shutdown()

    elapsed = time.monotonic() - t0

    # Print after cm.run() returns (Dragon/asyncflow fully shut down) so stdout
    # is no longer intercepted and box-drawing characters render correctly.
    if triples:
        _log_comparison(triples)
    for cname, cpolicy in policies:
        print(f"\n[POLICY] {cname}:")
        _print_policy_summary(cpolicy, gcfg.scheduling.policy)
    print(f"\nTotal elapsed: {elapsed:.1f}s", flush=True)


# ── Argument parser ───────────────────────────────────────────────────────────


def _build_parser(gcfg: GlobalConfig, ccfg: CampaignConfig) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="MiniApps AuCaM campaign — parallel variant exploration",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    p.add_argument("--global-config", default=None, help="path to global YAML config")
    p.add_argument("--campaign-config", default=None, help="path to campaign YAML config")

    p.add_argument("-v", "--verbose", action="store_true", default=gcfg.verbose)
    p.add_argument("--engine", choices=["default", "concurrent", "dragon"], default=gcfg.engine)
    p.add_argument(
        "--telemetry-dir", default=gcfg.telemetry.dir, help="directory for telemetry JSONL"
    )
    p.add_argument(
        "--num-campaigns",
        type=int,
        default=gcfg.driver.num_campaigns,
        help="total number of campaigns to run",
    )
    p.add_argument(
        "--max-concurrent-campaigns",
        type=int,
        default=gcfg.driver.max_concurrent,
        help="max campaigns running at the same time",
    )
    p.add_argument("--policy", choices=["bandit", "fifo"], default=gcfg.scheduling.policy)

    # Controller / budget
    p.add_argument(
        "--goal-n",
        type=int,
        default=ccfg.controller.goal_n,
        help="miniapps completions to reach before campaign terminates",
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
        help="max concurrent miniapps replicas per campaign",
    )
    p.add_argument("--checkpoint-dir", default=ccfg.checkpoint_dir)

    # MiniApps workflow parameters
    p.add_argument(
        "--total-num-sim",
        type=int,
        default=ccfg.workflows.miniapps.total_num_sim,
        help="total simulations per miniapps replica",
    )
    p.add_argument(
        "--num-step",
        type=int,
        default=ccfg.workflows.miniapps.num_step,
        help="simulation timesteps per run (fast variant baseline)",
    )
    p.add_argument(
        "--num-epochs",
        type=int,
        default=ccfg.workflows.miniapps.num_epochs,
        help="training epochs per cycle",
    )

    # Per-campaign spending
    p.add_argument(
        "--spending-cpu-hours",
        type=str,
        default=None,
        help="CPU-hours cap per campaign, comma-separated",
    )
    p.add_argument(
        "--spending-gpu-hours",
        type=str,
        default=None,
        help="GPU-hours cap per campaign, comma-separated",
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

    parser = _build_parser(gcfg, ccfg)
    args = parser.parse_args()

    gcfg.engine = args.engine if args.engine != "default" else gcfg.engine
    gcfg.verbose = args.verbose
    gcfg.scheduling.policy = args.policy
    gcfg.telemetry.dir = args.telemetry_dir
    gcfg.driver.num_campaigns = args.num_campaigns
    gcfg.driver.max_concurrent = args.max_concurrent_campaigns

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
