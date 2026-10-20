#!/usr/bin/env python3
"""
Dummy AuCaM campaign — sim → analysis pipeline.

Three workflow types:
  sim_worker      — synthetic simulation that emits a score each step.
                    Diverging runs (score < score_floor) are cancelled.
  sim_worker_b    — faster, more reliable sim (~90% convergence, N_ITERS/3 steps).
                    The bandit learns to prefer this arm's higher throughput.
  analysis_worker — triggered via DAG dependency after each successful sim.

Campaign terminates when goal_analyses analyses have completed.

Config-driven usage
-------------------
    # Local — load from config files (all settings in YAML):
    python run_campaign.py \\
        --global-config  campaign_configs/global.yaml \\
        --campaign-config campaign_configs/campaign.yaml

    # HPC (Dragon) — config files + runtime overrides:
    python run_campaign.py \\
        --global-config  campaign_configs/global_hpc.yaml \\
        --campaign-config campaign_configs/campaign_hpc.yaml \\
        --telemetry-dir /path/to/tel

    # Driver mode — campaigns submitted dynamically; each outcome informs the next:
    python run_campaign.py \\
        --global-config campaign_configs/global.yaml \\
        --campaign-config campaign_configs/campaign.yaml \\
        --driver --num-campaigns 3 --driver-delay 1.0 \\
        --max-concurrent-campaigns 2

Legacy / quick-start usage (no config files — defaults match campaign_configs/global.yaml)
----------------------------------------------------------------------------------
    python run_campaign.py --policy bandit
    python run_campaign.py --policy adr --num-campaigns 2
    python run_campaign.py --spending-cpu-hours 0.01
    python run_campaign.py -v
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

from src.aucam import (
    CampaignBudget,
    CampaignManager,
    CampaignOutcome,
    CampaignSpec,
    RadexDragonBackend,
    RhapsodyRedisBackend,
    GlobalBudget,
    SchedulingPolicy,
    SubmitResult,
    WorkflowConfig,
    WorkflowDependency,
    WorkflowResult,
)

from config import (
    CampaignConfig,
    GlobalConfig,
    load_campaign,
    load_global,
)
from controller import ScoreController
from scheduler import make_policy, print_policy_summary
import workflows as _workflows_mod
from workflows import analysis_worker, sim_worker, sim_worker_b


# ── Results writer ─────────────────────────────────────────────────────────────

class _ResultsWriter:
    """Appends each WorkflowResult to a JSONL file for post-hoc analysis."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._f = open(self._path, "a")

    def write(self, result: WorkflowResult) -> None:
        record = {
            "workflow_id":      result.workflow_id,
            "workflow_type":    result.workflow_type,
            "returncode":       result.returncode,
            "wall_time_s":      result.wall_time_s,
            "resource_profile": result.resource_profile,
            "final_metrics":    result.final_metrics,
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
    results_writer: "_ResultsWriter | None" = None,
    use_asyncflow_blocks: bool = False,
    data_backend=None,
    scheduling_policy: SchedulingPolicy | None = None,
    use_tier2: bool = False,
) -> tuple[CampaignSpec, ScoreController]:
    """Build a CampaignSpec and its ScoreController from a CampaignConfig template."""
    ctrl = ScoreController(goal_analyses=ccfg.controller.goal_analyses, use_tier2=use_tier2)

    def _on_complete(result: WorkflowResult) -> None:
        # ctrl.on_complete is already called by CampaignManager; only do extras here.
        if results_writer is not None:
            results_writer.write(result)

    spending: dict[str, float] = {}
    if ccfg.budget.cpu_hours is not None:
        spending["cpu_hours"] = ccfg.budget.cpu_hours
    if ccfg.budget.gpu_hours is not None:
        spending["gpu_hours"] = ccfg.budget.gpu_hours

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
                name="sim_worker",
                block=sim_worker,
                stale_timeout_s=ccfg.workflows.sim_worker.stale_timeout_s,
                launch_on_start=True,
                on_completion=_on_complete,
                declared_spending=ccfg.workflows.sim_worker.declared_spending,
            ),
            WorkflowConfig(
                name="sim_worker_b",
                block=sim_worker_b,
                stale_timeout_s=ccfg.workflows.sim_worker_b.stale_timeout_s,
                launch_on_start=True,
                on_completion=_on_complete,
                declared_spending=ccfg.workflows.sim_worker_b.declared_spending,
            ),
            WorkflowConfig(
                name="analysis_worker",
                block=analysis_worker,
                launch_on_start=False,
                on_completion=_on_complete,
                declared_spending=ccfg.workflows.analysis_worker.declared_spending,
            ),
            *(
                [WorkflowConfig(
                    name="tier2_sim_worker",
                    launch_cmd=[sys.executable, str(Path(__file__).parent / "tier2_sim_worker.py")],
                    env={
                        "CM_T2_N_ITERS": str(ccfg.workflows.sim_worker.n_iters),
                        "CM_T2_SLEEP_S":  str(ccfg.workflows.sim_worker.iter_sleep_s),
                    },
                    stale_timeout_s=ccfg.workflows.sim_worker.stale_timeout_s,
                    launch_on_start=True,
                    on_completion=_on_complete,
                    declared_spending=ccfg.workflows.sim_worker.declared_spending,
                )]
                if use_tier2 else []
            ),
        ],
        dependencies=[
            WorkflowDependency(
                source="sim_worker",
                target="analysis_worker",
                condition=lambda r: r.returncode == 0,
            ),
            WorkflowDependency(
                source="sim_worker_b",
                target="analysis_worker",
                condition=lambda r: r.returncode == 0,
            ),
            *(
                [WorkflowDependency(
                    source="tier2_sim_worker",
                    target="analysis_worker",
                    condition=lambda r: r.returncode == 0,
                )]
                if use_tier2 else []
            ),
        ],
        policy_interval_s=ccfg.policy_interval_s,
        shutdown_timeout_s=ccfg.shutdown_timeout_s,
        checkpoint_dir=ccfg.checkpoint_dir,
        use_asyncflow_blocks=use_asyncflow_blocks,
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
    return None  # CampaignManager default


async def _make_data_backend(engine: str, data_backend_type: str = "auto", redis_server_path: str | None = None):
    resolved = data_backend_type if data_backend_type != "auto" else engine

    if resolved == "dragon":
        print("[TRACE] _make_data_backend: starting DDict via Rhapsody DragonDataBackend", flush=True)
        from rhapsody.backends.data import DragonDataBackend as _RhapsodyDDB
        rdb = await _RhapsodyDDB(managers_per_node=1, n_nodes=1, total_mem=256 * 1024 * 1024)
        descriptor = rdb.endpoints[0].serialize()
        print("[TRACE] _make_data_backend: RadexDragonBackend ready", flush=True)
        return RadexDragonBackend(descriptor), rdb

    if resolved == "redis":
        print("[TRACE] _make_data_backend: starting Redis via Rhapsody RedisDataBackend", flush=True)
        from rhapsody.backends.data import RedisDataBackend as _RhapsodyRDB
        kwargs = {}
        if redis_server_path:
            kwargs["redis_server_path"] = redis_server_path
        rdb = await _RhapsodyRDB(**kwargs)
        descriptor = rdb.endpoints[0].serialize()
        print(f"[TRACE] _make_data_backend: RhapsodyRedisBackend ready at {descriptor}", flush=True)
        return RhapsodyRedisBackend(descriptor), rdb

    return None, None


# ── Driver ────────────────────────────────────────────────────────────────────

async def _driver(
    cm: CampaignManager,
    gcfg: GlobalConfig,
    ccfg: CampaignConfig,
    *,
    n_campaigns: int,
    policy_name: str,
    policies: list,
    use_asyncflow_blocks: bool,
    data_backend,
    telemetry_dir: str | None,
) -> None:
    """
    Submit campaigns one-by-one using the dynamic cm.submit() API.
    Each campaign's CampaignOutcome is awaited before the next submission,
    so the driver can inspect results and adapt subsequent campaign parameters.

    In this demo the adaptation is logged — real science drivers would update
    ccfg fields (e.g. increase goal_analyses if cancel rate is high, or adjust
    score_floor based on observed score distributions).
    """
    labels = [chr(ord("a") + i) for i in range(n_campaigns)] if n_campaigns > 1 else [""]
    prev_outcome: CampaignOutcome | None = None

    for i, label in enumerate(labels):
        name = f"demo-campaign-{label}" if label else "demo-campaign"

        if prev_outcome is not None:
            # ── Adapt based on previous outcome ──────────────────────────────
            sims_ok = (
                prev_outcome.completed.get("sim_worker", 0)
                + prev_outcome.completed.get("sim_worker_b", 0)
            )
            sims_can = (
                prev_outcome.cancelled.get("sim_worker", 0)
                + prev_outcome.cancelled.get("sim_worker_b", 0)
            )
            cancel_rate = sims_can / max(1, sims_ok + sims_can)
            logging.info(
                "[DRIVER] %s → planning %s | prev: goal_reached=%s  "
                "cancel_rate=%.0f%%  analyses=%d  elapsed=%.1fs",
                prev_outcome.campaign_name, name,
                prev_outcome.goal_reached,
                cancel_rate * 100,
                prev_outcome.completed.get("analysis_worker", 0),
                prev_outcome.elapsed_s,
            )
            # Example adaptation: if cancel rate was high, future campaigns
            # could tighten the score floor or reduce max_concurrent.
            # Here we just log; real drivers implement domain-specific logic.
            if gcfg.driver.delay_s > 0:
                await asyncio.sleep(gcfg.driver.delay_s)

        rw = None
        if telemetry_dir:
            fname = f"results_{label}.jsonl" if label else "results.jsonl"
            rw = _ResultsWriter(Path(telemetry_dir) / fname)

        # Each campaign gets its own scheduling policy (no cross-campaign learning).
        policy = make_policy(policy_name)
        policies.append((name, policy))
        spec, ctrl = build_spec(
            name=name,
            ccfg=ccfg,
            results_writer=rw,
            use_asyncflow_blocks=use_asyncflow_blocks,
            data_backend=data_backend,
            scheduling_policy=policy,
            use_tier2=data_backend is not None,
        )

        result: SubmitResult = await cm.submit(spec)
        if not result.accepted:
            logging.warning("[DRIVER] %s: rejected — %s", name, result.reason)
            if rw:
                rw.close()
            continue

        logging.info("[DRIVER] %s: accepted", name)

        # Wait for the campaign to finish before proceeding.
        prev_outcome = await result.wait()
        if rw:
            rw.close()

        # Log actual spending from this campaign.
        if prev_outcome.spent:
            spent_str = "  ".join(
                f"{k}={v:.5f}" for k, v in sorted(prev_outcome.spent.items())
            )
            logging.info("[DRIVER] %s: actual spent: %s", prev_outcome.campaign_name, spent_str)

        # Log scientific summary so driver decisions are traceable.
        if prev_outcome.summary:
            best  = prev_outcome.summary.get("best_score")
            avg   = prev_outcome.summary.get("avg_score")
            n_s   = prev_outcome.summary.get("n_scores")
            logging.info(
                "[DRIVER] %s: science summary — n_scores=%s  best=%.3f  avg=%.3f",
                prev_outcome.campaign_name,
                n_s,
                best if best is not None else float("nan"),
                avg  if avg  is not None else float("nan"),
            )


# ── Main run ──────────────────────────────────────────────────────────────────

async def run(gcfg: GlobalConfig, ccfg: CampaignConfig, args: argparse.Namespace) -> None:
    print("[TRACE] run() started — Python is alive", flush=True)
    n      = gcfg.num_campaigns
    labels = [chr(ord("a") + i) for i in range(n)] if n > 1 else [""]

    # Each campaign gets its own scheduling policy instance.
    policies: list = []          # (campaign name, policy)
    print(f"[TRACE] making execution backend (engine={gcfg.engine})", flush=True)
    backend       = await _make_backend(gcfg.engine)
    print("[TRACE] execution backend ready, making data backend", flush=True)
    data_backend, _rdb = await _make_data_backend(gcfg.engine, gcfg.data_backend, gcfg.redis_server_path)

    # Global budget — only meaningful with multiple campaigns.
    global_budget: GlobalBudget | None = None
    gb = gcfg.global_budget
    if (
        gb.max_concurrent_campaigns is not None
        or gb.max_total_concurrent_workflows is not None
        or gb.spending
        or gb.n_campaigns is not None
        or gb.max_rejections is not None
    ):
        global_budget = GlobalBudget(
            max_concurrent_campaigns=gb.max_concurrent_campaigns,
            max_total_concurrent_workflows=gb.max_total_concurrent_workflows,
            spending=gb.spending,
            n_campaigns=gb.n_campaigns,
            max_rejections=gb.max_rejections,
        )

    print("[TRACE] creating CampaignManager", flush=True)
    cm = CampaignManager(
        backend=backend,
        global_budget=global_budget,
        enable_telemetry=gcfg.asyncflow_blocks,
        telemetry_checkpoint_dir=gcfg.telemetry.dir,
    )
    print("[TRACE] CampaignManager created", flush=True)

    t0 = time.monotonic()

    if gcfg.driver.enabled:
        # ── Driver mode: dynamic submission ───────────────────────────────────
        # Declare driver intent before cm.run() checks _has_driver; the task
        # hasn't executed yet so submit() hasn't set the flag itself.
        cm.expect_driver()
        driver_task = asyncio.ensure_future(
            _driver(
                cm, gcfg, ccfg,
                n_campaigns=n,
                policy_name=gcfg.scheduling.policy,
                policies=policies,
                use_asyncflow_blocks=gcfg.asyncflow_blocks,
                data_backend=data_backend,
                telemetry_dir=gcfg.telemetry.dir,
            )
        )
        # cm.close() must run after the driver finishes, but concurrently with
        # cm.run() — the driver awaits campaign outcomes, which requires cm.run()
        # to be processing. Schedule a closer coroutine that waits for the driver
        # then signals the CM to shut down.
        async def _close_when_done():
            try:
                await driver_task
            except Exception as exc:
                logging.exception("[CM] _driver raised: %s", exc)
            finally:
                cm.driver_done()
        asyncio.ensure_future(_close_when_done())
    else:
        # ── Legacy mode: submit all upfront ───────────────────────────────────
        writers: list["_ResultsWriter | None"] = []
        for i, label in enumerate(labels):
            name = f"demo-campaign-{label}" if label else "demo-campaign"
            rw = None
            if gcfg.telemetry.dir:
                fname = f"results_{label}.jsonl" if label else "results.jsonl"
                rw = _ResultsWriter(Path(gcfg.telemetry.dir) / fname)
            writers.append(rw)

            policy = make_policy(gcfg.scheduling.policy)
            policies.append((name, policy))
            spec, _ = build_spec(
                name=name,
                ccfg=ccfg,
                results_writer=rw,
                use_asyncflow_blocks=gcfg.asyncflow_blocks,
                data_backend=data_backend,
                scheduling_policy=policy,
                use_tier2=data_backend is not None,
            )
            cm.submit_campaign(spec)
            logging.info("[DRIVER] submitted '%s'", name)

    print("[TRACE] calling cm.run()", flush=True)
    await cm.run()
    print("[TRACE] cm.run() returned", flush=True)
    elapsed = time.monotonic() - t0

    # ── Summary ───────────────────────────────────────────────────────────────
    # In legacy mode the controller objects live in the specs; in driver mode
    # the driver already logged outcomes via result.wait().
    if not gcfg.driver.enabled:
        for i, label in enumerate(labels):
            name = f"demo-campaign-{label}" if label else "demo-campaign"
            rw = writers[i]
            if rw is not None:
                rw.close()

        # Re-derive summary from spec controllers (legacy only; driver logs its own).
        # We don't have direct access to ctrls here in legacy mode — they're embedded
        # in the specs which live inside _CampaignInstance after run().
        # The summary below relies on CampaignManager not exposing controllers yet;
        # see the driver path for the outcome-driven summary.
        pass

    for cname, cpolicy in policies:
        print(f"\n  campaign {cname}:")
        print_policy_summary(cpolicy, gcfg.scheduling.policy)
    print(f"\nTotal elapsed: {elapsed:.1f}s", flush=True)

    if _rdb is not None:
        if hasattr(data_backend, "close"):
            data_backend.close()
        print("[TRACE] shutting down data backend", flush=True)
        await _rdb.shutdown()
        print("[TRACE] data backend shutdown complete", flush=True)
    if backend is not None and hasattr(backend, "shutdown"):
        print("[TRACE] shutting down exec backend", flush=True)
        await backend.shutdown()
        print("[TRACE] exec backend shutdown complete", flush=True)

    # Cancel any background asyncio tasks left by Dragon/CM so that
    # asyncio.run() can close the event loop without hanging.
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    if pending:
        print(f"[TRACE] cancelling {len(pending)} remaining asyncio tasks", flush=True)
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
    print("[TRACE] run() complete", flush=True)


# ── Argument parser ───────────────────────────────────────────────────────────

def _build_parser(gcfg: GlobalConfig, ccfg: CampaignConfig) -> argparse.ArgumentParser:
    """Build an argparse parser whose defaults come from the loaded configs."""
    p = argparse.ArgumentParser(
        description="Dummy AuCaM campaign",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Config file paths
    p.add_argument("--global-config",   default="campaign_configs/global.yaml",
                   help="path to global YAML config (default: campaign_configs/global.yaml)")
    p.add_argument("--campaign-config", default="campaign_configs/campaign.yaml",
                   help="path to campaign YAML config (default: campaign_configs/campaign.yaml)")

    # Operational overrides — these CLI flags override config file values.
    p.add_argument("-v", "--verbose", action="store_true", default=gcfg.verbose)
    p.add_argument("--engine", choices=["default", "concurrent", "dragon"],
                   default=gcfg.engine,
                   help="execution backend")
    p.add_argument("--asyncflow-blocks", action="store_true",
                   default=gcfg.asyncflow_blocks,
                   help="run Tier 1 blocks via asyncflow.block")
    p.add_argument("--telemetry-dir", default=gcfg.telemetry.dir,
                   help="directory for telemetry JSONL (implies --asyncflow-blocks)")
    p.add_argument("--num-campaigns", type=int, default=gcfg.num_campaigns,
                   help="number of concurrent / sequential campaigns")
    p.add_argument("--policy",
                   choices=["bandit", "fifo", "adr", "llm"],
                   default=gcfg.scheduling.policy,
                   help="scheduling policy")

    # Driver
    p.add_argument("--driver", action="store_true", default=gcfg.driver.enabled,
                   help="use dynamic cm.submit()/cm.close() driver API")
    p.add_argument("--driver-delay", type=float, default=gcfg.driver.delay_s,
                   help="seconds between campaign submissions in driver mode")
    p.add_argument("--max-concurrent-campaigns", type=int,
                   default=gcfg.global_budget.max_concurrent_campaigns,
                   help="global budget: max concurrent campaigns (driver mode)")

    # Campaign overrides
    p.add_argument("--goal-analyses", type=int, default=ccfg.controller.goal_analyses,
                   help="number of analyses before terminating")
    p.add_argument("--max-concurrent", type=int, default=ccfg.budget.max_concurrent,
                   help="max concurrent workflows per campaign")
    p.add_argument("--checkpoint-dir", default=ccfg.checkpoint_dir,
                   help="directory for checkpoint files")

    # Workflow timing
    p.add_argument("--n-iters", type=int, default=ccfg.workflows.sim_worker.n_iters,
                   help="iterations per sim_worker block")
    p.add_argument("--iter-sleep", type=float, default=ccfg.workflows.sim_worker.iter_sleep_s,
                   help="sleep seconds per iteration")
    p.add_argument("--analysis-steps", type=int,
                   default=ccfg.workflows.analysis_worker.steps,
                   help="steps per analysis_worker block")

    return p


def main() -> None:
    # ── Step 1: pre-parse for config file paths (no help, ignore unknowns) ────
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--global-config",   default="campaign_configs/global.yaml")
    pre.add_argument("--campaign-config", default="campaign_configs/campaign.yaml")
    pre_args, _ = pre.parse_known_args()

    # ── Step 2: load configs (CLI flags will override individual fields) ───────
    gcfg = load_global(pre_args.global_config)    if Path(pre_args.global_config).exists()   else GlobalConfig()
    ccfg = load_campaign(pre_args.campaign_config) if Path(pre_args.campaign_config).exists() else CampaignConfig()

    # ── Step 3: full parse with config-derived defaults ────────────────────────
    parser = _build_parser(gcfg, ccfg)
    args   = parser.parse_args()

    # Apply CLI overrides back into the config objects so run() sees one source.
    gcfg.engine              = args.engine if args.engine != "default" else gcfg.engine
    gcfg.verbose             = args.verbose
    gcfg.asyncflow_blocks    = args.asyncflow_blocks
    gcfg.num_campaigns       = args.num_campaigns
    gcfg.scheduling.policy   = args.policy
    gcfg.driver.enabled      = args.driver
    gcfg.driver.delay_s      = args.driver_delay
    gcfg.telemetry.dir       = args.telemetry_dir
    if args.telemetry_dir:
        gcfg.asyncflow_blocks = True
    if gcfg.engine == "concurrent":
        gcfg.asyncflow_blocks = False
    if args.max_concurrent_campaigns is not None:
        gcfg.global_budget.max_concurrent_campaigns = args.max_concurrent_campaigns

    ccfg.controller.goal_analyses            = args.goal_analyses
    ccfg.budget.max_concurrent               = args.max_concurrent
    ccfg.checkpoint_dir                      = args.checkpoint_dir
    ccfg.workflows.sim_worker.n_iters        = args.n_iters
    ccfg.workflows.sim_worker.iter_sleep_s   = args.iter_sleep
    ccfg.workflows.analysis_worker.steps     = args.analysis_steps

    # Apply workflow timing so workers see the configured values.
    _workflows_mod.configure(
        n_iters=args.n_iters,
        iter_sleep_s=args.iter_sleep,
        analysis_steps=args.analysis_steps,
    )

    level = logging.DEBUG if gcfg.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)-30s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,  # route to stdout so Dragon captures it alongside [TRACE] prints
        force=True,         # Dragon's api_setup pre-configures the root logger; force overrides it
    )

    raise SystemExit(asyncio.run(run(gcfg, ccfg, args)))


if __name__ == "__main__":
    main()
