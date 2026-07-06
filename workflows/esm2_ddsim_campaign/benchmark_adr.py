#!/usr/bin/env python3
"""
ADR policy benchmark for the ESM2/DDSim campaign.

Mirrors dreamer_campaign/benchmark_adr.py but adapted for the real HPC
workflow stack (inference + dummy + md + miniapps).  Runs the campaign N
times under each scheduling policy using the ConcurrentExecutionBackend so it
executes without Dragon or GPUs (inference auto-stubs; dummy/md/miniapps run
in-process via their concurrent-mode paths).

Requires the full environment to be set up beforehand (DDSIM_DIR, MD_HOME,
MD_INPUT, VE_HOME, INF_DIR, etc.) — identical to running the campaign itself.

Policies compared
-----------------
    none   — static group priorities from config (baseline)
    rule   — DownstreamFirstPolicy (deterministic; keeps bottleneck fed)
    bandit — BanditSchedulingPolicy (Thompson-sampling; learns from history)
    llm    — LLMSchedulingPolicy (only if API key / local endpoint available)

Metrics collected per run
-------------------------
    wall_time_s        : elapsed wall-clock seconds
    time_to_target_s   : time to TARGET_N-th completion of TARGET_STAGE
    leads_by_deadline  : completions of TARGET_STAGE within DEADLINE_S (deadline mode)
    replica_events     : full event log (group, event, t) from cm.metrics()
    group_stats        : finished/started counts per group

Usage
-----
    # from the esm2_ddsim_campaign directory:
    python benchmark_adr.py --runs 3 --out benchmark_adr_results.json
    python benchmark_adr.py --policies none rule --runs 5 --timeout 180
    python benchmark_adr.py --mode deadline-yield --deadline 120
"""

import argparse
import asyncio
import copy
import json
import os
import sys
import time
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

# ── Benchmark constants ────────────────────────────────────────────────────────
# Per-run wall-time cap (time-to-target mode safety net).
RUN_TIMEOUT_S = 300

# ADR operator decision cadence.
TICK_S = 1.0

# Primary terminal stage to track completions of. "dummy" is the high-volume
# downstream group (inference → dummy); miniapps is the md → miniapps terminus.
TARGET_STAGE = "dummy"
TARGET_N     = 3   # time-to-target: wall-clock until the N-th completion

# Deadline-yield mode: measure how many TARGET_STAGE replicas finish within this window.
DEADLINE_S = 120.0

MODE = "time-to-target"

ALL_POLICIES = ["none", "rule", "bandit", "llm"]
LOG_DIR      = Path(__file__).parent / "adr-logs"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _llm_available(config: dict) -> bool:
    adr = config.get("cm", {}).get("adr", {})
    base_url = adr.get("base_url", "") or ""
    if "localhost" in base_url or "127.0.0.1" in base_url:
        return True
    env = adr.get("llm_api_key_env", "OPENROUTER_API_KEY")
    return bool(os.environ.get(env))


async def _drive_with_timeout(cm, operator, timeout: float) -> bool:
    """Run the ADR operator loop alongside cm.wait(). Returns finished flag."""
    async def _loop():
        async for _snap in operator.run():
            await asyncio.sleep(TICK_S)

    drive = asyncio.ensure_future(_loop())
    try:
        finished = await cm.wait(timeout=timeout)
    finally:
        await operator.shutdown()
        if not drive.done():
            drive.cancel()
            try:
                await drive
            except asyncio.CancelledError:
                pass
    return finished


def _prepare_config(config: dict, config_path: Path) -> dict:
    """Expand env-var refs and merge per-workflow config_file entries."""
    from src.utils.workflow import _expand_env
    from run_campaing import _expand_workflow_configs

    config = _expand_env(config)
    _expand_workflow_configs(config, config_path.parent)
    return config


async def _run_once(config: dict, config_path: Path, seed_offset: int,
                    policy_kind: str, log_path: Path) -> dict:
    """Run the campaign once under the given policy and return metrics."""
    from src.campaign import AsyncCampaignManager as CampaignManager
    from run_campaing import _build_registry
    from radical.asyncflow import WorkflowEngine
    from rhapsody.backends import ConcurrentExecutionBackend

    # Force concurrent backend for benchmarking (Dragon not required).
    cfg = copy.deepcopy(config)
    cfg["engine"] = "concurrent"

    # Per-run telemetry directory so successive runs don't overwrite each other.
    tel_cfg = cfg.setdefault("telemetry", {})
    if tel_cfg.get("collect_telemetry", False):
        base_dir = tel_cfg.get("telemetry_dir", "telemetry-results")
        tel_cfg["telemetry_dir"] = f"{base_dir}-{policy_kind}-{log_path.stem}"

    # Deadline-yield: disable early-stop so the campaign runs the full window.
    if MODE == "deadline-yield":
        for wf_cfg in cfg.get("workflows", {}).values():
            wf_cfg.pop("campaign_target", None)

    # Disable the MD→miniapps pipeline in benchmark mode.  MD runs in a
    # blocking thread and takes 24+ min on CPU; it is independent of the
    # measured inference→dummy funnel and causes asyncflow.shutdown() to hang
    # indefinitely after the deadline because the thread cannot be cancelled.
    for name in ("md", "miniapps"):
        wf = cfg.get("workflows", {}).get(name)
        if wf is not None:
            wf["replicas"] = 0

    # Force stub (debug) mode for inference so the benchmark produces actual
    # dummy leads even on CPU-only nodes where assigned_gpu_ids is never
    # populated.  Without this, the no-GPU guard skips all inference replicas
    # and dummy is never triggered — the benchmark measures nothing.
    inf_wf = cfg.get("workflows", {}).get("inference")
    if inf_wf is not None:
        inf_wf["debug"] = True

    backend   = await ConcurrentExecutionBackend()
    asyncflow = await WorkflowEngine.create(backend)

    # Start telemetry if configured (mirrors run_campaing.py main()).
    telemetry = None
    if tel_cfg.get("collect_telemetry", False):
        tel_dir = tel_cfg["telemetry_dir"]
        if hasattr(asyncflow, "start_telemetry"):
            try:
                telemetry = await asyncflow.start_telemetry(
                    resource_poll_interval=tel_cfg.get("resource_poll_interval", 0.5),
                    checkpoint_path=tel_dir,
                )
                print(f"[telemetry → {tel_dir}]", end=" ", flush=True)
            except ImportError as exc:
                print(f"[telemetry disabled: {exc}]", end=" ", flush=True)

    registry = _build_registry(cfg)
    cm = CampaignManager.from_config(cfg, registry, asyncflow=asyncflow)

    operator = None
    final_summary: dict = {}
    if policy_kind != "none":
        from src.campaign.adr import (
            CampaignView, CampaignOperator, PolicyRecorder,
            make_scheduling_policy, resolve_system_prompt, TelemetrySubscriber,
        )
        adr_cfg  = cfg.get("cm", {}).get("adr", {})
        tel_sub  = TelemetrySubscriber(telemetry) if telemetry is not None else None
        view     = CampaignView(cm, telemetry_subscriber=tel_sub)
        recorder = PolicyRecorder(log_path, policy_kind=policy_kind)
        operator = CampaignOperator(view, engine=asyncflow, observer=recorder)

        api_key, kw = None, {}
        if policy_kind == "bandit":
            kw = {"warmstart": bool(adr_cfg.get("warmstart", True)),
                  "seed": seed_offset}
        elif policy_kind == "llm":
            env      = adr_cfg.get("llm_api_key_env", "OPENROUTER_API_KEY")
            api_key  = os.environ.get(env)
            base_url = adr_cfg.get("base_url", "") or ""
            if base_url:
                kw["base_url"] = base_url
            if adr_cfg.get("llm_timeout_s") is not None:
                kw["timeout_s"] = float(adr_cfg["llm_timeout_s"])
            if adr_cfg.get("llm_max_retries") is not None:
                kw["instructor_retries"] = int(adr_cfg["llm_max_retries"])
            # llm_tick_s sets the minimum interval between real LLM API calls.
            # Without this, a down endpoint is hammered every TICK_S (1 s) and
            # dumps a 120-line traceback per failure into the log.
            kw["min_call_interval_s"] = float(adr_cfg.get("llm_tick_s", 10.0))
            if not api_key and ("localhost" in base_url or "127.0.0.1" in base_url):
                api_key = "sk-noauth"
            prompt = resolve_system_prompt(adr_cfg)
            if prompt:
                kw["system_prompt"] = prompt

        operator.policy = make_scheduling_policy(
            operator, kind=policy_kind, llm_api_key=api_key,
            model=adr_cfg.get("model", "openai/gpt-4o-mini"), **kw)
        recorder.bind(view=view, policy=operator.policy)

    run_timeout = DEADLINE_S if MODE == "deadline-yield" else RUN_TIMEOUT_S

    dnf = False
    try:
        await cm.start()
        if operator is not None:
            finished = await _drive_with_timeout(cm, operator, run_timeout)
        else:
            finished = await cm.wait(timeout=run_timeout)
        if not finished and MODE != "deadline-yield":
            dnf = True
        if operator is not None:
            final_summary = getattr(operator.policy, "summary", {}) or {}
    finally:
        await cm.close()
        if telemetry is not None:
            await telemetry.stop()
        await asyncflow.shutdown()

    m = cm.metrics().to_dict()
    m["policy"] = policy_kind
    if tel_cfg.get("collect_telemetry") and telemetry is not None:
        m["telemetry_dir"] = tel_cfg["telemetry_dir"]
    if dnf:
        m["dnf"] = True
    if operator is not None:
        m["decision_log"] = str(log_path)
        if final_summary:
            m["final_posteriors"] = final_summary

    # Time-to-target: wall-clock to the N-th TARGET_STAGE completion.
    target_finishes = sorted(
        e["t"] for e in m.get("replica_events", [])
        if e["group"] == TARGET_STAGE and e["event"] == "finish"
    )
    m["time_to_target_s"] = (
        target_finishes[TARGET_N - 1] if len(target_finishes) >= TARGET_N else None
    )

    # Secondary: miniapps completions (md → miniapps terminus).
    miniapps_finishes = sorted(
        e["t"] for e in m.get("replica_events", [])
        if e["group"] == "miniapps" and e["event"] == "finish"
    )
    m["miniapps_finished"] = len(miniapps_finishes)
    m["miniapps_first_finish_s"] = miniapps_finishes[0] if miniapps_finishes else None

    if MODE == "deadline-yield":
        m["deadline_s"]       = DEADLINE_S
        m["leads_by_deadline"] = sum(1 for t in target_finishes if t <= DEADLINE_S)

    return m


# ── Main benchmark loop ────────────────────────────────────────────────────────

async def run_benchmark(config_path: str, n_runs: int, out_path: str,
                        policies: list[str]) -> None:
    with open(config_path) as f:
        raw_config = yaml.safe_load(f)

    path = Path(config_path)

    if "llm" in policies and not _llm_available(raw_config):
        print("llm policy requested but no API key / local endpoint found — skipping.")
        policies = [p for p in policies if p != "llm"]

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    results: dict = {}

    for policy in policies:
        print(f"\n{'='*60}\nPolicy: {policy}\n{'='*60}")
        cfg_results = []
        for run_idx in range(n_runs):
            print(f"  Run {run_idx + 1}/{n_runs}...", end=" ", flush=True)
            # Expand env-vars and merge config_file entries fresh each run.
            cfg = _prepare_config(copy.deepcopy(raw_config), path)
            log_path = LOG_DIR / f"{policy}-run{run_idx}.jsonl"
            t0 = time.time()
            try:
                m = await _run_once(cfg, path, run_idx * 100, policy, log_path)
                elapsed = time.time() - t0
                m["wall_time_s"] = elapsed
                if MODE == "deadline-yield":
                    print(f"{m.get('leads_by_deadline', 0)} leads  "
                          f"miniapps={m.get('miniapps_finished', 0)}  "
                          f"({elapsed:.0f}s wall)")
                elif m.get("dnf"):
                    print(f"DNF ({elapsed:.0f}s, hit {RUN_TIMEOUT_S}s limit)  "
                          f"miniapps={m.get('miniapps_finished', 0)}")
                else:
                    print(f"done in {elapsed:.1f}s  "
                          f"ttt={m.get('time_to_target_s')}  "
                          f"miniapps={m.get('miniapps_finished', 0)}")
                cfg_results.append(m)
            except Exception as exc:
                elapsed = time.time() - t0
                print(f"FAILED: {exc}")
                cfg_results.append({"error": str(exc), "wall_time_s": elapsed,
                                    "policy": policy})
        results[policy] = cfg_results

    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults written to {out_path}")
    if any(p != "none" for p in policies):
        print(f"Per-cycle decision logs under {LOG_DIR}/")
        print("Plot:  python ../dreamer_campaign/plot_policy_comparison.py "
              + " ".join(f"adr-logs/{p}-run0.jsonl" for p in policies if p != "none"))
        print(f"Plot:  python ../dreamer_campaign/plot_optimizations.py "
              + f"--results  {out_path} --out-dir plots/optimizations")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ADR scheduling-policy benchmark for the ESM2/DDSim campaign"
    )
    parser.add_argument("--config",   default="config.yaml",
                        help="Campaign config YAML (default: config.yaml)")
    parser.add_argument("--runs",     type=int,   default=3,
                        help="Runs per policy (default: 3)")
    parser.add_argument("--out",      default="benchmark_adr_results.json",
                        help="Output JSON path (default: benchmark_adr_results.json)")
    parser.add_argument("--policies", nargs="+", default=["none", "rule", "bandit"],
                        choices=ALL_POLICIES,
                        help="Policies to benchmark (default: none rule bandit)")
    parser.add_argument("--timeout",  type=int,   default=RUN_TIMEOUT_S,
                        help="Per-run wall-time cap in seconds (time-to-target mode)")
    parser.add_argument("--tick",     type=float, default=TICK_S,
                        help="ADR operator decision cadence in seconds")
    parser.add_argument("--mode",     default="time-to-target",
                        choices=["time-to-target", "deadline-yield"],
                        help="Objective: time-to-target (lower=better) or "
                             "deadline-yield (higher=better within DEADLINE_S)")
    parser.add_argument("--deadline", type=float, default=DEADLINE_S,
                        help="Fixed window in seconds for deadline-yield mode")
    parser.add_argument("--target-stage", default=TARGET_STAGE,
                        help=f"Workflow group to track for time-to-target (default: {TARGET_STAGE})")
    parser.add_argument("--target-n",    type=int, default=TARGET_N,
                        help=f"N-th completion to measure time to (default: {TARGET_N})")
    args = parser.parse_args()

    RUN_TIMEOUT_S = args.timeout
    TICK_S        = args.tick
    MODE          = args.mode
    DEADLINE_S    = args.deadline
    TARGET_STAGE  = args.target_stage
    TARGET_N      = args.target_n

    asyncio.run(run_benchmark(args.config, args.runs, args.out, args.policies))
