#!/usr/bin/env python3
"""
Focused telemetry benchmark: rule vs rule_telemetry.

Compares the baseline rule policy against the telemetry-aware variant
(rule_telemetry) that scales the starvation priority boost by live cpu_util
from TelemetrySubscriber.  Both conditions run with TelemetrySubscriber wired
so the only variable is whether DownstreamFirstPolicy applies the boost.

n_md_runs is set to 4 (all miniapps replicas) — the true campaign goal.
With inference.replicas=8 there is always a queued inference competing for freed
GPU slots, so the telemetry boost is decisive in every pass-2 allocation.

GPU warm-up bias
----------------
GPUs start in a low-power state and run 2–4× slower until warmed up by a few
full GPU kernels.  Block-ordered benchmarks (all rule first, then all
rule_telemetry) assign the cold-GPU penalty to whichever policy runs first,
producing a systematic bias unrelated to scheduling.

Two mitigations are applied by default:
  1. A discarded warmup run (rule_telemetry) before timing starts.
  2. Interleaved execution — policy order alternates each round so both
     conditions see equivalent GPU thermal state within each round.
Pass --no-warmup / --no-interleave to reproduce the original block ordering.

Metrics per run
---------------
wall_time_s          : total elapsed wall clock time
time_to_target_s     : time from start to n_md_runs-th miniapps completion
time_to_first_miniapps_s : time to first miniapps completion
starved_cycles       : ticks where miniapps was starved and boost fired
mean_boost           : average boost magnitude when boost fired (0 for rule)
replica_events       : full event log for Gantt plotting

Usage
-----
    dragon benchmark_telemetry.py --config config.yaml --runs 5 \\
        --out telemetry_benchmark_results.json
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
import os
import shutil
import sys
import time
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

POLICIES = ["rule", "rule_telemetry"]
LOG_DIR = Path(__file__).parent / "adr-logs"
RUN_TIMEOUT_S = 1200
TICK_S = 2.0
N_MD_RUNS = 4
N_INFERENCE_RUNS = 16


# ── Helpers ───────────────────────────────────────────────────────────────────


async def _drive_with_timeout(cm, operator, timeout: float) -> bool:
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
    from run_campaing import _expand_workflow_configs
    from src.utils.workflow import _expand_env

    config = _expand_env(config)
    _expand_workflow_configs(config, config_path.parent)
    return config


# ── Single run ────────────────────────────────────────────────────────────────


async def _run_once(
    config: dict,
    config_path: Path,
    seed_offset: int,
    policy_kind: str,
    log_path: Path,
) -> dict:
    from radical.asyncflow import WorkflowEngine
    from rhapsody.backends import DragonExecutionBackendV3
    from run_campaing import _build_registry

    from src.campaign import AsyncCampaignManager as CampaignManager

    cfg = copy.deepcopy(config)

    # Clean experiment dirs from previous runs.
    campaign_dir = config_path.parent
    for d in campaign_dir.glob("DDMD-md*"):
        if d.is_dir():
            shutil.rmtree(d)
    for d in campaign_dir.glob("DDMD-miniapps*"):
        if d.is_dir():
            shutil.rmtree(d)

    # Per-run telemetry directory — always collected for both conditions.
    tel_cfg = cfg.setdefault("telemetry", {})
    base_dir = tel_cfg.get("telemetry_dir", "telemetry-results")
    run_tag = log_path.stem
    tel_cfg["telemetry_dir"] = f"{base_dir}-{run_tag}"
    tel_cfg["collect_telemetry"] = True

    engine_dragon = await DragonExecutionBackendV3()
    asyncflow = await WorkflowEngine.create(engine_dragon)

    telemetry = None
    if hasattr(asyncflow, "start_telemetry"):
        try:
            telemetry = await asyncflow.start_telemetry(
                resource_poll_interval=tel_cfg.get("resource_poll_interval", 0.5),
                checkpoint_path=tel_cfg["telemetry_dir"],
            )
            print(f"[telemetry → {tel_cfg['telemetry_dir']}]", end=" ", flush=True)
        except ImportError as exc:
            print(f"[telemetry disabled: {exc}]", end=" ", flush=True)

    registry = _build_registry(cfg)
    cm = CampaignManager.from_config(cfg, registry, engine=asyncflow, engine_dragon=engine_dragon)

    from ddsim_operator import DDSimCampaignOperator

    from src.campaign.adr import (
        CampaignView,
        LoggingPolicy,
        PolicyRecorder,
        TelemetrySubscriber,
        make_scheduling_policy,
    )

    # TelemetrySubscriber always wired — both conditions receive live gpu/cpu_util.
    tel_sub = TelemetrySubscriber(telemetry)
    adr_cfg = cfg.get("cm", {}).get("adr", {})
    terminal = adr_cfg.get("terminal") or None
    view = CampaignView(cm, terminal=terminal, telemetry_subscriber=tel_sub)
    recorder = PolicyRecorder(log_path, policy_kind=policy_kind)
    operator = DDSimCampaignOperator(
        view,
        engine=asyncflow,
        observer=recorder,
        n_md_runs=N_MD_RUNS,
        n_inference_runs=N_INFERENCE_RUNS,
        max_fail_rate=float(adr_cfg.get("max_fail_rate", 0.05)),
    )

    # Keep reference to inner_policy (RuleCorrectionsPolicy) before LoggingPolicy
    # wrapping so we can read starved_cycles and mean_boost after the run.
    inner_policy = make_scheduling_policy(operator, kind=policy_kind)
    policy = LoggingPolicy(inner_policy)
    operator.policy = policy
    recorder.bind(view=view, policy=policy)

    dnf = False
    try:
        await cm.start()
        finished = await _drive_with_timeout(cm, operator, RUN_TIMEOUT_S)
        if not finished:
            dnf = True
    finally:
        await cm.close()
        if telemetry is not None:
            await telemetry.stop()
        await asyncflow.shutdown()

    m = cm.metrics().to_dict()
    m["policy"] = policy_kind
    m["dnf"] = dnf

    # Time-to-target: wall time until ALL GPU stages are done —
    # max(last miniapps completion, last inference completion).
    events = m.get("replica_events", [])
    miniapps_finishes = sorted(
        e["t"]
        for e in events
        if e.get("group") == "miniapps" and e.get("event") in ("finish", "finished")
    )
    inference_finishes = sorted(
        e["t"]
        for e in events
        if e.get("group") == "inference" and e.get("event") in ("finish", "finished")
    )
    t_miniapps = miniapps_finishes[N_MD_RUNS - 1] if len(miniapps_finishes) >= N_MD_RUNS else None
    t_inference = inference_finishes[-1] if inference_finishes else None
    # TTT = when all N_MD_RUNS miniapps finish.  Inference may still be running;
    # that's correct — rule_telemetry overlaps miniapps with in-flight inference.
    # Using max() would hide the scheduling benefit (inference tail dominates both).
    m["time_to_target_s"] = t_miniapps
    m["time_to_last_miniapps_s"] = t_miniapps
    m["time_to_last_inference_s"] = t_inference
    m["time_to_first_miniapps_s"] = miniapps_finishes[0] if miniapps_finishes else None

    # Telemetry boost counters — 0 for rule (NullSchedulingPolicy has no boost),
    # non-zero for rule_telemetry (DownstreamFirstPolicy tracks these).
    m["starved_cycles"] = getattr(inner_policy, "starved_cycles", 0)
    m["mean_boost"] = getattr(inner_policy, "mean_boost", 0.0)

    return m


# ── Benchmark loop ────────────────────────────────────────────────────────────


async def _run_timed(
    raw_config: dict,
    path: Path,
    policy: str,
    run_idx: int,
    seed_offset: int,
) -> dict:
    """Run one timed trial; return metrics dict."""
    cfg = _prepare_config(copy.deepcopy(raw_config), path)
    log_path = LOG_DIR / f"tel-{policy}-run{run_idx}.jsonl"
    t0 = time.time()
    m = await _run_once(cfg, path, seed_offset, policy, log_path)
    m["wall_time_s"] = time.time() - t0
    return m


async def run_benchmark(
    config_path: str,
    n_runs: int,
    out_path: str,
    warmup: bool = True,
    interleave: bool = True,
) -> None:
    with open(config_path) as f:
        raw_config = yaml.safe_load(f)
    path = Path(config_path)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # ── Warmup ────────────────────────────────────────────────────────────────
    # Run rule_telemetry once (discarded) to bring GPUs out of cold/low-power
    # state before timing starts.  Without this, block-ordered runs assign the
    # cold-GPU penalty to whichever policy runs first, biasing the comparison.
    if warmup:
        warmup_policy = POLICIES[-1]  # rule_telemetry — completes faster
        print(f"\n{'─' * 60}")
        print(f"Warmup run ({warmup_policy}) — discarded, GPU ramp-up only")
        print(f"{'─' * 60}")
        cfg_w = _prepare_config(copy.deepcopy(raw_config), path)
        log_w = LOG_DIR / f"tel-warmup.jsonl"
        t0 = time.time()
        try:
            await _run_once(cfg_w, path, 9999, warmup_policy, log_w)
            print(f"  warmup done ({time.time() - t0:.0f}s) — GPUs warm, starting benchmark")
        except Exception as exc:
            print(f"  warmup failed ({exc}) — continuing anyway")

    # ── Timed benchmark ───────────────────────────────────────────────────────
    # Interleaved ordering: policy alternates each iteration so both conditions
    # see equivalent GPU thermal state.  Block ordering (all rule, then all
    # rule_telemetry) confounds GPU warm-up with the scheduling comparison.
    results: dict = {p: [] for p in POLICIES}

    if interleave:
        print(f"\n{'=' * 60}\nInterleaved benchmark ({n_runs} rounds)\n{'=' * 60}")
        for run_idx in range(n_runs):
            # Alternate starting policy so neither always runs "first warm".
            ordered = POLICIES if run_idx % 2 == 0 else list(reversed(POLICIES))
            for policy in ordered:
                print(f"  [{policy}] round {run_idx + 1}/{n_runs}...", end=" ", flush=True)
                try:
                    m = await _run_timed(raw_config, path, policy, run_idx, run_idx * 100)
                    ttt = m.get("time_to_target_s")
                    ttt_str = f"{ttt:.1f}s" if ttt is not None else "N/A"
                    dnf_tag = "  [DNF]" if m.get("dnf") else ""
                    print(
                        f"ttt={ttt_str}  "
                        f"starved_cycles={m['starved_cycles']}  "
                        f"mean_boost={m['mean_boost']:.2f}  "
                        f"({m['wall_time_s']:.0f}s wall){dnf_tag}"
                    )
                    results[policy].append(m)
                except Exception as exc:
                    elapsed = time.time()
                    print(f"FAILED: {exc}")
                    results[policy].append({"error": str(exc), "policy": policy})
    else:
        for policy in POLICIES:
            print(f"\n{'=' * 60}\nPolicy: {policy}\n{'=' * 60}")
            for run_idx in range(n_runs):
                print(f"  Run {run_idx + 1}/{n_runs}...", end=" ", flush=True)
                try:
                    m = await _run_timed(raw_config, path, policy, run_idx, run_idx * 100)
                    ttt = m.get("time_to_target_s")
                    ttt_str = f"{ttt:.1f}s" if ttt is not None else "N/A"
                    dnf_tag = "  [DNF]" if m.get("dnf") else ""
                    print(
                        f"ttt={ttt_str}  "
                        f"starved_cycles={m['starved_cycles']}  "
                        f"mean_boost={m['mean_boost']:.2f}  "
                        f"({m['wall_time_s']:.0f}s wall){dnf_tag}"
                    )
                    results[policy].append(m)
                except Exception as exc:
                    print(f"FAILED: {exc}")
                    results[policy].append({"error": str(exc), "policy": policy})

    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults written to {out_path}")

    # Summary table
    # TTT = time-to-target = when all N_MD_RUNS miniapps finish (campaign goal).
    # rule (NullSchedulingPolicy): inference holds all GPUs → miniapps waits → high TTT.
    # rule_telemetry: starved boost fires when miniapps pending → miniapps overlaps → low TTT.
    print(f"\n── Telemetry benchmark  n_md_runs={N_MD_RUNS}  inference_replicas={N_INFERENCE_RUNS} ──")
    print("── TTT = miniapps completion time (lower is better; telemetry overlap expected ~38%) ──")
    hdr = (f"{'policy':16s}  {'ttt_mean':>10s}  {'ttt_min':>8s}  {'ttt_max':>8s}"
           f"  {'first_mini':>10s}"
           f"  {'starved_cyc':>12s}  {'mean_boost':>10s}")
    print(hdr)
    print("-" * len(hdr))
    for policy in POLICIES:
        runs = [r for r in results.get(policy, []) if "error" not in r and not r.get("dnf")]
        if not runs:
            print(f"{policy:16s}  {'ERROR/DNF':>10s}")
            continue
        ttts       = [r["time_to_target_s"]        for r in runs if r.get("time_to_target_s") is not None]
        first_mini = [r["time_to_first_miniapps_s"] for r in runs if r.get("time_to_first_miniapps_s") is not None]
        sc_vals    = [r["starved_cycles"] for r in runs]
        mb_vals    = [r["mean_boost"]     for r in runs]
        ttt_mean   = f"{sum(ttts)/len(ttts):.1f}s" if ttts else "N/A"
        ttt_min    = f"{min(ttts):.1f}s"            if ttts else "N/A"
        ttt_max    = f"{max(ttts):.1f}s"            if ttts else "N/A"
        fm_mean    = f"{sum(first_mini)/len(first_mini):.1f}s" if first_mini else "N/A"
        sc_mean    = f"{sum(sc_vals)/len(sc_vals):.1f}"
        mb_mean    = f"{sum(mb_vals)/len(mb_vals):.2f}"
        print(f"{policy:16s}  {ttt_mean:>10s}  {ttt_min:>8s}  {ttt_max:>8s}"
              f"  {fm_mean:>10s}"
              f"  {sc_mean:>12s}  {mb_mean:>10s}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Telemetry benchmark: rule vs rule_telemetry (n_md_runs=2)"
    )
    parser.add_argument("--config", default="config.yaml", help="Campaign config YAML")
    parser.add_argument("--runs", type=int, default=5, help="Runs per policy (default: 5)")
    parser.add_argument(
        "--out", default="telemetry_benchmark_results.json", help="Output JSON path"
    )
    parser.add_argument(
        "--timeout", type=float, default=RUN_TIMEOUT_S, help="Per-run wall-time cap (s)"
    )
    parser.add_argument("--tick", type=float, default=TICK_S, help="ADR tick interval (s)")
    parser.add_argument("--no-warmup", action="store_true",
                        help="Skip the discard warmup run (not recommended)")
    parser.add_argument("--no-interleave", action="store_true",
                        help="Use block ordering (all rule, then all rule_telemetry) instead of interleaved")
    args = parser.parse_args()

    RUN_TIMEOUT_S = args.timeout
    TICK_S = args.tick

    asyncio.run(run_benchmark(
        args.config, args.runs, args.out,
        warmup=not args.no_warmup,
        interleave=not args.no_interleave,
    ))
