#!/usr/bin/env python3
"""
GPU ADR policy benchmark for the ESM2/DDSim campaign.

Runs ALL FOUR workflows (inference → dummy, md → miniapps) on real GPU hardware
via the Dragon backend.  All four policies run the SAME config — same replicas,
same resource pool — so results are directly comparable.

Design
------
Pass-1 (guaranteed, all policies):
    inference  floor=2 → always 2 GPU slots
    md         floor=1 → always 1 GPU slot  ← ensures md runs under every policy

Pass-2 (contested: 1 remaining GPU):
    inference (priority 9) vs miniapps (priority 3)

    none   → inference wins every pass-2 tick → miniapps delayed until
             inference exhausts all replicas and releases GPUs.
    rule   → ADR detects miniapps pending, boosts it above inference →
             miniapps gets pass-2 GPU promptly after md triggers it.
    bandit → learns the GPU-fair ordering within a few cycles.
    llm    → prompted with the GPU topology; should match rule.

Primary metric: time_to_first_miniapps_s — wall-clock from campaign start
to the first miniapps replica completing (lower = better).

All four policies complete all four workflows.  The difference is HOW FAST
Pipeline B (md → miniapps) produces its first result.

Usage
-----
    # from esm2_ddsim_campaign/ — must be launched via Dragon:
    dragon benchmark_adr_gpu.py --config config_stress_gpu.yaml \\
        --policies none rule bandit llm --runs 1 --out benchmark_adr_gpu.json

    # longer timeout if MD takes > 20 min per replica:
    dragon benchmark_adr_gpu.py --timeout 2400

Notes
-----
* Each policy creates / destroys its own DragonExecutionBackendV3 +
  WorkflowEngine inside the same Dragon process.
* Under 'none' the campaign still completes (md has floor=1, md runs, miniapps
  eventually gets GPU once inference finishes).  The campaign never deadlocks.
* Adjust inference.replicas in the config so inference keeps running while md
  is active — otherwise there is no GPU contention for miniapps to reveal.
"""

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

# ── Benchmark constants ────────────────────────────────────────────────────────
# Per-run wall-time cap.  Campaign completes naturally under all policies
# (md has floor=1 so it always runs); this is just a safety net.
# 16 inference × 114 s / 3 concurrent ≈ 608 s; 1200 s gives 2× headroom.
RUN_TIMEOUT_S = 1200

# ADR operator decision cadence.
TICK_S = 2.0

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
    """Drive the ADR operator alongside cm.wait(). Returns True if campaign finished."""
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
    from src.utils.workflow import _expand_env
    from run_campaing import _expand_workflow_configs

    config = _expand_env(config)
    _expand_workflow_configs(config, config_path.parent)
    return config


async def _run_once(config: dict, config_path: Path, seed_offset: int,
                    policy_kind: str, log_path: Path) -> dict:
    """Run one GPU campaign under the given policy; return metrics dict."""
    from src.campaign import AsyncCampaignManager as CampaignManager
    from run_campaing import _build_registry
    from radical.asyncflow import WorkflowEngine
    from rhapsody.backends import DragonExecutionBackendV3

    cfg = copy.deepcopy(config)

    # Clean up MD experiment directories from previous policy runs.
    # DDMdWorkflow raises FileNotFoundError if DDMD-md{N} already exists.
    campaign_dir = config_path.parent
    for d in campaign_dir.glob("DDMD-md*"):
        if d.is_dir():
            shutil.rmtree(d)
    for d in campaign_dir.glob("DDMD-miniapps*"):
        if d.is_dir():
            shutil.rmtree(d)

    # Per-run telemetry directory.
    tel_cfg = cfg.setdefault("telemetry", {})
    if tel_cfg.get("collect_telemetry", False):
        base_dir = tel_cfg.get("telemetry_dir", "telemetry-results")
        tel_cfg["telemetry_dir"] = f"{base_dir}-gpu-{policy_kind}-{log_path.stem}"

    # Remove early-stop targets — run each policy to full completion.
    for wf_cfg in cfg.get("workflows", {}).values():
        wf_cfg.pop("campaign_target", None)

    # GPU run: all four workflows execute on real hardware.
    # Do NOT zero md/miniapps.  Do NOT force debug on inference.
    engine_dragon = await DragonExecutionBackendV3()
    asyncflow     = await WorkflowEngine.create(engine_dragon)

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
    cm = CampaignManager.from_config(cfg, registry, asyncflow=asyncflow,
                                     engine_dragon=engine_dragon)

    # ── Build ADR operator ────────────────────────────────────────────────────
    operator = None
    if policy_kind != "none":
        from src.campaign.adr import (
            CampaignView, PolicyRecorder,
            make_scheduling_policy, resolve_system_prompt, TelemetrySubscriber,
        )
        from ddsim_operator import DDSimCampaignOperator
        adr_cfg  = cfg.get("cm", {}).get("adr", {})
        tel_sub  = TelemetrySubscriber(telemetry) if telemetry is not None else None
        terminal = adr_cfg.get("terminal") or None
        view     = CampaignView(cm, terminal=terminal, telemetry_subscriber=tel_sub)
        recorder = PolicyRecorder(log_path, policy_kind=policy_kind)
        operator = DDSimCampaignOperator(
            view, engine=asyncflow, observer=recorder,
            n_md_runs=int(adr_cfg.get("n_md_runs", 4)),
            max_fail_rate=float(adr_cfg.get("max_fail_rate", 0.05)),
        )

        api_key, kw = None, {}
        if policy_kind == "bandit":
            kw = {"warmstart": bool(adr_cfg.get("warmstart", True)),
                  "seed": adr_cfg.get("seed", 0) + seed_offset}
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

    dnf = False
    try:
        await cm.start()
        if operator is not None:
            finished = await _drive_with_timeout(cm, operator, RUN_TIMEOUT_S)
        else:
            finished = await cm.wait(timeout=RUN_TIMEOUT_S)
        if not finished:
            dnf = True
    finally:
        await cm.close()
        if telemetry is not None:
            await telemetry.stop()
        await asyncflow.shutdown()

    m = cm.metrics().to_dict()
    m["policy"]  = policy_kind
    m["dnf"]     = dnf
    if operator is not None:
        m["decision_log"] = str(log_path)
    if tel_cfg.get("collect_telemetry") and telemetry is not None:
        m["telemetry_dir"] = tel_cfg["telemetry_dir"]

    # ── Per-stage completion counts (from group_stats) ────────────────────────
    gs = m.get("group_stats", {})
    for stage, s in gs.items():
        m[f"{stage}_started"]  = s.get("n_started",  0)
        m[f"{stage}_finished"] = s.get("n_finished", 0)

    # ── Primary metric: time to first miniapps completion ─────────────────────
    # Lower is better.  Under 'none', miniapps waits for inference to exhaust
    # all pass-2 GPU slots before it can start.  Under ADR policies, miniapps
    # gets the pass-2 GPU promptly after md triggers it.
    events = m.get("replica_events", [])
    miniapps_finishes = sorted(
        e["t"] for e in events
        if e.get("group") == "miniapps" and e.get("event") in ("finish", "finished")
    )
    m["time_to_first_miniapps_s"] = miniapps_finishes[0] if miniapps_finishes else None

    # Time to first md completion (useful for understanding the timing gap).
    md_finishes = sorted(
        e["t"] for e in events
        if e.get("group") == "md" and e.get("event") in ("finish", "finished")
    )
    m["time_to_first_md_s"] = md_finishes[0] if md_finishes else None

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
            cfg      = _prepare_config(copy.deepcopy(raw_config), path)
            log_path = LOG_DIR / f"gpu-{policy}-run{run_idx}.jsonl"
            t0 = time.time()
            try:
                m       = await _run_once(cfg, path, run_idx * 100, policy, log_path)
                elapsed = time.time() - t0
                m["wall_time_s"] = elapsed

                ttm = m.get("time_to_first_miniapps_s")
                ttm_str = f"{ttm:.1f}s" if ttm is not None else "N/A (timed out)"
                dnf_tag = "  [DNF]" if m.get("dnf") else ""
                print(
                    f"miniapps_t={ttm_str}  "
                    f"md={m.get('md_finished', 0)}/{m.get('md_started', 0)}  "
                    f"dummy={m.get('dummy_finished', 0)}  "
                    f"({elapsed:.0f}s wall){dnf_tag}"
                )
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

    # ── Summary table ─────────────────────────────────────────────────────────
    print("\n── GPU scheduling comparison (all policies, same config) ──")
    print(f"{'policy':8s}  {'miniapps_t':>12s}  {'wall_time':>10s}  "
          f"{'md_done':>7s}  {'dummy_done':>10s}")
    print("-" * 58)
    for policy in policies:
        runs = results.get(policy, [])
        if not runs or "error" in runs[0]:
            print(f"{policy:8s}  {'ERROR':>12s}")
            continue
        r   = runs[0]
        ttm = r.get("time_to_first_miniapps_s")
        ttm_str  = f"{ttm:.1f}s"  if ttm  is not None else "N/A"
        wall_str = f"{r.get('wall_time_s', 0):.0f}s"
        md_str   = f"{r.get('md_finished', 0)}/{r.get('md_started', 0)}"
        dum_str  = str(r.get("dummy_finished", 0))
        dnf_tag  = " [DNF]" if r.get("dnf") else ""
        print(f"{policy:8s}  {ttm_str:>12s}  {wall_str:>10s}  "
              f"{md_str:>7s}  {dum_str:>10s}{dnf_tag}")

    if any(p != "none" for p in policies):
        logs = " ".join(
            f"adr-logs/gpu-{p}-run0.jsonl" for p in policies if p != "none")
        print(f"\nPer-cycle decision logs: {LOG_DIR}/gpu-*.jsonl")
        print(f"Plot:  python ../dreamer_campaign/plot_policy_comparison.py {logs}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ADR GPU benchmark — all 4 workflows, same config, compare timing"
    )
    parser.add_argument(
        "--config", default="config_stress_gpu.yaml",
        help="Campaign config YAML (default: config_stress_gpu.yaml)",
    )
    parser.add_argument(
        "--runs", type=int, default=1,
        help="Runs per policy (default: 1)",
    )
    parser.add_argument(
        "--out", default="benchmark_adr_gpu.json",
        help="Output JSON path (default: benchmark_adr_gpu.json)",
    )
    parser.add_argument(
        "--policies", nargs="+", default=ALL_POLICIES, choices=ALL_POLICIES,
        help="Policies to benchmark (default: none rule bandit llm)",
    )
    parser.add_argument(
        "--timeout", type=float, default=RUN_TIMEOUT_S,
        help=f"Per-run wall-time cap in seconds (default: {RUN_TIMEOUT_S}). "
             "Increase if real MD replicas take longer than 30 min.",
    )
    parser.add_argument(
        "--tick", type=float, default=TICK_S,
        help=f"ADR operator decision cadence in seconds (default: {TICK_S})",
    )
    args = parser.parse_args()

    RUN_TIMEOUT_S = args.timeout
    TICK_S        = args.tick

    asyncio.run(run_benchmark(args.config, args.runs, args.out, args.policies))
