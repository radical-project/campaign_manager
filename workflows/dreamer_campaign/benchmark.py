#!/usr/bin/env python3
"""
ADR policy benchmark — measures campaign performance across scheduling policies.

Mirrors benchmark.py, but instead of feature-flag configurations it varies the
*scheduling policy* that drives the campaign:

    none    — no ADR supervision: static group priorities (baseline)
    rule    — DownstreamFirstPolicy   (deterministic, the rule the bandit learns)
    bandit  — BanditSchedulingPolicy  (the same bandit, wrapped as an ADR agent)
    llm     — LLMSchedulingPolicy      (only if an API key is set)

For every policy the campaign is run N times and the same metrics as benchmark.py
are collected (wall_time_s, replica_events, time_to_target_s, group_stats), so the
results JSON is plottable the same way.  Each run also writes its per-cycle ADR
decision log to ``adr-logs/<policy>-run<idx>.jsonl`` (for plot_policy_comparison.py),
and the representative run's log path is recorded in the metrics.

Submit all policies in one job:
    python benchmark_adr.py --runs 5 --out benchmark_results.json
    # restrict / add policies:
    python benchmark_adr.py --policies none rule bandit --runs 5
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

# Per-run wall-time cap (same rationale as benchmark.py).
RUN_TIMEOUT_S = 120
TICK_S = 1.0
TARGET_STAGE = "s5_fep_ranking"
TARGET_N = 5

# Benchmark objective:
#   "time-to-target" — wall-clock until the TARGET_N-th terminal lead (lower=better).
#                      Downstream-first (rule) is near-optimal: it rushes the leading
#                      edge straight to the terminal stage.
#   "deadline-yield" — total terminal leads produced within a FIXED wall-clock budget
#                      DEADLINE_S (higher=better). Realistic HPC framing (a fixed
#                      allocation window). Yield is gated by the BOTTLENECK's
#                      throughput, so a policy that keeps the bottleneck fed (the LLM)
#                      beats one that starves it to greedily drain the leading edge
#                      (rule). Early-stop (campaign_target) is disabled in this mode so
#                      the campaign runs the full window.
MODE = "time-to-target"
DEADLINE_S = 60.0

ALL_POLICIES = ["none", "rule", "bandit", "llm"]
LOG_DIR = Path(__file__).parent / "adr-logs"


def _llm_available(config: dict) -> bool:
    adr = config.get("cm", {}).get("adr", {})
    base_url = adr.get("base_url", "") or ""
    # Local endpoints (Ollama/llama.cpp) need no key.
    if "localhost" in base_url or "127.0.0.1" in base_url:
        return True
    env = adr.get("llm_api_key_env", "OPENROUTER_API_KEY")
    return bool(os.environ.get(env))


async def _drive_with_timeout(cm, operator, timeout: float) -> bool:
    """Run the operator loop alongside cm.wait(timeout). Returns finished flag."""

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


async def _run_once(config: dict, seed_offset: int, policy_kind: str, log_path: Path) -> dict:
    """Run one campaign under the given policy and return its metrics dict."""
    import random as _random

    _random.seed(seed_offset + 1337)  # identical score-cascade across policies

    # Reset DreamerWorkflow class-level state between runs.
    from dreamer_workflow import DreamerWorkflow
    from run_campaign import _build_from_plan, _build_registry

    from src.campaign import AsyncCampaignManager as CampaignManager

    DreamerWorkflow._group_state = {}

    # Deadline-yield mode: disable early-stop so the campaign runs the full window
    # (we measure leads produced by the deadline, not time to a fixed lead count).
    if MODE == "deadline-yield" and "stages" in config:
        for s in config["stages"]:
            if "campaign_target" in s:
                s["campaign_target"] = 0

    if "stages" in config:
        cm_cfg = config.get("cm", {})
        config["workflows"] = _build_from_plan(config)
        for key in ("engine", "resources", "telemetry", "workflow_registry", "features"):
            if key in cm_cfg and key not in config:
                config[key] = cm_cfg[key]
        config["debug"] = bool(cm_cfg.get("debug", False))

    if "provenance" in config:
        for k in config["provenance"].get("seeds", {}):
            config["provenance"]["seeds"][k] += seed_offset

    from radical.asyncflow import WorkflowEngine
    from rhapsody.backends import ConcurrentExecutionBackend

    backend = await ConcurrentExecutionBackend()
    asyncflow = await WorkflowEngine.create(backend)

    registry = _build_registry(config)
    cm = CampaignManager.from_config(config, registry, asyncflow=asyncflow)

    # Build the operator (policy != none). The CM has no in-loop bandit; the
    # ADR policy owns scheduling priority via group.priority.
    operator = None
    final_summary: dict = {}
    if policy_kind != "none":
        from src.campaign.adr import (
            CampaignOperator,
            CampaignView,
            PolicyRecorder,
            make_scheduling_policy,
            resolve_system_prompt,
        )

        adr_cfg = config.get("cm", {}).get("adr", {})
        view = CampaignView(cm)
        recorder = PolicyRecorder(log_path, policy_kind=policy_kind)
        operator = CampaignOperator(view, engine=asyncflow, observer=recorder)
        api_key = None
        kw = {}
        if policy_kind == "bandit":
            kw = {"warmstart": bool(adr_cfg.get("warmstart", True)), "seed": seed_offset}
        elif policy_kind == "llm":
            # Mirror run_campaign._build_adr_operator: honour base_url / timeout
            # and supply a placeholder key for local (Ollama/llama.cpp) endpoints.
            env = adr_cfg.get("llm_api_key_env", "OPENROUTER_API_KEY")
            api_key = os.environ.get(env)
            base_url = adr_cfg.get("base_url", "") or ""
            if base_url:
                kw["base_url"] = base_url
            if adr_cfg.get("llm_timeout_s") is not None:
                kw["timeout_s"] = float(adr_cfg["llm_timeout_s"])
            if adr_cfg.get("llm_max_retries") is not None:
                kw["instructor_retries"] = int(adr_cfg["llm_max_retries"])
            if not api_key and ("localhost" in base_url or "127.0.0.1" in base_url):
                api_key = "sk-noauth"
            prompt = resolve_system_prompt(adr_cfg)  # cwd-relative for file paths
            if prompt:
                kw["system_prompt"] = prompt
        operator.policy = make_scheduling_policy(
            operator,
            kind=policy_kind,
            llm_api_key=api_key,
            model=adr_cfg.get("model", "openai/gpt-4o-mini"),
            **kw,
        )
        recorder.bind(view=view, policy=operator.policy)

    # In deadline-yield mode the run is cut off at DEADLINE_S by design (the
    # campaign never finishes naturally); in time-to-target mode it runs until
    # completion or the RUN_TIMEOUT_S safety cap.
    run_timeout = DEADLINE_S if MODE == "deadline-yield" else RUN_TIMEOUT_S

    dnf = False
    try:
        await cm.start()
        if operator is not None:
            finished = await _drive_with_timeout(cm, operator, run_timeout)
        else:
            finished = await cm.wait(timeout=run_timeout)
        # Not-finishing is a DNF only in time-to-target mode; in deadline-yield
        # the cutoff is expected and the metric is leads produced by then.
        if not finished and MODE != "deadline-yield":
            dnf = True
        if operator is not None:
            final_summary = getattr(operator.policy, "summary", {}) or {}
    finally:
        await cm.close()
        await asyncflow.shutdown()

    m = cm.metrics().to_dict()
    m["policy"] = policy_kind
    if dnf:
        m["dnf"] = True
    if operator is not None:
        m["decision_log"] = str(log_path)
        if final_summary:
            m["final_posteriors"] = final_summary

    s5_finishes = sorted(
        e["t"]
        for e in m.get("replica_events", [])
        if e["group"] == TARGET_STAGE and e["event"] == "finish"
    )
    m["time_to_target_s"] = s5_finishes[TARGET_N - 1] if len(s5_finishes) >= TARGET_N else None
    if MODE == "deadline-yield":
        # Primary metric for this mode: terminal leads produced within the window.
        m["deadline_s"] = DEADLINE_S
        m["leads_by_deadline"] = sum(1 for t in s5_finishes if t <= DEADLINE_S)
    return m


async def run_benchmark(config_path: str, n_runs: int, out_path: str, policies: list[str]) -> None:
    with open(config_path) as f:
        base_config = yaml.safe_load(f)

    if "llm" in policies and not _llm_available(base_config):
        print("llm policy requested but no API key in env — skipping it.")
        policies = [p for p in policies if p != "llm"]

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    results: dict = {}
    for policy in policies:
        print(f"\n{'=' * 60}\nPolicy: {policy}\n{'=' * 60}")
        cfg_results = []
        for run_idx in range(n_runs):
            print(f"  Run {run_idx + 1}/{n_runs}...", end=" ", flush=True)
            cfg = copy.deepcopy(base_config)
            log_path = LOG_DIR / f"{policy}-run{run_idx}.jsonl"
            t0 = time.time()
            try:
                m = await _run_once(cfg, run_idx * 100, policy, log_path)
                elapsed = time.time() - t0
                if MODE == "deadline-yield":
                    m["wall_time_s"] = elapsed
                    print(
                        f"{m.get('leads_by_deadline', 0)} leads in "
                        f"{DEADLINE_S:.0f}s window  ({elapsed:.0f}s wall)"
                    )
                elif m.get("dnf"):
                    m["wall_time_s"] = elapsed
                    print(f"DNF ({elapsed:.0f}s, hit {RUN_TIMEOUT_S}s limit)")
                else:
                    print(
                        f"done in {elapsed:.1f}s  "
                        f"(wall={m.get('wall_time_s', 0):.1f}s  "
                        f"ttt={m.get('time_to_target_s')})"
                    )
                cfg_results.append(m)
            except Exception as exc:
                print(f"FAILED: {exc}")
                cfg_results.append({"error": str(exc), "wall_time_s": None, "policy": policy})
        results[policy] = cfg_results

    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults written to {out_path}")
    print(f"Per-cycle decision logs under {LOG_DIR}/")
    print(
        "Plot:  python plot_policy_comparison.py "
        + " ".join(f"adr-logs/{p}-run0.jsonl" for p in policies if p != "none")
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ADR scheduling-policy benchmark")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--out", default="benchmark_results.json")
    parser.add_argument(
        "--policies",
        nargs="+",
        default=ALL_POLICIES,
        choices=ALL_POLICIES,
        help="which policies to benchmark (default: all)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=RUN_TIMEOUT_S,
        help="per-run wall-time cap (seconds, time-to-target mode)",
    )
    parser.add_argument(
        "--tick",
        type=float,
        default=TICK_S,
        help="operator decision cadence in seconds (lower = more responsive)",
    )
    parser.add_argument(
        "--mode",
        default="time-to-target",
        choices=["time-to-target", "deadline-yield"],
        help="objective: time-to-target (wall-clock to Nth lead, lower=better) "
        "or deadline-yield (leads within a fixed window, higher=better)",
    )
    parser.add_argument(
        "--deadline",
        type=float,
        default=DEADLINE_S,
        help="fixed wall-clock window in seconds (deadline-yield mode)",
    )
    args = parser.parse_args()
    RUN_TIMEOUT_S = args.timeout
    TICK_S = args.tick
    MODE = args.mode
    DEADLINE_S = args.deadline
    asyncio.run(run_benchmark(args.config, args.runs, args.out, args.policies))
