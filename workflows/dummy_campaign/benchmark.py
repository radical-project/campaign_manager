#!/usr/bin/env python3
"""
ADR policy benchmark for the dummy minimization campaign.

Runs the two-stage search → refine campaign under multiple ADR scheduling
policies and compares how quickly each finds low-scoring candidates.

Because both stages use the same ``DummyWorkflow`` class, all scoring state
lives in class-level variables (``_best_score``, ``_n_evaluated``) that are
reset between runs so results are independent.

Design
------
  search: 50 replicas generate random scores; candidates < refine_threshold
          trigger a refine replica.
  refine: improves the upstream score; campaign stops at campaign_target hits.

Policies compared:
  none    — static priorities; search (priority 10) always wins scheduling ties.
  rule    — DownstreamFirstPolicy: when refine has pending replicas it boosts
            refine priority above search, reducing queue buildup.
  bandit  — BanditSchedulingPolicy: learns the priority ordering via Thompson
            sampling; converges to rule-like behaviour within a few cycles.

Primary metrics:
  best_score     — lowest refined score achieved (lower is better)
  n_evaluated    — total search + refine replicas completed
  refine_done    — refine replicas that finished
  wall_time_s    — wall-clock seconds for the full campaign

Usage
-----
    # from dummy_campaign/ directory:
    python benchmark.py --config config.yaml --policies none rule bandit --runs 5

    # Quick single run per policy:
    python benchmark.py --runs 1
"""

import argparse
import asyncio
import copy
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

from src.utils.workflow import load_config  # noqa: E402

RUN_TIMEOUT_S = 120.0  # safety cap per run (campaign completes well before this)
TICK_S = 0.5  # ADR decision cadence

ALL_POLICIES = ["none", "rule", "bandit"]


async def _run_once(config: dict, policy_kind: str, seed_offset: int) -> dict:
    """Run one campaign under the given policy; return metrics dict."""
    import copy as _copy

    from dummy_workflow import DummyWorkflow
    from radical.asyncflow import WorkflowEngine
    from rhapsody.backends import ConcurrentExecutionBackend
    from run_campaign import _build_adr_operator, _build_registry

    from src.campaign import AsyncCampaignManager as CampaignManager

    # Reset per-campaign shared state before each run.
    DummyWorkflow.reset_state()

    cfg = _copy.deepcopy(config)

    backend = await ConcurrentExecutionBackend()
    asyncflow = await WorkflowEngine.create(backend)

    registry = _build_registry(cfg)
    cm = CampaignManager.from_config(cfg, registry, asyncflow=asyncflow)

    adr_cfg = dict(cfg.get("cm", {}).get("adr", {}))
    if policy_kind == "bandit":
        adr_cfg["seed"] = adr_cfg.get("seed", 0) + seed_offset
    operator, tick = _build_adr_operator(cm, asyncflow, adr_cfg, policy_override=policy_kind)

    dnf = False
    try:
        await cm.start()
        if operator is not None:
            # Drive operator loop alongside cm.wait() — mirrors esm2 benchmark.
            async def _drive():
                async for _ in operator.run():
                    await asyncio.sleep(tick)

            drive_task = asyncio.ensure_future(_drive())
            try:
                finished = await cm.wait(timeout=RUN_TIMEOUT_S)
                dnf = not finished
            finally:
                await operator.shutdown()
                if not drive_task.done():
                    drive_task.cancel()
                    try:
                        await drive_task
                    except asyncio.CancelledError:
                        pass
        else:
            finished = await cm.wait(timeout=RUN_TIMEOUT_S)
            dnf = not finished
    finally:
        await cm.close()
        await asyncflow.shutdown()

    gs = cm.status()["groups"]
    search_done = gs.get("search", {}).get("replicas_finished", 0)
    refine_done = gs.get("refine", {}).get("replicas_finished", 0)

    return {
        "policy": policy_kind,
        "best_score": DummyWorkflow._best_score,
        "n_evaluated": DummyWorkflow._n_evaluated,
        "search_done": search_done,
        "refine_done": refine_done,
        "dnf": dnf,
    }


async def run_benchmark(config_path: str, n_runs: int, out_path: str, policies: list[str]) -> None:
    config = load_config(config_path)
    results: dict = {}

    for policy in policies:
        print(f"\n{'=' * 55}\nPolicy: {policy}\n{'=' * 55}")
        policy_results = []
        for run_idx in range(n_runs):
            print(f"  Run {run_idx + 1}/{n_runs}...", end=" ", flush=True)
            t0 = time.time()
            try:
                m = await _run_once(copy.deepcopy(config), policy, run_idx * 100)
                elapsed = time.time() - t0
                m["wall_time_s"] = elapsed
                dnf_tag = "  [DNF]" if m.get("dnf") else ""
                print(
                    f"best={m['best_score']:.4f}  "
                    f"refine={m['refine_done']}  "
                    f"n_eval={m['n_evaluated']}  "
                    f"({elapsed:.2f}s){dnf_tag}"
                )
                policy_results.append(m)
            except Exception as exc:
                elapsed = time.time() - t0
                print(f"FAILED: {exc}")
                policy_results.append({"error": str(exc), "wall_time_s": elapsed, "policy": policy})
        results[policy] = policy_results

    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults written to {out_path}")

    # ── Summary table ─────────────────────────────────────────────────────────
    print("\n── Minimization policy comparison ──")
    print(
        f"{'policy':8s}  {'best_score':>10s}  {'refine_done':>11s}  "
        f"{'n_eval':>6s}  {'wall_time':>9s}"
    )
    print("-" * 52)
    for policy in policies:
        runs = [r for r in results.get(policy, []) if "error" not in r]
        if not runs:
            print(f"{policy:8s}  ERROR")
            continue
        avg_best = sum(r["best_score"] for r in runs) / len(runs)
        avg_ref = sum(r["refine_done"] for r in runs) / len(runs)
        avg_eval = sum(r["n_evaluated"] for r in runs) / len(runs)
        avg_wall = sum(r["wall_time_s"] for r in runs) / len(runs)
        dnf_count = sum(1 for r in runs if r.get("dnf"))
        dnf_tag = f"  [{dnf_count} DNF]" if dnf_count else ""
        print(
            f"{policy:8s}  {avg_best:>10.4f}  {avg_ref:>11.1f}  "
            f"{avg_eval:>6.0f}  {avg_wall:>8.2f}s{dnf_tag}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dummy minimization — ADR policy benchmark")
    parser.add_argument(
        "--config", default="config.yaml", help="Campaign config YAML (default: config.yaml)"
    )
    parser.add_argument("--runs", type=int, default=5, help="Runs per policy (default: 5)")
    parser.add_argument(
        "--out",
        default="benchmark_results.json",
        help="Output JSON path (default: benchmark_results.json)",
    )
    parser.add_argument(
        "--policies",
        nargs="+",
        default=ALL_POLICIES,
        choices=ALL_POLICIES,
        help="Policies to benchmark (default: none rule bandit)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=RUN_TIMEOUT_S,
        help=f"Per-run timeout in seconds (default: {RUN_TIMEOUT_S})",
    )
    parser.add_argument(
        "--tick",
        type=float,
        default=TICK_S,
        help=f"ADR decision cadence in seconds (default: {TICK_S})",
    )
    args = parser.parse_args()

    RUN_TIMEOUT_S = args.timeout
    TICK_S = args.tick

    asyncio.run(run_benchmark(args.config, args.runs, args.out, args.policies))
