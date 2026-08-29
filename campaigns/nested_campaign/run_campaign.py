#!/usr/bin/env python3
"""Nested-loop campaign demo runner.

Demonstrates sequential outer rounds where each round runs an inner
``sim → analysis`` loop, tightening the analysis score gate cutoff between
rounds using the best score reported by the previous inner loop.

ADR concepts demonstrated:
  - Hierarchical child.start() / SNAPSHOT messaging (parent reads
    child's best_score_p50 via snapshot.objectives each cycle)
  - Fresh child operator per round (clean state.objectives per round)
  - set_score_cutoff() between rounds (inter-round information passing)
  - view.reactivate() before triggering new replicas each round

Usage
-----
    cd campaigns/nested_campaign
    python run_campaign.py
    python run_campaign.py --n-rounds 4 --n-sim 30 --tick 0.3
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from pathlib import Path

# Make src/ importable from any working directory.
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

import argparse  # noqa: E402

from nested_operator import OuterOperator, OuterPolicy  # noqa: E402
from nested_workflow import AnalysisWorkflow, SimWorkflow  # noqa: E402

from src.campaign import AsyncCampaignManager as CampaignManager  # noqa: E402
from src.campaign import enable_logging  # noqa: E402

_log = logging.getLogger(__name__)


async def main(
    n_rounds: int      = 3,
    n_per_round: int   = 10,
    n_sim: int         = 50,
    sim_sleep_s: float = 0.05,
    analysis_sleep_s: float = 0.02,
    shrink_factor: float    = 0.85,
    cutoff_floor: float     = 0.10,
    tick_s: float           = 0.5,
) -> None:
    enable_logging(logging.INFO, configure_stack=True)

    # ── Backend + asyncflow ────────────────────────────────────────────────────
    from radical.asyncflow import WorkflowEngine
    from rhapsody.backends import ConcurrentExecutionBackend

    backend   = await ConcurrentExecutionBackend()
    asyncflow = await WorkflowEngine.create(backend)

    # ── Campaign manager ───────────────────────────────────────────────────────
    cm = CampaignManager(engine=asyncflow)

    # Both groups start empty — sims are triggered by OuterOperator.launch_inner()
    # each round; analysis replicas are triggered by SimWorkflow.on_replica_done().
    cm.register_workflow(
        "sim",
        SimWorkflow,
        replicas=0,
        concurrency_cap=8,
        config={"duration": sim_sleep_s},
    )
    cm.register_workflow(
        "analysis",
        AnalysisWorkflow,
        replicas=0,
        concurrency_cap=4,
        config={"duration": analysis_sleep_s},
    )

    # ── Score-tracking infrastructure ─────────────────────────────────────────
    # Bypassing from_config means we must wire three things manually:
    #
    #  1. Sharder — routes trigger_dependent() calls through the shard buffer so
    #     candidate scores populate shard_events (→ score_p50 in view.observe()).
    #     Without this, all triggers bypass score tracking entirely.
    #
    #  2. CandidateLog — stores per-candidate score history.  The executor reads
    #     it to inject "candidate_score" into the triggered workflow's config.
    #     Without this, AnalysisWorkflow.config["candidate_score"] is absent.
    #
    #  3. Triage (score gate) — enforces set_score_cutoff() between rounds.
    #     Without it, view.set_score_cutoff("analysis", cutoff) returns False
    #     and all sims unconditionally trigger analysis regardless of score.
    from src.campaign.candidate_log import CandidateLog  # noqa: E402
    from src.campaign.sharder import Sharder, ShardingSpec  # noqa: E402
    from src.campaign.triage import Triage  # noqa: E402

    _spec = ShardingSpec(target_size=1, min_size=1, max_size=200)
    _sharder = Sharder(name="analysis", spec=_spec)
    _sharder._log_fn = cm._log.info
    _sharder._metrics_fn = lambda sid, n, sc, pr: cm._metrics.record_shard(
        "analysis", sid, n, sc, pr
    )
    cm._sharders["analysis"] = _sharder
    cm._candidate_log = CandidateLog()
    cm._triages["analysis"] = Triage(
        stage_id="analysis",
        score_cutoff=0.0,               # open gate initially; OuterOperator tightens it
        score_cutoff_bounds=(0.0, 1.0),
        uncertainty_cutoff=1.0,
        uncertainty_cutoff_bounds=(0.0, 1.0),
    )

    # Reset workflow ClassVar state (important for benchmark isolation).
    SimWorkflow.reset_state()
    AnalysisWorkflow.reset_state()

    # ── Operator setup ─────────────────────────────────────────────────────────
    from src.campaign.adr import CampaignView, LoggingPolicy

    view  = CampaignView(cm)
    outer = OuterOperator(
        view,
        engine=asyncflow,
        n_rounds=n_rounds,
        n_per_round=n_per_round,
        n_sim=n_sim,
        shrink_factor=shrink_factor,
        cutoff_floor=cutoff_floor,
    )
    outer.policy = LoggingPolicy(OuterPolicy(outer), log_every=5)

    # ── Startup banner ─────────────────────────────────────────────────────────
    print("=" * 64)
    print("  Nested-loop campaign demo")
    print(f"  Rounds      : {n_rounds}")
    print(f"  Per round   : {n_per_round} analysis hits (stop condition)")
    print(f"  Sims/round  : {n_sim} sim replicas triggered per round")
    print(f"  Cutoff      : shrink={shrink_factor}  floor={cutoff_floor}")
    print(f"  Tick        : {tick_s}s")
    print("=" * 64)
    print("  ADR: child.start() | SNAPSHOT | set_score_cutoff | reactivate")
    print("=" * 64)

    t0 = time.monotonic()

    # ── Custom driver loop ─────────────────────────────────────────────────────
    # Cannot use run_supervised here: cm.wait() returns as soon as the first
    # round's replicas drain, before later rounds are triggered.  Instead we
    # drive the outer operator directly and wait for Decision(stop=True).
    async def _drive_outer() -> None:
        async for _snapshot in outer.run():
            await asyncio.sleep(tick_s)

    try:
        await cm.start()
        print("Campaign running …")
        await _drive_outer()
        # Outer stopped.  Wait for any trailing sim/analysis replicas to drain.
        try:
            await asyncio.wait_for(cm.wait(), timeout=120.0)
        except asyncio.TimeoutError:
            _log.warning("CM did not drain within 120 s — forcing stop")
            await cm.stop()
    finally:
        elapsed = time.monotonic() - t0
        _log.info("Shutdown  t=%.2fs — outer.shutdown()", elapsed)
        await outer.shutdown()
        _log.info("Shutdown  t=%.2fs — cm.close()", elapsed)
        await cm.close()
        _log.info("Shutdown  t=%.2fs — asyncflow.shutdown()", elapsed)
        await asyncflow.shutdown()

    # ── Summary ────────────────────────────────────────────────────────────────
    elapsed = time.monotonic() - t0
    gs = cm.status()["groups"]

    print("\n── Nested-loop campaign complete ──────────────────────────────")
    for r in range(1, n_rounds + 1):
        key   = f"round_{r}_best"
        score = outer.state.artifacts.get(key, None)
        label = f"{score:.4f}" if score is not None else "n/a"
        print(f"  Round {r} best_p50   = {label}")

    print(f"  Total sims done     = {SimWorkflow._total_sims}")
    print(f"  Total analysis done = {AnalysisWorkflow._total_hits}")
    print(f"  Analysis best score = {AnalysisWorkflow._best_score:.4f}")
    print(f"  Wall-clock time     = {elapsed:.2f}s")
    print(f"  CM groups           = sim:{gs.get('sim',{}).get('replicas_finished',0)}"
          f"  analysis:{gs.get('analysis',{}).get('replicas_finished',0)}")
    print("─" * 64)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Nested-loop campaign demo")
    parser.add_argument("--n-rounds",      type=int,   default=3,    help="number of outer rounds")
    parser.add_argument("--n-per-round",   type=int,   default=10,   help="analysis hits per round")
    parser.add_argument("--n-sim",         type=int,   default=50,   help="sim replicas per round")
    parser.add_argument("--sim-sleep",     type=float, default=0.05, help="sim replica sleep (s)")
    parser.add_argument("--analysis-sleep",type=float, default=0.02, help="analysis sleep (s)")
    parser.add_argument("--shrink",        type=float, default=0.85, help="score cutoff shrink factor")
    parser.add_argument("--floor",         type=float, default=0.10, help="minimum score cutoff")
    parser.add_argument("--tick",          type=float, default=0.5,  help="ADR tick interval (s)")
    args = parser.parse_args()

    asyncio.run(main(
        n_rounds=args.n_rounds,
        n_per_round=args.n_per_round,
        n_sim=args.n_sim,
        sim_sleep_s=args.sim_sleep,
        analysis_sleep_s=args.analysis_sleep,
        shrink_factor=args.shrink,
        cutoff_floor=args.floor,
        tick_s=args.tick,
    ))
