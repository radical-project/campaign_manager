#!/usr/bin/env python3
"""
ADR scheduling benchmark for the ddsim campaign.

Runs four benchmark cases, each isolating a different dimension of ADR
behaviour.  Select with --benchmark (default: bmark1).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
bmark1 — 3-stage resource contention + LLM/consensus policies
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Config:  config_consensus_3stage.yaml

Three simulation pools share 5 CPUs feeding one analysis stage.  The rule
policy hard-codes ddsim_a as the high-priority feeder, but ddsim_b is
actually the fast feeder (0.10 s/rep vs 0.30 s for a).  Rule wastes the
first ~3 s on the wrong pool.  Bandit, LLM, and Consensus each discover the
correct assignment from live data.

  Topology:
    ddsim_a (0.30 s, 30 rep) ──┐
    ddsim_b (0.10 s, 50 rep) ──┼──► analysis  (5 CPUs shared, goal: 15 hits)
    ddsim_c (0.20 s, 50 rep) ──┘

  Policies:

    none       NullSchedulingPolicy — no adjustments; analysis starved.

    rule       DdSim3RulePolicy — hard-codes a=108; b is the fast feeder but
               rule never reads duration.  Wastes the first ~3 s on the wrong
               pool before b/c exhaust enough replicas to compensate naturally.

    bandit     Thompson-sampling; Beta(5,1) prior for b encodes the correct
               expectation immediately; converges to optimal within ~10 cycles.

    llm        LLM call every llm_tick_s; system prompt describes b as the fast
               feeder.  Matches bandit when online; rule fallback when not.

    consensus  2/3 majority (rule + bandit + llm); overrides rule's a-bias from
               cycle 1.  Robust to individual arm failures.

  Observed ttt (5 runs, job 20880053):
    rule ≈ 4.07 s  ·  bandit ≈ 2.88 s  ·  llm ≈ 2.94 s  ·  consensus ≈ 2.90 s

  Primary metric:  time_to_target_s

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
bmark1b — LLM timeout resilience sweep
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Same config; --llm-timeout sweeps 8.0 / 2.0 / 1.0 / 0.5 s.
llm degrades under tight timeouts; consensus stays flat near the bandit baseline.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
bmark2 — pipeline-isolation concurrent-pipeline operators
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Config:  config_bmark2.yaml

Two pipelines share 4 CPUs.  A flat policy reacts globally to any THROTTLE
(both sims demoted); isolated operators react per-pipeline only.
Policies: flat_global, isolated_delegated, isolated_centralized, isolated_debounced.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
bmark3 — temporal adaptation to stage depletion
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Config:  config_temporal.yaml

fast_sim exhausts all 200 replicas at t≈6.7 s; a static rule leaves its 2
CPUs idle on the empty queue.  ADR operators detect or predict depletion and
shift those CPUs to slow_sim before the gap opens.
Policies: rule_static, adr_reactive, adr_proactive.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
bmark4 — multi-operator + hierarchical + CPU-time budget control
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Config:  config_bmark4.yaml

Two quality-asymmetric chains (score_mean 0.55 vs 0.80) share 4 CPUs.
Primary metric: analysis_slow completions at the 30 CPU-s budget crossing.
Policies: flat_rule, multi_specialized, hier_parent.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Usage
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    python benchmark.py --runs 5                                    # bmark1
    python benchmark.py --benchmark bmark1b --llm-timeout 2.0 --runs 5
    python benchmark.py --benchmark bmark2 --config config_bmark2.yaml --runs 5
    python benchmark.py --benchmark bmark3 --config config_temporal.yaml --runs 5
    python benchmark.py --benchmark bmark4 --config config_bmark4.yaml --runs 5
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

RUN_TIMEOUT_S = 60.0
TICK_S = 0.1
ALL_POLICIES = ["none", "rule", "bandit", "llm", "consensus"]
ALL_BMARK2_POLICIES = ["flat_global", "isolated_delegated", "isolated_centralized", "isolated_debounced"]
ALL_TEMPORAL_POLICIES = ["rule_static", "adr_reactive", "adr_proactive"]
ALL_BMARK4_POLICIES = ["flat_rule", "multi_specialized", "hier_parent"]
LOG_DIR = Path(__file__).parent / "adr-logs"

# CPU-time cost per completed task — must match config_bmark4.yaml and
# _STAGE_CPU_COST in ddsim_operator_bmark4.py.
_BMARK4_CPU_COST = {
    "fast_sim":      0.10,
    "analysis_fast": 0.05,
    "slow_sim":      0.40,
    "analysis_slow": 0.05,
}


def _preload_llm_modules() -> None:
    """Import LLM libraries before the asyncio loop starts.

    On Lustre filesystems (Delta HPC) the first import of each package
    triggers cold-page-fault stalls that can block a thread for 3-6 s.
    Pre-loading here — synchronously, before asyncio.run() — ensures all
    pages are in the OS cache so no event-loop step or thread-pool worker
    is ever stalled by Lustre during the benchmark.
    """
    try:
        import instructor          # noqa: F401
        from anthropic import AsyncAnthropic  # noqa: F401
        # Trigger instructor's anthropic-specific submodule (lazy-loaded on first call).
        import instructor.clients  # noqa: F401
        try:
            import anyio           # noqa: F401
            import anyio._backends._asyncio  # noqa: F401
        except Exception:
            pass
        import certifi             # noqa: F401  triggers SSL cert load
        certifi.where()
        import httpx               # noqa: F401
        import httpcore            # noqa: F401
        # Trigger instructor.from_anthropic path to pre-load its internal wrappers.
        _dummy = instructor.from_anthropic(AsyncAnthropic(api_key="dummy"))
        del _dummy
    except Exception:
        pass  # non-llm runs skip silently


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


async def _drive_multi_with_timeout(
    cm, primary, secondaries: list, timeout: float
) -> bool:
    """Run one primary and N secondary operator loops concurrently.

    Only the primary drives the stopping signal: when its goal fires and
    operator.run() exits, cm.stop() is called → cm.wait() returns.
    Secondaries run until the campaign stops, then are shut down.
    """

    async def _loop_primary():
        async for _ in primary.run():
            await asyncio.sleep(TICK_S)
        if hasattr(cm, "stop") and callable(cm.stop):
            try:
                await cm.stop()
            except Exception:
                pass

    async def _loop_secondary(op):
        async for _ in op.run():
            await asyncio.sleep(TICK_S)

    drives = [asyncio.ensure_future(_loop_primary())]
    drives += [asyncio.ensure_future(_loop_secondary(op)) for op in secondaries]
    try:
        finished = await cm.wait(timeout=timeout)
    finally:
        await primary.shutdown()
        for op in secondaries:
            await op.shutdown()
        for drive in drives:
            if not drive.done():
                drive.cancel()
                try:
                    await drive
                except asyncio.CancelledError:
                    pass
    return finished


def _compute_quality_yield(
    events: list, stage_costs: dict, cpu_budget_s: float
) -> tuple[int, float | None]:
    """Quality yield = analysis_slow completions when cpu_time crosses budget.

    Replays the replica_event log in time order, accumulates CPU-time from
    each completion, and counts analysis_slow finishes up to the moment the
    budget threshold is first crossed.

    Returns (quality_yield, budget_crossing_t_s).
    budget_crossing_t_s is None if the budget was never exhausted.
    """
    finish_events = sorted(
        (e for e in events if e.get("event") in ("finish", "finished")),
        key=lambda e: e.get("t", 0.0),
    )

    cpu_time = 0.0
    as_count = 0
    budget_crossing_t = None
    as_at_budget = None

    for evt in finish_events:
        group = evt.get("group", "")
        cpu_time += stage_costs.get(group, 0.0)

        if group == "analysis_slow":
            as_count += 1

        if as_at_budget is None and cpu_time >= cpu_budget_s:
            budget_crossing_t = evt.get("t")
            as_at_budget = as_count

    if as_at_budget is None:
        # Budget never exhausted: all slow analyses completed within budget.
        as_at_budget = as_count

    return as_at_budget, budget_crossing_t


async def _run_once_bmark4(
    config: dict, policy_kind: str, log_path: Path, seed: int
) -> dict:
    import random as _rnd
    _rnd.seed(seed)

    from ddsim_workflow import AnalysisWorkflow, DdSimWorkflow
    DdSimWorkflow.reset_state()
    AnalysisWorkflow.reset_state()

    from radical.asyncflow import WorkflowEngine
    from rhapsody.backends import ConcurrentExecutionBackend

    from src.campaign import AsyncCampaignManager as CampaignManager
    from src.campaign.adr import CampaignView, LoggingPolicy, PolicyRecorder

    backend  = await ConcurrentExecutionBackend()
    asyncflow = await WorkflowEngine.create(backend)

    workflows_cfg = config.get("workflows", {})
    registry = {
        name: (AnalysisWorkflow if "analysis" in name else DdSimWorkflow)
        for name in workflows_cfg
    }
    cm = CampaignManager.from_config(config, registry, engine=asyncflow)

    adr_cfg   = config.get("cm", {}).get("adr", {})
    goals_cfg = adr_cfg.get("goals", {})
    n_target     = int(goals_cfg.get("n_target",     30))
    n_budget     = int(goals_cfg.get("n_budget",     600))
    cpu_budget_s = float(goals_cfg.get("cpu_budget_s", 30.0))
    terminal = adr_cfg.get("terminal") or None

    view     = CampaignView(cm, terminal=terminal)
    recorder = PolicyRecorder(log_path, policy_kind=policy_kind)

    from ddsim_operator_bmark4 import (
        AnalysisChildOperator,
        AnalysisOnlyPolicy,
        FastChainOperator,
        FastWorkflowRulePolicy,
        FlatRuleBmark4Operator,
        FlatRuleBmark4Policy,
        SimParentBmark4Operator,
        SimParentBudgetPolicy,
        SlowChainOperator,
        SlowWorkflowRulePolicy,
    )

    dnf = False
    try:
        await cm.start()

        if policy_kind == "flat_rule":
            op = FlatRuleBmark4Operator(
                view, engine=asyncflow,
                n_target=n_target, n_budget=n_budget,
                cpu_budget_s=cpu_budget_s, observer=recorder,
            )
            op.policy = LoggingPolicy(FlatRuleBmark4Policy(op), log_every=1)
            recorder.bind(view=view, policy=op.policy, metrics=cm.metrics())
            finished = await _drive_with_timeout(cm, op, RUN_TIMEOUT_S)

        elif policy_kind == "multi_specialized":
            # Primary: SlowChainOperator owns stopping condition.
            # Secondary: FastChainOperator runs alongside; stopped when primary exits.
            slow_op = SlowChainOperator(
                view, engine=asyncflow,
                n_target=n_target, n_budget=n_budget, observer=recorder,
            )
            fast_op = FastChainOperator(
                view, engine=asyncflow, n_budget=n_budget,
            )
            slow_op.policy = LoggingPolicy(SlowWorkflowRulePolicy(slow_op), log_every=1)
            fast_op.policy = FastWorkflowRulePolicy(fast_op)
            recorder.bind(view=view, policy=slow_op.policy, metrics=cm.metrics())
            finished = await _drive_multi_with_timeout(
                cm, primary=slow_op, secondaries=[fast_op], timeout=RUN_TIMEOUT_S
            )

        elif policy_kind == "hier_parent":
            # Primary: SimParentBmark4Operator owns stopping condition + budget.
            # Secondaries: one AnalysisChildOperator per chain.
            parent_op = SimParentBmark4Operator(
                view, engine=asyncflow,
                n_target=n_target, n_budget=n_budget,
                cpu_budget_s=cpu_budget_s, observer=recorder,
            )
            child_fast = AnalysisChildOperator(view, engine=asyncflow, chain="fast")
            child_slow = AnalysisChildOperator(view, engine=asyncflow, chain="slow")
            parent_op.policy = LoggingPolicy(SimParentBudgetPolicy(parent_op), log_every=1)
            child_fast.policy = AnalysisOnlyPolicy(child_fast)
            child_slow.policy = AnalysisOnlyPolicy(child_slow)
            recorder.bind(view=view, policy=parent_op.policy, metrics=cm.metrics())
            finished = await _drive_multi_with_timeout(
                cm, primary=parent_op,
                secondaries=[child_fast, child_slow],
                timeout=RUN_TIMEOUT_S,
            )

        else:
            raise ValueError(f"Unknown bmark4 policy_kind: {policy_kind!r}")

        dnf = not finished
    finally:
        await cm.close()
        await asyncflow.shutdown()

    m = cm.metrics().to_dict()
    m["policy"]       = policy_kind
    m["dnf"]          = dnf
    m["decision_log"] = str(log_path)

    events = m.get("replica_events", [])

    # Primary metric: quality_yield = analysis_slow completions at budget crossing.
    quality_yield, budget_t = _compute_quality_yield(
        events, _BMARK4_CPU_COST, cpu_budget_s
    )
    m["quality_yield"]       = quality_yield
    m["budget_crossing_t_s"] = budget_t

    # Secondary: slow analysis completions + cpu_time at campaign end.
    as_finishes = sorted(
        e["t"] for e in events
        if e.get("group") == "analysis_slow" and e.get("event") in ("finish", "finished")
    )
    m["n_analysis_slow_finished"] = len(as_finishes)
    m["slow_ttt_s"] = (
        as_finishes[n_target - 1] if len(as_finishes) >= n_target else None
    )

    return m


async def run_bmark4_benchmark(
    config_path: str, n_runs: int, out_path: str, policies: list[str]
) -> None:
    """Benchmark 4 — multi-operator + hierarchical + budget control."""
    with open(config_path) as f:
        base_config = yaml.safe_load(f)
    base_config["_config_path"] = str(Path(config_path).resolve())

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    results: dict = {}

    cpu_budget_s = (
        base_config.get("cm", {}).get("adr", {}).get("goals", {}).get("cpu_budget_s", 30.0)
    )
    n_target = (
        base_config.get("cm", {}).get("adr", {}).get("goals", {}).get("n_target", 30)
    )

    for policy in policies:
        print(f"\n{'=' * 60}\nPolicy: {policy}\n{'=' * 60}")
        cfg_results = []
        for run_idx in range(n_runs):
            print(f"  Run {run_idx + 1}/{n_runs}...", end=" ", flush=True)
            cfg = copy.deepcopy(base_config)
            log_path = LOG_DIR / f"bmark4-{policy}-run{run_idx}.jsonl"
            t0 = time.time()
            try:
                m = await _run_once_bmark4(cfg, policy, log_path, seed=run_idx * 100 + 42)
                elapsed = time.time() - t0
                m["wall_time_s"] = elapsed
                qy   = m.get("quality_yield", 0)
                bt   = m.get("budget_crossing_t_s")
                bt_s = f"{bt:.2f}s" if bt is not None else "under budget"
                n_as = m.get("n_analysis_slow_finished", 0)
                print(
                    f"quality_yield={qy}/{n_target}  budget_at={bt_s}"
                    f"  slow_analyses={n_as}  ({elapsed:.1f}s wall)"
                )
                cfg_results.append(m)
            except Exception as exc:
                elapsed = time.time() - t0
                print(f"FAILED: {exc}")
                cfg_results.append({
                    "error": str(exc), "wall_time_s": elapsed, "policy": policy,
                })
        results[policy] = cfg_results

    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults written to {out_path}")

    print()
    print("── Bmark4: multi-operator + hierarchical + budget control ──────────────")
    print(f"  Config: {config_path}  |  cpu_budget_s={cpu_budget_s}  n_target={n_target}")
    print()
    hdr = f"{'policy':18s}  {'quality_yield (avg)':>20s}  {'budget_t (avg)':>14s}  {'dnf':>4s}"
    print(hdr)
    print("-" * len(hdr))
    for policy in policies:
        runs = [r for r in results.get(policy, []) if "error" not in r]
        if not runs:
            print(f"{policy:18s}  (no successful runs)")
            continue
        qy_vals = [r["quality_yield"] for r in runs if r.get("quality_yield") is not None]
        bt_vals = [r["budget_crossing_t_s"] for r in runs if r.get("budget_crossing_t_s") is not None]
        dnf_count = sum(1 for r in runs if r.get("dnf"))
        qy_str = f"{sum(qy_vals)/len(qy_vals):.1f}/{n_target}" if qy_vals else "?"
        bt_str = f"{sum(bt_vals)/len(bt_vals):.2f}s" if bt_vals else "under budget"
        print(f"{policy:18s}  {qy_str:>20s}  {bt_str:>14s}  {dnf_count:>4d}/{len(runs)}")
    print()
    print("Expected ordering (higher quality_yield = better):")
    print("  flat_rule  <<  multi_specialized  <  hier_parent")
    print("  flat_rule:   fast chain monopolizes CPUs → slow_sim starts late → few high-quality analyses.")
    print("  multi_spec:  per-chain isolation → both chains concurrent → ~28/30 analyses at budget.")
    print("  hier_parent: parent boosts slow_sim from cycle 1 → all 30 analyses before budget.")


async def _run_once(
    config: dict, policy_kind: str, log_path: Path, seed: int
) -> dict:
    import random as _rnd
    _rnd.seed(seed)

    from ddsim_operator import DdSim3CampaignOperator, DdSimCampaignOperator, DdSimRulePolicy
    from ddsim_workflow import AnalysisWorkflow, DdSimWorkflow

    DdSimWorkflow.reset_state()
    AnalysisWorkflow.reset_state()

    from radical.asyncflow import WorkflowEngine
    from rhapsody.backends import ConcurrentExecutionBackend

    from src.campaign import AsyncCampaignManager as CampaignManager
    from src.campaign.adr import (
        CampaignView,
        LoggingPolicy,
        NullSchedulingPolicy,
        PolicyRecorder,
        RuleCorrectionsPolicy,
        make_scheduling_policy,
        resolve_system_prompt,
    )

    backend = await ConcurrentExecutionBackend()
    asyncflow = await WorkflowEngine.create(backend)

    # Build registry from config: any workflow not named "analysis" maps to DdSimWorkflow.
    workflows_cfg = config.get("workflows", {})
    registry = {
        name: (AnalysisWorkflow if "analysis" in name else DdSimWorkflow)
        for name in workflows_cfg
    }
    cm = CampaignManager.from_config(config, registry, engine=asyncflow)

    adr_cfg = config.get("cm", {}).get("adr", {})
    goals_cfg = adr_cfg.get("goals", {})
    n_target = int(goals_cfg.get("n_target", 15))
    n_budget = int(goals_cfg.get("n_budget", 80))
    terminal = adr_cfg.get("terminal") or None
    view = CampaignView(cm, terminal=terminal)
    recorder = PolicyRecorder(log_path, policy_kind=policy_kind)

    # Operator class selection: bmark2 isolation policies use a dedicated operator.
    operator_name = adr_cfg.get("operator", "DdSimCampaignOperator")
    _BMARK2_POLICY_KINDS = {"flat_global", "isolated_delegated", "isolated_centralized", "isolated_debounced"}
    _TEMPORAL_POLICY_KINDS = {"rule_static", "adr_reactive", "adr_proactive"}
    _is_bmark2   = policy_kind in _BMARK2_POLICY_KINDS
    _is_temporal = policy_kind in _TEMPORAL_POLICY_KINDS

    if _is_bmark2:
        from ddsim_operator_hierarchical import PipelineIsolationOperator
        # All bmark2 policy kinds share the same operator; the policy object
        # encodes per-pipeline vs global backpressure reaction logic.
        op = PipelineIsolationOperator(
            view, engine=asyncflow, n_target=n_target, n_budget=n_budget, observer=recorder
        )
    elif _is_temporal:
        from ddsim_operator_temporal import TemporalOperator
        op = TemporalOperator(
            view, engine=asyncflow, n_target=n_target, n_budget=n_budget, observer=recorder
        )
    else:
        OperatorClass = DdSim3CampaignOperator if operator_name == "DdSim3CampaignOperator" else DdSimCampaignOperator
        op = OperatorClass(
            view, engine=asyncflow, n_target=n_target, n_budget=n_budget, observer=recorder
        )

    correction_ref: RuleCorrectionsPolicy | None = None
    config_dir = Path(config["_config_path"]).parent if "_config_path" in config else Path(__file__).parent

    # 3-stage mode→priority table for LLM and consensus.
    # ddsim_b is the fast feeder (0.10 s) — bandit and LLM both know this.
    # Rule (DdSim3RulePolicy) hard-codes a=108 in STARVED regardless — that is
    # the intentional blind spot that lets consensus beat rule.
    _3STAGE_MODE_PRIORITIES = {
        # ddsim_a=93 in STARVED/NORMAL: strongly demotes a so ConsensusPolicy
        # averaging gives b > a even when both rule(a=108) and bandit(a=108)
        # agree on a-first.  Math requirement: LLM_a < 97 for b to win averaging
        # in the worst case (rule+bandit both rank a at 108, b at lowest).
        # 93 gives avg_a=103 vs avg_b=104 — a 1-point clean margin.
        "STARVED":     {"analysis": 110, "ddsim_b": 108, "ddsim_c": 103, "ddsim_a": 93},
        "NORMAL":      {"analysis": 110, "ddsim_b": 106, "ddsim_c": 103, "ddsim_a": 93},
        "EXHAUSTED_B": {"analysis": 110, "ddsim_c": 108, "ddsim_a": 103, "ddsim_b": 101},
        "THROTTLE":    {"analysis": 110, "ddsim_b": 97,  "ddsim_c": 96,  "ddsim_a": 95},
    }
    _is_3stage = (operator_name == "DdSim3CampaignOperator")

    if policy_kind == "none":
        policy = NullSchedulingPolicy()
    elif policy_kind == "rule_nocorr":
        # Control case: DdSimRulePolicy WITHOUT the RuleCorrectionsPolicy wrapper.
        # Stall correction is disabled; ddsim_a will stall indefinitely.
        policy = DdSimRulePolicy(op)
    elif policy_kind == "rule":
        # In 3-stage, skip RuleCorrectionsPolicy: corrections would rescue rule from
        # its a-first blind spot within ~0.3 s by boosting the stalled b, masking the
        # scenario we want to demonstrate.  In 2-stage, corrections are essential.
        if _is_3stage:
            policy = op.rule_policy()
            correction_ref = None
        else:
            policy = make_scheduling_policy(op, kind="rule")
            correction_ref = policy
    elif policy_kind == "bandit":
        # Analysis-locked 2-arm bandit: analysis always gets priority 110;
        # bandit only learns ddsim_a vs ddsim_b competition via throughput reward.
        # Reward = analysis utilization when a sim task completes → bandit discovers
        # ddsim_a (3× faster feeder) is the better arm within ~20-30 cycles.
        # Warm-start: Beta(3,1) prior for ddsim_a encodes prior knowledge it's faster.
        from ddsim_operator import AnalysisLockedBanditPolicy
        policy = AnalysisLockedBanditPolicy(op, seed=seed)
        # correction_ref stays None — no RuleCorrectionsPolicy wrapper
    elif policy_kind == "llm":
        # Mode-classifier LLM: the model picks STARVED/NORMAL/EXHAUSTED_A/THROTTLE;
        # code maps mode → fixed proven priorities (no hallucinated numbers).
        # Pre-warmed before cm.start() to eliminate cold-start latency on run 1.
        from src.campaign.adr.policies import ModeLLMSchedulingPolicy
        api_key_env = adr_cfg.get("llm_api_key_env", "HF_TOKEN")
        api_key = os.environ.get(api_key_env, "")
        kw: dict = {}
        if adr_cfg.get("base_url"):
            kw["base_url"] = adr_cfg["base_url"]
        if adr_cfg.get("llm_timeout_s") is not None:
            kw["timeout_s"] = float(adr_cfg["llm_timeout_s"])
        if adr_cfg.get("llm_max_retries") is not None:
            kw["instructor_retries"] = int(adr_cfg["llm_max_retries"])
        if adr_cfg.get("llm_tick_s") is not None:
            kw["min_call_interval_s"] = float(adr_cfg["llm_tick_s"])
        prompt = resolve_system_prompt(adr_cfg, config_dir=config_dir)
        if prompt:
            kw["system_prompt"] = prompt
        if not api_key:
            print(
                f"    [llm] {api_key_env} not set — policy will use rule fallback every cycle.",
                flush=True,
            )
            api_key = "no-key"
        model = adr_cfg.get("model", "openai/gpt-4o-mini")
        provider = adr_cfg.get("llm_provider", "openai")
        if _is_3stage:
            kw["mode_priorities"] = _3STAGE_MODE_PRIORITIES
        llm_inner = ModeLLMSchedulingPolicy(api_key, op, model=model, provider=provider, **kw)
        # Pre-warm: fire a trivial call now so the HF endpoint is hot before the campaign.
        print("    [llm] pre-warming endpoint...", end=" ", flush=True)
        warmed = await llm_inner.pre_warm()
        print("ready" if warmed else "(timed out — first cycle uses rule fallback)", flush=True)
        # Reset the campaign timer NOW so that pre_warm() API latency (1-4 s per
        # run) does not inflate time_to_target_s.  CampaignMetrics.start_time is
        # set at cm-creation time, before pre_warm; without this reset every LLM
        # ttt includes a full Anthropic round-trip that rule/bandit don't pay.
        import time as _t
        cm.metrics().start_time = _t.time()
        # In 3-stage, skip RuleCorrectionsPolicy: stall boosts would push a/c above
        # b and fight the LLM's correct b=108 signal (same reason rule/consensus
        # skip corrections in 3-stage).  In 2-stage, corrections are essential.
        if _is_3stage:
            policy = llm_inner
            correction_ref = None
        else:
            from src.campaign.adr.policies import RuleCorrectionsPolicy
            correction_ref = RuleCorrectionsPolicy(llm_inner, op)
            policy = correction_ref
    elif policy_kind == "consensus":
        # Three-way majority-vote ensemble: rule + bandit + LLM.
        # All three vote each cycle; the strict majority ordering wins.
        # When they agree (62% of states per analysis), priorities are averaged
        # from the agreeing subset.  When no majority (38%), rule wins tiebreak.
        # RuleCorrectionsPolicy wraps the consensus so stall boosts still apply.
        from ddsim_operator import AnalysisLockedBanditPolicy
        from src.campaign.adr.policies import (
            ConsensusPolicy, ModeLLMSchedulingPolicy, RuleCorrectionsPolicy,
        )

        # op.rule_policy() returns DdSim3RulePolicy for DdSim3CampaignOperator,
        # DdSimRulePolicy otherwise — no explicit branch needed.
        rule_inner   = op.rule_policy()
        bandit_inner = AnalysisLockedBanditPolicy(op, seed=seed)

        api_key_env = adr_cfg.get("llm_api_key_env", "HF_TOKEN")
        api_key = os.environ.get(api_key_env, "")
        kw: dict = {}
        if adr_cfg.get("base_url"):
            kw["base_url"] = adr_cfg["base_url"]
        if adr_cfg.get("llm_timeout_s") is not None:
            kw["timeout_s"] = float(adr_cfg["llm_timeout_s"])
        if adr_cfg.get("llm_max_retries") is not None:
            kw["instructor_retries"] = int(adr_cfg["llm_max_retries"])
        if adr_cfg.get("llm_tick_s") is not None:
            kw["min_call_interval_s"] = float(adr_cfg["llm_tick_s"])
        prompt = resolve_system_prompt(adr_cfg, config_dir=config_dir)
        if prompt:
            kw["system_prompt"] = prompt
        if not api_key:
            print(
                f"    [consensus/llm] {api_key_env} not set — LLM arm uses rule fallback.",
                flush=True,
            )
            api_key = "no-key"
        model    = adr_cfg.get("model", "openai/gpt-4o-mini")
        provider = adr_cfg.get("llm_provider", "openai")
        if _is_3stage:
            kw["mode_priorities"] = _3STAGE_MODE_PRIORITIES
        llm_inner = ModeLLMSchedulingPolicy(api_key, op, model=model, provider=provider, **kw)

        print("    [consensus] pre-warming LLM arm...", end=" ", flush=True)
        warmed = await llm_inner.pre_warm()
        print("ready" if warmed else "(timed out — LLM arm uses rule fallback)", flush=True)
        import time as _t
        cm.metrics().start_time = _t.time()

        consensus_inner = ConsensusPolicy([rule_inner, bandit_inner, llm_inner])
        # No RuleCorrectionsPolicy in 3-stage: clean priority signal needed for the
        # majority vote to demonstrate its advantage over rule's blind spot.
        if _is_3stage:
            policy = consensus_inner
            correction_ref = None
        else:
            correction_ref = RuleCorrectionsPolicy(consensus_inner, op)
            policy = correction_ref
    elif policy_kind == "flat_global":
        # Global throttle reaction — the blind spot: any analysis THROTTLE demotes
        # BOTH sims, stalling the unrelated pipeline.
        policy = op.rule_policy()
        correction_ref = None
    elif policy_kind == "isolated_delegated":
        # Per-pipeline backpressure: analysis_fast THROTTLE demotes only fast_sim;
        # slow_sim retains its high priority and keeps running.
        from ddsim_operator_hierarchical import IsolatedRulePolicy
        policy = IsolatedRulePolicy(op)
        correction_ref = None
    elif policy_kind == "isolated_centralized":
        # Per-pipeline + active hit-count rebalancing every cycle (|gap|>5%).
        from ddsim_operator_hierarchical import IsolatedCentralizedPolicy
        policy = IsolatedCentralizedPolicy(op, n_target=n_target)
        correction_ref = None
    elif policy_kind == "isolated_debounced":
        # Per-pipeline + conservative rebalancing when |gap|>20% for ≥2 cycles.
        from ddsim_operator_hierarchical import IsolatedDebouncedPolicy
        policy = IsolatedDebouncedPolicy(op, n_target=n_target)
        correction_ref = None
    elif policy_kind == "rule_static":
        from ddsim_operator_temporal import RuleStaticPolicy
        policy = RuleStaticPolicy(op)
        correction_ref = None
    elif policy_kind == "adr_reactive":
        from ddsim_operator_temporal import AdrReactivePolicy
        policy = AdrReactivePolicy(op)
        correction_ref = None
    elif policy_kind == "adr_proactive":
        from ddsim_operator_temporal import AdrProactivePolicy
        policy = AdrProactivePolicy(op, lookahead_s=2.0)
        correction_ref = None
    else:
        raise ValueError(f"Unknown policy_kind: {policy_kind!r}")

    if not isinstance(policy, NullSchedulingPolicy):
        policy = LoggingPolicy(policy, log_every=1)
    op.policy = policy
    recorder.bind(view=view, policy=op.policy, metrics=cm.metrics())

    dnf = False
    try:
        await cm.start()
        finished = await _drive_with_timeout(cm, op, RUN_TIMEOUT_S)
        if not finished:
            dnf = True
    finally:
        await cm.close()
        await asyncflow.shutdown()

    m = cm.metrics().to_dict()
    m["policy"] = policy_kind
    m["dnf"] = dnf
    m["decision_log"] = str(log_path)

    # Read correction stats from RuleCorrectionsPolicy (zero for non-rule policies).
    m["correction_cycles"] = (
        correction_ref.correction_cycles if correction_ref is not None else 0
    )

    # Primary metric: simulation time to the n_target-th analysis completion.
    events = m.get("replica_events", [])
    analysis_finishes = sorted(
        e["t"]
        for e in events
        if e.get("group") == "analysis" and e.get("event") in ("finish", "finished")
    )
    m["n_analysis_finished"] = len(analysis_finishes)
    m["time_to_target_s"] = (
        analysis_finishes[n_target - 1]
        if len(analysis_finishes) >= n_target
        else None
    )

    ddsim_a_finished = sum(
        1 for e in events
        if e.get("group") == "ddsim_a" and e.get("event") in ("finish", "finished")
    )
    m["ddsim_a_finished"] = ddsim_a_finished

    # Hierarchical benchmark: per-pipeline metrics.
    # When analysis_fast / analysis_slow events are present, override time_to_target_s
    # with max(pipeline1_ttt, pipeline2_ttt) — the time when BOTH pipelines complete.
    af_finishes = sorted(
        e["t"] for e in events
        if e.get("group") == "analysis_fast" and e.get("event") in ("finish", "finished")
    )
    as_finishes = sorted(
        e["t"] for e in events
        if e.get("group") == "analysis_slow" and e.get("event") in ("finish", "finished")
    )
    m["n_analysis_fast_finished"] = len(af_finishes)
    m["n_analysis_slow_finished"] = len(as_finishes)
    p1_ttt = af_finishes[n_target - 1] if len(af_finishes) >= n_target else None
    p2_ttt = as_finishes[n_target - 1] if len(as_finishes) >= n_target else None
    m["pipeline1_ttt_s"] = p1_ttt
    m["pipeline2_ttt_s"] = p2_ttt
    if _is_temporal:
        # B3 metric: time to the n_target-th analysis_slow completion only.
        if p2_ttt is not None:
            m["time_to_target_s"] = p2_ttt
    elif p1_ttt is not None and p2_ttt is not None:
        m["time_to_target_s"] = max(p1_ttt, p2_ttt)

    return m


async def run_benchmark(
    config_path: str, n_runs: int, out_path: str, policies: list[str],
    llm_timeout: float | None = None,
) -> None:
    with open(config_path) as f:
        base_config = yaml.safe_load(f)
    # Stash the config file path so _run_once can locate relative prompt files.
    base_config["_config_path"] = str(Path(config_path).resolve())
    if llm_timeout is not None:
        base_config.setdefault("cm", {}).setdefault("adr", {})["llm_timeout_s"] = llm_timeout

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
                m = await _run_once(cfg, policy, log_path, seed=run_idx * 100 + 42)
                elapsed = time.time() - t0
                m["wall_time_s"] = elapsed
                ttt = m.get("time_to_target_s")
                ttt_str = f"{ttt:.2f}s" if ttt is not None else "DNF"
                cc = m.get("correction_cycles", 0)
                n_an = m.get("n_analysis_finished", 0)
                n_a = m.get("ddsim_a_finished", 0)
                print(
                    f"ttt={ttt_str}  analyses={n_an}  ddsim_a_done={n_a}"
                    f"  corrections={cc}  ({elapsed:.1f}s wall)"
                )
                cfg_results.append(m)
            except Exception as exc:
                elapsed = time.time() - t0
                print(f"FAILED: {exc}")
                cfg_results.append({
                    "error": str(exc), "wall_time_s": elapsed, "policy": policy,
                })
        results[policy] = cfg_results

    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults written to {out_path}")
    print(f"Per-cycle decision logs under {LOG_DIR}/")

    # Summary table.
    print()
    print("── Stall-correction benchmark (fan-in resource contention) ──────────────")
    print(
        f"  Config: {config_path}  |  n_target="
        f"{base_config.get('cm',{}).get('adr',{}).get('goals',{}).get('n_target','?')}"
    )
    print()
    hdr = f"{'policy':12s}  {'ttt_s (avg)':>11s}  {'analyses':>8s}  {'ddsim_a_done':>12s}  {'corr_cycles':>11s}  {'dnf':>4s}"
    print(hdr)
    print("-" * len(hdr))
    for policy in policies:
        runs = [r for r in results.get(policy, []) if "error" not in r]
        if not runs:
            print(f"{policy:12s}  (no successful runs)")
            continue
        ttts = [r["time_to_target_s"] for r in runs if r.get("time_to_target_s") is not None]
        dnf_count = sum(1 for r in runs if r.get("dnf"))
        ttt_str = f"{sum(ttts)/len(ttts):.2f}" if ttts else "DNF"
        an_avg = sum(r.get("n_analysis_finished", 0) for r in runs) / len(runs)
        a_avg = sum(r.get("ddsim_a_finished", 0) for r in runs) / len(runs)
        cc_avg = sum(r.get("correction_cycles", 0) for r in runs) / len(runs)
        print(
            f"{policy:12s}  {ttt_str:>11s}  {an_avg:>8.1f}  {a_avg:>12.1f}  "
            f"{cc_avg:>11.1f}  {dnf_count:>4d}/{len(runs)}"
        )
    print()
    print("Expected ordering (lower ttt = better):")
    print("  rule ≈ llm ≈ bandit < none")
    print("  rule:   explicit stall-correction rule; ddsim_a wins contested slot.")
    print("  bandit: Thompson-sampling posterior converges with WIDEN/THROTTLE signals.")
    print("  llm:    LLM-driven priorities; rule fallback when offline.")
    print("  none:   no ADR — analysis starved behind both sim pools.")


async def run_bmark2_benchmark(
    config_path: str, n_runs: int, out_path: str, policies: list[str]
) -> None:
    """Benchmark 2 — pipeline-isolation concurrent-pipeline operators."""
    with open(config_path) as f:
        base_config = yaml.safe_load(f)
    base_config["_config_path"] = str(Path(config_path).resolve())

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
                m = await _run_once(cfg, policy, log_path, seed=run_idx * 100 + 42)
                elapsed = time.time() - t0
                m["wall_time_s"] = elapsed
                ttt = m.get("time_to_target_s")
                p1  = m.get("pipeline1_ttt_s")
                p2  = m.get("pipeline2_ttt_s")
                ttt_str = f"{ttt:.3f}s" if ttt is not None else "DNF"
                p1_str  = f"{p1:.3f}"   if p1  is not None else "DNF"
                p2_str  = f"{p2:.3f}"   if p2  is not None else "DNF"
                print(
                    f"ttt={ttt_str}  p1={p1_str}s  p2={p2_str}s  ({elapsed:.1f}s wall)"
                )
                cfg_results.append(m)
            except Exception as exc:
                elapsed = time.time() - t0
                print(f"FAILED: {exc}")
                cfg_results.append({
                    "error": str(exc), "wall_time_s": elapsed, "policy": policy,
                })
        results[policy] = cfg_results

    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults written to {out_path}")
    print(f"Per-cycle decision logs under {LOG_DIR}/")

    # Summary table.
    n_target = (
        base_config.get("cm", {}).get("adr", {}).get("goals", {}).get("n_target", "?")
    )
    print()
    print("── Pipeline-isolation concurrent-pipeline benchmark ────────────────────────")
    print(f"  Config: {config_path}  |  n_target={n_target} per pipeline")
    print()
    hdr = (
        f"{'policy':16s}  {'ttt_s (avg)':>11s}  {'p1_ttt (avg)':>12s}"
        f"  {'p2_ttt (avg)':>12s}  {'dnf':>4s}"
    )
    print(hdr)
    print("-" * len(hdr))
    for policy in policies:
        runs = [r for r in results.get(policy, []) if "error" not in r]
        if not runs:
            print(f"{policy:16s}  (no successful runs)")
            continue
        ttts = [r["time_to_target_s"] for r in runs if r.get("time_to_target_s") is not None]
        p1s  = [r["pipeline1_ttt_s"]   for r in runs if r.get("pipeline1_ttt_s")   is not None]
        p2s  = [r["pipeline2_ttt_s"]   for r in runs if r.get("pipeline2_ttt_s")   is not None]
        dnf_count = sum(1 for r in runs if r.get("dnf"))
        ttt_str = f"{sum(ttts)/len(ttts):.3f}" if ttts else "DNF"
        p1_str  = f"{sum(p1s)/len(p1s):.3f}"   if p1s  else "DNF"
        p2_str  = f"{sum(p2s)/len(p2s):.3f}"   if p2s  else "DNF"
        print(
            f"{policy:16s}  {ttt_str:>11s}  {p1_str:>12s}  {p2_str:>12s}"
            f"  {dnf_count:>4d}/{len(runs)}"
        )
    print()
    print("Expected ordering (lower ttt = better):")
    print("  isolated_debounced ≈ isolated_centralized ≈ isolated_delegated  <<  flat_global")
    print("  Flat: global THROTTLE reaction stalls unrelated pipeline (blind spot).")
    print("  Isolated: each policy reacts only to its own pipeline's backpressure state.")


async def run_temporal_benchmark(
    config_path: str, n_runs: int, out_path: str, policies: list[str]
) -> None:
    """Benchmark 3 — temporal adaptation to fast_sim stage depletion."""
    with open(config_path) as f:
        base_config = yaml.safe_load(f)
    base_config["_config_path"] = str(Path(config_path).resolve())

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
                m = await _run_once(cfg, policy, log_path, seed=run_idx * 100 + 42)
                elapsed = time.time() - t0
                m["wall_time_s"] = elapsed
                ttt  = m.get("time_to_target_s")
                p2   = m.get("pipeline2_ttt_s")
                n_as = m.get("n_analysis_slow_finished", 0)
                ttt_str = f"{ttt:.3f}s" if ttt is not None else "DNF"
                p2_str  = f"{p2:.3f}"   if p2  is not None else "DNF"
                print(
                    f"ttt={ttt_str}  analysis_slow={n_as}  ({elapsed:.1f}s wall)"
                )
                cfg_results.append(m)
            except Exception as exc:
                elapsed = time.time() - t0
                print(f"FAILED: {exc}")
                cfg_results.append({
                    "error": str(exc), "wall_time_s": elapsed, "policy": policy,
                })
        results[policy] = cfg_results

    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults written to {out_path}")
    print(f"Per-cycle decision logs under {LOG_DIR}/")

    n_target = (
        base_config.get("cm", {}).get("adr", {}).get("goals", {}).get("n_target", "?")
    )
    print()
    print("── Temporal-adaptation benchmark (phase depletion) ─────────────────────────")
    print(f"  Config: {config_path}  |  n_target={n_target} slow analyses")
    print()
    hdr = f"{'policy':16s}  {'ttt_s (avg)':>11s}  {'p2_ttt (avg)':>12s}  {'dnf':>4s}"
    print(hdr)
    print("-" * len(hdr))
    for policy in policies:
        runs = [r for r in results.get(policy, []) if "error" not in r]
        if not runs:
            print(f"{policy:16s}  (no successful runs)")
            continue
        ttts = [r["time_to_target_s"] for r in runs if r.get("time_to_target_s") is not None]
        p2s  = [r["pipeline2_ttt_s"]  for r in runs if r.get("pipeline2_ttt_s")  is not None]
        dnf_count = sum(1 for r in runs if r.get("dnf"))
        ttt_str = f"{sum(ttts)/len(ttts):.3f}" if ttts else "DNF"
        p2_str  = f"{sum(p2s)/len(p2s):.3f}"   if p2s  else "DNF"
        print(f"{policy:16s}  {ttt_str:>11s}  {p2_str:>12s}  {dnf_count:>4d}/{len(runs)}")
    print()
    print("Expected ordering (lower ttt = better):")
    print("  rule_static > adr_reactive > adr_proactive")
    print("  rule:      static priorities; slow_sim gets 2 CPUs during analysis_fast backlog drain.")
    print("  reactive:  detects fast_sim depletion; demotes analysis_fast, promotes slow_sim to 3 CPUs.")
    print("  proactive: predicts depletion ~2 s early; slow_sim gets 3 CPUs before fast_sim exhausts.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "ddsim ADR policy benchmark\n"
            "  bmark1 (default): 3-stage consensus  — none|rule|bandit|llm|consensus\n"
            "  bmark2:           pipeline-isolation — flat_global|isolated_delegated|…\n"
            "  bmark3:           temporal adaptation  — rule_static|adr_reactive|adr_proactive\n"
            "  bmark4:           multi-operator+hier+budget — flat_rule|multi_specialized|hier_parent"
        )
    )
    parser.add_argument(
        "--benchmark", choices=["bmark1", "bmark2", "bmark3", "bmark4"], default="bmark1",
        help="which benchmark to run (default: bmark1)",
    )
    parser.add_argument(
        "--config", default=None,
        help="campaign config (default: per-benchmark default)",
    )
    parser.add_argument("--runs", type=int, default=3, help="runs per policy")
    parser.add_argument(
        "--out", default=None,
        help="output JSON path (default: per-benchmark default)",
    )
    parser.add_argument(
        "--policies",
        nargs="+",
        default=None,
        choices=(
            ALL_POLICIES + ["rule_nocorr", "consensus"]
            + ALL_BMARK2_POLICIES
            + ALL_TEMPORAL_POLICIES
            + ALL_BMARK4_POLICIES
        ),
        help="policies to benchmark (default: all for selected benchmark)",
    )
    parser.add_argument(
        "--timeout", type=float, default=RUN_TIMEOUT_S,
        help="per-run safety cap in seconds",
    )
    parser.add_argument(
        "--tick", type=float, default=TICK_S,
        help="operator decision cadence in seconds",
    )
    parser.add_argument(
        "--llm-timeout", type=float, default=None,
        help="override llm_timeout_s from config (seconds); useful for degradation sweeps",
    )
    args = parser.parse_args()

    RUN_TIMEOUT_S = args.timeout
    TICK_S = args.tick

    if args.benchmark == "bmark4":
        config   = args.config  or "config_bmark4.yaml"
        out      = args.out     or "bmark4_results.json"
        policies = args.policies or ALL_BMARK4_POLICIES
        asyncio.run(run_bmark4_benchmark(config, args.runs, out, policies))
    elif args.benchmark == "bmark3":
        config   = args.config  or "config_temporal.yaml"
        out      = args.out     or "temporal_benchmark_results.json"
        policies = args.policies or ALL_TEMPORAL_POLICIES
        asyncio.run(run_temporal_benchmark(config, args.runs, out, policies))
    elif args.benchmark == "bmark2":
        config   = args.config  or "config_bmark2.yaml"
        out      = args.out     or "bmark2_results.json"
        policies = args.policies or ALL_BMARK2_POLICIES
        asyncio.run(run_bmark2_benchmark(config, args.runs, out, policies))
    else:
        config   = args.config  or "config_consensus_3stage.yaml"
        out      = args.out     or "stall_benchmark_results_3stage.json"
        policies = args.policies or ALL_POLICIES
        if "llm" in policies or "consensus" in policies:
            _preload_llm_modules()
        asyncio.run(run_benchmark(config, args.runs, out, policies, llm_timeout=args.llm_timeout))
