"""Multi-operator + hierarchical + budget-control operators for Benchmark 4.

Three conditions demonstrated:

  flat_rule        — FlatRuleBmark4Operator (single operator, all 4 stages).
                     Any analysis THROTTLE demotes BOTH sims — blind spot.
                     fast chain's backpressure starves slow_sim for the whole run.

  multi_specialized — FastChainOperator (secondary) + SlowChainOperator (primary).
                     Each operator owns its full chain (sim + analysis).
                     FastWorkflowRulePolicy: analysis_fast THROTTLE only demotes fast_sim.
                     SlowWorkflowRulePolicy: analysis_slow THROTTLE only demotes slow_sim.
                     No cross-chain interference; sims run concurrently.

  hier_parent      — SimParentBmark4Operator (primary)
                    + AnalysisChildOperator×2 (secondary, one per chain).
                     Parent owns fast_sim + slow_sim priorities and CPU-time budget.
                     Parent boosts slow_sim from cycle 1; at budget exhaustion
                     demotes fast_sim to near-zero to maximise remaining slow analyses.
                     Children own their analysis stage priority only.

Primary metric: quality_yield — analysis_slow completions when cumulative CPU-time
                                first crosses cpu_budget_s (30 CPU-seconds).
CPU-time approximation: Σ(stage_done × avg_duration_s) from obs dict.
avg_duration_s is the mean wall-time of finished replicas, updated each tick by the
view layer.  All stages use required_cpus=1, so avg_duration_s == CPU-s per task.
Falls back to 0 for stages with no completions yet (avg_duration_s is None until
the first task finishes).

Expected budget costs per task (config_bmark4.yaml):
  fast_sim:      ~0.10 CPU-s/task   slow_sim:      ~0.40 CPU-s/task
  analysis_fast: ~0.05 CPU-s/task   analysis_slow: ~0.05 CPU-s/task
  Total campaign: 200×0.10 + 200×0.05 + 30×0.40 + 30×0.05 ≈ 43.5 CPU-s
"""

from __future__ import annotations

import logging

from radical.adr import Decision, goals, observe
from radical.adr.goals import Goal
from radical.adr.policy.base import Policy, decide

from src.campaign.adr import CampaignOperator
from src.campaign.adr.operator import CampaignOperator as _BaseCampaignOperator

log = logging.getLogger(__name__)

# ── Priority ladder ─────────────────────────────────────────────────────────────
_P_ANALYSIS   = 110   # analysis stages: always-first
_P_SIM_BOOST  = 109   # parent-boosted sim (slow_sim always preferred)
_P_SIM_HIGH   = 108   # STARVED: analysis has no queued work
_P_SIM_NORMAL = 106   # NORMAL: analysis processing normally
_P_SIM_DEMOTE = 95    # THROTTLE: analysis backed up — yield CPUs to other chain
_P_SIM_KILLED = 10    # BUDGET_EXCEEDED: near-zero; yields all CPUs to slow chain

def _cpu_time_from_obs(obs: dict) -> float:
    """Estimate total CPU-seconds consumed from obs telemetry.

    Uses avg_duration_s (mean wall-time of finished replicas, updated each tick
    by the view layer) rather than a static cost map, so the estimate adapts to
    actual task durations under jitter.  All stages have required_cpus=1, so
    avg_duration_s equals CPU-s per task directly.
    """
    stages = obs.get("stages", {})
    total = 0.0
    for stage_data in stages.values():
        finished = stage_data.get("finished", 0)
        avg_dur  = stage_data.get("avg_duration_s") or 0.0
        total   += finished * avg_dur
    return total


# ══════════════════════════════════════════════════════════════════════════════
# CONDITION 1: flat_rule  — single operator, global backpressure
# ══════════════════════════════════════════════════════════════════════════════

class FlatRuleBmark4Operator(CampaignOperator):
    """Single-operator baseline.  Stops when analysis_slow_hits ≥ n_target.

    Exposes analysis_slow_hits and budget_exceeded to FlatRuleBmark4Policy.
    """

    def __init__(
        self, view, engine=None, *,
        n_target: int = 30,
        n_budget: int = 600,
        cpu_budget_s: float = 30.0,
        **kwargs,
    ):
        super().__init__(view, engine=engine, **kwargs)
        self.n_target = int(n_target)
        self.n_budget = int(n_budget)
        self._cpu_budget_s = float(cpu_budget_s)
        self._validate_stopping_condition()

    @goals
    def criteria(self):
        return [Goal("slow_done", "analysis_slow_hits", self.n_target - 0.5, "maximize")]

    @observe
    def extract(self, snapshot) -> dict:
        obs = _BaseCampaignOperator.extract(self, snapshot)
        stages = obs.get("stages", {})
        obs["analysis_slow_hits"] = stages.get("analysis_slow", {}).get("finished", 0)
        obs["cpu_time_s"] = _cpu_time_from_obs(obs)
        obs["budget_exceeded"] = obs["cpu_time_s"] >= self._cpu_budget_s
        return obs


class FlatRuleBmark4Policy(Policy):
    """Global backpressure — any analysis THROTTLE demotes BOTH sims.

    Blind spot: analysis_fast THROTTLE (which fires constantly because fast_sim
    produces ~30 tasks/s but analysis_fast consumes only ~20 tasks/s with cap=1)
    unnecessarily demotes slow_sim, leaving it with 0 CPUs for most of the run.
    """

    def __init__(self, op: FlatRuleBmark4Operator) -> None:
        super().__init__()
        self._act = op.get_actions()
        self._regime: str = "INIT"

    @decide
    async def run(self, obs: dict) -> Decision:
        stages = obs.get("stages", {})
        if not stages:
            return Decision()

        an_fast = stages.get("analysis_fast", {})
        an_slow = stages.get("analysis_slow", {})
        af_pend = an_fast.get("pending", 0)
        af_run  = an_fast.get("running", 0)
        as_pend = an_slow.get("pending", 0)
        as_run  = an_slow.get("running", 0)

        if af_pend > 1 or as_pend > 1:
            # THROTTLE — demote BOTH sims (blind spot: slow pays for fast's backpressure).
            self._regime = "THROTTLE"
            p_fast = _P_SIM_DEMOTE   # 95
            p_slow = _P_SIM_DEMOTE   # 95 — slow_sim unnecessarily penalised

        elif af_pend == 0 and af_run == 0 and as_pend == 0 and as_run == 0:
            # Both analysis stages starved: fast_sim wins FIFO (config priority 9 > 7).
            self._regime = "STARVED"
            p_fast = _P_SIM_HIGH     # 108
            p_slow = _P_SIM_NORMAL   # 106

        else:
            self._regime = "NORMAL"
            p_fast = _P_SIM_NORMAL   # 106
            p_slow = _P_SIM_NORMAL - 2  # 104

        return Decision(actions=[
            self._act.set_priority(stage="analysis_fast", priority=_P_ANALYSIS),
            self._act.set_priority(stage="analysis_slow", priority=_P_ANALYSIS),
            self._act.set_priority(stage="fast_sim",      priority=p_fast),
            self._act.set_priority(stage="slow_sim",      priority=p_slow),
        ])


# ══════════════════════════════════════════════════════════════════════════════
# CONDITION 2: multi_specialized — two independent chain operators
# ══════════════════════════════════════════════════════════════════════════════

class FastChainOperator(CampaignOperator):
    """Secondary operator: fast_sim + analysis_fast chain only.

    Runs alongside SlowChainOperator (primary).  Uses max_cycles as a safety
    cap — in practice the campaign stops when SlowChainOperator's goal fires.
    """

    def __init__(self, view, engine=None, *, n_budget: int = 600, **kwargs):
        super().__init__(view, engine=engine, max_cycles=10_000, **kwargs)
        self.n_budget = int(n_budget)
        self._validate_stopping_condition()

    @observe
    def extract(self, snapshot) -> dict:
        obs = _BaseCampaignOperator.extract(self, snapshot)
        return obs

    # No @goals — max_cycles=10_000 is the backstop; primary stops campaign first.


class SlowChainOperator(CampaignOperator):
    """Primary operator: slow_sim + analysis_slow chain.  Stops campaign when done.

    In multi_specialized, this is the stopping operator — its goal fires when
    analysis_slow completes n_target analyses, which calls cm.stop().
    """

    def __init__(
        self, view, engine=None, *,
        n_target: int = 30,
        n_budget: int = 600,
        **kwargs,
    ):
        super().__init__(view, engine=engine, **kwargs)
        self.n_target = int(n_target)
        self.n_budget = int(n_budget)
        self._validate_stopping_condition()

    @goals
    def criteria(self):
        return [Goal("slow_done", "analysis_slow_hits", self.n_target - 0.5, "maximize")]

    @observe
    def extract(self, snapshot) -> dict:
        obs = _BaseCampaignOperator.extract(self, snapshot)
        stages = obs.get("stages", {})
        obs["analysis_slow_hits"] = stages.get("analysis_slow", {}).get("finished", 0)
        return obs


class FastWorkflowRulePolicy(Policy):
    """Per-chain backpressure for fast_sim + analysis_fast.

    analysis_fast THROTTLE → demote fast_sim only.
    analysis_fast STARVED  → boost fast_sim (more sim work needed).
    No effect on slow chain: SlowWorkflowRulePolicy handles that independently.
    """

    def __init__(self, op: FastChainOperator) -> None:
        super().__init__()
        self._act = op.get_actions()
        self._regime: str = "INIT"

    @decide
    async def run(self, obs: dict) -> Decision:
        stages = obs.get("stages", {})
        if not stages:
            return Decision()

        an_fast = stages.get("analysis_fast", {})
        af_pend = an_fast.get("pending", 0)
        af_run  = an_fast.get("running", 0)

        if af_pend > 1:
            self._regime = "THROTTLE"
            p_fast = _P_SIM_DEMOTE   # 95: analysis_fast backing up
        elif af_pend == 0 and af_run == 0:
            self._regime = "STARVED"
            p_fast = _P_SIM_HIGH     # 108: analysis_fast has no pending work
        else:
            self._regime = "NORMAL"
            p_fast = _P_SIM_NORMAL   # 106

        return Decision(actions=[
            self._act.set_priority(stage="analysis_fast", priority=_P_ANALYSIS),
            self._act.set_priority(stage="fast_sim",      priority=p_fast),
        ])


class SlowWorkflowRulePolicy(Policy):
    """Per-chain backpressure for slow_sim + analysis_slow.

    Mirror of FastWorkflowRulePolicy for the slow chain.
    No effect on fast chain: FastWorkflowRulePolicy handles that independently.
    """

    def __init__(self, op: SlowChainOperator) -> None:
        super().__init__()
        self._act = op.get_actions()
        self._regime: str = "INIT"

    @decide
    async def run(self, obs: dict) -> Decision:
        stages = obs.get("stages", {})
        if not stages:
            return Decision()

        an_slow = stages.get("analysis_slow", {})
        as_pend = an_slow.get("pending", 0)
        as_run  = an_slow.get("running", 0)

        if as_pend > 1:
            self._regime = "THROTTLE"
            p_slow = _P_SIM_DEMOTE
        elif as_pend == 0 and as_run == 0:
            self._regime = "STARVED"
            p_slow = _P_SIM_HIGH
        else:
            self._regime = "NORMAL"
            p_slow = _P_SIM_NORMAL

        return Decision(actions=[
            self._act.set_priority(stage="analysis_slow", priority=_P_ANALYSIS),
            self._act.set_priority(stage="slow_sim",      priority=p_slow),
        ])


# ══════════════════════════════════════════════════════════════════════════════
# CONDITION 3: hier_parent — parent + two analysis-only children
# ══════════════════════════════════════════════════════════════════════════════

class AnalysisChildOperator(CampaignOperator):
    """Secondary operator: analysis stage only (no sim control).

    Parameterised by chain name ("fast" or "slow").  Runs alongside
    SimParentBmark4Operator (primary).  Its only job: keep its analysis
    stage at maximum priority so the parent's sim scheduling is not
    blocked by stale default priorities on the analysis stages.
    """

    def __init__(self, view, engine=None, *, chain: str, **kwargs):
        if chain not in ("fast", "slow"):
            raise ValueError(f"chain must be 'fast' or 'slow', got {chain!r}")
        super().__init__(view, engine=engine, max_cycles=10_000, **kwargs)
        self._chain = chain
        self._analysis_stage = f"analysis_{chain}"
        self._validate_stopping_condition()

    @observe
    def extract(self, snapshot) -> dict:
        obs = _BaseCampaignOperator.extract(self, snapshot)
        return obs

    # No @goals — primary stops campaign; max_cycles is the safety backstop.


class SimParentBmark4Operator(CampaignOperator):
    """Primary operator: fast_sim + slow_sim priorities + CPU-time budget.

    Controls inter-chain CPU allocation.  Reads analysis state from obs dict
    to track budget and decide when to demote fast_sim.

    Stops campaign when analysis_slow_hits ≥ n_target.
    """

    def __init__(
        self, view, engine=None, *,
        n_target: int = 30,
        n_budget: int = 600,
        cpu_budget_s: float = 30.0,
        **kwargs,
    ):
        super().__init__(view, engine=engine, **kwargs)
        self.n_target = int(n_target)
        self.n_budget = int(n_budget)
        self._cpu_budget_s = float(cpu_budget_s)
        self._validate_stopping_condition()

    @goals
    def criteria(self):
        return [Goal("slow_done", "analysis_slow_hits", self.n_target - 0.5, "maximize")]

    @observe
    def extract(self, snapshot) -> dict:
        obs = _BaseCampaignOperator.extract(self, snapshot)
        stages = obs.get("stages", {})
        obs["analysis_slow_hits"] = stages.get("analysis_slow", {}).get("finished", 0)
        obs["cpu_time_s"] = _cpu_time_from_obs(obs)
        obs["budget_exceeded"] = obs["cpu_time_s"] >= self._cpu_budget_s
        return obs


class AnalysisOnlyPolicy(Policy):
    """Sets the child's analysis stage to max priority every tick.

    Trivial but necessary: without this, the analysis stage retains its
    config-file priority (8) and can lose CPU slots to sims at priority 9.
    """

    def __init__(self, op: AnalysisChildOperator) -> None:
        super().__init__()
        self._act = op.get_actions()
        self._stage = op._analysis_stage

    @decide
    async def run(self, obs: dict) -> Decision:
        return Decision(actions=[
            self._act.set_priority(stage=self._stage, priority=_P_ANALYSIS),
        ])


class SimParentBudgetPolicy(Policy):
    """Inter-chain sim scheduling with CPU-time budget awareness.

    Phase 1 (budget OK):
      Boost slow_sim to _P_SIM_BOOST (109) above fast_sim (_P_SIM_NORMAL=106).
      With 4 CPUs and both analysis children at 110 (1 CPU each):
        analysis_fast=110(1) + analysis_slow=110(1) + slow_sim=109(2) + fast_sim=106(0)
      → slow_sim gets 2 CPUs from the start; fast_sim starved until analysis drains.
      When analysis stages starved: slow_sim=109(3), fast_sim=106(1).

    Phase 2 (budget exceeded):
      Demote fast_sim to _P_SIM_KILLED (10); maximise slow_sim throughput.
      Remaining budget concentrates entirely on completing slow chain.
    """

    def __init__(self, op: SimParentBmark4Operator) -> None:
        super().__init__()
        self._act = op.get_actions()
        self._phase: str = "PHASE1"
        self._budget_latch: bool = False

    @decide
    async def run(self, obs: dict) -> Decision:
        if not self._budget_latch and obs.get("budget_exceeded", False):
            self._budget_latch = True
            self._phase = "PHASE2"
            log.info(
                "SimParentBudgetPolicy: budget exceeded (%.1f CPU-s) → demoting fast_sim",
                obs.get("cpu_time_s", 0.0),
            )

        if self._phase == "PHASE2":
            return Decision(actions=[
                self._act.set_priority(stage="fast_sim", priority=_P_SIM_KILLED),
                self._act.set_priority(stage="slow_sim", priority=_P_SIM_BOOST),
            ])

        # Phase 1: prefer slow_sim for quality yield.
        return Decision(actions=[
            self._act.set_priority(stage="fast_sim", priority=_P_SIM_NORMAL),
            self._act.set_priority(stage="slow_sim", priority=_P_SIM_BOOST),
        ])
