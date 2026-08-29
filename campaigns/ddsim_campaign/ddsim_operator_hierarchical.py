"""Pipeline-isolation ADR policies for the concurrent-pipeline benchmark (v3).

Demonstrates per-pipeline independent backpressure vs global (flat) reaction.

  Pipeline 1: fast_sim (0.10 s/task, cap=3) → analysis_fast  (cap=1, 0.05 s/task)
  Pipeline 2: slow_sim (0.30 s/task, cap=3) → analysis_slow  (cap=1, 0.05 s/task)
  Shared pool: total_cpus = 4

Key blind spot (flat):
  analysis_fast THROTTLE (pending > cap=1) fires constantly because fast_sim
  produces tasks 1.5× faster than analysis_fast can consume them (3 CPUs × 10/s
  vs 1 CPU × 20/s).  FlatGlobalRulePolicy reacts globally: demotes BOTH sims.
  With 4 CPUs: analysis(110)=1 + fast(95)=3 + slow(90)=0  →  slow starved.

Per-pipeline fix (isolated policies):
  analysis_fast THROTTLE only demotes fast_sim.  slow_sim retains its high
  priority → gets 3 CPUs during fast throttle.
  Priority order during fast THROTTLE:  analysis(110) > slow(108) > fast(95)
  4 CPUs: analysis(110)=1 + slow(108)=3 + fast(95)=0  →  slow runs freely.

Three isolated tiers:
  isolated_delegated   — per-pipeline backpressure only; no cross-pipeline awareness.
  isolated_centralized — adds hit-count rebalancing every cycle when |gap| > 5%.
  isolated_debounced    — adds hit-count rebalancing when |gap| > 20% for ≥2 cycles.

All four policies use the same PipelineIsolationOperator (shared stopping condition
and metrics).  AnalysisWorkflow stages do NOT expose bp_state; THROTTLE is
detected via  pending > concurrency_cap (= 1 for both analysis stages).
"""

from __future__ import annotations

import logging

from radical.adr import Decision, goals, observe
from radical.adr.goals import Goal
from radical.adr.policy.base import Policy, decide

from src.campaign.adr import CampaignOperator
from src.campaign.adr.operator import CampaignOperator as _BaseCampaignOperator

log = logging.getLogger(__name__)

# Priority ladder (shared by all policies).
_P_ANALYSIS   = 110   # analysis stages always run first
_P_SIM_BOOST  = 109   # rebalancing boost for lagging pipeline; below analysis
_P_SIM_HIGH   = 108   # STARVED: analysis has no queued work
_P_SIM_NORMAL = 106   # NORMAL/HOLD: analysis has work queued
_P_SIM_DEMOTE = 95    # THROTTLE or active demotion
_P_SIM_LOW    = 90    # secondary flat demote (flat policy only)

# ── Shared operator ────────────────────────────────────────────────────────────

class PipelineIsolationOperator(CampaignOperator):
    """Single operator managing all 4 stages.

    Stops when BOTH analysis_fast_hits ≥ n_target AND analysis_slow_hits ≥ n_target.
    Used by all four benchmark policies so stopping conditions are identical.
    """

    n_target: int = 30
    n_budget: int = 600

    def __init__(self, view, engine=None, *, n_target: int = 30, n_budget: int = 600, **kwargs):
        super().__init__(view, engine=engine, **kwargs)
        self.n_target = int(n_target)
        self.n_budget = int(n_budget)
        self._validate_stopping_condition()

    @goals
    def criteria(self):
        return [
            Goal("fast_done", "analysis_fast_hits", self.n_target - 0.5, "maximize"),
            Goal("slow_done", "analysis_slow_hits", self.n_target - 0.5, "maximize"),
        ]

    @observe
    def extract(self, snapshot) -> dict:
        obs = _BaseCampaignOperator.extract(self, snapshot)
        stages = obs.get("stages", {})
        obs["analysis_fast_hits"] = stages.get("analysis_fast", {}).get("finished", 0)
        obs["analysis_slow_hits"] = stages.get("analysis_slow", {}).get("finished", 0)
        return obs

    def rule_policy(self) -> "FlatGlobalRulePolicy":
        return FlatGlobalRulePolicy(self)

    def isolated_rule_policy(self) -> "IsolatedRulePolicy":
        return IsolatedRulePolicy(self)

    def isolated_centralized_policy(self, n_target: int = 30) -> "IsolatedCentralizedPolicy":
        return IsolatedCentralizedPolicy(self, n_target=n_target)

    def isolated_debounced_policy(self, n_target: int = 30) -> "IsolatedDebouncedPolicy":
        return IsolatedDebouncedPolicy(self, n_target=n_target)


# ── Flat policy (blind spot) ───────────────────────────────────────────────────

class FlatGlobalRulePolicy(Policy):
    """Global rule — any analysis THROTTLE demotes BOTH sims.

    Blind spot: analysis_fast THROTTLE causes unnecessary demotion of slow_sim
    even though slow pipeline has no pending work.  With 4 CPUs and flat THROTTLE
    (fast=95, slow=90, analysis=110): 1+3+0=4 — slow_sim gets ZERO CPUs.
    """

    def __init__(self, op: PipelineIsolationOperator) -> None:
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

        # AnalysisWorkflow stages do not expose bp_state; detect via pending > cap.
        af_cap  = 1
        as_cap  = 1
        af_pend = an_fast.get("pending", 0)
        af_run  = an_fast.get("running", 0)
        as_pend = an_slow.get("pending", 0)
        as_run  = an_slow.get("running", 0)

        if af_pend > af_cap or as_pend > as_cap:
            # THROTTLE — demote BOTH (blind spot: slow pays for fast's backpressure).
            self._regime = "THROTTLE"
            p_fast = _P_SIM_DEMOTE   # 95
            p_slow = _P_SIM_LOW      # 90: unnecessary penalty on unrelated pipeline

        elif af_pend == 0 and af_run == 0 and as_pend == 0 and as_run == 0:
            # STARVED — both analysis empty; fast wins FIFO tie.
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


# ── Isolated policies (per-pipeline backpressure) ────────────────────────────

class IsolatedRulePolicy(Policy):
    """Per-pipeline backpressure — each sim reacts only to its own analysis stage.

    Key contrast with FlatGlobalRulePolicy:
      Flat:  analysis_fast THROTTLE → slow_sim demoted to 90 (0 CPUs with 4 total).
      Hier:  analysis_fast THROTTLE → only fast_sim demoted; slow_sim stays at 108.
             4 CPUs: analysis(110)=1 + slow(108)=3 + fast(95)=0.  Slow runs freely.
    """

    def __init__(self, op: PipelineIsolationOperator) -> None:
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

        # Pipeline 1: fast_sim priority determined solely by analysis_fast state.
        if af_pend > 1:
            p_fast = _P_SIM_DEMOTE   # 95: analysis_fast backing up
        elif af_pend == 0 and af_run == 0:
            p_fast = _P_SIM_HIGH     # 108: analysis_fast starved
        else:
            p_fast = _P_SIM_NORMAL   # 106: analysis_fast processing

        # Pipeline 2: slow_sim priority determined solely by analysis_slow state.
        if as_pend > 1:
            p_slow = _P_SIM_DEMOTE   # 95: analysis_slow backing up
        elif as_pend == 0 and as_run == 0:
            p_slow = _P_SIM_HIGH     # 108: analysis_slow starved
        else:
            p_slow = _P_SIM_NORMAL   # 106: analysis_slow processing

        if af_pend > 1 and as_pend <= 1:
            self._regime = "P1_THROTTLE"
        elif as_pend > 1 and af_pend <= 1:
            self._regime = "P2_THROTTLE"
        elif af_pend == 0 and af_run == 0 and as_pend == 0 and as_run == 0:
            self._regime = "BOTH_STARVED"
        else:
            self._regime = "NORMAL"

        return Decision(actions=[
            self._act.set_priority(stage="analysis_fast", priority=_P_ANALYSIS),
            self._act.set_priority(stage="analysis_slow", priority=_P_ANALYSIS),
            self._act.set_priority(stage="fast_sim",      priority=p_fast),
            self._act.set_priority(stage="slow_sim",      priority=p_slow),
        ])


class IsolatedCentralizedPolicy(Policy):
    """Per-pipeline backpressure + centralized hit-count rebalancing every cycle.

    When neither pipeline is throttling and |p1_hits - p2_hits| / n_target > 0.05,
    the parent actively boosts the lagging pipeline to equalize completion times.
    """

    def __init__(self, op: PipelineIsolationOperator, n_target: int = 30) -> None:
        super().__init__()
        self._act = op.get_actions()
        self._n_target = int(n_target)
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

        p1_hits = obs.get("analysis_fast_hits", 0)
        p2_hits = obs.get("analysis_slow_hits", 0)
        imbalance = (p1_hits - p2_hits) / max(self._n_target, 1)

        # Per-pipeline backpressure takes precedence over rebalancing.
        if af_pend > 1:
            p_fast = _P_SIM_DEMOTE
            p_slow = _P_SIM_HIGH if (as_pend == 0 and as_run == 0) else _P_SIM_NORMAL
            self._regime = "P1_THROTTLE"
        elif as_pend > 1:
            p_slow = _P_SIM_DEMOTE
            p_fast = _P_SIM_HIGH if (af_pend == 0 and af_run == 0) else _P_SIM_NORMAL
            self._regime = "P2_THROTTLE"
        elif abs(imbalance) > 0.05:
            # No throttling: rebalance toward the lagging pipeline.
            if imbalance > 0:
                p_fast = _P_SIM_DEMOTE   # p1 ahead: slow fast down
                p_slow = _P_SIM_BOOST    # boost slow
            else:
                p_fast = _P_SIM_BOOST    # p2 ahead: boost fast
                p_slow = _P_SIM_DEMOTE
            self._regime = f"REBALANCE(d={imbalance:+.2f})"
        else:
            # Balanced and not throttling: per-pipeline STARVED/NORMAL.
            p_fast = _P_SIM_HIGH if (af_pend == 0 and af_run == 0) else _P_SIM_NORMAL
            p_slow = _P_SIM_HIGH if (as_pend == 0 and as_run == 0) else _P_SIM_NORMAL
            self._regime = "NORMAL"

        return Decision(actions=[
            self._act.set_priority(stage="analysis_fast", priority=_P_ANALYSIS),
            self._act.set_priority(stage="analysis_slow", priority=_P_ANALYSIS),
            self._act.set_priority(stage="fast_sim",      priority=p_fast),
            self._act.set_priority(stage="slow_sim",      priority=p_slow),
        ])


class IsolatedDebouncedPolicy(Policy):
    """Per-pipeline backpressure + adaptive rebalancing when |gap| > 20% for ≥2 cycles.

    More conservative than centralized: avoids oscillation from transient imbalance
    spikes. Only rebalances when the imbalance is large AND sustained.
    """

    def __init__(self, op: PipelineIsolationOperator, n_target: int = 30) -> None:
        super().__init__()
        self._act = op.get_actions()
        self._n_target = int(n_target)
        self._imbalance_streak: int = 0
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

        p1_hits = obs.get("analysis_fast_hits", 0)
        p2_hits = obs.get("analysis_slow_hits", 0)
        imbalance = (p1_hits - p2_hits) / max(self._n_target, 1)

        # Track streak for adaptive threshold.
        if abs(imbalance) > 0.20:
            self._imbalance_streak += 1
        else:
            self._imbalance_streak = 0

        # Per-pipeline backpressure takes precedence.
        if af_pend > 1:
            p_fast = _P_SIM_DEMOTE
            p_slow = _P_SIM_HIGH if (as_pend == 0 and as_run == 0) else _P_SIM_NORMAL
            self._regime = "P1_THROTTLE"
        elif as_pend > 1:
            p_slow = _P_SIM_DEMOTE
            p_fast = _P_SIM_HIGH if (af_pend == 0 and af_run == 0) else _P_SIM_NORMAL
            self._regime = "P2_THROTTLE"
        elif self._imbalance_streak >= 2:
            # Large sustained imbalance: rebalance.
            if imbalance > 0:
                p_fast = _P_SIM_DEMOTE
                p_slow = _P_SIM_BOOST
            else:
                p_fast = _P_SIM_BOOST
                p_slow = _P_SIM_DEMOTE
            self._regime = f"REBALANCE(streak={self._imbalance_streak})"
        else:
            p_fast = _P_SIM_HIGH if (af_pend == 0 and af_run == 0) else _P_SIM_NORMAL
            p_slow = _P_SIM_HIGH if (as_pend == 0 and as_run == 0) else _P_SIM_NORMAL
            self._regime = "NORMAL"

        return Decision(actions=[
            self._act.set_priority(stage="analysis_fast", priority=_P_ANALYSIS),
            self._act.set_priority(stage="analysis_slow", priority=_P_ANALYSIS),
            self._act.set_priority(stage="fast_sim",      priority=p_fast),
            self._act.set_priority(stage="slow_sim",      priority=p_slow),
        ])
