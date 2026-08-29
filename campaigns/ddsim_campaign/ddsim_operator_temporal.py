"""Temporal-adaptation ADR policies for the phase-depletion benchmark (Benchmark 3).

Demonstrates ADR's ability to detect and predict a mid-campaign phase transition
(fast_sim depletion) and rebalance resources before/when the event occurs.

  fast_sim  (0.10 s, 200 rep, cap=3) → analysis_fast  (0.20 s, cap=2)
  slow_sim  (0.50 s,  30 rep, cap=3) → analysis_slow  (0.05 s, cap=1)
  Shared pool: total_cpus = 4

Phase dynamics:
  fast_sim produces 30 tasks/s; analysis_fast processes only 10 tasks/s (2 CPUs × 1/0.20).
  Backlog builds at 20 tasks/s for ~6.7 s → ~67 analysis_fast tasks pending at depletion.
  After fast_sim depletes: analysis_fast(110, cap=2) fills 2 CPUs for ~6.7 s of drain time.

Rule blind spot (rule_static):
  Always gives analysis_fast priority 110; slow_sim always at priority 90.
  After fast_sim depletes: analysis_fast fills 2 CPUs at priority 110; slow_sim gets 2 CPUs.
  slow_sim drains in 30×0.50/2 = 7.5 s instead of 5 s.

ADR reactive fix (adr_reactive):
  Latches on fast_sim.pending==0 AND running==0 → demotes analysis_fast to 95,
  promotes slow_sim to 108 → slow_sim gets 3 CPUs from depletion point.

ADR proactive fix (adr_proactive):
  Uses eta_s = (pending+running)×dur_s/running (from obs["fast_eta_s"]).
  When eta_s < lookahead_s (default 2.0 s): same rebalancing as reactive.
  Gives slow_sim a ~2 s head start, completing ~2 s earlier than reactive.

Expected ordering: rule_static >> adr_reactive > adr_proactive (lower ttt = better).
"""

from __future__ import annotations

import logging

from radical.adr import Decision, goals, observe
from radical.adr.goals import Goal
from radical.adr.policy.base import Policy, decide

from src.campaign.adr import CampaignOperator
from src.campaign.adr.operator import CampaignOperator as _BaseCampaignOperator

log = logging.getLogger(__name__)

# Priority ladder.
_P_ANALYSIS   = 110   # analysis stages: always run first
_P_SIM_HIGH   = 108   # promoted: post-depletion slow_sim or pre-depletion boost
_P_SIM_NORMAL = 106   # normal operation
_P_SIM_DEMOTE = 95    # demoted: analysis_fast after rebalancing
_P_SIM_LOW    = 90    # static low: slow_sim under rule_static


# ── Shared operator ────────────────────────────────────────────────────────────

class TemporalOperator(CampaignOperator):
    """Operator for B3.  Stops when analysis_slow.finished >= n_target.

    Exports three extra observation fields for ADR policies and logs:
      analysis_slow_hits — the stopping metric
      fast_depleted      — True once fast_sim.pending==0 AND running==0 (latched)
      fast_eta_s         — estimated seconds until fast_sim exhausts all tasks
    """

    n_target: int = 30
    n_budget: int = 600

    def __init__(
        self,
        view,
        engine=None,
        *,
        n_target: int = 30,
        n_budget: int = 600,
        fast_sim_duration_s: float = 0.1,
        **kwargs,
    ):
        super().__init__(view, engine=engine, **kwargs)
        self.n_target = int(n_target)
        self.n_budget = int(n_budget)
        self._fast_sim_duration_s = float(fast_sim_duration_s)
        self._fast_depleted_latch = False

    @goals
    def criteria(self):
        return [
            Goal("slow_done", "analysis_slow_hits", self.n_target - 0.5, "maximize"),
        ]

    @observe
    def extract(self, snapshot) -> dict:
        obs = _BaseCampaignOperator.extract(self, snapshot)
        stages = obs.get("stages", {})

        fast     = stages.get("fast_sim", {})
        slow_an  = stages.get("analysis_slow", {})

        obs["analysis_slow_hits"] = slow_an.get("finished", 0)

        fast_pending  = fast.get("pending", 0)
        fast_running  = fast.get("running", 0)
        fast_finished = fast.get("finished", 0)

        # Latch depletion: once depleted, never un-deplete.
        if not self._fast_depleted_latch:
            self._fast_depleted_latch = (
                fast_pending == 0 and fast_running == 0 and fast_finished > 0
            )
        obs["fast_depleted"] = self._fast_depleted_latch

        if self._fast_depleted_latch:
            eta_s = 0.0
        elif fast_running > 0:
            eta_s = (fast_pending + fast_running) * self._fast_sim_duration_s / fast_running
        else:
            eta_s = float("inf")
        obs["fast_eta_s"] = eta_s

        return obs


# ── Rule baseline ──────────────────────────────────────────────────────────────

class RuleStaticPolicy(Policy):
    """Static rule — always demotes slow_sim; never detects fast_sim depletion.

    Blind spot: after fast_sim exhausts its 200 replicas, the analysis_fast
    backlog (~67 tasks, cap=2) continues to occupy 2 CPUs at priority 110.
    slow_sim gets only 2 CPUs at priority 90 instead of 3.
    Expected rule ttt ≈ 15.7 s vs reactive/proactive ≈ 11–13 s.
    """

    def __init__(self, op: TemporalOperator) -> None:
        super().__init__()
        self._act = op.get_actions()

    @decide
    async def run(self, obs: dict) -> Decision:
        return Decision(actions=[
            self._act.set_priority(stage="analysis_fast", priority=_P_ANALYSIS),
            self._act.set_priority(stage="analysis_slow", priority=_P_ANALYSIS),
            self._act.set_priority(stage="fast_sim",      priority=_P_SIM_HIGH),
            self._act.set_priority(stage="slow_sim",      priority=_P_SIM_LOW),
        ])


# ── ADR reactive ───────────────────────────────────────────────────────────────

class AdrReactivePolicy(Policy):
    """Reactive ADR — detects fast_sim depletion and immediately rebalances.

    Phase 1 (fast_sim active): same as rule_static (fast=108, slow=90).
    Phase 2 (fast_sim depleted): demotes analysis_fast to 95, promotes slow_sim
    to 108 → slow_sim wins 3 of 4 CPUs; analysis_fast drains on the 4th.
    """

    def __init__(self, op: TemporalOperator) -> None:
        super().__init__()
        self._act = op.get_actions()
        self._phase: str = "PHASE1"

    @decide
    async def run(self, obs: dict) -> Decision:
        if obs.get("fast_depleted", False):
            self._phase = "PHASE2"

        if self._phase == "PHASE2":
            return Decision(actions=[
                self._act.set_priority(stage="analysis_fast", priority=_P_SIM_DEMOTE),
                self._act.set_priority(stage="analysis_slow", priority=_P_ANALYSIS),
                self._act.set_priority(stage="fast_sim",      priority=_P_SIM_NORMAL),
                self._act.set_priority(stage="slow_sim",      priority=_P_SIM_HIGH),
            ])

        # Phase 1: identical to rule_static.
        return Decision(actions=[
            self._act.set_priority(stage="analysis_fast", priority=_P_ANALYSIS),
            self._act.set_priority(stage="analysis_slow", priority=_P_ANALYSIS),
            self._act.set_priority(stage="fast_sim",      priority=_P_SIM_HIGH),
            self._act.set_priority(stage="slow_sim",      priority=_P_SIM_LOW),
        ])


# ── ADR proactive ──────────────────────────────────────────────────────────────

class AdrProactivePolicy(Policy):
    """Proactive ADR — predicts depletion and rebalances before it occurs.

    Reads obs["fast_eta_s"] (computed by TemporalOperator.extract) to estimate
    seconds remaining until fast_sim exhausts all tasks.  When eta_s < lookahead_s,
    the policy pre-promotes slow_sim — giving it a ~lookahead_s head start over
    the reactive policy.

    Constructor parameters
    ----------------------
    lookahead_s : float
        How far ahead (in seconds) to trigger rebalancing before depletion.
        Default 2.0 s; increase for earlier promotion, decrease to be more
        conservative.
    """

    def __init__(self, op: TemporalOperator, *, lookahead_s: float = 2.0) -> None:
        super().__init__()
        self._act = op.get_actions()
        self._lookahead_s = float(lookahead_s)
        self._phase: str = "PHASE1"
        self._rebalance_latch: bool = False

    @decide
    async def run(self, obs: dict) -> Decision:
        eta_s    = obs.get("fast_eta_s", float("inf"))
        fast_dep = obs.get("fast_depleted", False)

        # Latch: once rebalancing begins, never revert to Phase 1.
        # Without the latch, demoting fast_sim to 106 causes fast_running→0 the
        # next cycle (slow_sim+analysis_slow consume all 4 CPUs), which makes
        # eta_s→inf and the policy reverts — creating a destructive oscillation.
        if not self._rebalance_latch:
            self._rebalance_latch = fast_dep or (eta_s < self._lookahead_s)

        if self._rebalance_latch:
            self._phase = "PHASE2" if fast_dep else "PRE_DEPLETION"
            return Decision(actions=[
                self._act.set_priority(stage="analysis_fast", priority=_P_SIM_DEMOTE),
                self._act.set_priority(stage="analysis_slow", priority=_P_ANALYSIS),
                self._act.set_priority(stage="fast_sim",      priority=_P_SIM_NORMAL),
                self._act.set_priority(stage="slow_sim",      priority=_P_SIM_HIGH),
            ])

        # Phase 1: identical to rule_static.
        return Decision(actions=[
            self._act.set_priority(stage="analysis_fast", priority=_P_ANALYSIS),
            self._act.set_priority(stage="analysis_slow", priority=_P_ANALYSIS),
            self._act.set_priority(stage="fast_sim",      priority=_P_SIM_HIGH),
            self._act.set_priority(stage="slow_sim",      priority=_P_SIM_LOW),
        ])
