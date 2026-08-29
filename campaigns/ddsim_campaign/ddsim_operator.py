"""DdSimCampaignOperator — ADR operator for the ddsim campaign.

The ddsim campaign has two independent MD simulation pools (ddsim_a, ddsim_b)
that both trigger a shared analysis stage.  This operator demonstrates five
ADR runtime-goal features:

  1. AnyGoal (OR semantics)
     Static goal: stop when EITHER n_target analyses finish OR the total-sim
     budget is exhausted — whichever comes first.

  2. New name → independent AND requirement
     After the first backpressure THROTTLE event, propose ``quality_gate``
     (score_p50_analysis > 0.35).  Runtime goals are evaluated together with
     the static AnyGoal every cycle; the campaign won't stop until BOTH are
     satisfied simultaneously.

  3. Same name → upsert in place (tighten on data signal)
     Once ≥8 analyses have finished, the score distribution is stable enough
     to trust.  Re-propose ``quality_gate`` with the same name but a higher
     threshold (0.45).  The operator replaces the existing goal in-place —
     no duplicate goal is created.

  4. Same name → upsert in place (tighten on budget scarcity)
     When total sims exceed 70 % of the budget, re-propose ``quality_gate``
     at 0.55 AND call set_score_cutoff("analysis", 0.55) so the triage gate
     also rises — only the best remaining candidates consume scarce budget.
     Strictly ordered: fires only after step 3 has already upserted.

  5. Decision(remove_goals=[...])
     When analyses_done is within 5 of n_target the quality gate has served
     its purpose.  Revoke it so the AnyGoal can fire cleanly on the next
     cycle that satisfies it.

Steps 2–5 are managed by a single if/elif chain inside DdSimRulePolicy.run()
so at most one goal-mutation fires per cycle, keeping transitions readable in
the log.
"""

from __future__ import annotations

import logging

from radical.adr import goals, observe
from radical.adr import Decision
from radical.adr.goals import AnyGoal, Goal
from radical.adr.policy.base import Policy, decide

from src.campaign.adr import CampaignOperator
from src.campaign.adr.operator import CampaignOperator as _BaseCampaignOperator
from src.campaign.bandit import SchedulingBandit

log = logging.getLogger(__name__)


class DdSimRulePolicy(Policy):
    """Campaign-specific scheduling rule for the ddsim fan-in topology.

    Scheduling regimes (checked in order each cycle):

      THROTTLE  analysis backpressure queue is full → demote both sim pools
                below analysis priority so CM slots drain the queue first.
      STARVED   analysis has no pending or running work → always promote ddsim_a
                (the fast pool). ddsim_a (0.10s/replica) feeds analysis 3× faster
                per CPU than ddsim_b (0.30s/replica), so maximising ddsim_a
                throughput minimises time-to-target.
      NORMAL    baseline downstream-first with a standing +1 boost to ddsim_a
                (the faster feeder). ddsim_b gets a lower standing priority since
                it contributes fewer analysis events per CPU-second.

    Runtime goal state machine (Steps 2–5 above, at most one per cycle):

      Step 2  first THROTTLE      → propose  quality_gate @ 0.35
      Step 3  an_finished ≥ 8     → upsert   quality_gate @ 0.45  (data signal)
      Step 4  n_sims > 70% budget → upsert   quality_gate @ 0.55  (budget scarcity)
                                    + set_score_cutoff("analysis", 0.55)
      Step 5  an_finished ≥ 20    → remove   quality_gate
    """

    # ── Scheduling priority constants ──────────────────────────────────────────
    _P_ANALYSIS       = 110
    _P_SIM_A          = 102  # standing +1 vs ddsim_b: ddsim_a feeds analysis faster
    _P_SIM_B          = 101
    _P_SIM_B_THROTTLE = 97
    _P_SIM_A_THROTTLE = 96
    _P_SIM_STARVED    = 108  # boost ddsim_a when analysis is starved; large gap over ddsim_b=101

    # ── Quality gate thresholds and triggers ───────────────────────────────────
    # Step 2: loose initial bar — confirms the pipeline is producing scored output.
    _GATE_THRESHOLD_INITIAL  = 0.35
    # Step 3: tighter bar once the score distribution is stable.
    _GATE_THRESHOLD_DATA     = 0.45
    _GATE_DATA_TRIGGER       = 8     # min finished analyses to trust the distribution
    # Step 4: strictest bar when budget is nearly consumed.
    _GATE_THRESHOLD_BUDGET   = 0.55
    _GATE_BUDGET_FRACTION    = 0.70  # fraction of n_budget that triggers scarcity mode
    # Step 5: remove quality gate this many analyses before the count target.
    _GATE_NEAR_END           = 5

    def __init__(self, op: "DdSimCampaignOperator") -> None:
        super().__init__()
        self._act             = op.get_actions()
        self._n_target        = op.n_target
        self._n_budget        = op.n_budget
        self._regime: str     = "INIT"

        # One bool flag per step; each flips True exactly once, in order.
        self._gate_proposed:      bool = False  # step 2: initial propose fired
        self._data_upsert_done:   bool = False  # step 3: data-signal upsert fired
        self._budget_upsert_done: bool = False  # step 4: budget-scarcity upsert fired
        self._gate_removed:       bool = False  # step 5: revoke fired

    @decide
    async def run(self, obs: dict) -> Decision:
        stages = obs.get("stages", {})
        if not stages:
            return Decision()

        an = stages.get("analysis", {})
        a  = stages.get("ddsim_a",  {})
        b  = stages.get("ddsim_b",  {})

        an_bp       = an.get("bp_state", "HOLD")
        an_pending  = an.get("pending",  0)
        an_running  = an.get("running",  0)
        an_finished = an.get("finished", 0)
        a_pending   = a.get("pending",   0)
        b_pending   = b.get("pending",   0)
        n_sims      = obs.get("n_sims",  0)

        # ── Scheduling regime ──────────────────────────────────────────────────
        p_analysis = self._P_ANALYSIS

        if an_bp == "THROTTLE":
            self._regime = "THROTTLE"
            p_a = self._P_SIM_A_THROTTLE
            p_b = self._P_SIM_B_THROTTLE

        elif an_pending == 0 and an_running == 0:
            self._regime = "STARVED"
            # Always boost ddsim_a — it is 3× faster per CPU (0.10s vs 0.30s),
            # so it feeds the analysis queue at 3× the rate of ddsim_b.
            # Boosting whichever pool has more pending (old logic) accidentally
            # promoted ddsim_b when pending counts were equal at t=0, causing
            # ddsim_b to monopolise all slots and starve the faster feeder.
            p_a = self._P_SIM_STARVED
            p_b = self._P_SIM_B

        else:
            self._regime = "NORMAL"
            p_a = self._P_SIM_A
            p_b = self._P_SIM_B

        actions = [
            self._act.set_priority(stage="analysis", priority=p_analysis),
            self._act.set_priority(stage="ddsim_a",  priority=p_a),
            self._act.set_priority(stage="ddsim_b",  priority=p_b),
        ]

        # ── Runtime goal state machine ─────────────────────────────────────────
        # At most one branch fires per cycle.  Each branch sets a flag so it
        # fires exactly once, and later branches are gated on earlier flags so
        # the progression is strictly ordered 2 → 3 → 4 → 5.
        new_goals:    list = []
        remove_goals: list = []

        if self._regime == "THROTTLE" and not self._gate_proposed:
            # Step 2 — new name → adds an independent AND requirement.
            # The campaign must now satisfy BOTH the static AnyGoal AND quality_gate
            # every cycle before it will stop.
            self._gate_proposed = True
            new_goals = [
                Goal(
                    name="quality_gate",
                    metric="score_p50_analysis",
                    threshold=self._GATE_THRESHOLD_INITIAL,
                    direction="maximize",
                )
            ]
            log.info(
                "ADR [step 2] quality_gate PROPOSED @ %.2f"
                " — new independent requirement added (analysis queue first backed up).",
                self._GATE_THRESHOLD_INITIAL,
            )

        elif (self._gate_proposed
              and not self._data_upsert_done
              and an_finished >= self._GATE_DATA_TRIGGER):
            # Step 3 — same name → upsert in place (tighten on data signal).
            # Re-proposing with the same name replaces the existing goal's threshold;
            # it does NOT create a second quality_gate in the runtime list.
            self._data_upsert_done = True
            new_goals = [
                Goal(
                    name="quality_gate",       # same name → upsert, not append
                    metric="score_p50_analysis",
                    threshold=self._GATE_THRESHOLD_DATA,
                    direction="maximize",
                )
            ]
            log.info(
                "ADR [step 3] quality_gate UPSERTED @ %.2f"
                " (data signal: %d analyses finished, distribution is stable).",
                self._GATE_THRESHOLD_DATA, an_finished,
            )

        elif (self._data_upsert_done
              and not self._budget_upsert_done
              and n_sims > self._GATE_BUDGET_FRACTION * self._n_budget):
            # Step 4 — same name → upsert in place again (tighten on budget scarcity).
            # Also raises the triage score_cutoff via set_score_cutoff so low-scoring
            # candidates are rejected before they even enter the analysis queue —
            # every remaining analysis slot must count.
            self._budget_upsert_done = True
            new_goals = [
                Goal(
                    name="quality_gate",       # same name → upsert again
                    metric="score_p50_analysis",
                    threshold=self._GATE_THRESHOLD_BUDGET,
                    direction="maximize",
                )
            ]
            actions.append(
                self._act.set_score_cutoff(
                    stage="analysis",
                    value=self._GATE_THRESHOLD_BUDGET,
                )
            )
            log.info(
                "ADR [step 4] quality_gate UPSERTED @ %.2f"
                " + score_cutoff raised to %.2f"
                " (budget %.0f%% consumed: %d/%d sims).",
                self._GATE_THRESHOLD_BUDGET,
                self._GATE_THRESHOLD_BUDGET,
                100.0 * n_sims / self._n_budget,
                n_sims, self._n_budget,
            )

        elif (self._gate_proposed
              and not self._gate_removed
              and an_finished >= self._n_target - self._GATE_NEAR_END):
            # Step 5 — Decision(remove_goals=[...]).
            # Quality gate has served its purpose; revoking it lets the AnyGoal
            # (analyses_done OR budget_out) fire cleanly on the next satisfied cycle.
            # Runtime goals are re-evaluated every cycle, so removal takes effect
            # immediately — the gate is gone from the next tick onward.
            self._gate_removed = True
            remove_goals = ["quality_gate"]
            log.info(
                "ADR [step 5] quality_gate REMOVED"
                " (analyses_done=%d, within %d of target=%d).",
                an_finished, self._GATE_NEAR_END, self._n_target,
            )

        return Decision(actions=actions, goals=new_goals, remove_goals=remove_goals)


class DdSim3RulePolicy(Policy):
    """Three-pool scheduling rule for the consensus_3stage benchmark.

    Deliberately has NO QUALITY_PRESSURE regime — that is the rule's blind spot
    in the 3-stage benchmark.  When the quality gate tightens and ddsim_a output
    fails the score threshold, this rule keeps pushing ddsim_a in NORMAL/STARVED,
    wasting analysis slots on low-quality candidates.

    LLM classifies QUALITY_PRESSURE immediately; bandit learns it via BP rewards
    (~20 cycles).  Once bandit adapts, LLM + bandit outvote rule 2-1 inside
    ConsensusPolicy → consensus beats rule.

    Scheduling regimes:
      THROTTLE:    analysis queue full → demote all sims
      STARVED:     analysis empty → push ddsim_a (fastest feeder)
      EXHAUSTED_A: ddsim_a done → push ddsim_b (next fastest)
      NORMAL:      a > b > c by speed (misses quality-pressure optimal ordering)
    """

    _P_ANALYSIS      = 110
    _P_A_STARVED     = 108;  _P_B_STARVED     = 103;  _P_C_STARVED     = 101
    _P_A_EXHAUSTED   = 101;  _P_B_EXHAUSTED   = 108;  _P_C_EXHAUSTED   = 103
    _P_A_NORMAL      = 105;  _P_B_NORMAL      = 103;  _P_C_NORMAL      = 101
    _P_A_THROTTLE    = 96;   _P_B_THROTTLE    = 97;   _P_C_THROTTLE    = 95

    def __init__(self, op) -> None:
        super().__init__()
        self._act    = op.get_actions()
        self._regime: str = "INIT"

    @decide
    async def run(self, obs: dict) -> Decision:
        stages = obs.get("stages", {})
        if not stages:
            return Decision()

        an = stages.get("analysis", {})
        a  = stages.get("ddsim_a",  {})

        an_bp      = an.get("bp_state", "HOLD")
        an_pending = an.get("pending",  0)
        an_running = an.get("running",  0)
        a_pending  = a.get("pending",   0)
        a_running  = a.get("running",   0)

        if an_bp == "THROTTLE":
            self._regime = "THROTTLE"
            pa, pb, pc = self._P_A_THROTTLE, self._P_B_THROTTLE, self._P_C_THROTTLE

        elif a_pending == 0 and a_running == 0:
            self._regime = "EXHAUSTED_A"
            pa, pb, pc = self._P_A_EXHAUSTED, self._P_B_EXHAUSTED, self._P_C_EXHAUSTED

        elif an_pending == 0 and an_running == 0:
            self._regime = "STARVED"
            pa, pb, pc = self._P_A_STARVED, self._P_B_STARVED, self._P_C_STARVED

        else:
            self._regime = "NORMAL"
            pa, pb, pc = self._P_A_NORMAL, self._P_B_NORMAL, self._P_C_NORMAL

        return Decision(actions=[
            self._act.set_priority(stage="analysis", priority=self._P_ANALYSIS),
            self._act.set_priority(stage="ddsim_a",  priority=pa),
            self._act.set_priority(stage="ddsim_b",  priority=pb),
            self._act.set_priority(stage="ddsim_c",  priority=pc),
        ])


class AnalysisLockedBanditPolicy(Policy):
    """N-arm Thompson-sampling bandit for sim pools; analysis locked at 110.

    Decouples the two learning problems:
      - Analysis priority is hard-coded to 110 every cycle (known correct answer).
      - A 2-arm bandit runs only on ddsim_a vs ddsim_b, discovering which feeder
        maximises analysis throughput.

    Reward: analysis utilization when a sim task completes —
    (running + min(pending, cap)) / cap, floored at 0.1 so the bandit still
    gets a gradient when analysis is idle.

    Warm-start: ddsim_b gets Beta(5,1) prior (mean=0.83) — fast feeder, start high;
    ddsim_a gets Beta(1,3) (mean=0.25) — slow feeder, start low so the bandit shifts
    toward b quickly.  Expected convergence: 20–30 cycles.
    """

    _ANALYSIS_PRIORITY = 110
    _SIM_HIGH          = 109   # winner's priority (109 < analysis=110; 1-pt above LLM max)
    _SIM_LOW           = 101   # loser's priority
    _ANALYSIS_CAP      = 2     # analysis concurrency cap from campaign config

    def __init__(self, op, seed: int = 0) -> None:
        super().__init__()
        self._act  = op.get_actions()
        self._seed = seed
        self._bandit: SchedulingBandit | None = None
        self._prev_finished: dict[str, int] = {}

    def _ensure_bandit(self, sim_names: list[str]) -> SchedulingBandit:
        if self._bandit is None:
            # Priors encode expected speed advantage, but stay moderate enough
            # that the bandit can adapt within ~20 cycles when the reward signal
            # shifts (e.g. quality gate penalising fast-but-low-quality pools).
            def _prior(s: str):
                if "ddsim_b" in s:
                    return (5.0, 1.0)   # b is the fast feeder — start high
                if "ddsim_a" in s:
                    return (1.0, 3.0)   # a is slow — start low so bandit shifts fast
                return (3.0, 1.0)       # c is medium (ddsim_c and others)
            priors = {s: _prior(s) for s in sim_names}
            self._bandit = SchedulingBandit(sim_names, seed=self._seed, stage_priors=priors)
        return self._bandit

    @decide
    async def run(self, obs: dict) -> Decision:
        stages = obs.get("stages", {})
        if not stages:
            return Decision()

        an         = stages.get("analysis", {})
        sim_stages = {k: v for k, v in stages.items() if k != "analysis"}
        actions    = [self._act.set_priority(stage="analysis", priority=self._ANALYSIS_PRIORITY)]

        if not sim_stages:
            return Decision(actions=actions)

        bandit = self._ensure_bandit(list(sim_stages))

        # Reward: how loaded was analysis when this sim task completed?
        an_running = int(an.get("running", 0))
        an_pending = int(an.get("pending", 0))
        effective  = an_running + min(an_pending, self._ANALYSIS_CAP)
        reward     = max(0.1, min(1.0, effective / self._ANALYSIS_CAP))

        for s, info in sim_stages.items():
            finished = int(info.get("finished", 0))
            delta    = finished - self._prev_finished.get(s, 0)
            if delta > 0:
                for _ in range(min(delta, 32)):
                    bandit.update(s, reward)
            self._prev_finished[s] = finished

        # Rank N arms; build a descending priority ladder spanning SIM_HIGH→SIM_LOW.
        class _NS:
            __slots__ = ("name",)
            def __init__(self, n: str) -> None: self.name = n

        ranked = bandit.rank([_NS(s) for s in sim_stages])
        n = len(ranked)
        if n <= 1:
            pri_slots = [self._SIM_HIGH]
        else:
            step = max(1, (self._SIM_HIGH - self._SIM_LOW) // (n - 1))
            pri_slots = [self._SIM_HIGH - i * step for i in range(n)]
        for i, stage in enumerate(ranked):
            actions.append(self._act.set_priority(stage=stage.name, priority=pri_slots[i]))

        return Decision(actions=actions)

    @property
    def summary(self) -> dict:
        return self._bandit.summary() if self._bandit else {}


class DdSimCampaignOperator(CampaignOperator):
    """ADR operator for the two-pool ddsim campaign."""

    n_target: int = 15
    n_budget: int = 80  # total-sim budget; stop early if exceeded

    def __init__(
        self,
        view,
        engine=None,
        *,
        n_target: int = 15,
        n_budget: int = 80,
        **kwargs,
    ):
        super().__init__(view, engine=engine, **kwargs)
        self.n_target = int(n_target)
        self.n_budget = int(n_budget)
        self._validate_stopping_condition()

    def rule_policy(self):
        return DdSimRulePolicy(self)

    # ── Goals ──────────────────────────────────────────────────────────────────

    @goals
    def criteria(self):
        """Stop when EITHER the analysis count target is met OR the sim budget burns out.

        AnyGoal provides OR semantics: whichever sub-goal fires first wins.
        Runtime goals proposed by the policy (quality_gate) are evaluated together
        with this static AnyGoal every cycle — the campaign stops only when ALL
        active goals are satisfied simultaneously.
        """
        if self.n_target <= 0:
            return []
        return AnyGoal([
            Goal(
                name="analyses_done",
                metric="n_hits",
                threshold=self.n_target - 0.5,
                direction="maximize",
            ),
            Goal(
                name="budget_out",
                metric="n_sims",
                threshold=self.n_budget - 0.5,
                direction="maximize",
            ),
        ])

    # ── Observe ────────────────────────────────────────────────────────────────

    @observe
    def extract(self, snapshot) -> dict:
        """Extend the base observation with campaign-specific flat metrics.

        ``score_p50_analysis`` flattens ``stages["analysis"]["score_p50"]`` to a
        top-level key so it can be referenced directly by Goal(metric=...).
        None until ≥5 scored candidates have been dispatched through the sharder.
        """
        obs = _BaseCampaignOperator.extract(self, snapshot)
        stages = obs.get("stages", {})
        obs["score_p50_analysis"] = stages.get("analysis", {}).get("score_p50")
        return obs


class DdSim3CampaignOperator(DdSimCampaignOperator):
    """ADR operator for the three-pool consensus benchmark.

    Inherits all goal/observe logic from DdSimCampaignOperator; overrides
    rule_policy() to return DdSim3RulePolicy (which has no QUALITY_PRESSURE
    regime — that blind spot is the mechanism for consensus to beat rule).
    """

    def rule_policy(self):
        return DdSim3RulePolicy(self)
