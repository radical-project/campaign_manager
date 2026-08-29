"""Nested-loop ADR operators for the nested campaign demo.

Demonstrates sequential outer rounds each containing an inner sim→analysis
loop, with the analysis score gate cutoff tightened between rounds using the
best score reported by the previous inner loop via SNAPSHOT messaging.

ADR concepts demonstrated:

  InnerOperator (child)
      Runs one round: monitors analysis hits from a baseline, stops when
      ``n_per_round`` new analyses complete.  Reports ``best_score_p50``
      (annotated class attribute → state.objectives → SNAPSHOT to parent)
      each cycle so the parent can read it before launching the next round.

  OuterOperator (parent)
      Manages N sequential inner loops.  Each ``launch_inner()`` @act:
        1. Tightens the analysis score gate cutoff using the previous round's
           best score (× shrink_factor, floored at cutoff_floor).
        2. Reactivates the CM (safe no-op if still running) and triggers
           n_sim new sim replicas.
        3. Constructs a fresh InnerOperator with the current analysis-done
           count as baseline, wires it as a child (parent= and policy), then
           calls child.start().
      The UID of the resulting AsyncFlow Future is stored in
      state.artifacts["inner_uid"] so ``watch()`` can track it across cycles.

  OuterPolicy
      Three-state machine:
        inner_uid is None  → launch round 1
        inner DONE         → tighten cutoff, launch next round (or stop)
        otherwise          → idle, log every 5 cycles
"""

from __future__ import annotations

import asyncio
import logging

from radical.adr import Decision, act, goals, observe
from radical.adr.goals import Goal
from radical.adr.policy.base import Policy, decide

from src.campaign.adr import (
    CampaignOperator,
    DownstreamFirstPolicy,
    RuleCorrectionsPolicy,
)
from src.campaign.adr.operator import CampaignOperator as _BaseCampaignOperator

log = logging.getLogger(__name__)

# Stable child operator ID — parent uses this to key into SNAPSHOT objectives.
_INNER_ID = "InnerOperator"


# ── Inner Operator — one sim→analysis round ────────────────────────────────────

class InnerOperator(CampaignOperator):
    """Supervises sim → analysis for one round, stopping after n_per_round hits.

    ``best_score_p50`` is an annotated class attribute proxied to
    ``state.objectives`` by the ADR state machinery.  Because the parent sets
    ``parent=self`` on this instance, every cycle the ADR framework serialises
    it into a SNAPSHOT sent to the parent, where it arrives as::

        snapshot.objectives["child:InnerOperator:obj:best_score_p50"]

    ``_baseline_hits`` is a plain instance attribute (not annotated at the class
    level) set at construction time from the current analysis.finished count.
    ``extract()`` computes ``hits_this_round = total_done - _baseline_hits``
    so that only analyses from this round count toward the stopping condition.
    """

    best_score_p50: float = 0.0  # proxied → state.objectives → SNAPSHOT

    def __init__(
        self,
        view,
        engine=None,
        *,
        n_per_round: int = 10,
        baseline_hits: int = 0,
        **kwargs,
    ) -> None:
        super().__init__(view, engine=engine, **kwargs)
        self.n_per_round = int(n_per_round)
        self._baseline_hits = int(baseline_hits)
        self._validate_stopping_condition()

    @goals
    def criteria(self):
        return Goal(
            name="inner_done",
            metric="hits_this_round",
            threshold=self.n_per_round - 0.5,
            direction="maximize",
        )

    @observe
    def extract(self, snapshot) -> dict:
        obs = _BaseCampaignOperator.extract(self, snapshot)
        stages = obs.get("stages", {})
        analysis = stages.get("analysis", {})

        total_done = int(analysis.get("finished", 0))
        hits_this_round = max(0, total_done - self._baseline_hits)

        # Update best_score_p50 running max.  Writing self.best_score_p50
        # persists it into state.objectives → SNAPSHOT to parent each cycle.
        p50 = analysis.get("score_p50")
        if p50 is not None and p50 > self.best_score_p50:
            self.best_score_p50 = p50

        obs["hits_this_round"] = hits_this_round
        return obs


# ── Outer Operator — sequences N inner loops ───────────────────────────────────

class OuterOperator(CampaignOperator):
    """Parent operator that sequences N inner sim→analysis rounds.

    Lifecycle (managed by OuterPolicy):

      No inner launched yet
          ``launch_inner()`` constructs a fresh InnerOperator, reactivates the
          CM, triggers n_sim sim replicas, sets analysis score gate cutoff (rounds
          2+), wires child→parent SNAPSHOT pipeline, calls child.start().

      InnerOperator DONE
          Reads this round's best score from artifacts.  If more rounds remain,
          calls ``launch_inner()`` again.  Otherwise returns Decision(stop=True).

      Otherwise
          Idle Decision(); logs status every 5 cycles.
    """

    def __init__(
        self,
        view,
        engine=None,
        *,
        n_rounds: int = 3,
        n_per_round: int = 10,
        n_sim: int = 50,
        shrink_factor: float = 0.85,
        cutoff_floor: float = 0.10,
        **kwargs,
    ) -> None:
        # max_cycles is the safety net; outer normally stops via Decision(stop=True).
        super().__init__(view, engine=engine, max_cycles=1000, **kwargs)
        self.n_rounds     = int(n_rounds)
        self.n_per_round  = int(n_per_round)
        self.n_sim        = int(n_sim)
        self.shrink_factor = float(shrink_factor)
        self.cutoff_floor  = float(cutoff_floor)
        self._engine = engine
        self._current_inner: InnerOperator | None = None

    @goals
    def criteria(self):
        # Outer has no static goal — it stops via Decision(stop=True) from
        # OuterPolicy once all rounds complete.  max_cycles is the safety net.
        return []

    # ── Launch act ────────────────────────────────────────────────────────────

    @act
    async def launch_inner(self) -> asyncio.Future:
        """Start the next round: tighten cutoff, trigger sims, launch InnerOperator.

        Constructs a *fresh* InnerOperator each call so state.objectives
        (and therefore ``best_score_p50`` in SNAPSHOT) start clean for the
        new round.  The previous round's best score was already persisted into
        ``state.artifacts[f"round_{N}_best"]`` by ``watch()``.
        """
        current_round = self.state.artifacts.get("current_round", 0) + 1
        self.state.artifacts["current_round"] = current_round

        # Tighten analysis score gate cutoff for rounds 2+.
        if current_round > 1:
            prev_key  = f"round_{current_round - 1}_best"
            prev_best = self.state.artifacts.get(prev_key, 0.0) or 0.0
            cutoff    = round(max(self.cutoff_floor, prev_best * self.shrink_factor), 3)
            self.view.set_score_cutoff("analysis", cutoff)
            log.info(
                "[outer r%d/%d] prev_best=%.3f → score gate=%.3f",
                current_round, self.n_rounds, prev_best, cutoff,
            )

        # Reactivate CM (safe no-op if still running) then trigger sim replicas.
        await self.view.reactivate()
        await self.view.trigger("sim", self.n_sim)
        log.info(
            "[outer r%d/%d] triggered %d sim replicas",
            current_round, self.n_rounds, self.n_sim,
        )

        # Snapshot baseline *after* trigger so only new analyses count this round.
        baseline = int(
            self.view.observe().get("stages", {}).get("analysis", {}).get("finished", 0)
        )

        # Fresh InnerOperator for this round.
        inner = InnerOperator(
            self.view,
            engine=self._engine,
            n_per_round=self.n_per_round,
            baseline_hits=baseline,
        )
        inner.policy = RuleCorrectionsPolicy(
            DownstreamFirstPolicy(inner), inner,
            correct_stalls=True, correct_budget=False,
        )
        # Wire child → parent SNAPSHOT pipeline (parent= not exposed in __init__).
        object.__setattr__(inner, "parent", self)
        self._current_inner = inner

        fut = inner.start()
        uid = str(getattr(fut, "id", id(fut)))
        self.state.artifacts["inner_uid"] = uid
        log.info(
            "[outer r%d/%d] InnerOperator started (uid=%s  baseline=%d)",
            current_round, self.n_rounds, uid, baseline,
        )
        return fut

    # ── Observe ───────────────────────────────────────────────────────────────

    @observe
    def watch(self, snapshot) -> dict:
        """Assemble outer's view: inner Future status + best score from SNAPSHOT.

        The running-max of ``best_score_p50`` from the inner SNAPSHOT is
        persisted in ``state.artifacts[f"round_{N}_best"]`` so it survives
        the per-cycle observation clear and is available to ``launch_inner()``
        for the next round's cutoff calculation.
        """
        current_round = self.state.artifacts.get("current_round", 0)
        inner_uid     = self.state.artifacts.get("inner_uid")

        inner_status = (
            snapshot.runtime.get(inner_uid, "NOT_STARTED")
            if inner_uid else "NOT_STARTED"
        )

        # Accumulate best_score_p50 from inner SNAPSHOT into per-round artifact.
        p50_now = snapshot.objectives.get(f"child:{_INNER_ID}:obj:best_score_p50") or 0.0
        if current_round > 0:
            round_key = f"round_{current_round}_best"
            stored    = self.state.artifacts.get(round_key, 0.0) or 0.0
            if p50_now > stored:
                self.state.artifacts[round_key] = p50_now

        return {
            "cycle":         snapshot.cycle,
            "current_round": current_round,
            "inner_uid":     inner_uid,
            "inner_status":  inner_status,
            "inner_best":    self.state.artifacts.get(f"round_{current_round}_best", 0.0) or 0.0,
        }


# ── Outer Policy — sequences N inner loops ────────────────────────────────────

class OuterPolicy(Policy):
    """Sequences N inner sim→analysis loops, stopping after the last one completes.

    Transition table::

      inner_uid is None         → launch round 1
      inner DONE, more rounds   → launch next round
      inner DONE, all done      → Decision(stop=True)
      inner FAILED / CANCELED   → Decision(stop=True)  (fast-fail)
      otherwise                 → Decision()  (idle, log every 5 cycles)
    """

    def __init__(self, op: OuterOperator) -> None:
        super().__init__()
        self._act      = op.get_actions()
        self._n_rounds = op.n_rounds

    @decide
    async def run(self, obs: dict) -> Decision:
        inner_uid     = obs["inner_uid"]
        inner_status  = obs["inner_status"]
        current_round = obs["current_round"]
        cycle         = obs["cycle"]
        inner_best    = obs["inner_best"]

        # No inner launched yet → start round 1.
        if inner_uid is None:
            log.info("[outer policy c%d] → launching round 1/%d", cycle, self._n_rounds)
            return Decision(actions=[self._act.launch_inner()])

        # Inner just finished.
        if inner_status == "DONE":
            log.info(
                "[outer policy c%d] round %d/%d DONE  best_p50=%.3f",
                cycle, current_round, self._n_rounds, inner_best,
            )
            if current_round < self._n_rounds:
                return Decision(actions=[self._act.launch_inner()])
            log.info("[outer policy c%d] all %d rounds complete → stopping", cycle, self._n_rounds)
            return Decision(stop=True)

        # Fast-fail on unexpected terminal status.
        if inner_status in ("FAILED", "CANCELED"):
            log.error(
                "[outer policy c%d] round %d inner %s → stopping",
                cycle, current_round, inner_status,
            )
            return Decision(stop=True)

        # Idle — log progress every 5 cycles.
        if cycle % 5 == 0:
            log.info(
                "[outer policy c%d] round %d/%d  inner=%s  best_p50=%.3f",
                cycle, current_round, self._n_rounds, inner_status, inner_best,
            )
        return Decision()
