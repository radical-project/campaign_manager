"""Two-phase hierarchical ADR operator for the ddsim campaign.

Demonstrates the ADR hierarchical operator pattern (example 08) in a real campaign:

  Stage 1  DdSim-A screening (fast/cheap sims)
           Stage1Operator supervises ddsim_a + analysis until all ddsim_a finish.
           Persists best_score_p50 in state.objectives → parent reads via SNAPSHOT.

  Stage 2  DdSim-B refinement (slow/fine sims)
           Parent reads best_score_p50 from Stage 1 SNAPSHOT, tightens the
           analysis triage cutoff, triggers ddsim_b replicas, then launches
           Stage2Operator once Stage1 is confirmed DONE by the policy.

ADR concepts demonstrated:

  child.start()
      child.start() returns an AsyncFlow block Future tracked by the parent
      via _track_future.  Ordering between phases is enforced at the ADR
      policy level (not via AsyncFlow dep_future chaining) because the ADR
      operator prunes the Stage1 Future from state.task_futures on DONE, so
      a reconstructed future passed to Stage2.start() would lack the .block
      attribute required by AsyncFlow's _detect_dependencies.

  SNAPSHOT messaging
      Each cycle, a child with parent= set serialises its RuntimeState into
      a Snapshot and puts it in the parent's inbox.  Stage1's best_score_p50
      (an annotated class attribute → state.objectives) arrives under the key
      ``child:Stage1Operator:obj:best_score_p50`` in parent.state.objectives.

  Information passing between phases
      Parent reads Stage1's final best_score_p50 from artifacts (persisted
      from the running-max across SNAPSHOT cycles), tightens the analysis
      triage cutoff to that level (×0.85 margin), then triggers ddsim_b
      replicas before launching Stage2.
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

# Stable child operator IDs — parent uses these to key into SNAPSHOT observations.
_STAGE1_ID = "Stage1Operator"
_STAGE2_ID = "Stage2Operator"


# ── Stage 1 Operator — ddsim_a screening ──────────────────────────────────────

class Stage1Operator(CampaignOperator):
    """Supervises ddsim_a + analysis until all ddsim_a replicas finish.

    ``best_score_p50`` is an annotated class attribute that the ADR state-proxy
    persists in ``state.objectives``.  Each cycle, a SNAPSHOT is sent to the
    parent (because ``parent=`` is set in ParentOperator.__init__), making
    the latest ``best_score_p50`` visible to the parent as::

        snapshot.objectives["child:Stage1Operator:obj:best_score_p50"]
    """

    n_ddsim_a:      int   = 20
    best_score_p50: float = 0.0  # proxied to state.objectives → SNAPSHOT each cycle

    def __init__(self, view, engine=None, *, n_ddsim_a: int = 20, **kwargs) -> None:
        super().__init__(view, engine=engine, **kwargs)
        self.n_ddsim_a = int(n_ddsim_a)
        self._validate_stopping_condition()

    @goals
    def criteria(self):
        return Goal(
            name="stage1_done",
            metric="ddsim_a_finished",
            threshold=self.n_ddsim_a - 0.5,
            direction="maximize",
        )

    @observe
    def extract(self, snapshot) -> dict:
        obs = _BaseCampaignOperator.extract(self, snapshot)
        stages = obs.get("stages", {})
        obs["ddsim_a_finished"] = stages.get("ddsim_a", {}).get("finished", 0)
        # Track the best analysis score seen so far.
        # Writing self.best_score_p50 persists the value into state.objectives
        # so it arrives in parent's SNAPSHOT observation next cycle.
        p50 = stages.get("analysis", {}).get("score_p50")
        if p50 is not None and p50 > self.best_score_p50:
            self.best_score_p50 = p50
        return obs


# ── Stage 2 Operator — ddsim_b refinement ─────────────────────────────────────

class Stage2Operator(CampaignOperator):
    """Supervises ddsim_b + analysis until all ddsim_b replicas finish.

    ddsim_b starts at 0 replicas in config.  The parent triggers them just
    before launching Stage2, so this operator only has meaningful work once
    ddsim_b is running.
    """

    n_ddsim_b: int = 20

    def __init__(self, view, engine=None, *, n_ddsim_b: int = 20, **kwargs) -> None:
        super().__init__(view, engine=engine, **kwargs)
        self.n_ddsim_b = int(n_ddsim_b)
        self._validate_stopping_condition()

    @goals
    def criteria(self):
        return Goal(
            name="stage2_done",
            metric="ddsim_b_finished",
            threshold=self.n_ddsim_b - 0.5,
            direction="maximize",
        )

    @observe
    def extract(self, snapshot) -> dict:
        obs = _BaseCampaignOperator.extract(self, snapshot)
        stages = obs.get("stages", {})
        obs["ddsim_b_finished"] = stages.get("ddsim_b", {}).get("finished", 0)
        return obs


# ── Parent Operator — drives the two-phase lifecycle ──────────────────────────

class ParentOperator(CampaignOperator):
    """Parent operator that sequences Stage1 → Stage2 through its own ADR loop.

    Lifecycle:

      Cycle 0         ``@act launch_stage1()`` starts Stage1 as an AsyncFlow
                      block; returns its Future; uid stored in state.artifacts.

      Stage1 DONE     parent reads ``best_score_p50`` from the last SNAPSHOT,
                      tightens analysis triage cutoff, triggers ddsim_b, then
                      ``@act launch_stage2()`` starts Stage2.

      Stage2 DONE     ``Decision(stop=True)``; campaign complete.

    Child SNAPSHOTs arrive in parent.inbox each cycle and are merged into
    parent.state.observations.  The parent's ``@observe watch()`` reads them via
    ``snapshot.observations["child:Stage1Operator:obj:best_score_p50"]`` etc.
    """

    def __init__(
        self,
        view,
        engine=None,
        *,
        n_ddsim_a: int = 20,
        n_ddsim_b: int = 20,
        **kwargs,
    ) -> None:
        # max_cycles is the parent's safety net; normally stops via Decision(stop=True).
        super().__init__(view, engine=engine, max_cycles=500, **kwargs)

        # Children share the same CampaignView since they run sequentially.
        # Default operator_id for each class is type(self).__name__:
        #   Stage1Operator → "Stage1Operator" == _STAGE1_ID
        #   Stage2Operator → "Stage2Operator" == _STAGE2_ID
        self._stage1 = Stage1Operator(view, engine=engine, n_ddsim_a=n_ddsim_a)
        self._stage2 = Stage2Operator(view, engine=engine, n_ddsim_b=n_ddsim_b)

        # Wire children → parent SNAPSHOT pipeline.
        # parent= is not exposed through CampaignOperator.__init__, so set directly.
        object.__setattr__(self._stage1, "parent", self)
        object.__setattr__(self._stage2, "parent", self)

        # Assign scheduling policies to children.
        self._stage1.policy = RuleCorrectionsPolicy(
            DownstreamFirstPolicy(self._stage1), self._stage1,
            correct_stalls=True, correct_budget=False,
        )
        self._stage2.policy = RuleCorrectionsPolicy(
            DownstreamFirstPolicy(self._stage2), self._stage2,
            correct_stalls=True, correct_budget=False,
        )

    # ── Launch acts ───────────────────────────────────────────────────────────

    @act
    async def launch_stage1(self) -> asyncio.Future:
        """Start Stage 1 as an AsyncFlow block; return its block Future.

        Stores the Future's uid in state.artifacts so the parent's observe loop
        can track it across cycles and the policy can pass it as depends_on to
        launch_stage2.
        """
        fut = self._stage1.start()
        uid = str(getattr(fut, "id", id(fut)))
        self.state.artifacts["stage1_uid"] = uid
        log.info("[parent] Stage 1 launched (uid=%s).", uid)
        return fut

    @act
    async def launch_stage2(self) -> asyncio.Future:
        """Start Stage 2 once the parent policy has confirmed Stage 1 is DONE.

        The ADR policy only reaches this action when stage1_status == "DONE",
        so no AsyncFlow dep_future chain is needed — ordering is enforced at
        the policy level.  Passing a reconstructed asyncio.Future (from
        state.resolved) to self._stage2.start() would cause AsyncFlow's
        _detect_dependencies to append a plain Future without a .block
        attribute, triggering a TypeError inside _update_dependency_tracking
        and leaving the Stage2 block future in FAILED state.

        Reads Stage 1's best_score_p50 from artifacts, tightens the analysis
        triage cutoff, then triggers ddsim_b replicas before starting Stage 2.
        """
        p50 = self.state.artifacts.get("stage1_best_score", 0.0) or 0.0
        cutoff = round(max(0.10, p50 * 0.85), 3)
        self.view.set_score_cutoff("analysis", cutoff)
        log.info(
            "[parent] Stage 1 best_score_p50=%.3f → "
            "triage cutoff set to %.3f for Stage 2.",
            p50, cutoff,
        )

        n_b = self._stage2.n_ddsim_b
        # The CM may have "completed" after Stage 1 (ddsim_a + analysis done).
        # Reactivate it before triggering ddsim_b so the scheduler can start
        # Stage 2 replicas.  Safe no-op if the CM is still running.
        await self.view.reactivate()
        await self.view.trigger("ddsim_b", n_b)
        log.info(
            "[parent] %d ddsim_b replicas triggered → launching Stage 2.",
            n_b,
        )

        fut = self._stage2.start()
        uid = str(getattr(fut, "id", id(fut)))
        self.state.artifacts["stage2_uid"] = uid
        log.info("[parent] Stage 2 launched (uid=%s).", uid)
        return fut

    # ── Observe ───────────────────────────────────────────────────────────────

    @observe
    def watch(self, snapshot) -> dict:
        """Assemble parent's view: child Future statuses + Stage 1 score.

        stage1_uid / stage2_uid come from state.artifacts (persist across cycles).
        stage1_status / stage2_status come from state.runtime (set by TASK_EVENT
        done-callbacks when child blocks complete).
        stage1_score arrives via SNAPSHOT messages from Stage1Operator._dispatch()
        each cycle; the value is stored in parent.state.objectives (not
        state.observations) so watch() reads it from snapshot.objectives.

        The best Stage 1 score seen so far is persisted in state.artifacts so
        that launch_stage2 can read it even if the final SNAPSHOT and TASK_EVENT
        arrive in different parent cycles (observations are cleared per cycle;
        artifacts persist).
        """
        stage1_uid = snapshot.artifacts.get("stage1_uid")
        stage2_uid = snapshot.artifacts.get("stage2_uid")

        # Persist the running-max Stage 1 score into artifacts (survives clear_cycle).
        # Stage1's best_score_p50 is a proxied objective → arrives in parent's
        # state.objectives (via SNAPSHOT merge), not state.observations.
        p50_now = (
            snapshot.objectives.get(
                f"child:{_STAGE1_ID}:obj:best_score_p50"
            )
            or 0.0
        )
        stored_best = snapshot.artifacts.get("stage1_best_score", 0.0) or 0.0
        if p50_now > stored_best:
            self.state.artifacts["stage1_best_score"] = p50_now

        return {
            "cycle":         snapshot.cycle,
            "stage1_uid":    stage1_uid,
            "stage2_uid":    stage2_uid,
            "stage1_status": (
                snapshot.runtime.get(stage1_uid, "NOT_STARTED")
                if stage1_uid else "NOT_STARTED"
            ),
            "stage2_status": (
                snapshot.runtime.get(stage2_uid, "NOT_STARTED")
                if stage2_uid else "NOT_STARTED"
            ),
            # Best Stage 1 score seen so far (from artifacts for cross-cycle persistence).
            "stage1_score": snapshot.artifacts.get("stage1_best_score", 0.0) or 0.0,
        }

    @goals
    def criteria(self):
        # Parent has no static goal — it stops via Decision(stop=True) once
        # Stage 2 completes.  max_cycles=500 (set in __init__) is the safety net.
        return []


# ── Parent Policy — drives the Stage1 → Stage2 lifecycle ──────────────────────

class ParentPolicy(Policy):
    """Sequences Stage1 → Stage2 in the parent operator's ADR loop.

    The parent does no scheduling itself — children manage their own priorities
    via their own policies.  The parent's sole responsibility is child lifecycle
    management and stopping when Stage 2 completes.

    Transition table::

      cycle == 0 and no stage1_uid   → launch_stage1()
      phase1 DONE and no stage2_uid  → launch_stage2()
      phase2 DONE                    → Decision(stop=True)
      otherwise                      → Decision()  (idle, log every 5 cycles)
    """

    def __init__(self, op: ParentOperator) -> None:
        super().__init__()
        self._act = op.get_actions()

    @decide
    async def run(self, obs: dict) -> Decision:
        stage1_uid    = obs["stage1_uid"]
        stage2_uid    = obs["stage2_uid"]
        stage1_status = obs["stage1_status"]
        stage2_status = obs["stage2_status"]
        cycle         = obs["cycle"]

        # Cycle 0 (or first cycle with no child yet): launch Stage 1.
        if stage1_uid is None:
            log.info("[parent c%d] → launching Stage 1.", cycle)
            return Decision(actions=[self._act.launch_stage1()])

        # Stage 1 finished → tighten triage, trigger ddsim_b, launch Stage 2.
        if stage1_status == "DONE" and stage2_uid is None:
            log.info(
                "[parent c%d] Stage 1 DONE"
                " (best_score_p50=%.3f) → launching Stage 2.",
                cycle, obs["stage1_score"],
            )
            return Decision(actions=[self._act.launch_stage2()])

        # Stage 2 finished → campaign complete.
        if stage2_status == "DONE":
            log.info(
                "[parent c%d] Stage 2 DONE → stopping campaign.", cycle
            )
            return Decision(stop=True)

        # Phase failed or was canceled — stop immediately rather than looping to
        # max_cycles.  Without this, a FAILED child status falls through to the
        # idle branch and the parent loops until the 500-cycle safety net fires.
        if stage1_status in ("FAILED", "CANCELED") and stage2_uid is None:
            log.error(
                "[parent c%d] Stage 1 %s → stopping campaign.",
                cycle, stage1_status,
            )
            return Decision(stop=True)
        if stage2_status in ("FAILED", "CANCELED"):
            log.error(
                "[parent c%d] Stage 2 %s → stopping campaign.",
                cycle, stage2_status,
            )
            return Decision(stop=True)

        # LOST — job was killed while the phase was in-flight (loaded from checkpoint).
        # Re-launch the affected phase so the campaign can continue.
        if stage1_status == "LOST" and stage2_uid is None:
            log.warning("[parent c%d] Stage 1 LOST → re-launching.", cycle)
            return Decision(actions=[self._act.launch_stage1()])
        if stage2_status == "LOST":
            log.warning("[parent c%d] Stage 2 LOST → re-launching.", cycle)
            return Decision(actions=[self._act.launch_stage2()])

        # Idle — waiting for the active phase to finish; log every 5 cycles.
        if cycle % 5 == 0:
            active = "Stage 1" if stage2_uid is None else "Stage 2"
            log.info(
                "[parent c%d] waiting — %s running"
                "  p1=%s  p2=%s  p1_score=%.3f",
                cycle, active, stage1_status, stage2_status, obs["stage1_score"],
            )
        return Decision()
