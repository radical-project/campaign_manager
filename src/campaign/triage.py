"""Per-stage Triage: score + uncertainty gate with budget-nudgeable cutoffs.

The Triage sits between the Sharder's priority-ranked dispatch and the runtime
execution.  For each candidate it issues one of three decisions:

    RUN      — execute the candidate (the default outcome)
    DISCARD  — drop without running (saves compute when score or surrogate
               confidence says the candidate isn't worth measuring)
    ADVANCE  — skip the run and pass through to the next stage (reserved for
               surrogate-confident high-quality candidates; aggressive — use
               only when the surrogate is well-calibrated)

Cutoffs and bounds
------------------
The Planner sets initial values AND the bounds within which the CM may nudge:

    plan.surrogate.score_cutoff                  initial value
    plan.surrogate.score_cutoff_nudge_bounds     (low, high) the CM can move in
    plan.surrogate.uncertainty_cutoff            initial value
    plan.surrogate.uncertainty_cutoff_nudge_bounds  (low, high)

The BudgetController calls ``nudge_cutoffs(score_delta, unc_delta)`` each
controller tick — positive score_delta tightens (raise the bar), positive
unc_delta loosens (accept noisier predictions).  Both are clamped to bounds
and the method returns whether either knob hit a bound.

Sign conventions
----------------
    score_cutoff  ↑   = stricter (fewer accepted) ↔ slower burn
    uncertainty_cutoff ↑ = looser  (accept noisier predictions) ↔ more exploration

The BudgetController's control law uses these conventions:

    burn_ratio > 1   →   over-budget   →   tighten:
        score_delta > 0  (raise score floor)
        unc_delta   < 0  (lower uncertainty ceiling — reject noisy preds)

    burn_ratio < 1   →   under-budget  →   loosen:
        score_delta < 0
        unc_delta   > 0  (accept noisier — more active learning)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .plan import SurrogateSpec


class TriageDecision(Enum):
    """Outcome of a per-candidate triage call."""

    RUN = "run"
    ADVANCE = "advance"
    DISCARD = "discard"


@dataclass
class Triage:
    """Per-stage score + uncertainty gate with nudgeable cutoffs.

    Stateless w.r.t. candidates — each ``decide()`` is a pure function of
    the current cutoffs and the candidate's signals.  The cutoffs themselves
    are mutated by ``nudge_cutoffs`` between dispatch cycles.
    """

    stage_id: str
    score_cutoff: float
    score_cutoff_bounds: tuple[float, float]
    uncertainty_cutoff: float
    uncertainty_cutoff_bounds: tuple[float, float]

    # ADVANCE only fires when surrogate_pred ≥ this — set well above
    # score_cutoff so it's a "high-confidence" decision.  Disabled (set to
    # +inf) by default so RUN is always preferred over ADVANCE; turn on by
    # passing advance_threshold explicitly when the surrogate is known to be
    # well-calibrated.
    advance_threshold: float = float("inf")
    # Initial values for reset(), captured in __post_init__.
    _score_initial: float = field(default=0.0, init=False)
    _unc_initial: float = field(default=0.0, init=False)

    def __post_init__(self) -> None:
        # Snapshot initial values so reset() / state_log() can reference them.
        self._score_initial = self.score_cutoff
        self._unc_initial = self.uncertainty_cutoff
        # Sanity-check bounds shape.
        sc_lo, sc_hi = self.score_cutoff_bounds
        if sc_lo > sc_hi:
            raise ValueError(f"score_cutoff_bounds must be (low, high); got ({sc_lo}, {sc_hi})")
        un_lo, un_hi = self.uncertainty_cutoff_bounds
        if un_lo > un_hi:
            raise ValueError(
                f"uncertainty_cutoff_bounds must be (low, high); got ({un_lo}, {un_hi})"
            )

    # ── Factory ───────────────────────────────────────────────────────────

    @classmethod
    def from_surrogate_spec(
        cls,
        stage_id: str,
        spec: SurrogateSpec,
        advance_threshold: float = float("inf"),
    ) -> Triage:
        """Build a Triage from a plan-side SurrogateSpec."""
        return cls(
            stage_id=stage_id,
            score_cutoff=spec.score_cutoff,
            score_cutoff_bounds=spec.score_cutoff_nudge_bounds,
            uncertainty_cutoff=spec.uncertainty_cutoff,
            uncertainty_cutoff_bounds=spec.uncertainty_cutoff_nudge_bounds,
            advance_threshold=advance_threshold,
        )

    # ── Per-candidate decision ────────────────────────────────────────────

    def decide(
        self,
        score: float,
        surrogate_pred: float = 0.0,
        surrogate_unc: float = 0.0,
    ) -> TriageDecision:
        """Triage one candidate.

        DISCARD when:
          - surrogate uncertainty exceeds the cutoff (model can't help here)
          - AND neither score nor surrogate_pred clears the score floor
        ADVANCE when:
          - surrogate is confident (unc ≤ cutoff) AND prediction is very high
            (≥ advance_threshold)
          - reserved for well-calibrated surrogates; default disabled
        RUN otherwise.

        The DISCARD branch uses "AND" across score/surrogate_pred — either
        signal clearing the score floor is enough to RUN.  This avoids
        discarding a candidate that the surrogate happens to dislike when
        the upstream score is solid.
        """
        # High uncertainty AND low signal both ways → DISCARD
        if surrogate_unc > self.uncertainty_cutoff:
            if score < self.score_cutoff and surrogate_pred < self.score_cutoff:
                return TriageDecision.DISCARD

        # Confident high-quality → ADVANCE (only when explicitly enabled)
        if surrogate_unc <= self.uncertainty_cutoff and surrogate_pred >= self.advance_threshold:
            return TriageDecision.ADVANCE

        # Low score AND low surrogate prediction → DISCARD
        if score < self.score_cutoff and surrogate_pred < self.score_cutoff:
            return TriageDecision.DISCARD

        return TriageDecision.RUN

    # ── Budget-driven adjustment ──────────────────────────────────────────

    def nudge_cutoffs(
        self,
        score_delta: float,
        unc_delta: float,
    ) -> tuple[bool, bool]:
        """Adjust cutoffs by the requested deltas, clamped to bounds.

        Returns ``(score_at_bound, unc_at_bound)`` — True when the resulting
        value sits at either end of its nudge_bounds (within float epsilon).
        BudgetController uses this signal to detect that nudging can no
        longer correct burn and that escalation to replan is needed.
        """
        new_score = self.score_cutoff + score_delta
        new_unc = self.uncertainty_cutoff + unc_delta

        sc_lo, sc_hi = self.score_cutoff_bounds
        un_lo, un_hi = self.uncertainty_cutoff_bounds

        # Clamp first, then check whether we're at a bound after clamping.
        self.score_cutoff = max(sc_lo, min(sc_hi, new_score))
        self.uncertainty_cutoff = max(un_lo, min(un_hi, new_unc))

        eps = 1e-9
        score_at_bound = (
            abs(self.score_cutoff - sc_lo) < eps or abs(self.score_cutoff - sc_hi) < eps
        )
        unc_at_bound = (
            abs(self.uncertainty_cutoff - un_lo) < eps or abs(self.uncertainty_cutoff - un_hi) < eps
        )
        return score_at_bound, unc_at_bound

    def reset(self) -> None:
        """Restore the initial Plan-set cutoffs.  Used by ReplanningController
        on RESUME after a drain → new-plan handshake (each new plan re-arms
        the bands; in-flight nudges are discarded)."""
        self.score_cutoff = self._score_initial
        self.uncertainty_cutoff = self._unc_initial

    # ── Inspection ────────────────────────────────────────────────────────

    def state(self) -> dict:
        """Snapshot of current cutoffs and bounds — for logging / status()."""
        return {
            "stage_id": self.stage_id,
            "score_cutoff": self.score_cutoff,
            "score_cutoff_bounds": list(self.score_cutoff_bounds),
            "score_cutoff_initial": self._score_initial,
            "uncertainty_cutoff": self.uncertainty_cutoff,
            "uncertainty_cutoff_bounds": list(self.uncertainty_cutoff_bounds),
            "uncertainty_cutoff_initial": self._unc_initial,
            "advance_threshold": self.advance_threshold,
        }
