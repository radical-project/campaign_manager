"""Per-stage BudgetController: adapts Triage cutoffs to stay within budget.

The Plan declares a hard budget per stage (``budget_node_hours``).  This
controller is the *soft* feedback loop that adjusts surrogate cutoffs to
keep the actual burn rate close to the planned trajectory.

Control law
-----------
At each evaluate() tick:

    progress    = finished_replicas / downstream_input_target
    expected    = budget_node_hours × progress
    actual      = pilot.nodes × pilot.walltime_h × finished_replicas
    burn_ratio  = actual / expected

If |burn_ratio - 1| ≤ burn_rate_band → in-band, no action.
Otherwise the controller nudges the Triage:

    error       = burn_ratio - 1
    score_delta = +kp × error    (positive when over-budget → tighten)
    unc_delta   = -kp × error    (negative when over-budget → reject noisy)

Bounds are enforced by Triage.nudge_cutoffs; the at-bound signal feeds into
the escalation counter.  After ``consecutive_bound_threshold`` consecutive
locked ticks, evaluate() returns a BudgetEvent of kind ``bound_locked``
which the Monitor lifts into a DriftEvent / replan request.

Warmup
------
The controller does nothing until both:
  - ``finished_replicas ≥ warmup_min_finished`` (absolute floor)
  - ``progress ≥ warmup_progress``              (relative floor)

This avoids overreacting to noise in the first few completions.

Surrogate-drift freeze
----------------------
When the Monitor's surrogate_recall check fires, the controller's signal
becomes unreliable (cutoffs are operating on a degenerate surrogate).
``freeze(True)`` halts nudging until the next tick that clears.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .plan import StageSpec
    from .triage import Triage


@dataclass
class BudgetEvent:
    """Outcome of one BudgetController.evaluate() tick.

    Attributes
    ----------
    stage_id:           which stage was evaluated
    kind:               "in_band" (no nudge), "nudged" (adjusted within bounds),
                        "bound_locked" (escalation — repeated bound hits)
    burn_ratio:         actual / expected node-hours at current progress
    progress:           finished / target (0..1)
    score_cutoff:       cutoff after this tick (post-nudge)
    uncertainty_cutoff: cutoff after this tick (post-nudge)
    score_at_bound:     True when score_cutoff sits at a nudge_bound
    unc_at_bound:       True when uncertainty_cutoff sits at a nudge_bound
    consecutive_hits:   how many consecutive ticks hit a bound
    frozen:             True when controller is paused (surrogate drift)
    """
    stage_id:           str
    kind:               str
    burn_ratio:         float
    progress:           float
    score_cutoff:       float
    uncertainty_cutoff: float
    score_at_bound:     bool = False
    unc_at_bound:       bool = False
    consecutive_hits:   int  = 0
    frozen:             bool = False


@dataclass
class BudgetController:
    """Per-stage budget feedback loop."""
    stage_id:                       str
    triage:                         "Triage"
    budget_node_hours:              float
    pilot_nodes:                    int
    pilot_walltime_h:               float
    downstream_target:              int

    # Tunable parameters (Plan can override, otherwise defaults apply)
    burn_rate_band:                 float = 0.15
    kp:                             float = 0.05
    # warmup_min_finished defaults to 3 so stages with small targets
    # (e.g., terminal s5 with target=5 in a benchmark cascade) still
    # engage the controller — the previous default of 10 effectively
    # disabled the controller for any stage whose target wasn't deep
    # into double digits.
    warmup_min_finished:            int   = 3
    warmup_progress:                float = 0.10
    consecutive_bound_threshold:    int   = 3

    # Runtime state
    _consecutive_bound_hits:        int   = field(default=0, init=False)
    _frozen:                        bool  = field(default=False, init=False)

    def __post_init__(self) -> None:
        if self.budget_node_hours < 0:
            raise ValueError(f"budget_node_hours must be ≥ 0, got {self.budget_node_hours}")
        if not (0.0 <= self.burn_rate_band <= 1.0):
            raise ValueError(f"burn_rate_band must be in [0, 1], got {self.burn_rate_band}")
        if self.kp <= 0:
            raise ValueError(f"kp must be > 0, got {self.kp}")
        if self.warmup_min_finished < 1:
            raise ValueError(
                f"warmup_min_finished must be ≥ 1, got {self.warmup_min_finished}"
            )
        if self.consecutive_bound_threshold < 1:
            raise ValueError(
                f"consecutive_bound_threshold must be ≥ 1, "
                f"got {self.consecutive_bound_threshold}"
            )

    # ── Factory ───────────────────────────────────────────────────────────

    @classmethod
    def from_stage_spec(
        cls,
        spec: "StageSpec",
        triage: "Triage",
        kp: float = 0.05,
        consecutive_bound_threshold: int = 3,
        warmup_min_finished: int = 3,
    ) -> "BudgetController":
        """Build a controller from a plan-side StageSpec + an attached Triage."""
        return cls(
            stage_id=spec.id,
            triage=triage,
            budget_node_hours=spec.budget_node_hours,
            pilot_nodes=spec.pilot.nodes,
            pilot_walltime_h=spec.pilot.walltime_h,
            downstream_target=spec.downstream_input_target,
            burn_rate_band=spec.burn_rate_band,
            kp=kp,
            consecutive_bound_threshold=consecutive_bound_threshold,
            warmup_min_finished=warmup_min_finished,
        )

    # ── Freeze controls ───────────────────────────────────────────────────

    def freeze(self, frozen: bool = True) -> None:
        """Pause/resume nudging (called when Monitor flags surrogate-recall drift)."""
        self._frozen = frozen

    @property
    def frozen(self) -> bool:
        return self._frozen

    # ── Main control loop ────────────────────────────────────────────────

    def evaluate(
        self,
        finished_replicas: int,
        actual_node_hours: Optional[float] = None,
    ) -> Optional[BudgetEvent]:
        """One controller tick.

        Returns:
          - ``None`` if warmup not complete or no controllable budget
          - ``BudgetEvent`` describing the action taken (in_band / nudged /
            bound_locked) and the resulting cutoff state

        actual_node_hours: if None, compute as
            pilot_nodes × pilot_walltime_h × finished_replicas
            (i.e., conservative — assume each replica burned its full pilot
            allocation).  Pass a measured value for higher fidelity.
        """
        # No budget configured → nothing to control
        if self.budget_node_hours <= 0:
            return None
        # No target → can't compute progress
        if self.downstream_target <= 0:
            return None
        # Warmup
        if finished_replicas < self.warmup_min_finished:
            return None
        progress = finished_replicas / self.downstream_target
        if progress < self.warmup_progress:
            return None

        # Frozen by surrogate-drift detector → return a snapshot but don't nudge
        if self._frozen:
            return BudgetEvent(
                stage_id=self.stage_id,
                kind="in_band",
                burn_ratio=1.0,
                progress=progress,
                score_cutoff=self.triage.score_cutoff,
                uncertainty_cutoff=self.triage.uncertainty_cutoff,
                consecutive_hits=self._consecutive_bound_hits,
                frozen=True,
            )

        # Compute burn ratio
        if actual_node_hours is None:
            actual = self.pilot_nodes * self.pilot_walltime_h * finished_replicas
        else:
            actual = actual_node_hours
        expected = self.budget_node_hours * progress
        if expected <= 0:
            return None
        burn_ratio = actual / expected

        # In-band: no action
        if abs(burn_ratio - 1.0) <= self.burn_rate_band:
            self._consecutive_bound_hits = 0
            return BudgetEvent(
                stage_id=self.stage_id,
                kind="in_band",
                burn_ratio=burn_ratio,
                progress=progress,
                score_cutoff=self.triage.score_cutoff,
                uncertainty_cutoff=self.triage.uncertainty_cutoff,
                consecutive_hits=0,
            )

        # Out-of-band: nudge
        # Sign convention: error > 0 means over-budget → tighten.
        error       = burn_ratio - 1.0
        score_delta = +self.kp * error
        unc_delta   = -self.kp * error

        score_at_bound, unc_at_bound = self.triage.nudge_cutoffs(
            score_delta, unc_delta,
        )
        if score_at_bound or unc_at_bound:
            self._consecutive_bound_hits += 1
        else:
            self._consecutive_bound_hits = 0

        kind = "nudged"
        if self._consecutive_bound_hits >= self.consecutive_bound_threshold:
            kind = "bound_locked"

        return BudgetEvent(
            stage_id=self.stage_id,
            kind=kind,
            burn_ratio=burn_ratio,
            progress=progress,
            score_cutoff=self.triage.score_cutoff,
            uncertainty_cutoff=self.triage.uncertainty_cutoff,
            score_at_bound=score_at_bound,
            unc_at_bound=unc_at_bound,
            consecutive_hits=self._consecutive_bound_hits,
        )

    # ── Inspection ────────────────────────────────────────────────────────

    def state(self) -> dict:
        """Snapshot of controller + attached Triage state — for status()."""
        return {
            "stage_id":                 self.stage_id,
            "budget_node_hours":        self.budget_node_hours,
            "burn_rate_band":           self.burn_rate_band,
            "kp":                       self.kp,
            "downstream_target":        self.downstream_target,
            "consecutive_bound_hits":   self._consecutive_bound_hits,
            "frozen":                   self._frozen,
            "triage":                   self.triage.state(),
        }
