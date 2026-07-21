"""
Per-edge backpressure hysteresis controller.

Ported from cm-prototype/src/cm/components/backpressure.py.

Controls how many replicas are allowed to queue up for a downstream stage.
When the queue depth (triggered but not yet started) crosses high_water,
the controller enters THROTTLE state and the scheduler will not start new
downstream replicas.  When depth drops back below low_water it enters WIDEN.

The hysteresis gap between high_water and low_water prevents oscillation.
"""

from dataclasses import dataclass
from enum import Enum


class BPState(Enum):
    HOLD = "hold"  # normal — neither throttling nor widening
    THROTTLE = "throttle"  # queue too deep — block new starts
    WIDEN = "widen"  # queue drained — allow new starts freely


@dataclass
class BackpressureNegotiator:
    """Hysteresis state machine for one downstream stage queue."""

    edge_name: str
    high_water: int
    low_water: int
    state: BPState = BPState.HOLD
    # score_slack: when > 0, a high-quality upstream buffer raises the effective
    # high-water mark so we throttle less aggressively when the sharder holds
    # good candidates.  Set via backpressure_score_slack in the per-stage
    # workflow config (default 0.0 = feature off).
    #
    # Example: high_water=10, score_slack=0.3, score_quality=0.8
    #   → effective_high = int(10 * (1 + 0.8 * 0.3)) = 12
    #
    # low_water is intentionally kept fixed — once the queue hits THROTTLE we
    # still drain aggressively to low_water before recovering to WIDEN.
    score_slack: float = 0.0

    def __post_init__(self) -> None:
        if self.low_water >= self.high_water:
            raise ValueError(
                f"BackpressureNegotiator [{self.edge_name}]: "
                f"low_water ({self.low_water}) must be < high_water ({self.high_water})"
            )

    def step(self, depth: int, score_quality: float = 0.5) -> BPState:
        """Update state given current queue depth; return new state.

        score_quality: [0, 1] signal from the upstream sharder's buffer.
          0.0 = buffer holds only poor candidates (below the all-time mean).
          1.0 = buffer holds only top-of-history candidates.
          Default 0.5 = neutral (no sharder attached, or buffer is empty).

        When score_slack > 0 a high-quality buffer raises the effective
        high_water, permitting more depth before throttling.  low_water is
        unchanged so draining is always aggressive once THROTTLE is entered.

        State transitions:
          depth ≥ effective_high_water → THROTTLE
          depth ≤ low_water            → WIDEN
          otherwise                    → HOLD
        """
        effective_high = int(self.high_water * (1.0 + score_quality * self.score_slack))
        if depth >= effective_high:
            self.state = BPState.THROTTLE
        elif depth <= self.low_water:
            self.state = BPState.WIDEN
        else:
            self.state = BPState.HOLD
        return self.state

    def __repr__(self) -> str:
        return (
            f"BackpressureNegotiator({self.edge_name!r}, "
            f"hi={self.high_water}, lo={self.low_water}, state={self.state.value})"
        )
