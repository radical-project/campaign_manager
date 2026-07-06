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
    HOLD     = "hold"      # normal — neither throttling nor widening
    THROTTLE = "throttle"  # queue too deep — block new starts
    WIDEN    = "widen"     # queue drained — allow new starts freely


@dataclass
class BackpressureNegotiator:
    """Hysteresis state machine for one downstream stage queue."""

    edge_name:  str
    high_water: int
    low_water:  int
    state:      BPState = BPState.HOLD

    def __post_init__(self) -> None:
        if self.low_water >= self.high_water:
            raise ValueError(
                f"BackpressureNegotiator [{self.edge_name}]: "
                f"low_water ({self.low_water}) must be < high_water ({self.high_water})"
            )

    def step(self, depth: int) -> BPState:
        """Update state given current queue depth; return new state.

        Pure 3-state function — state is determined solely by depth:
          depth ≥ high_water → THROTTLE  (queue too deep, stop dispatching)
          depth ≤ low_water  → WIDEN     (queue drained, dispatch more)
          otherwise          → HOLD      (queue balanced, dispatch at normal rate)

        This makes HOLD reachable from both sides: queue rising through the
        (low_water, high_water) band gives HOLD on the way to THROTTLE, and
        queue draining gives HOLD on the way back to WIDEN.
        """
        if depth >= self.high_water:
            self.state = BPState.THROTTLE
        elif depth <= self.low_water:
            self.state = BPState.WIDEN
        else:
            self.state = BPState.HOLD
        return self.state

    def __repr__(self) -> str:
        return (f"BackpressureNegotiator({self.edge_name!r}, "
                f"hi={self.high_water}, lo={self.low_water}, state={self.state.value})")
