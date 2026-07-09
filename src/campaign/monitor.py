"""
Drift detection monitor for campaign stage deviations.

Ported from cm-prototype/src/cm/components/monitor.py.

Tracks two signals per stage:
  pass_through  — fraction of upstream completions that triggered downstream
                  vs. the plan's threshold_top_fraction / trigger_fraction
  budget_burn   — node-hours spent vs. proportional plan budget

Each check returns a DriftEvent when the deviation exceeds the configured
percentage threshold.  Consecutive breaches increment a counter; once it
reaches breaches_to_escalate the monitor flags escalation.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class DriftKind(Enum):
    BUDGET_BURN = "budget_burn"  # spend > expected by Monitor's threshold
    PASS_THROUGH = "pass_through"  # observed pass-through ≠ planned fraction
    SURROGATE_RECALL = "surrogate_recall"  # surrogate model accuracy degraded
    BUDGET_LOCKED = "budget_locked"  # BudgetController exhausted its nudge envelope


@dataclass
class DriftEvent:
    kind: DriftKind
    stage_id: str
    observed: float
    expected: float
    deviation_pct: float
    breach_count: int = 1


@dataclass
class Monitor:
    """Detect plan vs. actual deviations and escalate after N consecutive breaches."""

    burn_dev_pct: float = 20.0  # % deviation allowed for budget burn
    passthrough_dev_pct: float = 25.0  # % deviation allowed for pass-through rate
    recall_floor: float = 0.90  # minimum surrogate recall before alert
    breaches_to_escalate: int = 2  # consecutive breaches before escalation

    # internal breach counters: (stage_id, DriftKind) → count
    _counts: dict = field(default_factory=dict)

    def check_passthrough(
        self, stage_id: str, observed: float, expected: float
    ) -> Optional[DriftEvent]:
        """Fire when |observed - expected| / expected > passthrough_dev_pct."""
        return self._check(
            stage_id,
            DriftKind.PASS_THROUGH,
            observed,
            expected,
            self.passthrough_dev_pct,
        )

    def check_budget(self, stage_id: str, spent: float, expected: float) -> Optional[DriftEvent]:
        """Fire when |spent - expected| / expected > burn_dev_pct."""
        return self._check(
            stage_id,
            DriftKind.BUDGET_BURN,
            spent,
            expected,
            self.burn_dev_pct,
        )

    def check_recall(self, stage_id: str, recall: float) -> Optional[DriftEvent]:
        """Fire when surrogate recall drops below recall_floor."""
        if recall >= self.recall_floor:
            self._reset(stage_id, DriftKind.SURROGATE_RECALL)
            return None
        dev_pct = (self.recall_floor - recall) / self.recall_floor * 100
        key = (stage_id, DriftKind.SURROGATE_RECALL)
        self._counts[key] = self._counts.get(key, 0) + 1
        return DriftEvent(
            DriftKind.SURROGATE_RECALL,
            stage_id,
            recall,
            self.recall_floor,
            dev_pct,
            self._counts[key],
        )

    def is_escalating(self, ev: DriftEvent) -> bool:
        """True when the breach count has reached the escalation threshold."""
        return ev.breach_count >= self.breaches_to_escalate

    def reset(self, stage_id: str, kind: DriftKind) -> None:
        self._reset(stage_id, kind)

    # ── internal ─────────────────────────────────────────────────────────────

    def _check(
        self,
        stage_id: str,
        kind: DriftKind,
        observed: float,
        expected: float,
        threshold_pct: float,
    ) -> Optional[DriftEvent]:
        if expected <= 0:
            return None
        dev_pct = abs(observed - expected) / expected * 100
        key = (stage_id, kind)
        if dev_pct > threshold_pct:
            self._counts[key] = self._counts.get(key, 0) + 1
            return DriftEvent(kind, stage_id, observed, expected, dev_pct, self._counts[key])
        self._reset(stage_id, kind)
        return None

    def _reset(self, stage_id: str, kind: DriftKind) -> None:
        self._counts.pop((stage_id, kind), None)
