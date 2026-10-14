"""BudgetAllocator: shared budget bookkeeping across all campaigns."""

import threading


class BudgetAllocator:
    """
    Tracks the shared budget pool and per-campaign reservations.

    Two separate accounting models:
      _in_use  — concurrency (borrow/return): charged on workflow launch,
                 released on finish; headroom() and fits() use this.
      _spending — lifetime spending limits (per campaign); the CM computes
                  actual spending from telemetry and surfaces it via
                  TelemetrySnapshot.spent; the controller checks against this.

    Thread-safe; all public methods acquire a single lock.
    """

    def __init__(self, total: dict[str, float] | None = None) -> None:
        self._total: dict[str, float] = dict(total or {})
        self._reserved: dict[str, dict[str, float]] = {}
        self._in_use: dict[str, dict[str, float]] = {}
        self._spending: dict[str, dict[str, float]] = {}
        self._lock = threading.Lock()

    def reserve(
        self,
        campaign_id: str,
        budget: dict[str, float],
        spending: dict[str, float] | None = None,
    ) -> bool:
        """Reserve concurrency `budget` for `campaign_id`.  Returns False if insufficient."""
        with self._lock:
            avail = self._available_locked()
            for dim, amount in budget.items():
                if self._total and avail.get(dim, 0.0) < amount:
                    return False
            self._reserved[campaign_id] = dict(budget)
            self._in_use[campaign_id] = {k: 0.0 for k in budget}
            self._spending[campaign_id] = dict(spending or {})
            return True

    def release(self, campaign_id: str) -> None:
        """Return all reserved budget to the shared pool (campaign teardown)."""
        with self._lock:
            self._reserved.pop(campaign_id, None)
            self._in_use.pop(campaign_id, None)
            self._spending.pop(campaign_id, None)

    def charge(self, campaign_id: str, usage: dict[str, float]) -> None:
        """Mark resources as in-use when a workflow launches."""
        with self._lock:
            in_use = self._in_use.get(campaign_id)
            if in_use is None:
                return
            for dim, amount in usage.items():
                in_use[dim] = in_use.get(dim, 0.0) + amount

    def release_workflow(self, campaign_id: str, resources: dict[str, float]) -> None:
        """Free in-use resources when a workflow finishes or is cancelled."""
        with self._lock:
            in_use = self._in_use.get(campaign_id)
            if in_use is None:
                return
            for dim, amount in resources.items():
                in_use[dim] = max(0.0, in_use.get(dim, 0.0) - amount)

    def headroom(self, campaign_id: str) -> dict[str, float]:
        """Remaining concurrency headroom (reserved minus currently in-use)."""
        with self._lock:
            reserved = self._reserved.get(campaign_id, {})
            in_use = self._in_use.get(campaign_id, {})
            return {dim: reserved.get(dim, 0.0) - in_use.get(dim, 0.0) for dim in reserved}

    def fits(self, campaign_id: str, resources: dict[str, float]) -> bool:
        """Return True if launching `resources` would not exceed the concurrency ceiling."""
        if not resources:
            return True
        hw = self.headroom(campaign_id)
        return all(hw.get(dim, float("inf")) >= amount for dim, amount in resources.items())

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "total": dict(self._total),
                "reserved": {k: dict(v) for k, v in self._reserved.items()},
                "in_use": {k: dict(v) for k, v in self._in_use.items()},
                "spending": {k: dict(v) for k, v in self._spending.items()},
            }

    def _available_locked(self) -> dict[str, float]:
        total_reserved: dict[str, float] = {}
        for dims in self._reserved.values():
            for dim, amount in dims.items():
                total_reserved[dim] = total_reserved.get(dim, 0.0) + amount
        return {
            dim: self._total.get(dim, 0.0) - total_reserved.get(dim, 0.0)
            for dim in self._total
        }
