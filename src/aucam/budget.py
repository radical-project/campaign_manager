"""BudgetAllocator: the campaign's spending ledger.

AuCaM budgets are expressed as *spending* — resource-hours per dimension
(e.g. ``cpu_hours``, ``gpu_hours``).  CM does not budget resources "in use at
once": a workflow is a set of heterogeneous tasks that RHAPSODY places on the
allocation as resources free up, so CM cannot know which GPUs or nodes a
workflow holds.  Capacity sharing is left to the allocation / RHAPSODY and to
the workflow-count limits (``CampaignBudget.max_concurrent_workflows`` etc.).

The ledger keeps, for the campaign:
  limits    — the campaign's spending limit (``CampaignBudget.spending``)
  spent     — actual spending of finished workflows
  in_flight — estimated spending of running workflows (their declared_spending),
              replaced by the actual amount when the workflow finishes

The "limit reached" check, the controller's budget observation and the
scheduler's remaining-budget fraction all read from this one place.  Entries
are keyed by campaign name.

Thread-safe; all public methods acquire a single lock.
"""

from __future__ import annotations

import logging
import threading

logger = logging.getLogger(__name__)


def _add(into: dict[str, float], usage: dict[str, float]) -> None:
    for k, v in usage.items():
        into[k] = into.get(k, 0.0) + float(v)


class BudgetAllocator:
    """Spending ledger of a campaign."""

    def __init__(self) -> None:
        self._limits: dict[str, dict[str, float]] = {}
        self._spent: dict[str, dict[str, float]] = {}
        self._in_flight: dict[str, dict[str, dict[str, float]]] = {}
        self._lock = threading.Lock()

    # ── Campaign lifecycle ───────────────────────────────────────────────────

    def open(self, campaign: str, limits: dict[str, float] | None = None) -> None:
        """Register a campaign and its spending limit (no-op if already open)."""
        with self._lock:
            if campaign not in self._limits:
                self._open_locked(campaign, limits or {})

    def close(self, campaign: str) -> dict[str, float]:
        """Close a campaign and return its actual spending.

        In-flight estimates are dropped (only finished workflows are charged).
        Safe to call more than once.
        """
        with self._lock:
            spent = self._spent.pop(campaign, None)
            self._limits.pop(campaign, None)
            self._in_flight.pop(campaign, None)
            return dict(spent) if spent is not None else {}

    # ── Workflow accounting ──────────────────────────────────────────────────

    def set_in_flight(self, campaign: str, wf_id: str, estimate: dict[str, float]) -> None:
        """Record the estimated spending of a running workflow."""
        with self._lock:
            if campaign not in self._limits:
                return
            est = {k: float(v) for k, v in estimate.items() if v and k != "wall_hours"}
            self._in_flight[campaign][wf_id] = est

    def clear_in_flight(self, campaign: str, wf_id: str) -> None:
        with self._lock:
            self._in_flight.get(campaign, {}).pop(wf_id, None)

    def record(self, campaign: str, usage: dict[str, float]) -> None:
        """Add actual spending (e.g. of a finished workflow) to a campaign."""
        with self._lock:
            if campaign not in self._limits:
                # Late charge after the campaign was closed (e.g. a workflow that
                # finished after the shutdown timeout): it is not in the outcome.
                logger.debug("[BUDGET] %s: late charge after close ignored: %s", campaign, usage)
                return
            _add(self._spent[campaign], usage)

    def restore(self, campaign: str, spent: dict[str, float]) -> None:
        """Set a campaign's actual spending (checkpoint recovery)."""
        with self._lock:
            self._ensure_locked(campaign)
            self._spent[campaign] = {k: float(v) for k, v in spent.items()}

    # ── Queries ──────────────────────────────────────────────────────────────

    def spent(self, campaign: str) -> dict[str, float]:
        with self._lock:
            return dict(self._spent.get(campaign, {}))

    def in_flight(self, campaign: str) -> dict[str, float]:
        """Sum of the in-flight estimates of a campaign's running workflows."""
        with self._lock:
            total: dict[str, float] = {}
            for est in self._in_flight.get(campaign, {}).values():
                _add(total, est)
            return total

    def limits(self, campaign: str) -> dict[str, float]:
        with self._lock:
            return dict(self._limits.get(campaign, {}))

    def limit_reached(self, campaign: str) -> tuple[str, float, float, float] | None:
        """Return (key, spent, in_flight, limit) for the first exceeded limit, else None.

        A limit is reached when actual spending plus the in-flight estimate of
        running workflows meets or exceeds it (conservative, like SLURM
        accounting).
        """
        with self._lock:
            limits = self._limits.get(campaign, {})
            if not limits:
                return None
            spent = self._spent.get(campaign, {})
            live: dict[str, float] = {}
            for est in self._in_flight.get(campaign, {}).values():
                _add(live, est)
            for key, limit in limits.items():
                inflight = live.get(key, 0.0)
                done = spent.get(key, 0.0)
                if done + inflight >= limit:
                    return key, done, inflight, limit
            return None

    def shortfall(self, campaign: str, cost: dict[str, float]) -> tuple[str, float, float] | None:
        """Return (key, cost, remaining) for the first limit ``cost`` does not fit, else None.

        A cost fits when spent + in-flight + cost stays within the limit; only
        dimensions the campaign limits are checked.
        """
        with self._lock:
            limits = self._limits.get(campaign, {})
            if not limits:
                return None
            spent = self._spent.get(campaign, {})
            live: dict[str, float] = {}
            for est in self._in_flight.get(campaign, {}).values():
                _add(live, est)
            for key, limit in limits.items():
                need = float(cost.get(key, 0.0) or 0.0)
                remaining = limit - spent.get(key, 0.0) - live.get(key, 0.0)
                if need > remaining + 1e-12:
                    return key, need, remaining
            return None

    def spending_fraction(self, campaign: str) -> float:
        """Fraction of budget consumed: max over all limit dimensions of
        (spent + in_flight) / limit.  Returns 0.0 when no limits are set,
        clamped to [0, 1].
        """
        with self._lock:
            limits = self._limits.get(campaign, {})
            if not limits:
                return 0.0
            spent = self._spent.get(campaign, {})
            live: dict[str, float] = {}
            for est in self._in_flight.get(campaign, {}).values():
                _add(live, est)
            max_frac = 0.0
            for key, limit in limits.items():
                if limit <= 0:
                    continue
                used = spent.get(key, 0.0) + live.get(key, 0.0)
                max_frac = max(max_frac, used / limit)
            return min(1.0, max_frac)

    def campaign_view(self, campaign: str) -> dict:
        """The campaign's ledger, passed to its controller as the ``budget``
        observation::

            {"limits": {...}, "spent": {...}, "in_flight": {...},
             "burn": {dim: {"remaining": float, "fraction_left": float}}}
        """
        with self._lock:
            live: dict[str, float] = {}
            for est in self._in_flight.get(campaign, {}).values():
                _add(live, est)
            limits = dict(self._limits.get(campaign, {}))
            spent = dict(self._spent.get(campaign, {}))
            burn: dict[str, dict[str, float]] = {}
            for key, limit in limits.items():
                if limit > 0:
                    used = spent.get(key, 0.0) + live.get(key, 0.0)
                    remaining = max(0.0, limit - used)
                    burn[key] = {
                        "remaining": remaining,
                        "fraction_left": remaining / limit,
                    }
            return {"limits": limits, "spent": spent, "in_flight": live, "burn": burn}

    # ── Internals (lock held) ────────────────────────────────────────────────

    def _open_locked(self, campaign: str, limits: dict[str, float]) -> None:
        self._limits[campaign] = dict(limits)
        self._spent.setdefault(campaign, {})
        self._in_flight.setdefault(campaign, {})

    def _ensure_locked(self, campaign: str) -> None:
        if campaign not in self._limits:
            self._open_locked(campaign, {})
