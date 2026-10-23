"""BudgetAllocator: the spending ledger for all campaigns.

AuCaM budgets are expressed as *spending* — resource-hours per dimension
(e.g. ``cpu_hours``, ``gpu_hours``).  CM does not budget resources "in use at
once": a workflow is a set of heterogeneous tasks that RHAPSODY places on the
allocation as resources free up, so CM cannot know which GPUs or nodes a
workflow holds.  Capacity sharing is left to the allocation / RHAPSODY and to
the workflow-count limits (``CampaignBudget.max_concurrent_workflows`` etc.).

The ledger keeps, per campaign:
  limits    — the campaign's spending limit (``CampaignBudget.spending``)
  spent     — actual spending of finished workflows
  in_flight — estimated spending of running workflows (their declared_spending),
              replaced by the actual amount when the workflow finishes

and, across campaigns:
  global spent — actual spending of closed campaigns
  committed    — spending limits of admitted, still-open campaigns

Admission against a global spending cap, the per-campaign "limit reached"
check, and the cross-campaign remaining budget all read from this one place.

Thread-safe; all public methods acquire a single lock.
"""

from __future__ import annotations

import threading


def _add(into: dict[str, float], usage: dict[str, float]) -> None:
    for k, v in usage.items():
        into[k] = into.get(k, 0.0) + float(v)


class BudgetAllocator:
    """Spending ledger shared by CampaignManager and all campaign instances."""

    def __init__(self) -> None:
        self._limits: dict[str, dict[str, float]] = {}
        self._spent: dict[str, dict[str, float]] = {}
        self._in_flight: dict[str, dict[str, dict[str, float]]] = {}
        self._global_spent: dict[str, float] = {}
        self._lock = threading.Lock()

    # ── Campaign lifecycle ───────────────────────────────────────────────────

    def admit(
        self,
        campaign: str,
        limits: dict[str, float],
        global_limits: dict[str, float] | None = None,
    ) -> str | None:
        """Check ``limits`` against the global spending cap and open the campaign.

        Admission rule, per dimension of ``global_limits``:
            global spent + committed (limits of open campaigns) + limits <= cap
        Returns a rejection reason, or None if the campaign was admitted
        (in which case its limits now count as committed).
        """
        with self._lock:
            for key, cap in (global_limits or {}).items():
                spent = self._global_spent.get(key, 0.0)
                committed = self._committed_locked(key)
                incoming = limits.get(key, 0.0)
                total = spent + committed + incoming
                if total > cap:
                    return (
                        f"global spending[{key}]={cap} would be exceeded "
                        f"(spent={spent:.5f}, committed={committed:.5f}, "
                        f"requesting={incoming:.5f}, total={total:.5f})"
                    )
            self._open_locked(campaign, limits)
            return None

    def open(self, campaign: str, limits: dict[str, float] | None = None) -> None:
        """Register a campaign without a global check (no-op if already open)."""
        with self._lock:
            if campaign not in self._limits:
                self._open_locked(campaign, limits or {})

    def close(self, campaign: str) -> dict[str, float]:
        """Close a campaign: move its actual spending into the global total.

        In-flight estimates are dropped (only finished workflows are charged).
        Returns the campaign's actual spending.  Safe to call more than once.
        """
        with self._lock:
            spent = self._spent.pop(campaign, None)
            self._limits.pop(campaign, None)
            self._in_flight.pop(campaign, None)
            if spent is None:
                return {}
            _add(self._global_spent, spent)
            return dict(spent)

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
                # finished after the shutdown timeout): count it globally.
                _add(self._global_spent, usage)
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
        accounting).  The legacy key ``cpu_wall_seconds`` is compared against
        in-flight ``cpu_hours`` converted to seconds.
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
                if key == "cpu_wall_seconds":
                    inflight = live.get("cpu_hours", 0.0) * 3600.0
                else:
                    inflight = live.get(key, 0.0)
                done = spent.get(key, 0.0)
                if done + inflight >= limit:
                    return key, done, inflight, limit
            return None

    def global_spent(self) -> dict[str, float]:
        with self._lock:
            return dict(self._global_spent)

    def committed(self, key: str, exclude: str | None = None) -> float:
        """Sum of the spending limits of open campaigns for ``key``."""
        with self._lock:
            return self._committed_locked(key, exclude)

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

    def global_remaining(self, key: str, cap: float) -> float:
        """cap − global spent − committed, for one dimension."""
        with self._lock:
            return cap - self._global_spent.get(key, 0.0) - self._committed_locked(key)

    def campaign_view(self, campaign: str) -> dict:
        """One campaign's part of the ledger, passed to its controller as the
        ``budget`` observation (controllers do not see other campaigns)::

            {"limits": {...}, "spent": {...}, "in_flight": {...},
             "global_spent": {...},
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
            return {
                "limits": limits,
                "spent": spent,
                "in_flight": live,
                "global_spent": dict(self._global_spent),
                "burn": burn,
            }

    def snapshot(self) -> dict:
        """Whole-ledger view (all open campaigns), for CM-level inspection."""
        with self._lock:
            campaigns = {}
            for name in self._limits:
                live: dict[str, float] = {}
                for est in self._in_flight.get(name, {}).values():
                    _add(live, est)
                campaigns[name] = {
                    "limits": dict(self._limits[name]),
                    "spent": dict(self._spent.get(name, {})),
                    "in_flight": live,
                }
            return {"campaigns": campaigns, "global_spent": dict(self._global_spent)}

    # ── Internals (lock held) ────────────────────────────────────────────────

    def _open_locked(self, campaign: str, limits: dict[str, float]) -> None:
        self._limits[campaign] = dict(limits)
        self._spent.setdefault(campaign, {})
        self._in_flight.setdefault(campaign, {})

    def _ensure_locked(self, campaign: str) -> None:
        if campaign not in self._limits:
            self._open_locked(campaign, {})

    def _committed_locked(self, key: str, exclude: str | None = None) -> float:
        return sum(lim.get(key, 0.0) for name, lim in self._limits.items() if name != exclude)
