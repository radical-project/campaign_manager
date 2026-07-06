"""ReplanningController — DRIFT → DRAIN → RESUME handshake with the Planner.

When the Monitor or BudgetController detects drift that can't be corrected
by in-band adjustments (e.g., cutoffs pinned at bounds for K cycles, or a
permanent surrogate-recall degradation), this controller orchestrates the
formal hand-off back to the Planner:

    NORMAL
      ↓     drift detected
    DRAINING                stop new triggers; wait for in-flight to settle
      ↓     drain complete (or drain_timeout_s elapsed)
    AWAITING_PLAN           emit ReplanRequest; wait for Planner response
      ↓     new plan received and verified
    RESUMING                install new plan, refresh per-stage triages
      ↓     installation complete
    NORMAL                  back to steady operation

I/O is pluggable.  ``request_sink`` writes the request out (file, HTTP, MQ,
in-process bus); ``response_source`` blocks until the Planner returns a new
``CampaignPlan``.  ``snapshot_fn`` produces the current campaign state to
ship with the request.

Verification on RESUMING checks:
  - plan_version > current_version          (monotonic)
  - parent_plan_ref == "<plan_id>@v<old_version>"
  - signature (when signing is implemented; currently no-op)

When ``response_source`` is None the controller stays in AWAITING_PLAN
and logs the request — useful for prototype runs where the Planner is
out-of-band and the user re-launches manually.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .monitor import DriftEvent
    from .plan import CampaignPlan


class ReplanningState(Enum):
    """States of the drain-replan-resume handshake."""
    NORMAL         = "normal"
    DRAINING       = "draining"
    AWAITING_PLAN  = "awaiting_plan"
    RESUMING       = "resuming"


@dataclass
class ReplanRequest:
    """Structured request emitted to the Planner.

    Carries the drift that triggered the handshake, current plan
    identifiers, and a snapshot of the campaign so the Planner can
    re-solve with up-to-date evidence.
    """
    plan_id:             str
    plan_version:        int
    triggering_kind:     str             # DriftKind.value
    triggering_stage_id: Optional[str]
    snapshot:            dict
    timestamp:           float           = field(default_factory=time.time)
    request_id:          str             = ""


@dataclass
class ReplanningController:
    """Drain → replan → resume orchestration."""
    plan_id:           str
    plan_version:      int

    # Pluggable I/O ────────────────────────────────────────────────────────
    request_sink:      Optional[Callable[["ReplanRequest"], Awaitable[None]]] = None
    response_source:   Optional[Callable[["ReplanRequest"], Awaitable["CampaignPlan"]]] = None
    snapshot_fn:       Optional[Callable[[], dict]]                              = None
    on_resume:         Optional[Callable[["CampaignPlan"], Awaitable[None]]]     = None

    # Tuning ───────────────────────────────────────────────────────────────
    drain_timeout_s:   float = 30.0

    # Diagnostics ──────────────────────────────────────────────────────────
    log:               Any   = None
    on_state_change:   Optional[Callable[[ReplanningState, ReplanningState], None]] = None

    # Runtime state ────────────────────────────────────────────────────────
    state:             ReplanningState = field(default=ReplanningState.NORMAL, init=False)
    request_seq:       int             = field(default=0,                       init=False)
    last_request:      Optional[ReplanRequest] = field(default=None,             init=False)

    _drained:          Optional[asyncio.Event] = field(default=None, init=False, repr=False)
    _new_plan:         Optional["CampaignPlan"] = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        # asyncio.Event must be created lazily inside a running loop in
        # some pytest configurations; defer until first on_drift call.
        self._drained = None

    # ── Public API ────────────────────────────────────────────────────────

    async def on_drift(
        self,
        event: "DriftEvent",
        policy: str = "drain_and_replan",
    ) -> Optional["CampaignPlan"]:
        """Process a drift event.

        policy is the value of ``ReplanThresholds.on_drift`` for the active
        plan — ``log_only`` exits immediately; ``drain_and_replan`` runs
        the full handshake.
        """
        if policy == "log_only":
            self._log_info(
                f"ReplanningController: drift {event.kind.value} on "
                f"{event.stage_id!r} (policy=log_only — no action)"
            )
            return None

        if self.state is not ReplanningState.NORMAL:
            self._log_info(
                f"ReplanningController: already in {self.state.value} — "
                f"ignoring drift {event.kind.value} on {event.stage_id!r}"
            )
            return None

        # NORMAL → DRAINING ────────────────────────────────────────────────
        self._set_state(ReplanningState.DRAINING)
        self._log_warning(
            f"ReplanningController: drift {event.kind.value} on {event.stage_id!r} "
            f"→ DRAINING (deadline={self.drain_timeout_s}s)"
        )
        if self._drained is None:
            self._drained = asyncio.Event()
        try:
            await asyncio.wait_for(self._drained.wait(), timeout=self.drain_timeout_s)
            self._log_info("ReplanningController: drain complete")
        except asyncio.TimeoutError:
            self._log_warning(
                "ReplanningController: drain timeout — proceeding to replan request"
            )

        # DRAINING → AWAITING_PLAN ─────────────────────────────────────────
        self._set_state(ReplanningState.AWAITING_PLAN)
        request = self._make_request(event)
        self.last_request = request
        self._log_info(
            f"ReplanningController: emitting replan request {request.request_id} "
            f"(plan {request.plan_id}@v{request.plan_version})"
        )
        if self.request_sink is not None:
            try:
                await self.request_sink(request)
            except Exception as exc:
                self._log_error(f"ReplanningController: request_sink raised: {exc}")

        if self.response_source is None:
            self._log_warning(
                "ReplanningController: no response_source — staying in AWAITING_PLAN; "
                "rerun with a new plan to resume"
            )
            return None

        try:
            new_plan = await self.response_source(request)
        except Exception as exc:
            self._log_error(f"ReplanningController: response_source raised: {exc}")
            self._set_state(ReplanningState.NORMAL)
            return None

        # AWAITING_PLAN → RESUMING ─────────────────────────────────────────
        self._set_state(ReplanningState.RESUMING)
        ok, reason = self._verify_plan(new_plan)
        if not ok:
            self._log_error(
                f"ReplanningController: plan verification failed ({reason}); reverting to NORMAL"
            )
            self._set_state(ReplanningState.NORMAL)
            return None

        # Apply the new plan via the user-supplied hook (refresh triages,
        # reset budget controllers, etc.).  Missing hook is OK — the
        # caller may apply the plan inline after on_drift returns.
        if self.on_resume is not None:
            try:
                await self.on_resume(new_plan)
            except Exception as exc:
                self._log_error(f"ReplanningController: on_resume raised: {exc}")

        self.plan_id      = new_plan.plan_id
        self.plan_version = new_plan.plan_version
        self._new_plan    = new_plan

        # Reset drain event so the next handshake starts clean.
        self._drained = asyncio.Event()

        # RESUMING → NORMAL ────────────────────────────────────────────────
        self._set_state(ReplanningState.NORMAL)
        self._log_info(
            f"ReplanningController: resumed with plan {new_plan.plan_id}@v{new_plan.plan_version}"
        )
        return new_plan

    def drained(self) -> None:
        """Signal that all in-flight work has settled.  Call from the
        executor's running_count→0 transition while DRAINING."""
        if self._drained is not None:
            self._drained.set()

    def is_paused(self) -> bool:
        """Whether the scheduler should stop accepting new triggers."""
        return self.state in (
            ReplanningState.DRAINING,
            ReplanningState.AWAITING_PLAN,
            ReplanningState.RESUMING,
        )

    # ── Internals ────────────────────────────────────────────────────────

    def _make_request(self, event: "DriftEvent") -> ReplanRequest:
        self.request_seq += 1
        snapshot = self.snapshot_fn() if self.snapshot_fn else {}
        return ReplanRequest(
            plan_id=self.plan_id,
            plan_version=self.plan_version,
            triggering_kind=event.kind.value,
            triggering_stage_id=event.stage_id,
            snapshot=snapshot,
            request_id=f"{self.plan_id}@v{self.plan_version}#{self.request_seq:04d}",
        )

    def _verify_plan(self, new_plan: "CampaignPlan") -> tuple[bool, str]:
        """Verify the new plan is a valid successor.

        Prototype rules:
          - plan_version strictly greater than current
          - parent_plan_ref (if set) matches "<old_id>@v<old_version>"
        Production extension: signature verification with the Planner's
        public key (currently a no-op — signature field exists in
        CampaignPlan but no sign/verify helpers).
        """
        if new_plan.plan_version <= self.plan_version:
            return False, (
                f"plan_version not monotonic: {new_plan.plan_version} <= {self.plan_version}"
            )
        if new_plan.parent_plan_ref:
            expected = f"{self.plan_id}@v{self.plan_version}"
            if new_plan.parent_plan_ref != expected:
                return False, (
                    f"parent_plan_ref={new_plan.parent_plan_ref!r} != expected={expected!r}"
                )
        # Signature placeholder — return True for now.
        return True, ""

    def _set_state(self, new_state: ReplanningState) -> None:
        old = self.state
        self.state = new_state
        if self.on_state_change is not None:
            try:
                self.on_state_change(old, new_state)
            except Exception:
                pass

    def _log_info(self, msg: str) -> None:
        if self.log is not None:
            try:
                self.log.info(msg)
            except AttributeError:
                self.log(msg)

    def _log_warning(self, msg: str) -> None:
        if self.log is not None:
            try:
                self.log.warning(msg)
            except AttributeError:
                self.log(msg)

    def _log_error(self, msg: str) -> None:
        if self.log is not None:
            try:
                self.log.error(msg)
            except AttributeError:
                self.log(msg)

    # ── Inspection ────────────────────────────────────────────────────────

    def state_dict(self) -> dict:
        """Snapshot of controller status — for cm.state and status()."""
        return {
            "plan_id":       self.plan_id,
            "plan_version":  self.plan_version,
            "state":         self.state.value,
            "request_seq":   self.request_seq,
            "last_request_id": (self.last_request.request_id
                                if self.last_request else None),
        }
