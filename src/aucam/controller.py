"""CampaignController base class, Rule primitives, and PolicyResult."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field

from .reporter import WorkflowReport
from .types import CampaignDecision, LaunchRequest, QueueSnapshot, WorkflowResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Rule primitives — live in src/aucam so any campaign can reuse them
# ---------------------------------------------------------------------------

@dataclass
class CancelRule:
    """Cancel a running workflow when `when(report)` returns True."""

    workflow_type: str
    when: Callable[[WorkflowReport], bool]


@dataclass
class LaunchRule:
    """Keep requesting a workflow type while the goal is not yet reached."""

    workflow_type: str


# ---------------------------------------------------------------------------
# Goal (kept for backward compatibility and future use)
# ---------------------------------------------------------------------------

@dataclass
class Goal:
    """A single campaign goal: satisfied when `metric` crosses `threshold`."""

    name: str
    metric: str
    threshold: float
    direction: str  # "minimize" or "maximize"

    def satisfied(self, value: float) -> bool:
        if self.direction == "minimize":
            return value <= self.threshold
        return value >= self.threshold


# ---------------------------------------------------------------------------
# PolicyResult
# ---------------------------------------------------------------------------

@dataclass
class PolicyResult:
    """The controller's decision for one policy cycle."""

    decisions: dict[str, CampaignDecision] = field(default_factory=dict)
    launch_requests: list[LaunchRequest] = field(default_factory=list)
    terminate: bool = False
    cancel_queued: dict[str, int] | None = None
    goals: list[Goal] = field(default_factory=list)
    remove_goals: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# CampaignController — concrete base class
# ---------------------------------------------------------------------------

class CampaignController:
    """
    Base campaign controller.  Fully functional with no subclassing needed
    for simple campaigns; every method is overridable for science-specific logic.

    Parameters
    ----------
    launch : list[str]
        Workflow types to keep requesting each cycle (until goal is reached).
        The scheduler enforces concurrency caps — the controller just expresses intent.
    goal_type : str | None
        Count successful completions of this workflow type toward the goal.
    goal_n : int | None
        Stop when `goal_n` completions of `goal_type` have been recorded.
    rules : list[CancelRule | LaunchRule] | None
        Declarative rule set applied by `decide()`.  `launch` is a shorthand
        that appends a `LaunchRule` for each named workflow type.

    Public counters (updated by `on_complete`, readable by any caller)
    ------------------------------------------------------------------
    completed : {workflow_type: count}   returncode == 0
    cancelled : {workflow_type: count}   returncode == -1
    failed    : {workflow_type: count}   any other returncode
    """

    def __init__(
        self,
        *,
        launch: list[str] | None = None,
        goal_type: str | None = None,
        goal_n: int | None = None,
        rules: list[CancelRule | LaunchRule] | None = None,
    ) -> None:
        self._goal_type = goal_type
        self._goal_n    = goal_n

        # Merge explicit rules + launch shorthand into one list
        self._rules: list[CancelRule | LaunchRule] = list(rules or [])
        for wt in (launch or []):
            self._rules.append(LaunchRule(wt))

        # Public lifecycle counters — readable from run_campaign.py summaries
        self.completed: dict[str, int] = {}
        self.cancelled: dict[str, int] = {}
        self.failed:    dict[str, int] = {}

    # ── Overridable hooks ─────────────────────────────────────────────────────

    def on_complete(self, result: WorkflowResult) -> None:
        """Update lifecycle counters.  Override to add domain-specific tracking;
        call super().on_complete(result) to keep the base counts accurate.

        Subclasses that override __init__ without calling super().__init__() will
        not have these counters initialized; the method skips any counter that is
        not present as a dict so it never crashes in that case.
        """
        iv = vars(self)
        if result.returncode == 0:
            c = iv.get('completed')
            if isinstance(c, dict):
                c[result.workflow_type] = c.get(result.workflow_type, 0) + 1
        elif result.returncode == -1:
            c = iv.get('cancelled')
            if isinstance(c, dict):
                c[result.workflow_type] = c.get(result.workflow_type, 0) + 1
        else:
            c = iv.get('failed')
            if isinstance(c, dict):
                c[result.workflow_type] = c.get(result.workflow_type, 0) + 1

    def get_summary(self) -> dict:
        """Return scientific aggregates collected during this campaign's run.

        Called once by the CM when the campaign completes; the result is stored
        in ``CampaignOutcome.summary`` and returned to the driver via ``await
        result.wait()``.  Override to expose domain-specific data (best score,
        score distribution, candidate list, etc.) so the driver can build the
        next campaign intelligently.  Base implementation returns ``{}``.
        """
        return {}

    def is_goal_reached(self) -> bool:
        """Return True when the campaign goal has been met.
        Default: goal_n completions of goal_type.
        Override for domain-specific termination logic."""
        if self._goal_type is None or self._goal_n is None:
            return False
        return self.completed.get(self._goal_type, 0) >= self._goal_n

    def decide(self, obs: dict) -> PolicyResult:
        """Apply self._rules and check termination.
        Override or call _apply_rules() with a custom rule set."""
        return self._apply_rules(self._rules, obs)

    # ── Helpers available to subclasses ──────────────────────────────────────

    def _apply_rules(
        self,
        rules: list[CancelRule | LaunchRule],
        obs: dict,
    ) -> PolicyResult:
        """Apply a rule list against the current observation and return a PolicyResult.
        Subclass decide() can call this with self._rules or a custom list."""
        live_reports: dict = obs.get("live_reports", {})
        wf_types: dict     = obs.get("wf_types", {})
        q: QueueSnapshot   = obs["queue_snapshot"]

        decisions: dict[str, CampaignDecision] = {}
        requests:  list[LaunchRequest]         = []

        for rule in rules:
            if isinstance(rule, CancelRule):
                for wf_id, report in live_reports.items():
                    if wf_types.get(wf_id) == rule.workflow_type and rule.when(report):
                        logger.info(
                            "[CTRL] CancelRule(%s): cancelling %s score=%s",
                            rule.workflow_type, wf_id,
                            f"{report.score:.3f}" if report.score is not None else "None",
                        )
                        decisions[wf_id] = CampaignDecision.CANCEL

            elif isinstance(rule, LaunchRule) and not self.is_goal_reached():
                # Only enqueue when nothing is already queued for this type.
                # This prevents queue runaway when active=1 but completions stall:
                # each policy cycle would otherwise add another request indefinitely.
                if q.by_type.get(rule.workflow_type, 0) == 0:
                    requests.append(LaunchRequest.make(rule.workflow_type))

        # Terminate as soon as the goal is reached.  Active instances are
        # cancelled gracefully by _shutdown(); the remaining queue is cleared.
        terminate = self.is_goal_reached()
        return PolicyResult(
            decisions=decisions,
            launch_requests=requests,
            terminate=terminate,
        )

    # ── CM integration (unchanged) ────────────────────────────────────────────

    def observe(
        self,
        queue_snapshot: QueueSnapshot,
        budget: dict,
        running: dict[str, int] | None = None,
        **kwargs,
    ) -> dict:
        return dict(
            queue_snapshot=queue_snapshot,
            budget=budget,
            running=running or {},
            **kwargs,
        )

    def goals(self) -> list[Goal]:
        return []
