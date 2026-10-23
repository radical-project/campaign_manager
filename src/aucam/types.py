"""Core enumerations and data classes used throughout AuCaM."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ScoreDirection(str, Enum):
    HIGHER = "higher"  # higher value = better (e.g. pLDDT, reward, diversity)
    LOWER = "lower"  # lower  value = better (e.g. binding energy, RMSD)


class WorkflowStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class CampaignDecision(str, Enum):
    CANCEL = "CANCEL"
    PAUSE = "PAUSE"
    RESUME = "RESUME"
    NOOP = "NOOP"


class LaunchPriority(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    NORMAL = "NORMAL"
    LOW = "LOW"


class SchedulingDecision(str, Enum):
    LAUNCH = "LAUNCH"
    HOLD = "HOLD"
    DROP = "DROP"


@dataclass
class WorkflowResult:
    workflow_id: str
    workflow_type: str
    # 0 completed, -1 cancelled; a failure gives the launch_cmd program's exit
    # code (128 + N if killed by signal N) or 1 (Python workflow, or no code).
    returncode: int
    wall_time_s: float
    output_dir: str | None = None
    # Last metrics snapshot the workflow emitted before finishing.
    # Empty dict if the workflow had no reporter or exited before its first emit.
    final_metrics: dict = field(default_factory=dict)
    # Per-block resource profile: keys include cpu_percent_avg, memory_percent_avg,
    # duration_s, node_id, etc.  Populated from BlockProfiler/TaskProfiler (Path 1)
    # or from telemetry JSONL correlation (Path 2).  Empty dict when unavailable.
    resource_profile: dict = field(default_factory=dict)
    # Score of the workflow's last report (None if it never reported).
    final_score: Any = None
    # Actual resource spending charged to this workflow (cpu_hours, gpu_hours, …).
    # Charged per task (AsyncFlow telemetry) when available, otherwise from the
    # workflow's reported values or proration (see docs/budget.md).  Empty when
    # nothing was charged.
    spent: dict = field(default_factory=dict)


@dataclass
class LaunchRequest:
    workflow_type: str
    priority: LaunchPriority
    enqueued_at: float
    campaign_name: str = ""  # stamped by _CampaignInstance.enqueue()
    context: dict = field(default_factory=dict)
    requester: str = "unknown"

    @classmethod
    def make(
        cls,
        workflow_type: str,
        priority: LaunchPriority = LaunchPriority.NORMAL,
        *,
        context: dict | None = None,
        requester: str = "policy_agent",
    ) -> LaunchRequest:
        return cls(
            workflow_type=workflow_type,
            priority=priority,
            enqueued_at=time.time(),
            context=context or {},
            requester=requester,
        )


@dataclass
class QueueSnapshot:
    total_queued: int
    by_priority: dict[str, int]
    by_type: dict[str, int]
    oldest_wait_s: float


@dataclass
class NodeResources:
    """Latest hardware utilization snapshot for one compute node."""

    backend: str
    node_id: str
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    gpu_percent: float = 0.0
    disk_read_bytes: float = 0.0
    disk_write_bytes: float = 0.0
    net_sent_bytes: float = 0.0
    net_recv_bytes: float = 0.0


@dataclass
class TelemetrySnapshot:
    """Live telemetry surfaced to the controller each policy cycle.

    running_wall_s  — seconds each active workflow has been running,
                      keyed by wf_id; always populated from CM-internal state.
    node_resources  — latest CPU/GPU/memory readings per node, keyed by
                      "{backend}/{node_id}"; populated when Rhapsody telemetry
                      is enabled, empty otherwise.
    spent                — cumulative spending for completed workflows in this
                           campaign; keyed by budget dimension, e.g.
                           "cpu_wall_seconds" (CM-measured wall_s, authoritative)
                           and "gpu_wall_seconds" (workflow self-reported, Path 1).
    running_consumption  — live resource consumption per active workflow,
                           keyed by wf_id → {"cpu_wall_seconds": float, ...};
                           uses workflow-reported cpu_seconds/gpu_seconds from
                           reporter.emit() when available, falls back to wall_s.
    wf_node              — maps each active wf_id to its node key
                           ("{backend}/{node_id}"); built from workflow-reported
                           node_id (primary) and asyncflow TaskStarted events
                           (fallback). Used by controllers to correlate
                           node_resources with running workflows.
    """

    running_wall_s: dict[str, float] = field(default_factory=dict)
    node_resources: dict[str, NodeResources] = field(default_factory=dict)
    spent: dict[str, float] = field(default_factory=dict)
    running_consumption: dict[str, dict[str, float]] = field(default_factory=dict)
    wf_node: dict[str, str] = field(default_factory=dict)


@dataclass
class GlobalBudget:
    """Cross-campaign resource limits enforced by CampaignManager at admission time.

    Checked when a driver calls ``cm.submit(spec)``; a campaign is rejected before
    it starts if accepting it would violate any limit.

    max_concurrent_campaigns          — hard cap on the number of campaigns running
                                        simultaneously.
    max_total_concurrent_workflows    — sum of each campaign's
                                        max_concurrent_workflows must not exceed this.
    spending                          — per-dimension global spending caps
                                        (same keys as CampaignBudget.spending,
                                        e.g. ``{"cpu_hours": 1.0}``).  Admission
                                        checks: actual spending by completed
                                        campaigns + declared limits of running
                                        campaigns + incoming campaign's declared
                                        limit must not exceed this cap.
    n_campaigns                       — auto-close after this many campaigns complete.
                                        The CM signals no-more-submissions internally;
                                        the driver does not need to call ``close()``.
    max_rejections                    — auto-close after this many budget-spending
                                        rejections.  Handles the case where remaining
                                        global budget is too small for any campaign the
                                        driver proposes but not exactly zero.
    """

    max_concurrent_campaigns: int | None = None
    max_total_concurrent_workflows: int | None = None
    spending: dict[str, float] = field(default_factory=dict)
    n_campaigns: int | None = None
    max_rejections: int | None = None


@dataclass
class CampaignOutcome:
    """Summary of a completed campaign, returned by ``SubmitResult.wait()``.

    Drivers use this to decide what to submit next::

        outcome = await result.wait()
        if not outcome.goal_reached:
            # re-submit with adjusted parameters
        cancel_rate = (
            sum(outcome.cancelled.values()) /
            max(1, sum(outcome.completed.values()) + sum(outcome.cancelled.values()))
        )
    """

    campaign_name: str
    completed: dict[str, int]  # workflow_type → successful completions
    cancelled: dict[str, int]  # workflow_type → cancellations (returncode == -1)
    failed: dict[str, int]  # workflow_type → hard failures (other returncode)
    elapsed_s: float
    goal_reached: bool
    spent: dict[str, float] = field(default_factory=dict)  # actual resource consumption
    summary: dict = field(default_factory=dict)  # controller-provided scientific aggregates


@dataclass
class SubmitResult:
    """Return value of ``CampaignManager.submit()``.

    Call ``await result.wait()`` to block until the campaign finishes and
    receive its :class:`CampaignOutcome`.
    """

    accepted: bool
    campaign_name: str
    reason: str | None = None
    # Set by CampaignManager.submit() when accepted; None for rejected submissions
    # and for campaigns submitted via the legacy submit_campaign() API.
    outcome_future: asyncio.Future[CampaignOutcome] | None = field(
        default=None, init=False, repr=False, compare=False
    )

    async def wait(self) -> CampaignOutcome:
        """Wait for the campaign to finish and return its outcome.

        Raises :exc:`RuntimeError` if the campaign was rejected or was
        submitted via the legacy ``submit_campaign()`` API.
        """
        if not self.accepted:
            raise RuntimeError(f"Campaign '{self.campaign_name}' was not accepted: {self.reason}")
        if self.outcome_future is None:
            raise RuntimeError(
                f"Campaign '{self.campaign_name}' has no outcome future; "
                "it was submitted via submit_campaign() (legacy API)."
            )
        return await self.outcome_future
