"""Core enumerations and data classes used throughout AuCaM."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


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


@dataclass
class WorkflowResult:
    workflow_id: str
    workflow_type: str
    # 0 completed, -1 cancelled; a failure gives the launch_cmd program's exit
    # code (128 + N if killed by signal N) or 1 (Python workflow, or no code).
    returncode: int
    wall_time_s: float
    # Last metrics snapshot the workflow emitted before finishing.
    # Empty dict if the workflow had no reporter or exited before its first emit.
    final_metrics: dict = field(default_factory=dict)
    # Per-block resource profile: keys include cpu_percent_avg, memory_percent_avg,
    # duration_s, node_id, etc.  Populated from telemetry JSONL correlation or, if
    # absent, from BlockProfiler/TaskProfiler data the workflow reported.  Empty dict when unavailable.
    resource_profile: dict = field(default_factory=dict)
    # Score of the workflow's last report (None if it never reported).
    final_score: Any = None
    # Actual resource spending charged to this workflow (cpu_hours, gpu_hours, …).
    # Charged per task (AsyncFlow telemetry) when available, otherwise from the
    # workflow's reported values or proration (see docs/budget.md).  Empty when
    # nothing was charged.
    spent: dict = field(default_factory=dict)
    # The spending limit this run was launched with (empty: none), and whether
    # AuCaM cancelled it for spending past that limit.
    spending_limit: dict = field(default_factory=dict)
    over_budget: bool = False
    # The resources this run was given at once (e.g. {"cpus": 8, "gpus": 1}),
    # whether AuCaM cancelled it for using more, and whether the scheduler gave
    # it less than requested because the campaign budget was short.
    resources: dict = field(default_factory=dict)
    over_resources: bool = False
    capped: bool = False


@dataclass
class LaunchRequest:
    workflow_type: str
    priority: LaunchPriority
    enqueued_at: float
    campaign_name: str = ""  # stamped by _CampaignInstance.enqueue()
    context: dict = field(default_factory=dict)
    requester: str = "unknown"
    # Spending limit for this run (e.g. {"cpu_hours": 2.0}); None uses the
    # workflow type's WorkflowConfig.spending_limit.
    spending_limit: dict | None = None
    # Resources this run may use at once (e.g. {"cpus": 8, "gpus": 1}); None
    # uses WorkflowConfig.resources.
    resources: dict | None = None
    # The controller's intent as ranges, key -> (min, max), for spending
    # dimensions ("cpu_hours", ...) and resources ("cpus", "gpus").  The
    # scheduling policy's size() picks the final amounts within them.
    bounds: dict | None = None

    @classmethod
    def make(
        cls,
        workflow_type: str,
        priority: LaunchPriority = LaunchPriority.NORMAL,
        *,
        context: dict | None = None,
        requester: str = "policy_agent",
        spending_limit: dict | None = None,
        resources: dict | None = None,
        bounds: dict | None = None,
    ) -> LaunchRequest:
        return cls(
            workflow_type=workflow_type,
            priority=priority,
            enqueued_at=time.time(),
            context=context or {},
            requester=requester,
            spending_limit=spending_limit,
            resources=resources,
            bounds=bounds,
        )


# Resource keys (amounts in use at once); every other key in an allocation or in
# LaunchRequest.bounds is a spending dimension (cpu_hours, gpu_hours, ...).
RESOURCE_KEYS = ("cpus", "gpus")


@dataclass
class Allocation:
    """What one run is given: a spending limit and resources in use at once.

    capped — the scheduler gave less spending than requested because the
             campaign budget was short.
    """

    spending_limit: dict[str, float] = field(default_factory=dict)
    resources: dict[str, float] = field(default_factory=dict)
    capped: bool = False


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
    spent                — spending charged to this campaign so far (from the
                           ledger), keyed by budget dimension, e.g. "cpu_hours".
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
class CampaignOutcome:
    """Summary of a finished campaign, returned by ``CampaignManager.run()``.

    An agent or script uses it to decide what to run next::

        outcome = await cm.run(spec)
        if not outcome.goal_reached:
            ...  # run again with adjusted parameters
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
    # Why the campaign ended: goals_satisfied, controller_terminate,
    # spending_limit, campaign_idle or signal.
    exit_reason: str = "unknown"
    spent: dict[str, float] = field(default_factory=dict)  # actual resource consumption
    summary: dict = field(default_factory=dict)  # controller-provided scientific aggregates
    # Workflows cancelled for spending past their own spending limit, or for
    # using more resources at once than they were given.
    budget_cancels: int = 0
    resource_cancels: int = 0
    # Launch requests dropped because even their minimum did not fit the
    # remaining campaign budget.
    unfit_dropped: int = 0
