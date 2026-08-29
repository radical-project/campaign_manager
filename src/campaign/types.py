"""
Shared data types for the campaign manager.

  _WorkflowInfo   — internal per-workflow runtime state (not public API)
  ResourcePool    — CPU/GPU/memory availability tracker
  WorkflowStats   — public per-workflow statistics snapshot
  CampaignState   — explicit container for cross-mixin shared state
"""

from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    import asyncio

    from .backpressure import BackpressureNegotiator
    from .base_workflow import BaseWorkflow
    from .budget_controller import BudgetController
    from .candidate_log import CandidateLog
    from .metrics import CampaignMetrics
    from .monitor import Monitor
    from .plan import CampaignPlan
    from .replanning import ReplanningController
    from .sharder import Sharder
    from .surrogate import Surrogate
    from .triage import Triage


@dataclass
class _WorkflowInfo:
    """Internal per-workflow runtime state.

    State-machine invariants (enforced by validate()):
      0 <= finished_replicas <= started_count <= _queued_count <= replicas
      replicas >= configured_replicas  (grows on signal_done / trigger_dependent)
      running_count = started_count - finished_replicas  (derived; never mutated directly)
      concurrency_floor <= concurrency_cap

    Counter semantics with the warm-pool semaphore:
      _queued_count  — tasks created as asyncio tasks by the scheduler
                       (some may be waiting on _semaphore, some executing)
      started_count  — tasks that have acquired _semaphore and are executing
      finished_replicas — tasks that have completed (success or failure)
      running_count  — tasks currently executing = started_count - finished_replicas

    Status transitions: pending → running → done.  Once "done", the workflow
    no longer schedules new replicas; downstream sharders are notified to
    flush any partial tail.
    """

    name: str
    workflow_class: "type[BaseWorkflow]"
    replicas: int
    dependencies: list[str]
    workflow_config: Optional[dict]
    configured_replicas: int = 0
    concurrency_floor: int = 0
    concurrency_cap: int = 0
    priority: int = 0
    required_cpus: int = 0
    required_gpus: int = 0
    required_memory_gb: float = 0.0
    dep_threshold: int = 1
    entry_point: str = "run"
    status: str = "pending"
    # started_count: tasks currently executing (have acquired _semaphore).
    # Incremented in _run_replica AFTER semaphore.acquire() returns.
    started_count: int = 0
    # finished_replicas counts BOTH successful and failed terminations — i.e.,
    # anything no longer running.  Failure stats live in CampaignMetrics.
    finished_replicas: int = 0
    # failed_replicas: subset of finished_replicas that ended in final_state="failed".
    # Exposed in view.observe() as n_failed so policies can react to failure rates.
    failed_replicas: int = 0
    # max_retries: how many times the executor will automatically re-submit a
    # failed replica before giving up.  0 = no automatic retry.  Per-workflow.
    max_retries: int = 0
    # Set to True when the workflow explicitly signals it has produced enough
    # data (via cm.signal_ready).  Takes precedence over dep_threshold check.
    ready: bool = False
    # GPU IDs currently held by all running replicas of this group.
    # Populated by _allocate_locked; cleared by _on_replica_finished.
    running_gpu_ids: list[int] = field(default_factory=list)
    # Candidate IDs waiting to be assigned to the next replica that starts.
    # Populated by _flush_sharders_locked when dispatch returns candidate IDs;
    # consumed FIFO by _allocate_locked so replica_idx → candidate_id is stable.
    _pending_candidates: deque = field(default_factory=deque, repr=False)
    # Consecutive stall counter — incremented each time the group is found
    # resource-stalled during _schedule_locked, reset when it successfully
    # starts a replica.  The stall WARNING is only emitted on the first stall
    # and then every _STALL_WARN_EVERY-th consecutive stall to avoid flooding
    # the log when ADVANCE replicas cycle at sub-millisecond rates.
    _consecutive_stalls: int = field(default=0, repr=False)
    # Maps original_replica_id → attempts_so_far for in-flight retry tracking.
    # Cleared when the replica finally succeeds or exhausts max_retries.
    _retry_counts: dict = field(default_factory=dict, repr=False)
    # Maps new_replica_id → (original_replica_id, attempt_number) so the
    # executor can tag the retry's metrics event with lineage at start time.
    _retry_lineage: dict = field(default_factory=dict, repr=False)
    # _queued_count: asyncio tasks created by the scheduler for this group.
    # Replaces started_count in _allocate_locked as the task-creation index.
    # The scheduler stops creating tasks when _queued_count >= replicas.
    _queued_count: int = field(default=0, repr=False)
    # _semaphore: per-group concurrency ceiling, created in register_workflow
    # when concurrency_cap > 0 (explicit cap).  None means unlimited concurrency.
    # Replaces the running_count < cap counter-check in _can_start_locked so
    # within-group cycling no longer requires a scheduler lock round-trip.
    _semaphore: Optional[Any] = field(default=None, repr=False)

    @property
    def running_count(self) -> int:
        """Replicas currently executing (started but not yet finished).

        Derived from started_count - finished_replicas so the invariant
        running_count >= 0 holds automatically.  Do not mutate.
        """
        return self.started_count - self.finished_replicas

    def validate(self) -> None:
        """Assert state-machine invariants.  Use in dev mode to catch
        off-by-one errors in scheduler/executor updates early."""
        assert self.finished_replicas >= 0, (
            f"{self.name}: finished_replicas={self.finished_replicas} < 0"
        )
        assert self.started_count >= self.finished_replicas, (
            f"{self.name}: started_count={self.started_count} < "
            f"finished_replicas={self.finished_replicas}"
        )
        assert self._queued_count >= self.started_count, (
            f"{self.name}: _queued_count={self._queued_count} < "
            f"started_count={self.started_count}"
        )
        assert self._queued_count <= self.replicas, (
            f"{self.name}: _queued_count={self._queued_count} > replicas={self.replicas}"
        )
        assert self.concurrency_floor >= 0
        assert self.concurrency_cap >= 0
        assert self.concurrency_floor <= max(self.concurrency_cap, self.replicas), (
            f"{self.name}: concurrency_floor={self.concurrency_floor} > "
            f"effective_cap={max(self.concurrency_cap, self.replicas)}"
        )


@dataclass
class ResourcePool:
    """
    Tracks available CPU cores, GPU slots, and memory (GB) for the campaign.

    All counters are optional: a value of 0 disables tracking for that
    resource type (treated as unlimited).
    """

    total_cpus: int = 0
    total_gpus: int = 0
    total_memory_gb: float = 0.0
    available_cpus: int = field(default=0, init=False)
    available_gpus: int = field(default=0, init=False)
    available_memory_gb: float = field(default=0.0, init=False)

    def __post_init__(self) -> None:
        self.available_cpus = self.total_cpus
        self.available_gpus = self.total_gpus
        self.available_memory_gb = self.total_memory_gb

    def can_fit(self, cpus: int, gpus: int, memory_gb: float = 0.0) -> bool:
        if self.total_cpus > 0 and cpus > self.available_cpus:
            return False
        if self.total_gpus > 0 and gpus > self.available_gpus:
            return False
        if self.total_memory_gb > 0 and memory_gb > self.available_memory_gb:
            return False
        return True

    def allocate(self, cpus: int, gpus: int, memory_gb: float = 0.0) -> None:
        self.available_cpus -= cpus
        self.available_gpus -= gpus
        self.available_memory_gb -= memory_gb

    def release(self, cpus: int, gpus: int, memory_gb: float = 0.0) -> None:
        self.available_cpus += cpus
        self.available_gpus += gpus
        self.available_memory_gb += memory_gb

    def usage_str(self) -> str:
        parts = []
        if self.total_cpus > 0:
            parts.append(f"cpus={self.total_cpus - self.available_cpus}/{self.total_cpus}")
        if self.total_gpus > 0:
            parts.append(f"gpus={self.total_gpus - self.available_gpus}/{self.total_gpus}")
        if self.total_memory_gb > 0:
            used = self.total_memory_gb - self.available_memory_gb
            parts.append(f"mem={used:.0f}/{self.total_memory_gb:.0f}GB")
        return "  ".join(parts) if parts else "—"

    def available_str(self) -> str:
        parts = []
        if self.total_cpus > 0:
            parts.append(f"cpus={self.available_cpus}/{self.total_cpus}")
        if self.total_gpus > 0:
            parts.append(f"gpus={self.available_gpus}/{self.total_gpus}")
        if self.total_memory_gb > 0:
            parts.append(f"mem={self.available_memory_gb:.0f}/{self.total_memory_gb:.0f}GB")
        return "  ".join(parts) if parts else "—"

    def as_dict(self) -> dict:
        return {
            "total_cpus": self.total_cpus,
            "available_cpus": self.available_cpus,
            "total_gpus": self.total_gpus,
            "available_gpus": self.available_gpus,
            "total_memory_gb": self.total_memory_gb,
            "available_memory_gb": self.available_memory_gb,
        }


@dataclass
class WorkflowStats:
    """Cumulative statistics for one workflow group."""

    replicas_started: int = 0
    replicas_finished: int = 0


@dataclass
class CampaignState:
    """Explicit container for cross-mixin shared state.

    Today this is a documentation contract: the existing mixins
    (SchedulerMixin, ExecutorMixin, MonitorMixin) still access state via
    self._X on the AsyncCampaignManager.  ``cm.state`` returns a
    CampaignState view of the same underlying objects (no copy) so:

      1. Tests can inspect or assert on state by structured field name
         instead of poking at the CM's private attributes.
      2. Future refactors that move mixin logic into standalone classes
         have an explicit contract to thread through.
      3. New contributors can see exactly which fields the mixins share.

    All fields are references to the live underlying objects — mutating
    a dict here mutates the CM's actual state.  Treat the container itself
    as read-only.
    """

    lock: "asyncio.Lock"
    workflows: dict[str, "_WorkflowInfo"]
    resources: "ResourcePool"
    sharders: dict[str, "Sharder"]
    bp: dict[str, "BackpressureNegotiator"]
    candidate_log: Optional["CandidateLog"]
    monitor: Optional["Monitor"]
    running_candidates: dict[str, str]
    replica_candidate_assignments: dict[str, str]
    replica_gpu_assignments: dict[str, list[int]]
    free_gpu_ids: list[int]
    gpu_pool: list[tuple[str, int]]
    all_done: "asyncio.Event"
    metrics: "CampaignMetrics"
    stats: dict[str, WorkflowStats]
    features: dict[str, bool]
    # Optional structured-plan extensions
    plan: Optional["CampaignPlan"] = None
    triages: dict[str, "Triage"] = field(default_factory=dict)
    budget_controllers: dict[str, "BudgetController"] = field(default_factory=dict)
    surrogates: dict[str, "Surrogate"] = field(default_factory=dict)
    replanning: Optional["ReplanningController"] = None
    log: Any = None  # Logger instance — kept Any to avoid an import cycle
