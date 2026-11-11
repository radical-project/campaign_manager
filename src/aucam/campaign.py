"""_CampaignInstance: manages the full lifecycle of one campaign."""

from __future__ import annotations

import asyncio
import dataclasses
import inspect
import json
import logging
import shlex
import threading
import time
import uuid
from collections import deque
from collections.abc import Iterable
from contextlib import suppress
from pathlib import Path
from typing import Any

from .budget import BudgetAllocator
from .checkpoint import (
    ActiveWorkflowRecord,
    CampaignCheckpoint,
    QueuedRequestRecord,
    checkpoint_path,
    read_checkpoint,
    write_checkpoint,
)
from .controller import CampaignController, Goal, PolicyResult
from .queue import WorkflowQueue
from .scheduler import (
    BanditSchedulingPolicy,
    SchedulingContext,
    SchedulingPolicy,
    SizingContext,
)
from .spec import CampaignSpec, WorkflowConfig
from .types import (
    RESOURCE_KEYS,
    Allocation,
    CampaignDecision,
    LaunchPriority,
    LaunchRequest,
    NodeResources,
    TelemetrySnapshot,
    WorkflowResult,
    WorkflowStatus,
)

logger = logging.getLogger(__name__)


class _DecideTimeoutError(Exception):
    """decide() did not return within spec.decide_timeout_s."""


def _call_in_thread(fn, *args) -> asyncio.Future:
    """Run fn(*args) in a daemon thread; return an asyncio future for its result.

    A daemon thread, not the loop's executor, so a decide() that never returns
    cannot keep the campaign (or the interpreter) from exiting.
    """
    loop = asyncio.get_running_loop()
    fut: asyncio.Future = loop.create_future()

    def _set(value, exc) -> None:
        if fut.done():
            return
        if exc is not None:
            fut.set_exception(exc)
        else:
            fut.set_result(value)

    def _run() -> None:
        try:
            value, exc = fn(*args), None
        except BaseException as e:  # noqa: BLE001 — handed to the awaiting coroutine
            value, exc = None, e
        with suppress(RuntimeError):  # the loop may already be closed
            loop.call_soon_threadsafe(_set, value, exc)

    # A result nobody awaits (the call timed out) must not log "never retrieved".
    fut.add_done_callback(lambda f: f.cancelled() or f.exception())
    threading.Thread(target=_run, daemon=True, name="aucam-decide").start()
    return fut


def _correlate_events(
    wf_id: str,
    wall_s: float,
    events: Any,
    node_id: str | None = None,
) -> tuple[dict, dict[str, float]]:
    """Scan an event stream for wf_id's TaskStarted/Completed window and average ResourceUpdates.

    Works with both dict events (JSONL) and attribute-based asyncflow event objects
    (converted to dicts by the caller).  Returns ({}, {}) when no data is found.

    node_id must be passed explicitly (from final_metrics["node_id"]) — TaskStarted
    events carry node_id=null so it cannot be derived from the event stream.

    GPU hours are computed by dividing the node's gpu_percent equally among all
    workflows that overlapped with this workflow on the same node.
    """
    start_t = end_t = None
    resource_updates: list[dict] = []
    # task_id → {start, end} for all tasks — used to count co-located workflows.
    task_timings: dict[str, dict] = {}

    for ev in events:
        etype = ev.get("event_type")
        et = ev.get("event_time")
        if et is None:
            continue
        if etype == "TaskStarted":
            tid = ev.get("task_id")
            if tid:
                task_timings.setdefault(tid, {}).update({"start": et})
            if tid == wf_id:
                start_t = et
        elif etype in ("TaskCompleted", "TaskFailed", "TaskCanceled", "TaskCancelled"):
            tid = ev.get("task_id")
            if tid and tid in task_timings:
                task_timings[tid]["end"] = et
            if tid == wf_id:
                end_t = et
        elif etype == "ResourceUpdate":
            resource_updates.append(ev)

    if start_t is None:
        return {}, {}

    end_t = end_t or (start_t + wall_s)
    window = [ru for ru in resource_updates if start_t <= ru["event_time"] <= end_t]

    if not window:
        return {}, {}

    cpu_vals = [ru.get("cpu_percent") or 0.0 for ru in window]
    mem_vals = [ru.get("memory_percent") or 0.0 for ru in window]
    dur = end_t - start_t

    # Which node(s) did this workflow run on?  Prefer explicit node_id; also
    # collect any node_id fields present on ResourceUpdate events in the window.
    wf_nodes: set[str] = {node_id} if node_id else set()
    for ru in window:
        nid = ru.get("node_id")
        if nid:
            wf_nodes.add(nid)

    # GPU hours: node gpu_percent split equally among co-located workflows.
    gpu_hours = 0.0
    for node in wf_nodes:
        node_gpu_vals = [ru.get("gpu_percent") or 0.0 for ru in window if ru.get("node_id") == node]
        if not node_gpu_vals:
            continue
        # Count workflows that overlapped [start_t, end_t] on this node.
        concurrent = sum(
            1
            for info in task_timings.values()
            if info.get("start", float("inf")) <= end_t
            and info.get("end", start_t) >= start_t  # no end = still running
        )
        concurrent = max(concurrent, 1)
        avg_gpu = sum(node_gpu_vals) / len(node_gpu_vals)
        gpu_hours += (avg_gpu / 100.0) / concurrent * dur / 3600.0

    prof = {
        "start_time": start_t,
        "end_time": end_t,
        "duration_s": dur,
        "cpu_percent_avg": sum(cpu_vals) / len(cpu_vals),
        "cpu_percent_peak": max(cpu_vals),
        "memory_percent_avg": sum(mem_vals) / len(mem_vals),
        "memory_percent_peak": max(mem_vals),
        "n_samples": len(window),
        "nodes": list(wf_nodes),
        "source": "telemetry_jsonl",
    }
    cpu_frac = prof["cpu_percent_avg"] / 100.0
    spending: dict[str, float] = {"cpu_hours": cpu_frac * dur / 3600.0}
    if gpu_hours > 0:
        spending["gpu_hours"] = gpu_hours
    return prof, spending


def _wants_reporter(fn: Any) -> bool:
    """Return True if fn accepts a 'reporter' keyword argument (any position)."""
    try:
        param = inspect.signature(fn).parameters.get("reporter")
    except (ValueError, TypeError):
        return False
    return param is not None and param.kind is not inspect.Parameter.POSITIONAL_ONLY


def _wants_flow(fn: Any) -> bool:
    """Return True if fn accepts a 'flow' keyword argument."""
    try:
        return "flow" in inspect.signature(fn).parameters
    except (ValueError, TypeError):
        return False


def _wants_context(fn: Any) -> bool:
    """Return True if fn accepts a 'context' keyword argument."""
    try:
        return "context" in inspect.signature(fn).parameters
    except (ValueError, TypeError):
        return False


class _WorkflowInstance:
    __slots__ = (
        "wf_id",
        "workflow_type",
        "future",
        "started_at",
        "declared_spending",
        "last_report",
        "request",
        "spending_limit",
        "over_budget",
        "resources",
        "over_resources",
        "capped",
        "peak_use",
    )

    def __init__(
        self,
        wf_id: str,
        workflow_type: str,
        future: asyncio.Future,
        started_at: float,
        declared_spending: dict[str, float],
        request: LaunchRequest,
        spending_limit: dict[str, float] | None = None,
        resources: dict[str, float] | None = None,
        capped: bool = False,
    ) -> None:
        self.wf_id = wf_id
        self.workflow_type = workflow_type
        self.future = future
        self.started_at = started_at
        self.declared_spending = declared_spending  # in-flight spending estimate
        self.request = request
        self.last_report: Any = None
        # Per-run spending limit; over_budget is set when AuCaM cancels the run
        # for spending past it.
        self.spending_limit: dict[str, float] = dict(spending_limit or {})
        self.over_budget = False
        # Resources the run may use at once; over_resources is set when AuCaM
        # cancels it for using more.  capped: the scheduler gave it less
        # spending than requested.
        self.resources: dict[str, float] = dict(resources or {})
        self.over_resources = False
        self.capped = capped
        # Most CPUs / GPUs seen in use at once (task telemetry or reports), U2.
        self.peak_use: dict[str, float] = {}


class _CampaignInstance:
    """
    Runs one campaign: policy cycle + scheduler loop + workflow lifecycle.

    Every workflow runs as one AsyncFlow block: a Python workflow is the block
    body; a command-line workflow (launch_cmd) is a block containing one
    executable task.
    """

    def __init__(
        self,
        spec: CampaignSpec,
        allocator: BudgetAllocator,
        flow: Any,
    ) -> None:
        self.spec = spec
        self.name = spec.name
        self._allocator = allocator
        self._flow = flow
        self._policy: SchedulingPolicy = (
            spec.scheduling_policy
            if spec.scheduling_policy is not None
            else BanditSchedulingPolicy()
        )

        self._queue = WorkflowQueue(age_threshold_s=spec.age_threshold_s)
        self._active: dict[str, _WorkflowInstance] = {}
        self._goals: list[Goal] = list(spec.controller.goals())

        # DAG state
        # _dep_satisfied: target types that have been "unlocked" (≥1 source succeeded)
        #   — guards enqueue() so requests for locked targets are held
        # _dep_pending:   requests queued before unlock; released on first trigger
        # _dep_triggered: (source_wf_id, target_type) pairs already triggered;
        #   prevents the same source instance from firing twice
        self._dep_satisfied: set[str] = set()
        self._dep_pending: dict[str, list[LaunchRequest]] = {}
        self._dep_triggered: set[tuple[str, str]] = set()

        self._done = False
        self._slot_event = asyncio.Event()
        self._completion_event = asyncio.Event()  # set on each workflow done → early policy wake
        # Spending is kept in the BudgetAllocator ledger.
        self._allocator.open(self.name, spec.budget.spending)

        # Observability counters — surfaced in summary log at shutdown.
        self._instances_run = 0
        self._completed = 0
        self._cancelled = 0
        self._failed = 0
        self._stale_cancels = 0
        self._walltime_cancels = 0
        self._budget_cancels = 0  # runs cancelled for exceeding their own spending limit
        self._resource_cancels = 0  # runs cancelled for using more resources than given
        self._unfit_dropped = 0  # requests dropped: their minimum did not fit the budget
        self._unfit_logged: set[int] = set()  # id() of queued requests logged as not fitting
        self._controller_errors = 0  # cycles where decide()/observe() failed or timed out
        self._slow_decide: asyncio.Future | None = None  # decide() still running past its timeout
        self._policy_cancels = 0  # CANCEL from controller (not auto-cancel)
        self._auto_cancelled: set[str] = set()  # wf_ids auto-cancelled this policy cycle
        self._policy_cycles = 0
        self._exit_reason = "unknown"

        # Recent workflow results for SchedulingContext.history (newest last, capped).
        self._history: list[WorkflowResult] = []
        self._history_max: int = 50

        # Per-type observed cost stats for learned in-flight estimates (B3).
        # {workflow_type: {dim: {"n": int, "mean": float}}}
        self._type_cost_stats: dict[str, dict[str, dict]] = {}
        # Per-type outcomes and total spending, all finished workflows (B1).
        # {workflow_type: {"completed", "cancelled", "failed": int,
        #                  "spent": {dim: float}, "score_sum": float, "score_n": int}}
        self._type_totals: dict[str, dict] = {}
        # Per-type peak CPUs / GPUs in use at once, successful workflows (U2):
        # {workflow_type: {key: {"n": int, "mean": float}}}
        self._type_resource_stats: dict[str, dict[str, dict]] = {}
        # (monotonic time, spent) at recent policy cycles, for the spending rate (B1).
        self._spent_samples: deque[tuple[float, dict[str, float]]] = deque(maxlen=20)

        # Last telemetry snapshot — updated each policy cycle, used by _drain_queue.
        self._last_telemetry: TelemetrySnapshot = TelemetrySnapshot()

        # Task-level accounting (requested resources × duration from AsyncFlow
        # task events).  _wf_tags maps an asyncflow.workflow_id tag (the wf_id,
        # or the uid of the AsyncFlow block a workflow runs in) to the wf_id.
        # _task_level holds workflows whose tasks are charged from events;
        # _task_usage is their charged spending so far.
        # _active_tasks tracks in-flight tasks for live burn-rate logging:
        #   task_uid → {"wf_id", "resources", "start_time"}
        self._wf_tags: dict[str, str] = {}
        self._task_level: set[str] = set()
        self._task_usage: dict[str, dict[str, float]] = {}
        self._active_tasks: dict[str, dict] = {}
        # Task states from AsyncFlow events, for backlog visibility:
        # task_uid → (wf_id, state), state in _TASK_STATES.  Filled only while
        # telemetry is on (_tracking_tasks).
        self._task_states: dict[str, tuple[str, str]] = {}
        self._tracking_tasks = False
        # Cost estimate set at launch (learned or declared, B3), per running
        # workflow.  The in-flight estimate is always derived from it.
        self._estimates: dict[str, dict[str, float]] = {}
        # Exit code of each finished command-line workflow's program.
        self._exit_codes: dict[str, int] = {}

        # Warn when spending limits are set but a workflow has no declared_spending.
        if spec.budget.spending:
            for wfc in spec.workflows:
                if not wfc.declared_spending:
                    logger.warning(
                        "[CM] %s: spending limits are set but workflow '%s' has no "
                        "declared_spending — its budget contribution will be zero. "
                        "Set declared_spending={wall_hours, cpu_hours, gpu_hours} "
                        "in WorkflowConfig.",
                        spec.name,
                        wfc.name,
                    )

        # Determine which types start without waiting for a DAG dependency.
        target_types = {d.target for d in spec.dependencies}
        for wfc in spec.workflows:
            if wfc.name not in target_types or wfc.launch_on_start:
                self._dep_satisfied.add(wfc.name)

    # ── Public helpers ───────────────────────────────────────────────────────

    def enqueue(self, request: LaunchRequest) -> None:
        # Stamp the owning campaign on the request (used in logs and checkpoints).
        if not request.campaign_name:
            request.campaign_name = self.name
        if request.workflow_type in self._dep_satisfied:
            self._queue.push(request)
            self._slot_event.set()
        else:
            self._dep_pending.setdefault(request.workflow_type, []).append(request)

    # ── Internals ────────────────────────────────────────────────────────────

    def _wf_config(self, workflow_type: str) -> WorkflowConfig:
        for wfc in self.spec.workflows:
            if wfc.name == workflow_type:
                return wfc
        raise KeyError(f"Unknown workflow type: {workflow_type!r}")

    def _running_by_type(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for inst in self._active.values():
            counts[inst.workflow_type] = counts.get(inst.workflow_type, 0) + 1
        return counts

    # ── Spending computation ─────────────────────────────────────────────────

    def _update_type_cost(self, wf_type: str, contribution: dict[str, float]) -> None:
        """Update per-type cost statistics with the actual spending of a completed workflow."""
        stats = self._type_cost_stats.setdefault(wf_type, {})
        for key, val in contribution.items():
            s = stats.setdefault(key, {"n": 0, "mean": 0.0})
            n = s["n"]
            s["mean"] = (s["mean"] * n + val) / (n + 1)
            s["n"] = n + 1

    def _learned_cost(
        self,
        wf_type: str,
        declared: dict[str, float],
        min_samples: int = 3,
    ) -> dict[str, float]:
        """Return the in-flight cost estimate for a workflow type.

        Uses the mean actual cost once ``min_samples`` completions are observed;
        falls back to ``declared`` before that threshold.
        """
        stats = self._type_cost_stats.get(wf_type, {})
        result = dict(declared)
        for key, s in stats.items():
            if s["n"] >= min_samples:
                result[key] = s["mean"]
        return result

    def _compute_workflow_spent(
        self,
        wf_id: str,
        declared_spending: dict[str, float],
        wall_s: float,
        final_metrics: dict,
    ) -> tuple[dict[str, float], dict]:
        """Compute actual spending and observability profile for one completed workflow.

        Returns (spending_contribution, resource_profile).

        In order of priority, per dimension:
          1. Reported: values the workflow emitted via reporter.emit().
          2. Prorated: declared × (actual_wall / declared_wall); the ratio can
             exceed 1 when the workflow runs longer than declared.
          3. Zero: declared_spending absent — campaign emitted a warning at init.
        """
        wall_h = wall_s / 3600.0
        spending: dict[str, float] = {}

        # 1. Reported values take top priority.
        # Always check cpu_hours/gpu_hours (supplied by AsyncFlow telemetry); also
        # pick up any custom dimensions declared in declared_spending (e.g. llm_tokens).
        reported_keys = {"cpu_hours", "gpu_hours"} | {
            k for k in declared_spending if k != "wall_hours"
        }
        for key in reported_keys:
            val = final_metrics.get(key)
            if val is not None:
                spending[key] = float(val)

        # 2. Prorate declared spending by actual / declared wall time.
        # Only prorate dimensions that appear in declared_spending (need a base value).
        decl_wall_h = declared_spending.get("wall_hours", 0.0)
        if decl_wall_h > 0:
            ratio = wall_h / decl_wall_h  # < 1 = underspend; > 1 = overrun
            for key in (k for k in declared_spending if k != "wall_hours"):
                if key not in spending:
                    spending[key] = declared_spending[key] * ratio

        # Observability profile: actual utilisation data for WorkflowResult.resource_profile.
        # Priority: asyncflow JSONL telemetry (CM-side) → workflow-reported resource_usage.
        prof: dict = {}
        telemetry = getattr(self._flow, "_telemetry", None)
        node_id = final_metrics.get("node_id") or final_metrics.get("node") or None
        if telemetry is not None:
            try:
                _prof, _jsonl_spending = self._correlate_from_jsonl(
                    wf_id, wall_s, telemetry, node_id=node_id
                )
                if _prof:
                    prof = _prof
                    for key, val in _jsonl_spending.items():
                        if key not in spending:
                            spending[key] = val
            except Exception:
                logger.debug("[CM] JSONL correlation failed for %s", wf_id, exc_info=True)
        if not prof:
            rp = final_metrics.get("resource_usage")
            if isinstance(rp, dict):
                prof = rp

        return spending, prof

    def _correlate_from_jsonl(
        self,
        wf_id: str,
        wall_s: float,
        telemetry: Any,
        node_id: str | None = None,
    ) -> tuple[dict, dict[str, float]]:
        """Find the block's time window and integrate ResourceUpdate samples.

        Returns (resource_profile, spending_contribution).  Both are empty dicts
        when no events for wf_id or no ResourceUpdate samples are in range.

        Live path: reads from the in-memory _event_log so the result is available
        immediately in _on_done, before the background JSONL writer has flushed.
        Post-hoc path (when event_log is absent):
        reads from the settled JSONL file on disk.
        """
        # ── Live path: in-memory event log (always current, no flush race) ──────
        event_log = getattr(telemetry, "_event_log", None)
        if event_log is not None:

            def _iter_log():
                for ev in event_log:
                    yield {
                        "event_type": getattr(ev, "event_type", None),
                        "event_time": getattr(ev, "event_time", None),
                        "task_id": getattr(ev, "task_id", None),
                        "cpu_percent": getattr(ev, "cpu_percent", None),
                        "memory_percent": getattr(ev, "memory_percent", None),
                        "gpu_percent": getattr(ev, "gpu_percent", None),
                        "node_id": getattr(ev, "node_id", None),
                    }

            prof, spending = _correlate_events(wf_id, wall_s, _iter_log(), node_id=node_id)
            if prof:
                return prof, spending

        # ── Post-hoc path: read from the settled JSONL file ──────────────────────
        ckpt_path = getattr(telemetry, "_checkpoint_path", None)
        session_id = getattr(telemetry, "_session_id", None)
        if not ckpt_path or not session_id:
            return {}, {}

        matches = list(Path(ckpt_path).glob(f"{session_id}.*.telemetry.jsonl"))
        if not matches:
            return {}, {}

        jsonl_path = matches[0]
        try:

            def _iter_jsonl():
                with open(jsonl_path) as fh:
                    for raw in fh:
                        raw = raw.strip()
                        if not raw:
                            continue
                        try:
                            yield json.loads(raw)
                        except json.JSONDecodeError:
                            continue

            return _correlate_events(wf_id, wall_s, _iter_jsonl(), node_id=node_id)
        except OSError:
            return {}, {}

    # ── Task-level accounting ───────────────────────────────────────────────

    _TASK_END_EVENTS = ("TaskCompleted", "TaskFailed", "TaskCanceled", "TaskCancelled")
    # Task state after each AsyncFlow event:
    #   waiting   — created, not yet handed to the backend (dependencies or engine)
    #   submitted — handed to the backend, not yet running (backend backlog)
    #   running   — the backend reports it running
    _TASK_STATES = ("waiting", "submitted", "running")
    _TASK_STATE_OF = {
        "TaskCreated": "waiting",
        "TaskQueued": "waiting",
        "asyncflow.TaskResolved": "waiting",
        "TaskSubmitted": "submitted",
        "TaskStarted": "running",
    }

    def _track_task_state(self, etype: str, tag: Any, task_uid: Any) -> None:
        """Record a task's state for task_counts(); blocks are not tasks."""
        if not task_uid or not tag or str(task_uid).startswith("block."):
            return
        if etype in self._TASK_END_EVENTS:
            self._task_states.pop(task_uid, None)
            return
        state = self._TASK_STATE_OF.get(etype)
        if state is None:
            return
        wf_id = self._wf_tags.get(tag)
        if wf_id is None:
            return
        prev = self._task_states.get(task_uid)
        order = self._TASK_STATES
        if prev is None or order.index(state) > order.index(prev[1]):
            self._task_states[task_uid] = (wf_id, state)

    def task_counts(self) -> dict[str, int]:
        """Number of this campaign's AsyncFlow tasks in each state."""
        counts = dict.fromkeys(self._TASK_STATES, 0)
        for _wf_id, state in self._task_states.values():
            counts[state] += 1
        return counts

    def _task_observation(self) -> dict | None:
        """obs["tasks"]: this campaign's AsyncFlow task counts per state.

        None when AsyncFlow telemetry is off (no task events to count).
        """
        if not self._tracking_tasks:
            return None
        return self.task_counts()

    def _on_task_event(self, event: Any) -> None:
        """Charge a workflow from AsyncFlow task events (telemetry subscriber).

        Uses the task's requested resources (``attributes["resources"]``,
        emitted by radical.asyncflow) × ``duration_seconds``.  A workflow switches
        to task-level accounting on the first ``TaskCreated`` event tagged with
        it; its completion-time estimate is then not charged.  While it runs,
        its in-flight estimate is reduced by what has already been charged.
        """
        etype = getattr(event, "event_type", "")
        attrs = getattr(event, "attributes", None) or {}
        res = attrs.get("resources")
        tag = attrs.get("asyncflow.workflow_id")
        task_uid = getattr(event, "task_id", None)
        self._track_task_state(etype, tag, task_uid)

        # Inner-block registration: asyncflow emits TaskCreated (no resources)
        # for every block, tagged with its parent block/workflow.  If the parent
        # is already mapped, propagate the mapping to the child so tasks inside
        # nested blocks are charged to the same wf_id.
        if not res and etype == "TaskCreated" and tag and task_uid:
            if isinstance(task_uid, str) and task_uid.startswith("block."):
                parent_wf_id = self._wf_tags.get(tag)
                if parent_wf_id is not None and task_uid not in self._wf_tags:
                    self._wf_tags[task_uid] = parent_wf_id
                    logger.debug(
                        "[BLOCK-TAG] inner block wf=%s parent=%s child=%s",
                        parent_wf_id,
                        tag,
                        task_uid,
                    )
            return

        if not res or not tag:
            return
        wf_id = self._wf_tags.get(tag)
        if wf_id is None:
            return

        if etype == "TaskCreated":
            self._task_level.add(wf_id)
            return

        if etype == "TaskStarted":
            self._task_level.add(wf_id)
            if task_uid:
                self._active_tasks[task_uid] = {
                    "wf_id": wf_id,
                    "resources": res,
                    "start_time": getattr(event, "event_time", None),
                }
            logger.info(
                "[SPEND] task_started  wf=%s  task=%s  cpus=%.0f  gpus=%.0f",
                wf_id,
                task_uid,
                float(res.get("cpus", 0)),
                float(res.get("gpus", 0)),
            )
            return

        if etype not in self._TASK_END_EVENTS:
            return

        # Remove from active-tasks window before computing burn rate.
        if task_uid:
            self._active_tasks.pop(task_uid, None)

        self._task_level.add(wf_id)
        hours = float(getattr(event, "duration_seconds", 0.0) or 0.0) / 3600.0
        usage = {
            "cpu_hours": float(res.get("cpus", 0.0)) * hours,
            "gpu_hours": float(res.get("gpus", 0.0)) * hours,
        }
        usage = {k: v for k, v in usage.items() if v > 0.0}
        if not usage:
            return
        self._allocator.record(self.name, usage)
        charged = self._task_usage.setdefault(wf_id, {})
        for k, v in usage.items():
            charged[k] = charged.get(k, 0.0) + v
        inst = self._active.get(wf_id)
        if inst is not None:
            # Charges are already in `spent`; in-flight is what is still expected.
            est = self._estimates.get(wf_id, inst.declared_spending)
            remaining = {
                k: max(0.0, v - charged.get(k, 0.0)) for k, v in est.items() if k != "wall_hours"
            }
            self._allocator.set_in_flight(self.name, wf_id, remaining)
        frac = self._allocator.spending_fraction(self.name)
        logger.info(
            "[SPEND] task_charged  wf=%s  task=%s  cpu_h=%.4f  gpu_h=%.4f"
            "  wf_cpu_h=%.4f  wf_gpu_h=%.4f  budget_used=%.1f%%",
            wf_id,
            task_uid,
            usage.get("cpu_hours", 0.0),
            usage.get("gpu_hours", 0.0),
            charged.get("cpu_hours", 0.0),
            charged.get("gpu_hours", 0.0),
            frac * 100,
        )

    # ── Workflow launch ──────────────────────────────────────────────────────

    def _make_future(
        self,
        wfc: WorkflowConfig,
        wf_id: str,
        context: dict | None = None,
        spending_limit: dict[str, float] | None = None,
        resources: dict[str, float] | None = None,
    ) -> asyncio.Future:
        # Every workflow runs as one AsyncFlow block: the block is CM's unit of
        # launch, cancellation (cancelling it cancels every task inside) and
        # accounting (tasks registered inside it are tagged with the block uid,
        # which _run_as_block maps to this workflow in _wf_tags).
        if wfc.block is not None:
            # Python workflow: the workflow function is the block body; its
            # compute steps are AsyncFlow tasks submitted from inside it.
            block_fn = wfc.block
            reporter = (
                self.spec.data_backend.make_reporter(wf_id) if _wants_reporter(block_fn) else None
            )
            # A Python workflow runs inside CM's process, so it gets what it needs
            # as arguments (never through the shared process environment):
            # the WorkflowReporter (bound to wf_id and the DataBackend),
            # flow= (the shared engine) and context= (LaunchRequest.context).
            call_args: tuple = ()
            call_kwargs: dict[str, Any] = {}
            if reporter is not None:
                call_kwargs["reporter"] = reporter
            if _wants_flow(block_fn):
                call_kwargs["flow"] = self._flow
            if _wants_context(block_fn):
                ctx = dict(context or {})
                if spending_limit:
                    ctx["spending_limit"] = dict(spending_limit)
                if resources:
                    ctx["resources"] = dict(resources)
                call_kwargs["context"] = ctx

            async def _body(*a, **kw):
                return await block_fn(*a, **kw)

            _body.__name__ = block_fn.__name__
            _body.__qualname__ = block_fn.__qualname__
            return self._run_as_block(_body, wf_id, call_args, call_kwargs)

        # Command-line workflow: a block containing one executable task that
        # runs launch_cmd through an inline wrapper script (bash -c).  The script
        # exports the CM variables into the program's own process; it is part of
        # the command line, so no file has to be visible on the node where the
        # task runs.  The task carries WorkflowConfig.task_description as its
        # resource request.
        command = self._wrap_cmd(
            wfc.launch_cmd, self._cmd_env(wfc, wf_id, context, spending_limit, resources)
        )
        task_description = dict(wfc.task_description)

        @self._flow.executable_task(capture_stdio=True)
        async def _run_cmd():
            return command

        async def _cmd_block():
            task = _run_cmd(task_description=task_description)
            try:
                return await task
            finally:
                # The program's own exit code (None if the backend reports none).
                code = getattr(task, "exit_code", None)
                if code is not None:
                    self._exit_codes[wf_id] = code

        _cmd_block.__name__ = f"cmd_{wfc.name}"
        _cmd_block.__qualname__ = _cmd_block.__name__
        return self._run_as_block(_cmd_block, wf_id, (), {})

    def _cmd_env(
        self,
        wfc: WorkflowConfig,
        wf_id: str,
        context: dict | None,
        spending_limit: dict[str, float] | None = None,
        resources: dict[str, float] | None = None,
    ) -> dict[str, str]:
        """Environment for a command-line workflow's process (wrapper script)."""
        env = {"CM_WORKFLOW_ID": wf_id, "CM_CAMPAIGN_ID": self.name, **wfc.env}
        # Per-instance context from LaunchRequest is surfaced as CM_CTX_{KEY}.
        for k, v in (context or {}).items():
            env[f"CM_CTX_{k.upper()}"] = str(v)
        # The run's spending limit, one variable per dimension.
        for k, v in (spending_limit or {}).items():
            env[f"CM_SPENDING_LIMIT_{k.upper()}"] = repr(float(v))
        # The resources it may use at once, one variable per key.
        for k, v in (resources or {}).items():
            env[f"CM_RESOURCES_{k.upper()}"] = repr(float(v))
        ep = self.spec.data_backend.endpoint() if self.spec.data_backend is not None else None
        if ep is not None:
            env["CM_DATA_ENDPOINT"] = ep
            # Tell aucam_client which backend protocol to use.
            backend_name = type(self.spec.data_backend).__name__
            if "Redis" in backend_name:
                env["CM_DATA_BACKEND"] = "redis"
            elif "Dragon" in backend_name:
                env["CM_DATA_BACKEND"] = "dragon"
        return env

    async def _read_final_report(self, wf_id: str) -> None:
        """Read a workflow's last report when its block ends.

        Remote backends (Redis, Dragon) have no synchronous read, so without
        this the WorkflowResult would carry the report CM read in the last
        policy cycle and miss a final emit().
        """
        backend = self.spec.data_backend
        if backend is None or hasattr(backend, "sync_get_report"):
            return
        try:
            report = await backend.get_report(wf_id)
        except Exception:
            logger.debug("[CM] final get_report(%s) failed", wf_id, exc_info=True)
            return
        inst = self._active.get(wf_id)
        if report is not None and inst is not None:
            inst.last_report = report

    def _run_as_block(
        self,
        body: Any,
        wf_id: str,
        call_args: tuple,
        call_kwargs: dict,
    ) -> asyncio.Future:
        """Register ``body`` as an AsyncFlow block for workflow ``wf_id`` and start it."""
        # _fut_box lets _block read its own future after registration
        # completes.  asyncflow sets block_fut.block["uid"] inside
        # async_wrapper (asynchronously), but that attribute is guaranteed to
        # be present by the time execute_block calls _block, because asyncflow's
        # execute_block reads block_fut.block["uid"] itself before calling it.
        _fut_box: list = [None]

        async def _block(*a, **kw):
            f = _fut_box[0]
            if f is not None:
                block_uid = getattr(f, "block", {}).get("uid")
                if block_uid:
                    self._wf_tags[block_uid] = wf_id
                    logger.debug("[BLOCK-TAG] wf=%s block_uid=%s", wf_id, block_uid)
            try:
                return await body(*a, **kw)
            finally:
                await self._read_final_report(wf_id)

        _block.__name__ = body.__name__
        _block.__qualname__ = body.__qualname__
        registered = self._flow.block(_block)
        _fut_box[0] = registered(*call_args, workflow_id=wf_id, **call_kwargs)
        return _fut_box[0]

    @staticmethod
    def _wrap_cmd(launch_cmd: list[str], env: dict[str, str]) -> str:
        """Inline wrapper: ``bash -c 'export K=V; ...; exec launch_cmd'``.

        ``exec`` replaces the shell, so the program's exit code is the task's.
        """
        lines = [f"export {k}={shlex.quote(str(v))}" for k, v in env.items()]
        lines.append("exec " + " ".join(shlex.quote(str(c)) for c in launch_cmd))
        return "bash -c " + shlex.quote("\n".join(lines))

    @staticmethod
    def _run_limit(wfc: WorkflowConfig, request: LaunchRequest) -> dict[str, float]:
        """The run's spending limit: the request's, else the workflow type's."""
        raw = request.spending_limit if request.spending_limit is not None else wfc.spending_limit
        limit: dict[str, float] = {}
        for k, v in (raw or {}).items():
            if k == "wall_hours" or v is None:  # run time is limited by max_wall_time_s
                continue
            if float(v) > 0:
                limit[k] = float(v)
        return limit

    def _proposal(self, wfc: WorkflowConfig, request: LaunchRequest) -> Allocation:
        """The request's own amounts, else the workflow type's defaults."""
        raw_res = request.resources if request.resources is not None else wfc.resources
        return Allocation(
            spending_limit=self._run_limit(wfc, request),
            resources={k: float(v) for k, v in (raw_res or {}).items() if v is not None},
        )

    def _size(self, request: LaunchRequest) -> Allocation | None:
        """Ask the scheduling policy what this run gets (None: drop the request)."""
        wfc = self._wf_config(request.workflow_type)
        stats = self._type_cost_stats.get(request.workflow_type, {})
        learned = {k: s["mean"] for k, s in stats.items() if s["n"] >= 3}
        rstats = self._type_resource_stats.get(request.workflow_type, {})
        burn = self._allocator.campaign_view(self.name)["burn"]
        ctx = SizingContext(
            proposal=self._proposal(wfc, request),
            learned_cost=learned,
            remaining={k: v["remaining"] for k, v in burn.items()},
            queued=len(self._queue) + 1,
            learned_resources={k: s["mean"] for k, s in rstats.items() if s["n"] >= 3},
            by_type=self._by_type(),
        )
        # The type's ranges (WorkflowConfig.bounds) apply unless the request
        # gives its own for the same key.
        bounds = {**wfc.bounds, **(request.bounds or {})}
        if bounds != (request.bounds or {}):
            request = dataclasses.replace(request, bounds=bounds)
        alloc = self._policy.size(request, ctx)
        if alloc is None:
            return None
        # Keep positive amounts only; run time is limited by max_wall_time_s.
        alloc.spending_limit = {
            k: float(v)
            for k, v in alloc.spending_limit.items()
            if k != "wall_hours" and v is not None and float(v) > 0
        }
        alloc.resources = {
            k: float(v) for k, v in alloc.resources.items() if v is not None and float(v) > 0
        }
        return alloc

    def _record_type_totals(self, result: WorkflowResult, spent: dict[str, float]) -> None:
        """Add one finished workflow to its type's totals (B1, ``budget["by_type"]``)."""
        t = self._type_totals.setdefault(
            result.workflow_type,
            {
                "completed": 0,
                "cancelled": 0,
                "failed": 0,
                "spent": {},
                "score_sum": 0.0,
                "score_n": 0,
            },
        )
        rc = result.returncode
        t["completed" if rc == 0 else "cancelled" if rc == -1 else "failed"] += 1
        for k, v in (spent or {}).items():
            if k != "wall_hours":
                t["spent"][k] = t["spent"].get(k, 0.0) + float(v)
        if rc == 0 and isinstance(result.final_score, (int, float)):
            t["score_sum"] += float(result.final_score)
            t["score_n"] += 1

    def _budget_view(self) -> dict:
        """The ledger view plus burn-down, passed to the controller as ``obs["budget"]``.

        Adds to each limited dimension in ``burn`` (B1):
          rate_per_hour  — spending charged per hour over the last (up to 20)
                           policy cycles; 0 in the first cycle;
          projected      — spent + in-flight + the estimated cost of queued requests;
          hours_to_limit — remaining / rate_per_hour (None while the rate is 0).
        and ``by_type``: per workflow type, finished-workflow counts, total and
        average spending, spending per completed workflow and mean final score.
        """
        view = self._allocator.campaign_view(self.name)
        now = time.monotonic()
        spent = view["spent"]
        self._spent_samples.append((now, dict(spent)))
        # Rate over the window of recent cycles; 0 until there are two samples
        # (a restored campaign's spending was not charged in this window).
        t0, spent0 = self._spent_samples[0]
        hours = max(now - t0, 1e-9) / 3600.0
        queued: dict[str, float] = {}
        for req in self._queue.candidates():
            wfc = self._wf_config(req.workflow_type)
            for k, v in self._held_estimate(req, self._run_limit(wfc, req)).items():
                if k != "wall_hours":
                    queued[k] = queued.get(k, 0.0) + float(v)
        for key, b in view["burn"].items():
            rate = max(0.0, spent.get(key, 0.0) - spent0.get(key, 0.0)) / hours
            b["rate_per_hour"] = rate
            b["projected"] = (
                spent.get(key, 0.0) + view["in_flight"].get(key, 0.0) + queued.get(key, 0.0)
            )
            b["hours_to_limit"] = b["remaining"] / rate if rate > 0 else None
        view["by_type"] = self._by_type()
        return view

    def _by_type(self) -> dict[str, dict]:
        """Per workflow type: finished-workflow counts, spending and mean score (B1)."""
        by_type: dict[str, dict] = {}
        for wt, t in self._type_totals.items():
            n = t["completed"] + t["cancelled"] + t["failed"]
            by_type[wt] = {
                "completed": t["completed"],
                "cancelled": t["cancelled"],
                "failed": t["failed"],
                "spent": dict(t["spent"]),
                "avg_cost": {k: v / n for k, v in t["spent"].items()} if n else {},
                "cost_per_completed": (
                    {k: v / t["completed"] for k, v in t["spent"].items()} if t["completed"] else {}
                ),
                "mean_score": t["score_sum"] / t["score_n"] if t["score_n"] else None,
            }
        return by_type

    def _held_estimate(self, request: LaunchRequest, limit: dict[str, float]) -> dict[str, float]:
        """What a workflow is held at against the campaign while it runs.

        Its own spending limit where it has one (it can never spend more), else
        the learned cost of its type, else the type's declared_spending.
        """
        wfc = self._wf_config(request.workflow_type)
        est = self._learned_cost(request.workflow_type, dict(wfc.declared_spending))
        return {**est, **limit} if limit else est

    def _launch(self, request: LaunchRequest, alloc: Allocation | None = None) -> None:
        wfc = self._wf_config(request.workflow_type)
        wf_id = f"{self.name}.{request.workflow_type}.{uuid.uuid4().hex[:8]}"
        if alloc is None:
            alloc = self._proposal(wfc, request)
        limit, resources = alloc.spending_limit, alloc.resources

        logger.info(
            f"[SCHEDULER-{self.name}] LAUNCH {request.workflow_type} "
            f"id={wf_id} requester={request.requester}"
            + (f" spending_limit={limit}" if limit else "")
            + (f" resources={resources}" if resources else "")
            + (" (capped to the remaining budget)" if alloc.capped else "")
        )

        self._wf_tags[wf_id] = wf_id
        future = self._make_future(wfc, wf_id, request.context, limit, resources)
        inst = _WorkflowInstance(
            wf_id=wf_id,
            workflow_type=request.workflow_type,
            future=future,
            started_at=time.time(),
            declared_spending=dict(wfc.declared_spending),
            request=request,
            spending_limit=limit,
            resources=resources,
            capped=alloc.capped,
        )
        self._active[wf_id] = inst
        # Use learned cost estimate for in-flight tracking once enough completions
        # exist; falls back to declared_spending before min_samples threshold (B3).
        # A run with its own spending limit holds that limit against the
        # campaign's (like a reservation): it can never spend more.
        in_flight_est = self._held_estimate(request, limit)
        self._estimates[wf_id] = in_flight_est
        self._allocator.set_in_flight(self.name, wf_id, in_flight_est)
        self._instances_run += 1

        self._flow._emit(
            "TaskStarted",
            task_id=wf_id,
            workflow_id=wf_id,
            attributes={"aucam.campaign": self.name, "aucam.wf_type": request.workflow_type},
        )

        def _on_done(fut: asyncio.Future, _inst: _WorkflowInstance = inst) -> None:
            wall = time.time() - _inst.started_at
            if fut.cancelled():
                rc, status = -1, WorkflowStatus.CANCELLED
                self._cancelled += 1
            elif fut.exception() is not None:
                # A command-line workflow reports its program's exit code; a
                # signal (negative code) maps to the shell convention 128 + N so
                # that -1 keeps meaning "cancelled".
                code = self._exit_codes.get(_inst.wf_id)
                if code is None or code == 0:
                    rc = 1
                else:
                    rc = 128 - code if code < 0 else code
                status = WorkflowStatus.FAILED
                self._failed += 1
                logger.error(
                    "[WORKFLOW-%s] worker raised",
                    _inst.wf_id,
                    exc_info=fut.exception(),
                )
            else:
                rc, status = 0, WorkflowStatus.COMPLETED
                self._completed += 1

            logger.info(f"[WORKFLOW-{_inst.wf_id}] {status.value} wall={wall:.1f}s rc={rc}")

            _status_to_event = {
                WorkflowStatus.COMPLETED: "TaskCompleted",
                WorkflowStatus.CANCELLED: "TaskCanceled",
                WorkflowStatus.FAILED: "TaskFailed",
            }
            _tel_kw: dict = {
                "task_id": _inst.wf_id,
                "workflow_id": _inst.wf_id,
                "duration_seconds": wall,
                "attributes": {
                    "aucam.campaign": self.name,
                    "aucam.wf_type": _inst.workflow_type,
                    "aucam.rc": rc,
                },
            }
            if status == WorkflowStatus.FAILED:
                _tel_kw["error_type"] = "unknown"
            self._flow._emit(_status_to_event[status], **_tel_kw)

            # Grab the last emitted report synchronously before async delete fires.
            # For in-process (LocalDictBackend) this is always available.
            # For remote backends (Dragon, Redis) use _inst.last_report, which
            # _read_final_report refreshed when the workflow's block ended.
            backend = self.spec.data_backend
            last_report = _inst.last_report
            if hasattr(backend, "sync_get_report"):
                last_report = backend.sync_get_report(_inst.wf_id) or last_report
            final_metrics: dict = dict(last_report.metrics or {}) if last_report is not None else {}
            final_score = last_report.score if last_report is not None else None
            if last_report is not None:
                _inst.last_report = last_report  # so the final report counts for peak use

            self._completion_event.set()

            self._active.pop(_inst.wf_id, None)
            self._exit_codes.pop(_inst.wf_id, None)
            # Forget this workflow's tasks; end events may never arrive after a cancel.
            for uid in [u for u, (w, _s) in self._task_states.items() if w == _inst.wf_id]:
                del self._task_states[uid]
            self._slot_event.set()

            # Replace the in-flight estimate with the workflow's actual spending.
            contribution, resource_profile = self._compute_workflow_spent(
                _inst.wf_id, _inst.declared_spending, wall, final_metrics
            )
            self._allocator.clear_in_flight(self.name, _inst.wf_id)
            # Workflows charged task by task from AsyncFlow events
            # (_on_task_event) are not charged again here; their events still
            # in flight are recorded when they arrive.
            if _inst.wf_id not in self._task_level:
                self._allocator.record(self.name, contribution)
            self._estimates.pop(_inst.wf_id, None)

            if _inst.wf_id in self._task_level:
                # .get(): a workflow can be marked task-level (TaskCreated) and
                # end before any task was charged (cancelled mid-task, zero
                # duration, or end events still in flight).
                actual_spent = dict(self._task_usage.get(_inst.wf_id, {}))
                spend_method = "exact"
                spend_source = "task_telemetry"
            else:
                actual_spent = contribution
                if any(final_metrics.get(k) is not None for k in ("cpu_hours", "gpu_hours")):
                    spend_method = "exact"
                    spend_source = "wf_reported"
                elif contribution:
                    spend_method = "estimated"
                    spend_source = "wall_proration"
                else:
                    spend_method = "none"
                    spend_source = "no_declared_spending"
            # Update the per-type cost model (B3) from successful runs only:
            # cancelled and failed runs are partial and would bias the mean.
            if rc == 0 and actual_spent:
                self._update_type_cost(_inst.workflow_type, actual_spent)
            self._note_peak_use(_inst)
            if rc == 0 and _inst.peak_use:
                rstats = self._type_resource_stats.setdefault(_inst.workflow_type, {})
                for key, val in _inst.peak_use.items():
                    s_ = rstats.setdefault(key, {"n": 0, "mean": 0.0})
                    s_["mean"] = (s_["mean"] * s_["n"] + val) / (s_["n"] + 1)
                    s_["n"] += 1
            logger.info(
                "[SPEND] wf_done  wf=%s  method=%s  source=%s  cpu_h=%.4f  gpu_h=%.4f  wall_s=%.1f  rc=%s",
                _inst.wf_id,
                spend_method,
                spend_source,
                actual_spent.get("cpu_hours", 0.0),
                actual_spent.get("gpu_hours", 0.0),
                wall,
                rc,
            )
            result = WorkflowResult(
                workflow_id=_inst.wf_id,
                workflow_type=_inst.workflow_type,
                returncode=rc,
                wall_time_s=wall,
                final_metrics=final_metrics,
                resource_profile=resource_profile,
                final_score=final_score,
                spent=actual_spent,
                spending_limit=dict(_inst.spending_limit),
                over_budget=_inst.over_budget,
                resources=dict(_inst.resources),
                over_resources=_inst.over_resources,
                capped=_inst.capped,
            )

            if self.spec.data_backend is not None:
                asyncio.ensure_future(self.spec.data_backend.delete_report(_inst.wf_id))

            # Update scheduler policy, controller counters, and history.
            # DAG-triggered and init requests were not selected by the policy,
            # so they must not feed back into the policy's arm statistics.
            if _inst.request.requester == "policy_agent":
                self._policy.on_outcome(_inst.request, result)
            self.spec.controller.on_complete(result)
            self._record_type_totals(result, actual_spent)
            self._history.append(result)
            if len(self._history) > self._history_max:
                self._history.pop(0)

            if wfc.on_completion is not None:
                try:
                    wfc.on_completion(result)
                except Exception:
                    logger.exception(f"[WORKFLOW-{_inst.wf_id}] on_completion hook raised")

            self._check_dag(result)

        future.add_done_callback(_on_done)

    def _check_dag(self, result: WorkflowResult) -> None:
        for dep in self.spec.dependencies:
            if dep.source != result.workflow_type:
                continue
            # Dedup: each source instance triggers each target at most once.
            trigger_key = (result.workflow_id, dep.target)
            if trigger_key in self._dep_triggered:
                continue
            try:
                ok = dep.condition(result)
            except Exception:
                logger.exception(f"[CM] DAG condition for {dep.target} raised")
                ok = False

            if ok:
                self._dep_triggered.add(trigger_key)
                first_unlock = dep.target not in self._dep_satisfied
                if first_unlock:
                    # Unlock the target type so future enqueue() calls go through.
                    self._dep_satisfied.add(dep.target)
                    held = self._dep_pending.pop(dep.target, [])
                    for req in held:
                        self._queue.push(req)
                    if held:
                        # Held requests represent pending work; no extra request.
                        self._slot_event.set()
                        logger.info(
                            f"[CM] DAG {dep.source}→{dep.target} unlocked "
                            f"— released {len(held)} held request(s)"
                        )
                        continue

                # Create one new request for this source completion.
                new_req = LaunchRequest(
                    workflow_type=dep.target,
                    priority=LaunchPriority.NORMAL,
                    enqueued_at=time.time(),
                    context={"reason": "dag", "source": result.workflow_id},
                    requester="dag",
                )
                self._queue.push(new_req)
                self._slot_event.set()
                logger.info(f"[CM] DAG {dep.source}→{dep.target} satisfied — created 1 request(s)")

    # ── Checkpointing ────────────────────────────────────────────────────────

    def _build_checkpoint(self) -> CampaignCheckpoint:
        """Snapshot current mutable state into a serializable checkpoint."""
        queue_records = [
            QueuedRequestRecord(
                workflow_type=req.workflow_type,
                priority=req.priority.value,
                enqueued_at=req.enqueued_at,
                context=req.context,
                requester=req.requester,
            )
            for _, _, _, req in self._queue._heap
        ]
        active_records = [
            ActiveWorkflowRecord(
                wf_id=inst.wf_id,
                workflow_type=inst.workflow_type,
                started_at=inst.started_at,
                declared_spending=dict(inst.declared_spending),
            )
            for inst in self._active.values()
        ]
        return CampaignCheckpoint(
            campaign_name=self.name,
            queue=queue_records,
            active=active_records,
            paused_wf_ids=[],  # populated by caller when needed
            dep_satisfied=list(self._dep_satisfied),
            spent=self._allocator.spent(self.name),
            instances_run=self._instances_run,
            completed=self._completed,
            cancelled=self._cancelled,
            failed=self._failed,
            stale_cancels=self._stale_cancels,
            walltime_cancels=self._walltime_cancels,
            budget_cancels=self._budget_cancels,
            resource_cancels=self._resource_cancels,
            unfit_dropped=self._unfit_dropped,
            controller_errors=self._controller_errors,
            policy_cancels=self._policy_cancels,
            policy_cycles=self._policy_cycles,
        )

    def _checkpoint_path(self):
        if self.spec.checkpoint_dir is None:
            return None
        return checkpoint_path(self.spec.checkpoint_dir, self.name)

    def _save_checkpoint(self) -> None:
        path = self._checkpoint_path()
        if path is None:
            return
        write_checkpoint(path, self._build_checkpoint())

    def _restore_from_checkpoint(self, ckpt: CampaignCheckpoint) -> None:
        """Apply checkpoint state before the first policy cycle.

        Queue items are re-pushed verbatim.
        Active workflows are re-enqueued at HIGH priority (they were running
        when CM died; for block-based tasks the coroutine is gone and must
        restart from scratch).
        Counters and spent are restored so the summary log is accurate.
        """
        # Restore DAG unlock state BEFORE re-pushing queued requests so that
        # enqueue() admits requests for already-satisfied targets immediately.
        self._dep_satisfied.update(ckpt.dep_satisfied)

        # Restore counters
        self._instances_run = ckpt.instances_run
        self._completed = ckpt.completed
        self._cancelled = ckpt.cancelled
        self._failed = ckpt.failed
        self._stale_cancels = ckpt.stale_cancels
        self._walltime_cancels = ckpt.walltime_cancels
        self._budget_cancels = ckpt.budget_cancels
        self._resource_cancels = ckpt.resource_cancels
        self._unfit_dropped = ckpt.unfit_dropped
        self._controller_errors = ckpt.controller_errors
        self._policy_cancels = ckpt.policy_cancels
        self._policy_cycles = ckpt.policy_cycles
        self._allocator.restore(self.name, ckpt.spent)

        # Re-push queued requests
        for rec in ckpt.queue:
            try:
                priority = LaunchPriority(rec.priority)
            except ValueError:
                priority = LaunchPriority.NORMAL
            req = LaunchRequest(
                workflow_type=rec.workflow_type,
                priority=priority,
                enqueued_at=rec.enqueued_at,
                context=rec.context,
                requester=rec.requester,
            )
            self.enqueue(req)

        # Re-enqueue workflows that were active — they need to be re-run
        for rec in ckpt.active:
            req = LaunchRequest(
                workflow_type=rec.workflow_type,
                priority=LaunchPriority.HIGH,  # bump priority for recovery
                enqueued_at=time.time(),
                context={"recovered_from": rec.wf_id, "original_started_at": rec.started_at},
                requester="cm:recovery",
            )
            self.enqueue(req)

        logger.info(
            f"[CKPT] {self.name}: restored "
            f"queued={len(ckpt.queue)} "
            f"re-enqueued_active={len(ckpt.active)} "
            f"cycles_before={ckpt.policy_cycles}"
        )

    # ── Scheduler loop ───────────────────────────────────────────────────────

    async def _scheduler_loop(self) -> None:
        _idle_since: float | None = None
        while not self._done:
            self._slot_event.clear()
            await self._drain_queue()
            if (
                not self._queue
                and not self._active
                and self._spending_limit_reached()
                and not self._done
            ):
                await self._shutdown(reason="spending_limit")
                return
            # Exit when the campaign is idle (CampaignSpec.idle_exit_s): queue
            # empty, nothing running, no pending DAG requests, and at least one
            # policy cycle has run (so the controller had a chance to add new
            # requests before we give up).
            idle_after = self.spec.idle_exit_after()
            no_pending = not any(self._dep_pending.values())
            if (
                idle_after is not None
                and not self._queue
                and not self._active
                and no_pending
                and self._policy_cycles > 0
            ):
                if _idle_since is None:
                    _idle_since = time.time()
                elif not self._done and time.time() - _idle_since >= idle_after:
                    await self._shutdown(reason="campaign_idle")
                    return
            else:
                _idle_since = None
            try:
                await asyncio.wait_for(self._slot_event.wait(), timeout=2.0)
            except TimeoutError:
                pass

    def _spending_limit_reached(self) -> bool:
        """Return True when actual + in-flight spending meets any campaign limit.

        The ledger counts each running workflow with its full declared_spending
        (conservative, like SLURM job accounting); the estimate is replaced by
        the actual amount when the workflow finishes.
        """
        hit = self._allocator.limit_reached(self.name)
        if hit is None:
            return False
        key, done, live, limit = hit
        if not getattr(self, "_spending_limit_logged", False):
            logger.info(
                f"[SCHEDULER-{self.name}] spending limit reached: "
                f"{key}: completed={done:.3f} "
                f"+ in-flight={live:.3f} >= {limit:.3f} — "
                f"no new workflows will be launched"
            )
            self._spending_limit_logged = True
        return True

    async def _drain_queue(self) -> None:
        """Select and launch workflow requests.

        CRITICAL-priority requests bypass the policy and launch immediately
        if a concurrency slot is available.  All other requests are passed to
        the SchedulingPolicy.select() call, which returns the subset to launch.
        Concurrency is limited by workflow counts; spending by the ledger.
        """
        if self._done:
            return
        if self._spending_limit_reached():
            if not self._active:
                asyncio.ensure_future(self._shutdown(reason="spending_limit"))
            return

        max_concurrent = self.spec.budget.max_concurrent_workflows
        if len(self._active) >= max_concurrent:
            return

        all_candidates = self._queue.candidates()
        if not all_candidates:
            return

        # Separate CRITICAL (bypass policy) from normal candidates.
        critical = [r for r in all_candidates if r.priority == LaunchPriority.CRITICAL]
        normal = [r for r in all_candidates if r.priority != LaunchPriority.CRITICAL]

        # Running instances per type, updated as requests launch this round;
        # the per-type limit is enforced here for every request, whatever the
        # policy returns.
        per_type_cap = self.spec.budget.max_concurrency_per_workflow
        counts = self._running_by_type()
        launched = dropped = 0
        unfit: set[int] = set()  # id() of requests that did not fit the budget this round

        def _try(req: LaunchRequest) -> str:
            """Launch one request if it fits: "full", "skip", "launched", "unfit" or "dropped".

            B2: a request starts only if spent + in-flight + its cost fits the
            campaign's limit in every dimension; one that does not fit stays
            queued, and a cheaper request may start instead.
            """
            nonlocal launched, dropped
            if len(self._active) >= max_concurrent:
                return "full"
            if counts.get(req.workflow_type, 0) >= per_type_cap:
                return "skip"  # this type is full; others may still fit
            alloc = self._size(req)
            if alloc is None:
                self._queue.remove_request(req)
                self._unfit_logged.discard(id(req))
                self._unfit_dropped += 1
                dropped += 1
                logger.info(
                    f"[SCHEDULER-{self.name}] DROP {req.workflow_type}: "
                    f"its minimum does not fit the remaining budget"
                )
                return "dropped"
            cost = self._held_estimate(req, alloc.spending_limit)
            short = self._allocator.shortfall(self.name, cost)
            if short is not None:
                unfit.add(id(req))
                if id(req) not in self._unfit_logged:
                    self._unfit_logged.add(id(req))
                    key, need, left = short
                    logger.info(
                        f"[SCHEDULER-{self.name}] WAIT {req.workflow_type}: "
                        f"{key} cost {need:.4g} > remaining {left:.4g}; stays queued"
                    )
                return "unfit"
            self._queue.remove_request(req)
            self._unfit_logged.discard(id(req))
            self._launch(req, alloc)
            counts[req.workflow_type] = counts.get(req.workflow_type, 0) + 1
            launched += 1
            return "launched"

        # CRITICAL requests always go first, no policy consultation.
        for req in critical:
            if _try(req) == "full":
                break

        # The policy selects from normal candidates for the remaining slots.  If
        # some of its picks did not fit the budget, it is asked again without
        # them, so a cheaper request can take the slot.
        pending = list(normal)
        while pending and len(self._active) < max_concurrent:
            ctx = SchedulingContext(
                running_by_type=self._running_by_type(),
                max_concurrency=max_concurrent,
                max_concurrency_per_type=self.spec.budget.max_concurrency_per_workflow,
                telemetry=self._last_telemetry,
                history=list(self._history),
                policy_cycles=self._policy_cycles,
                campaign_name=self.name,
                budget_remaining_fraction=1.0 - self._allocator.spending_fraction(self.name),
            )
            selected = self._policy.select(pending, ctx)
            if not selected:
                break
            outcomes = []
            for req in selected:
                r = _try(req)
                if r == "full":
                    break
                outcomes.append(r)
            picked = {id(r) for r in selected}
            pending = [r for r in pending if id(r) not in picked]
            if not any(r in ("unfit", "dropped") for r in outcomes):
                break

        # Nothing running, and what was asked for no longer fits (dropped this
        # round, or every queued request too costly): the budget is spent.
        if not self._active and not launched and not self._done:
            queued = self._queue.candidates()
            if (queued and all(id(r) in unfit for r in queued)) or (not queued and dropped):
                asyncio.ensure_future(self._shutdown(reason="spending_limit"))

    # ── Policy loop ──────────────────────────────────────────────────────────

    async def _policy_loop(self) -> None:
        while not self._done:
            # Wake either after policy_interval_s or immediately when any
            # workflow completes — whichever comes first.
            self._completion_event.clear()
            try:
                await asyncio.wait_for(
                    self._completion_event.wait(),
                    timeout=self.spec.policy_interval_s,
                )
            except TimeoutError:
                pass
            if self._done:
                break
            await self._run_policy_cycle()

    async def _collect_live_reports(self) -> dict:
        reports = {}
        for wf_id in list(self._active):
            try:
                report = await self.spec.data_backend.get_report(wf_id)
            except Exception:
                logger.debug("[CM] get_report(%s) raised — no report this cycle", wf_id)
                report = None
            if report is not None:
                self._active[wf_id].last_report = report
                reports[wf_id] = report
        return reports

    def _collect_telemetry(self) -> TelemetrySnapshot:
        now = time.time()
        running_wall_s = {wf_id: now - inst.started_at for wf_id, inst in self._active.items()}
        node_resources: dict[str, NodeResources] = {}
        # wf_id → "{backend}/{node_id}" — built from Rhapsody TaskStarted events.
        wf_node: dict[str, str] = {}

        telemetry = getattr(self._flow, "_telemetry", None)
        if telemetry is not None:
            try:
                event_log = telemetry._event_log
                # Latest ResourceUpdate per node.
                seen: set[str] = set()
                for ev in reversed(event_log):
                    if ev.event_type != "ResourceUpdate":
                        continue
                    key = f"{ev.backend}/{ev.node_id}"
                    if key not in seen:
                        seen.add(key)
                        node_resources[key] = NodeResources(
                            backend=ev.backend,
                            node_id=ev.node_id,
                            cpu_percent=getattr(ev, "cpu_percent", 0.0),
                            memory_percent=getattr(ev, "memory_percent", 0.0),
                            gpu_percent=getattr(ev, "gpu_percent", 0.0),
                            disk_read_bytes=getattr(ev, "disk_read_bytes", 0.0),
                            disk_write_bytes=getattr(ev, "disk_write_bytes", 0.0),
                            net_sent_bytes=getattr(ev, "net_sent_bytes", 0.0),
                            net_recv_bytes=getattr(ev, "net_recv_bytes", 0.0),
                        )
                # Map each active workflow to its node via TaskStarted events.
                for ev in event_log:
                    if ev.event_type != "TaskStarted":
                        continue
                    wf_id = getattr(ev, "workflow_id", None)
                    node_id = getattr(ev, "node_id", None)
                    backend = getattr(ev, "backend", None)
                    if wf_id in self._active and node_id and backend:
                        wf_node[wf_id] = f"{backend}/{node_id}"
            except Exception:
                logger.debug("[CM] Failed to read Rhapsody resource events", exc_info=True)

        # Per-workflow live consumption — from reported metrics or wall_s fallback.
        # Also overlay workflow-reported node_id (primary) over asyncflow-derived wf_node.
        running_consumption: dict[str, dict[str, float]] = {}
        for wf_id, inst in self._active.items():
            wall_s = running_wall_s[wf_id]
            report = inst.last_report
            if report is not None and report.metrics:
                cpu_s = float(report.metrics.get("cpu_seconds", wall_s))
                gpu_s = report.metrics.get("gpu_seconds")
                # Prefer workflow-reported node_id over asyncflow TaskStarted event.
                # Normalize: workflow may report FQDN ("cn106.hpc.example.edu")
                # while node_resources keys use "{backend}/{short}" ("dragon/cn106").
                # Match by short hostname so the formats align.
                reported_node = report.metrics.get("node_id")
                if reported_node and wf_id in self._active:
                    short = reported_node.split(".")[0]
                    matched = next(
                        (k for k in node_resources if k.rsplit("/", 1)[-1] == short), None
                    )
                    wf_node[wf_id] = matched if matched else reported_node
            else:
                cpu_s = wall_s
                gpu_s = None
            consumption: dict[str, float] = {"cpu_wall_seconds": cpu_s}
            if gpu_s is not None:
                consumption["gpu_wall_seconds"] = float(gpu_s)
            # Estimate in-flight spending: prorate declared by elapsed/declared_wall.
            decl = inst.declared_spending
            decl_wall_h = decl.get("wall_hours", 0.0)
            if decl_wall_h > 0:
                elapsed_h = wall_s / 3600.0
                ratio = elapsed_h / decl_wall_h
                for key in ("cpu_hours", "gpu_hours"):
                    if key in decl:
                        consumption[key] = decl[key] * ratio
            running_consumption[wf_id] = consumption

            # Live in-flight update for workflows not charged per task (those
            # update the ledger per task in _on_task_event and are skipped here).
            # These workflows are charged only when they finish, so nothing of
            # theirs is in `spent` yet: in-flight must hold their full expected
            # cost (consumed so far + still to come), never less than the launch
            # estimate.  It grows when a workflow reports, or runs, beyond it.
            if wf_id not in self._task_level:
                est = self._estimates.get(wf_id, decl)
                expected = {k: v for k, v in est.items() if k != "wall_hours"}
                metrics = report.metrics if report is not None and report.metrics else {}
                overrun = (wall_s / 3600.0) / decl_wall_h if decl_wall_h > 0 else 0.0
                for key, base in list(expected.items()):
                    reported_val = metrics.get(key)
                    if reported_val is not None:
                        expected[key] = max(base, float(reported_val))
                    elif overrun > 1.0:
                        expected[key] = base * overrun
                if expected:
                    self._allocator.set_in_flight(self.name, wf_id, expected)

        return TelemetrySnapshot(
            running_wall_s=running_wall_s,
            node_resources=node_resources,
            spent=self._allocator.spent(self.name),
            running_consumption=running_consumption,
            wf_node=wf_node,
        )

    def _spent_so_far(
        self, inst: _WorkflowInstance, now: float, keys: Iterable[str] | None = None
    ) -> dict[str, float]:
        """A running workflow's spending so far, per dimension of its limit (or ``keys``).

        Best source first, as when it is charged: per-task charges from AsyncFlow
        telemetry (finished tasks plus the elapsed share of running ones), then
        the running totals the workflow reports, then declared_spending prorated
        by elapsed / declared wall time.  A dimension with no source is absent.
        """
        out: dict[str, float] = {}
        task_level = inst.wf_id in self._task_level
        charged = self._task_usage.get(inst.wf_id, {})
        live: dict[str, float] = {}
        if task_level:
            for t in self._active_tasks.values():
                if t.get("wf_id") != inst.wf_id:
                    continue
                start = t.get("start_time")
                if not isinstance(start, (int, float)) or not 0 < start <= now:
                    continue
                hours = (now - start) / 3600.0
                res = t.get("resources") or {}
                for key, field_ in (("cpu_hours", "cpus"), ("gpu_hours", "gpus")):
                    live[key] = live.get(key, 0.0) + float(res.get(field_, 0.0)) * hours
        metrics = inst.last_report.metrics if inst.last_report is not None else None
        metrics = metrics or {}
        decl = inst.declared_spending
        decl_wall_h = decl.get("wall_hours", 0.0)
        elapsed_h = (now - inst.started_at) / 3600.0
        for key in inst.spending_limit if keys is None else keys:
            if task_level and key in ("cpu_hours", "gpu_hours"):
                out[key] = charged.get(key, 0.0) + live.get(key, 0.0)
            elif metrics.get(key) is not None:
                try:
                    out[key] = float(metrics[key])
                except (TypeError, ValueError):
                    pass
            elif decl_wall_h > 0 and key in decl:
                out[key] = decl[key] * elapsed_h / decl_wall_h
        return out

    def _wf_spend(self, now: float) -> dict[str, dict[str, float]]:
        """Spending so far of every running workflow (``obs["wf_spend"]``).

        Dimensions: CPU and GPU hours, the campaign's limits, and the
        workflow's own limit and declared spending; one with no source is absent.
        """
        base = {"cpu_hours", "gpu_hours", *self._allocator.limits(self.name)}
        out: dict[str, dict[str, float]] = {}
        for wf_id, inst in self._active.items():
            keys = base | set(inst.spending_limit) | set(inst.declared_spending)
            keys.discard("wall_hours")
            out[wf_id] = self._spent_so_far(inst, now, keys)
        return out

    def _note_peak_use(self, inst: _WorkflowInstance) -> None:
        """Remember the most CPUs / GPUs the workflow has used at once."""
        for key, val in self._in_use(inst).items():
            if val > inst.peak_use.get(key, 0.0):
                inst.peak_use[key] = val

    def _in_use(self, inst: _WorkflowInstance) -> dict[str, float]:
        """Resources a running workflow uses now: its running tasks' requested
        resources (AsyncFlow telemetry), else what it reports as
        cpus_in_use / gpus_in_use.  A key with neither is absent."""
        out: dict[str, float] = {}
        if inst.wf_id in self._task_level:
            for t in self._active_tasks.values():
                if t.get("wf_id") != inst.wf_id:
                    continue
                res = t.get("resources") or {}
                for key in RESOURCE_KEYS:
                    out[key] = out.get(key, 0.0) + float(res.get(key, 0.0))
            return out
        metrics = inst.last_report.metrics if inst.last_report is not None else None
        for key in RESOURCE_KEYS:
            val = (metrics or {}).get(f"{key}_in_use")
            if val is not None:
                try:
                    out[key] = float(val)
                except (TypeError, ValueError):
                    pass
        return out

    def _apply_auto_cancels(
        self,
        result: PolicyResult,
        live_reports: dict,
        now: float,
    ) -> None:
        """Inject CANCEL decisions for stale reporters, wall-time overruns, runs
        that spent past their own spending limit, and runs using more resources
        at once than they were given.

        Runs after the controller decides so the controller still sees the
        workflows in the observation.  Auto-cancels never override an existing
        CANCEL decision — they only fill in workflows the controller left as NOOP.
        """
        self._auto_cancelled = set()
        for wf_id, inst in list(self._active.items()):
            if result.decisions.get(wf_id) == CampaignDecision.CANCEL:
                continue
            wfc = self._wf_config(inst.workflow_type)

            # Stale reporter — only fires once the workflow has emitted ≥1 report.
            report = live_reports.get(wf_id)
            if report is not None and wfc.stale_timeout_s is not None:
                gap = now - report.emitted_at
                if gap > wfc.stale_timeout_s:
                    logger.warning(
                        f"[WORKFLOW-{wf_id}] stale reporter "
                        f"gap={gap:.1f}s > {wfc.stale_timeout_s}s — auto-cancel"
                    )
                    result.decisions[wf_id] = CampaignDecision.CANCEL
                    self._auto_cancelled.add(wf_id)
                    self._stale_cancels += 1
                    continue

            # Wall-time limit — applies to all workflows.
            if wfc.max_wall_time_s is not None:
                wall = now - inst.started_at
                if wall > wfc.max_wall_time_s:
                    logger.warning(
                        f"[WORKFLOW-{wf_id}] max_wall_time_s exceeded "
                        f"wall={wall:.1f}s > {wfc.max_wall_time_s}s — auto-cancel"
                    )
                    result.decisions[wf_id] = CampaignDecision.CANCEL
                    self._auto_cancelled.add(wf_id)
                    self._walltime_cancels += 1
                    continue

            # Per-run spending limit.
            if inst.spending_limit:
                spent = self._spent_so_far(inst, now)
                over = next(
                    (
                        (k, spent[k], lim)
                        for k, lim in inst.spending_limit.items()
                        if spent.get(k) is not None and spent[k] > lim
                    ),
                    None,
                )
                if over is not None:
                    key, used, lim = over
                    logger.warning(
                        f"[WORKFLOW-{wf_id}] spending limit exceeded "
                        f"{key}={used:.4g} > {lim:.4g} — auto-cancel"
                    )
                    result.decisions[wf_id] = CampaignDecision.CANCEL
                    self._auto_cancelled.add(wf_id)
                    inst.over_budget = True
                    self._budget_cancels += 1
                    continue

            # Resources in use at once.
            if inst.resources:
                used = self._in_use(inst)
                over = next(
                    (
                        (k, used[k], lim)
                        for k, lim in inst.resources.items()
                        if used.get(k) is not None and used[k] > lim + 1e-9
                    ),
                    None,
                )
                if over is not None:
                    key, val, lim = over
                    logger.warning(
                        f"[WORKFLOW-{wf_id}] resources exceeded "
                        f"{key}={val:.4g} > {lim:.4g} — auto-cancel"
                    )
                    result.decisions[wf_id] = CampaignDecision.CANCEL
                    self._auto_cancelled.add(wf_id)
                    inst.over_resources = True
                    self._resource_cancels += 1

    def _observe(self, **kwargs) -> dict:
        """Call the controller's observe(); on an error use the base observe()."""
        ctrl = self.spec.controller
        try:
            return ctrl.observe(**kwargs)
        except Exception as exc:
            self._controller_errors += 1
            logger.warning(
                "[POLICY-%s] observe() raised %s: %s — using the built-in observe this cycle",
                self.name,
                type(exc).__name__,
                exc,
                exc_info=self._controller_errors == 1,
            )
            return CampaignController.observe(ctrl, **kwargs)

    async def _decide(self, obs: dict) -> tuple[PolicyResult, str]:
        """Call the controller's decide(); on an error or timeout use the built-in rules.

        C1: an exception in decide() must not end the policy loop.  With
        ``spec.decide_timeout_s`` set, decide() runs in a thread; if it is not done
        in time, this cycle uses the built-in rules (the controller's own
        ``launch`` / ``LaunchRule`` / ``CancelRule`` and goal, as the base
        ``CampaignController.decide()`` applies them), and later cycles do too
        until the slow call returns.  Returns the result and which decide made it,
        for the [POLICY] log line: "controller" or "fallback(<why>)".
        """
        ctrl = self.spec.controller
        timeout = self.spec.decide_timeout_s
        if self._slow_decide is not None and not self._slow_decide.done():
            why = "busy"
            logger.warning(
                "[POLICY-%s] decide() from an earlier cycle is still running"
                " — using the built-in rules this cycle",
                self.name,
            )
        else:
            self._slow_decide = None
            try:
                if timeout is None:
                    result = ctrl.decide(obs)
                else:
                    fut = _call_in_thread(ctrl.decide, obs)
                    done, _ = await asyncio.wait({fut}, timeout=timeout)
                    if not done:
                        self._slow_decide = fut
                        raise _DecideTimeoutError
                    result = fut.result()
                if not isinstance(result, PolicyResult):
                    raise TypeError(f"decide() returned {type(result).__name__}, not PolicyResult")
                return result, "controller"
            except _DecideTimeoutError:
                why = "timeout"
                self._controller_errors += 1
                logger.warning(
                    "[POLICY-%s] decide() took longer than %g s — using the built-in rules"
                    " until it returns",
                    self.name,
                    timeout,
                )
            except Exception as exc:
                why = "error"
                self._controller_errors += 1
                logger.warning(
                    "[POLICY-%s] decide() raised %s: %s — using the built-in rules this cycle",
                    self.name,
                    type(exc).__name__,
                    exc,
                    exc_info=self._controller_errors == 1,  # traceback once
                )
        try:
            return CampaignController.decide(ctrl, obs), f"fallback({why})"
        except Exception as exc:
            # e.g. a controller that never called CampaignController.__init__().
            logger.warning(
                "[POLICY-%s] built-in rules failed too (%s: %s) — no decisions this cycle",
                self.name,
                type(exc).__name__,
                exc,
            )
            return PolicyResult(), f"fallback({why}, none)"

    async def _run_policy_cycle(self) -> None:
        self._policy_cycles += 1
        now = time.time()
        live_reports = await self._collect_live_reports()
        for inst in self._active.values():
            self._note_peak_use(inst)
        telemetry = self._collect_telemetry()
        self._last_telemetry = telemetry  # cache for _drain_queue
        obs = self._observe(
            queue_snapshot=self._queue.snapshot(),
            budget=self._budget_view(),
            running=self._running_by_type(),
            live_reports=live_reports,
            telemetry=telemetry,
            # wf_types: wf_id → workflow_type; lets controllers identify
            # which workflow type each live report belongs to.
            wf_types={wf_id: inst.workflow_type for wf_id, inst in self._active.items()},
            # learned_cost: workflow_type → mean spending of its successful
            # workflows, per dimension, once 3 have finished (B3).
            learned_cost={
                wt: {k: s["mean"] for k, s in st.items() if s["n"] >= 3}
                for wt, st in self._type_cost_stats.items()
                if any(s["n"] >= 3 for s in st.values())
            },
            # wf_spend: wf_id → spending so far per dimension (best source
            # available); used by CostCancelRule.
            wf_spend=self._wf_spend(now),
            # tasks: AsyncFlow task counts (waiting / submitted / running);
            # None without telemetry.
            tasks=self._task_observation(),
        )
        result, decided_by = await self._decide(obs)
        self._apply_auto_cancels(result, live_reports, now)

        active_count = len(self._active)
        logger.info(
            f"[POLICY-{self.name}] cycle: "
            f"active={active_count} queued={len(self._queue)} "
            f"decisions={len(result.decisions)} "
            f"launches={len(result.launch_requests)} "
            f"terminate={result.terminate} "
            f"decide={decided_by}"
            + (
                f" tasks: waiting={obs['tasks']['waiting']} "
                f"submitted={obs['tasks']['submitted']} running={obs['tasks']['running']}"
                if obs.get("tasks")
                else ""
            )
        )
        bv = self._allocator.campaign_view(self.name)
        spent = bv.get("spent", {})
        in_flight = bv.get("in_flight", {})
        frac = self._allocator.spending_fraction(self.name)
        if spent or in_flight:
            logger.info(
                "[SPEND] campaign=%s  spent_cpu_h=%.4f  spent_gpu_h=%.4f"
                "  in_flight_cpu_h=%.4f  in_flight_gpu_h=%.4f  budget_used=%.1f%%",
                self.name,
                spent.get("cpu_hours", 0.0),
                spent.get("gpu_hours", 0.0),
                in_flight.get("cpu_hours", 0.0),
                in_flight.get("gpu_hours", 0.0),
                frac * 100,
            )

        for wf_id, decision in result.decisions.items():
            inst = self._active.get(wf_id)
            if inst is None or inst.future.done():
                continue
            if decision == CampaignDecision.CANCEL:
                # Auto-cancels (stale, wall time, spending limit) have their own counters.
                auto = wf_id in self._auto_cancelled
                logger.info(f"[WORKFLOW-{wf_id}] CANCEL ({'auto' if auto else 'policy'})")
                inst.future.cancel()
                if not auto:
                    self._policy_cancels += 1
            elif decision in (CampaignDecision.PAUSE, CampaignDecision.RESUME):
                wfc = self._wf_config(inst.workflow_type)
                if not wfc.supports_pause:
                    logger.warning(
                        f"[WORKFLOW-{wf_id}] {decision.value} ignored — "
                        f"{inst.workflow_type!r} has supports_pause=False"
                    )
                else:
                    await self.spec.data_backend.write_signal(wf_id, decision)
                    logger.info(f"[WORKFLOW-{wf_id}] {decision.value} signal sent")

        if result.cancel_queued:
            for wf_type, count in result.cancel_queued.items():
                removed = self._queue.remove_by_type(wf_type, count)
                if removed:
                    logger.info(f"[POLICY-{self.name}] evicted {removed} queued {wf_type}")

        for req in result.launch_requests:
            if len(self._queue) >= self.spec.max_queue_depth:
                logger.warning(
                    f"[POLICY-{self.name}] queue depth {len(self._queue)} at max_queue_depth "
                    f"({self.spec.max_queue_depth}); enqueuing {req.workflow_type} anyway"
                )
            self.enqueue(req)

        if result.goals:
            by_name = {g.name: g for g in self._goals}
            for g in result.goals:
                by_name[g.name] = g
            self._goals = list(by_name.values())
        if result.remove_goals:
            drop = set(result.remove_goals)
            self._goals = [g for g in self._goals if g.name not in drop]

        # Write checkpoint after decisions are applied, before potentially
        # shutting down — so the checkpoint reflects the post-cycle state.
        # Build the snapshot in the event loop (thread-safe), then write to
        # disk in a thread so the I/O doesn't stall the policy cycle.
        if self.spec.checkpoint_dir is not None:
            _ckpt_snapshot = self._build_checkpoint()
            _ckpt_p = self._checkpoint_path()
            asyncio.ensure_future(
                asyncio.get_event_loop().run_in_executor(
                    None, write_checkpoint, _ckpt_p, _ckpt_snapshot
                )
            )

        if result.terminate:
            logger.info(f"[CM] {self.name}: controller requested terminate")
            await self._shutdown(reason="controller_terminate")
            return

        if self._goals and self._check_goals(obs):
            logger.info(f"[CM] {self.name}: all goals satisfied")
            await self._shutdown(reason="goals_satisfied")

    def _check_goals(self, obs: dict) -> bool:
        for goal in self._goals:
            value = obs.get(goal.metric)
            if value is None:
                return False
            if not goal.satisfied(value):
                return False
        return True

    # ── Shutdown ─────────────────────────────────────────────────────────────

    async def _shutdown(self, reason: str = "external") -> None:
        if self._done:
            return
        self._done = True
        self._exit_reason = reason
        self._slot_event.set()  # wake scheduler so it exits promptly

        # Purge the launch queue immediately — no new workflows should start
        # after termination, and LaunchRules may have pre-filled it.
        dropped = self._queue.clear()
        if dropped:
            logger.info(f"[CM] {self.name}: cleared {dropped} queued request(s) on shutdown")

        timeout = self.spec.shutdown_timeout_s

        logger.info(f"[CM] {self.name}: graceful shutdown ({len(self._active)} active instances)")

        for inst in list(self._active.values()):
            if not inst.future.done():
                inst.future.cancel()

        if self._active:
            futs = [inst.future for inst in list(self._active.values())]
            try:
                await asyncio.wait_for(
                    asyncio.gather(*futs, return_exceptions=True),
                    timeout=timeout,
                )
            except TimeoutError:
                logger.warning(f"[CM] {self.name}: instances did not exit within {timeout}s")
            # asyncio schedules done-callbacks via call_soon; yield once so they
            # fire before _save_checkpoint() reads the counters they update.
            await asyncio.sleep(0)

        # The campaign stays open in the ledger until CampaignManager closes it
        # (after building the CampaignOutcome); running estimates are dropped.
        for wf_id in list(self._active):
            self._allocator.clear_in_flight(self.name, wf_id)

        # Final checkpoint — written synchronously so it's always on disk before
        # the summary log (which is the last thing the caller sees).
        self._save_checkpoint()

        logger.info(
            f"[CM] Campaign {self.name} SUMMARY — "
            f"exit={self._exit_reason} "
            f"run={self._instances_run} "
            f"ok={self._completed} "
            f"cancelled={self._cancelled} "
            f"(stale={self._stale_cancels} "
            f"walltime={self._walltime_cancels} "
            f"budget={self._budget_cancels} "
            f"resources={self._resource_cancels} "
            f"policy={self._policy_cancels}) "
            f"failed={self._failed} "
            f"cycles={self._policy_cycles} "
            f"controller_errors={self._controller_errors} "
            f"spent={self._allocator.spent(self.name)}"
        )

    # ── Entry point ──────────────────────────────────────────────────────────

    async def run(self) -> None:
        logger.info(f"[CM] Campaign {self.name}: starting")

        # Task-level accounting needs AsyncFlow telemetry (enable_telemetry=True).
        telemetry = getattr(self._flow, "_telemetry", None)
        if telemetry is not None and hasattr(telemetry, "subscribe"):
            telemetry.subscribe(self._on_task_event)
            self._tracking_tasks = True

        # Attempt checkpoint recovery before any workflows are launched.
        restored = False
        ckpt_path = self._checkpoint_path()
        if ckpt_path is not None:
            ckpt = read_checkpoint(ckpt_path)
            if ckpt is not None:
                self._restore_from_checkpoint(ckpt)
                restored = True

        # Initial requests — not after a restore: the restored queue and the
        # re-queued running workflows already stand for them.
        now = time.time()
        for wfc in [] if restored else self.spec.workflows:
            if wfc.launch_on_start and wfc.name in self._dep_satisfied:
                self.enqueue(
                    LaunchRequest(
                        workflow_type=wfc.name,
                        priority=LaunchPriority.NORMAL,
                        enqueued_at=now,
                        context={},
                        requester="cm:init",
                    )
                )

        sched_task = asyncio.create_task(self._scheduler_loop(), name=f"sched-{self.name}")
        policy_task = asyncio.create_task(self._policy_loop(), name=f"policy-{self.name}")

        reason = "external"
        try:
            await asyncio.gather(sched_task, policy_task)
        except asyncio.CancelledError:
            # CampaignManager cancels the campaign on SIGTERM / SIGINT: shut it
            # down gracefully (cancel workflows, wait up to shutdown_timeout_s,
            # final checkpoint, summary) before the cancellation propagates.
            reason = "signal"
            raise
        finally:
            sched_task.cancel()
            policy_task.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.gather(sched_task, policy_task, return_exceptions=True)
            if not self._done:
                await self._shutdown(reason=reason)

    @property
    def exit_reason(self) -> str:
        """Why the campaign ended (goals_satisfied, controller_terminate, ...)."""
        return self._exit_reason

    @property
    def budget_cancels(self) -> int:
        """Runs AuCaM cancelled for spending past their own spending limit."""
        return self._budget_cancels

    @property
    def resource_cancels(self) -> int:
        """Runs AuCaM cancelled for using more resources at once than they were given."""
        return self._resource_cancels

    @property
    def controller_errors(self) -> int:
        """Policy cycles where decide() or observe() failed or timed out."""
        return self._controller_errors

    @property
    def unfit_dropped(self) -> int:
        """Launch requests dropped because their minimum did not fit the budget."""
        return self._unfit_dropped
