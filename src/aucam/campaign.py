"""_CampaignInstance: manages the full lifecycle of one campaign."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import shlex
import time
import uuid
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
from .controller import Goal, PolicyResult
from .queue import WorkflowQueue
from .scheduler import BanditSchedulingPolicy, SchedulingContext, SchedulingPolicy
from .spec import CampaignSpec, WorkflowConfig
from .types import (
    CampaignDecision,
    LaunchPriority,
    LaunchRequest,
    NodeResources,
    TelemetrySnapshot,
    WorkflowResult,
    WorkflowStatus,
)

logger = logging.getLogger(__name__)


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
    )

    def __init__(
        self,
        wf_id: str,
        workflow_type: str,
        future: asyncio.Future,
        started_at: float,
        declared_spending: dict[str, float],
        request: LaunchRequest,
    ) -> None:
        self.wf_id = wf_id
        self.workflow_type = workflow_type
        self.future = future
        self.started_at = started_at
        self.declared_spending = declared_spending  # in-flight spending estimate
        self.request = request
        self.last_report: Any = None


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
        # Spending is kept in the shared BudgetAllocator ledger (per campaign).
        self._allocator.open(self.name, spec.budget.spending)

        # Observability counters — surfaced in summary log at shutdown.
        self._instances_run = 0
        self._completed = 0
        self._cancelled = 0
        self._failed = 0
        self._stale_cancels = 0
        self._walltime_cancels = 0
        self._policy_cancels = 0  # CANCEL from controller (not auto-cancel)
        self._policy_cycles = 0
        self._exit_reason = "unknown"

        # Recent workflow results for SchedulingContext.history (newest last, capped).
        self._history: list[WorkflowResult] = []
        self._history_max: int = 50

        # Per-type observed cost stats for learned in-flight estimates (B3).
        # {workflow_type: {dim: {"n": int, "mean": float}}}
        self._type_cost_stats: dict[str, dict[str, dict]] = {}

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
            if etype in ("TaskStarted", "TaskCompleted", "TaskFailed"):
                logger.debug(
                    "[SPEND-DBG] %s filtered: res=%s tag=%s",
                    etype,
                    bool(res),
                    bool(tag),
                )
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
                call_kwargs["context"] = dict(context or {})

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
        command = self._wrap_cmd(wfc.launch_cmd, self._cmd_env(wfc, wf_id, context))
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

    def _cmd_env(self, wfc: WorkflowConfig, wf_id: str, context: dict | None) -> dict[str, str]:
        """Environment for a command-line workflow's process (wrapper script)."""
        env = {"CM_WORKFLOW_ID": wf_id, "CM_CAMPAIGN_ID": self.name, **wfc.env}
        # Per-instance context from LaunchRequest is surfaced as CM_CTX_{KEY}.
        for k, v in (context or {}).items():
            env[f"CM_CTX_{k.upper()}"] = str(v)
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

    def _launch(self, request: LaunchRequest) -> None:
        wfc = self._wf_config(request.workflow_type)
        wf_id = f"{self.name}.{request.workflow_type}.{uuid.uuid4().hex[:8]}"

        logger.info(
            f"[SCHEDULER-{self.name}] LAUNCH {request.workflow_type} "
            f"id={wf_id} requester={request.requester}"
        )

        self._wf_tags[wf_id] = wf_id
        future = self._make_future(wfc, wf_id, request.context)
        inst = _WorkflowInstance(
            wf_id=wf_id,
            workflow_type=request.workflow_type,
            future=future,
            started_at=time.time(),
            declared_spending=dict(wfc.declared_spending),
            request=request,
        )
        self._active[wf_id] = inst
        # Use learned cost estimate for in-flight tracking once enough completions
        # exist; falls back to declared_spending before min_samples threshold (B3).
        in_flight_est = self._learned_cost(request.workflow_type, inst.declared_spending)
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
            # For remote backends (Dragon, Redis) fall back to what the policy
            # cycle cached in _inst.last_report from _collect_live_reports.
            # For remote backends (Dragon, Redis) use _inst.last_report, which
            # _read_final_report refreshed when the workflow's block ended.
            backend = self.spec.data_backend
            last_report = _inst.last_report
            if hasattr(backend, "sync_get_report"):
                last_report = backend.sync_get_report(_inst.wf_id) or last_report
            final_metrics: dict = dict(last_report.metrics or {}) if last_report is not None else {}
            final_score = last_report.score if last_report is not None else None

            self._completion_event.set()

            self._active.pop(_inst.wf_id, None)
            self._exit_codes.pop(_inst.wf_id, None)
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
            )

            if self.spec.data_backend is not None:
                asyncio.ensure_future(self.spec.data_backend.delete_report(_inst.wf_id))

            # Update scheduler policy, controller counters, and history.
            self._policy.on_outcome(_inst.request, result)
            self.spec.controller.on_complete(result)
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
            except asyncio.TimeoutError:
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

        to_launch: list[LaunchRequest] = []

        # Running instances per type, including requests picked this round;
        # the per-type limit is enforced here for every request, whatever the
        # policy returns.
        per_type_cap = self.spec.budget.max_concurrency_per_workflow
        counts = self._running_by_type()

        def _take(req: LaunchRequest) -> bool:
            if len(self._active) + len(to_launch) >= max_concurrent:
                return False
            if counts.get(req.workflow_type, 0) >= per_type_cap:
                return True  # this type is full; others may still fit
            to_launch.append(req)
            counts[req.workflow_type] = counts.get(req.workflow_type, 0) + 1
            return True

        # CRITICAL requests always go first, no policy consultation.
        for req in critical:
            if not _take(req):
                break

        # Policy selects from normal candidates for remaining slots.
        if normal and len(self._active) + len(to_launch) < max_concurrent:
            running = self._running_by_type()
            # Add slots already committed to CRITICAL launches so the policy
            # sees accurate counts.
            for r in to_launch:
                running[r.workflow_type] = running.get(r.workflow_type, 0) + 1

            ctx = SchedulingContext(
                running_by_type=running,
                max_concurrency=max_concurrent,
                max_concurrency_per_type=self.spec.budget.max_concurrency_per_workflow,
                telemetry=self._last_telemetry,
                history=list(self._history),
                policy_cycles=self._policy_cycles,
                campaign_name=self.name,
                budget_remaining_fraction=1.0 - self._allocator.spending_fraction(self.name),
            )
            selected = self._policy.select(normal, ctx)
            for req in selected:
                if not _take(req):
                    break

        # Remove from queue and launch.
        for req in to_launch:
            self._queue.remove_request(req)
            self._launch(req)

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
            except asyncio.TimeoutError:
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

    def _apply_auto_cancels(
        self,
        result: PolicyResult,
        live_reports: dict,
        now: float,
    ) -> None:
        """Inject CANCEL decisions for stale reporters and wall-time overruns.

        Runs after the controller decides so the controller still sees the
        workflows in the observation.  Auto-cancels never override an existing
        CANCEL decision — they only fill in workflows the controller left as NOOP.
        """
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
                    self._walltime_cancels += 1

    async def _run_policy_cycle(self) -> None:
        self._policy_cycles += 1
        now = time.time()
        live_reports = await self._collect_live_reports()
        telemetry = self._collect_telemetry()
        self._last_telemetry = telemetry  # cache for _drain_queue
        obs = self.spec.controller.observe(
            queue_snapshot=self._queue.snapshot(),
            budget=self._allocator.campaign_view(self.name),
            running=self._running_by_type(),
            live_reports=live_reports,
            telemetry=telemetry,
            # wf_types: wf_id → workflow_type; lets controllers identify
            # which workflow type each live report belongs to.
            wf_types={wf_id: inst.workflow_type for wf_id, inst in self._active.items()},
        )
        result: PolicyResult = self.spec.controller.decide(obs)
        self._apply_auto_cancels(result, live_reports, now)

        active_count = len(self._active)
        logger.info(
            f"[POLICY-{self.name}] cycle: "
            f"active={active_count} queued={len(self._queue)} "
            f"decisions={len(result.decisions)} "
            f"launches={len(result.launch_requests)} "
            f"terminate={result.terminate}"
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
                logger.info(f"[WORKFLOW-{wf_id}] CANCEL (policy)")
                inst.future.cancel()
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
            except asyncio.TimeoutError:
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
            f"policy={self._policy_cancels}) "
            f"failed={self._failed} "
            f"cycles={self._policy_cycles} "
            f"spent={self._allocator.spent(self.name)}"
        )

    # ── Entry point ──────────────────────────────────────────────────────────

    async def run(self) -> None:
        logger.info(f"[CM] Campaign {self.name}: starting")

        # Task-level accounting needs AsyncFlow telemetry (enable_telemetry=True).
        telemetry = getattr(self._flow, "_telemetry", None)
        if telemetry is not None and hasattr(telemetry, "subscribe"):
            telemetry.subscribe(self._on_task_event)

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
