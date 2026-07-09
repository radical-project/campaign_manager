"""TelemetrySubscriber — feeds asyncflow telemetry events into ADR observations.

Subscribes to the rhapsody TelemetryManager event stream and maintains
exponentially-weighted averages of node/GPU resource utilisation plus running
task latency and failure statistics.  The resulting ``snapshot()`` dict is
merged into ``CampaignView.observe()`` so ADR policies can make telemetry-aware
scheduling decisions on real HPC hardware.

Usage::

    from src.campaign.adr.telemetry import TelemetrySubscriber

    # telemetry = await asyncflow.start_telemetry(...)  (or None)
    subscriber = TelemetrySubscriber(telemetry)

    view = CampaignView(cm, telemetry_subscriber=subscriber)

When ``telemetry`` is ``None`` (e.g. opentelemetry SDK not installed, or
concurrent backend without resource polling), ``snapshot()`` returns all-zero
values and the CampaignView observation is unchanged — the ADR policy still
works, just without real-hardware utilisation signals.

ResourceUpdate scope notes
--------------------------
  per_node events   → cpu_percent, memory_percent, gpu_percent (node aggregate)
  per_gpu events    → gpu_percent + gpu_id only; cpu/mem are None

Task duration tracking
----------------------
  TaskCompleted.duration_seconds  → rolling window (last 200 tasks, EWA median)
  TaskFailed                      → increments fail counter

All EWA smoothing uses alpha (default 0.3); lower = slower to react / smoother.
"""

from __future__ import annotations

from collections import deque


class TelemetrySubscriber:
    """Subscribe to a TelemetryManager and aggregate resource/task metrics.

    Parameters
    ----------
    telemetry:
        The TelemetryManager returned by ``asyncflow.start_telemetry()``, or
        ``None``.  When None the subscriber is a no-op: ``snapshot()`` returns
        zeros and ``CampaignView.observe()`` is unaffected.
    alpha:
        EWA smoothing factor (0 < alpha ≤ 1).  Higher = faster response to
        new samples; lower = smoother but slower.  Default 0.3.
    window:
        Number of recent task durations to keep for the rolling average.
    """

    def __init__(self, telemetry=None, *, alpha: float = 0.3, window: int = 200) -> None:
        self._alpha = alpha
        # EWA-smoothed node-level metrics
        self._gpu_util: float = 0.0
        self._cpu_util: float = 0.0
        self._mem_util: float = 0.0
        # Per-GPU latest readings (gpu_id → util%)
        self._per_gpu: dict[int, float] = {}
        # Task statistics
        self._task_durations: deque[float] = deque(maxlen=window)
        self._task_fails: int = 0
        self._task_completes: int = 0

        if telemetry is not None:
            telemetry.subscribe(self._on_event)

    # ── Event handler ──────────────────────────────────────────────────────

    def _on_event(self, event) -> None:
        et = getattr(event, "event_type", None)
        if et == "ResourceUpdate":
            self._handle_resource(event)
        elif et == "TaskCompleted":
            self._task_completes += 1
            dur = getattr(event, "duration_seconds", 0.0) or 0.0
            if dur > 0.0:
                self._task_durations.append(dur)
        elif et == "TaskFailed":
            self._task_fails += 1

    def _handle_resource(self, event) -> None:
        scope = getattr(event, "resource_scope", "")
        a = self._alpha
        if scope == "per_gpu":
            gpu_id = event.gpu_id
            pct = event.gpu_percent or 0.0
            self._per_gpu[gpu_id] = pct
            # Re-compute EWA of average across all known GPUs
            avg = sum(self._per_gpu.values()) / len(self._per_gpu)
            self._gpu_util = a * avg + (1 - a) * self._gpu_util
        elif scope == "per_node":
            if event.cpu_percent is not None:
                self._cpu_util = a * event.cpu_percent + (1 - a) * self._cpu_util
            if event.memory_percent is not None:
                self._mem_util = a * event.memory_percent + (1 - a) * self._mem_util
            # Node-level GPU aggregate (max across devices) — use when per_gpu
            # events are absent (e.g. single-GPU node or older backend).
            if not self._per_gpu and event.gpu_percent is not None:
                self._gpu_util = a * event.gpu_percent + (1 - a) * self._gpu_util

    # ── Snapshot (merged into CampaignView.observe()) ──────────────────────

    def snapshot(self) -> dict:
        """Return a dict of telemetry-derived fields for the ADR observation.

        Fields
        ------
        gpu_util              : float  — EWA GPU utilisation % averaged across GPUs (0–100)
        cpu_util              : float  — EWA CPU utilisation % (node aggregate, 0–100)
        mem_util              : float  — EWA memory utilisation % (0–100)
        gpu_utils_per_device  : dict   — {gpu_id: latest_gpu_pct} (empty before first poll)
        task_fail_rate        : float  — fraction of tasks that failed (0–1)
        avg_task_duration_s   : float | None — rolling mean of completed task durations
        """
        total = self._task_completes + self._task_fails
        fail_rate = self._task_fails / total if total > 0 else 0.0
        durs = list(self._task_durations)
        avg_dur: float | None = sum(durs) / len(durs) if durs else None
        return {
            "gpu_util": self._gpu_util,
            "cpu_util": self._cpu_util,
            "mem_util": self._mem_util,
            "gpu_utils_per_device": dict(self._per_gpu),
            "task_fail_rate": fail_rate,
            "avg_task_duration_s": avg_dur,
        }
