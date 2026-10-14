"""BlockProfiler and TaskProfiler: lightweight CPU/memory trackers for campaign workflows.

Two variants share the same sampling core:
  BlockProfiler — async context manager for block functions (head-node asyncio).
  TaskProfiler  — sync context manager for function tasks (Dragon worker nodes).

Both expose a `.metrics` dict after exit with cpu_percent_avg/peak,
memory_percent_avg/peak, duration_s, node_id, and start/end times.

Usage — block function (head node):

    async def sim_worker(reporter, flow=None):
        async with BlockProfiler() as prof:
            result = await actual_work()
        await reporter.emit(
            score=result.score,
            metrics={"resource_usage": prof.metrics},
        )

Usage — function task (Dragon worker node):

    @flow.function_task
    def compute(args):
        with TaskProfiler() as prof:
            result = heavy_computation(args)
        return {"output": result, "resource_usage": prof.metrics}

    # gather it in the block and forward to reporter:
    task_result = await compute(args)
    await reporter.emit(
        score=task_result["output"].score,
        metrics={"resource_usage": task_result["resource_usage"]},
    )
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any


def _sample_cpu_mem() -> tuple[float | None, float | None]:
    try:
        import psutil  # noqa: PLC0415

        return psutil.cpu_percent(), psutil.virtual_memory().percent
    except Exception:
        return None, None


class _ProfilerBase:
    """Shared background-sampling logic."""

    def __init__(self, interval: float = 2.0) -> None:
        self._interval = interval
        self._samples: list[dict[str, float]] = []
        self._start: float | None = None
        self._end: float | None = None
        self._node_id: str = os.uname().nodename
        self._stop_flag = threading.Event()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Internals

    def _start_sampling(self) -> None:
        self._start = time.time()
        self._stop_flag.clear()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def _stop_sampling(self) -> None:
        self._stop_flag.set()
        if self._thread is not None:
            self._thread.join(timeout=max(self._interval * 2, 5.0))
        self._end = time.time()

    def _sample_loop(self) -> None:
        while not self._stop_flag.wait(self._interval):
            cpu, mem = _sample_cpu_mem()
            if cpu is not None:
                self._samples.append({"t": time.time(), "cpu": cpu, "mem": mem or 0.0})

    # ------------------------------------------------------------------
    # Public

    @property
    def metrics(self) -> dict[str, Any]:
        """Resource metrics dict, populated after the context exits."""
        if self._start is None:
            return {}
        end = self._end or time.time()
        duration = end - self._start

        base = {
            "node_id": self._node_id,
            "start_time": self._start,
            "end_time": end,
            "duration_s": duration,
        }

        if self._samples:
            cpus = [s["cpu"] for s in self._samples]
            mems = [s["mem"] for s in self._samples]
            base.update(
                cpu_percent_avg=sum(cpus) / len(cpus),
                cpu_percent_peak=max(cpus),
                memory_percent_avg=sum(mems) / len(mems),
                memory_percent_peak=max(mems),
                n_samples=len(self._samples),
            )
        else:
            # Block completed before the first sampling tick — one instant reading.
            cpu, mem = _sample_cpu_mem()
            base.update(
                cpu_percent_avg=cpu,
                cpu_percent_peak=cpu,
                memory_percent_avg=mem,
                memory_percent_peak=mem,
                n_samples=0,
            )

        return base


class BlockProfiler(_ProfilerBase):
    """Async context manager — use inside block functions on the head node."""

    async def __aenter__(self) -> "BlockProfiler":
        self._start_sampling()
        return self

    async def __aexit__(self, *_: Any) -> None:
        self._stop_sampling()


class TaskProfiler(_ProfilerBase):
    """Sync context manager — use inside function tasks on Dragon worker nodes."""

    def __enter__(self) -> "TaskProfiler":
        self._start_sampling()
        return self

    def __exit__(self, *_: Any) -> None:
        self._stop_sampling()
