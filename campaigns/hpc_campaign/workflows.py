"""
Dummy campaign workflows.

Three workflow types:
  sim_worker      — synthetic simulation; ~65% convergence, N_ITERS steps.
                    Low score (< SCORE_FLOOR) → cancelled by controller.
  sim_worker_b    — faster, more reliable sim; ~90% convergence, N_ITERS_B
                    (≈ N_ITERS/3) steps.  The bandit should learn to prefer
                    this arm's higher throughput over sim_worker.
  analysis_worker — triggered by a successful sim (either type); performs a
                    short post-processing step.

When `flow` is provided (i.e. running inside an asyncflow.block), each
workflow registers internal @flow.task nodes so the block genuinely contains
a DAG — exercising member-task tracking, cascade cancellation, and per-task
telemetry.  When `flow=None` (plain asyncio path), both functions fall back
to sequential asyncio.sleep with no asyncflow dependency.
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
import socket
import threading
import time

import psutil

from src.aucam import BlockProfiler, TaskProfiler, WorkflowReporter

logger = logging.getLogger(__name__)

try:
    import pynvml as _nvml

    _nvml.nvmlInit()
    _N_GPUS = _nvml.nvmlDeviceGetCount()
    _GPU_AVAILABLE = _N_GPUS > 0
except Exception:
    _GPU_AVAILABLE = False
    _N_GPUS = 0


SCORE_FLOOR = 0.4  # simulations scoring below this are cancelled by the controller
N_ITERS = 10  # number of report iterations per simulation run
ITER_SLEEP_S = 0.08  # sleep per sim iteration (increase for longer blocks)
ANALYSIS_STEPS = 2  # number of steps in analysis_worker
N_ITERS_B = max(3, N_ITERS // 3)  # sim_worker_b: faster, fewer iters (auto-derived)
# Whether each workflow type stays within the spending limit AuCaM gives it
# (campaign_overuse.yaml turns this off for one type to show AuCaM cancelling it).
RESPECT_LIMIT = {"sim_worker": True, "sim_worker_b": True, "analysis_worker": True}


class RunBudget:
    """This run's spending limit from AuCaM, and its spending so far.

    Spending is counted as one CPU for the run's wall time, reported to AuCaM as
    a running ``cpu_hours`` total.  A run that respects its limit stops before a
    step that would take it past the limit.
    """

    def __init__(self, context: dict | None, name: str, cpus: float = 1.0) -> None:
        lim = (context or {}).get("spending_limit", {}).get("cpu_hours")
        self.limit = float(lim) if lim is not None else math.inf
        self.respect = RESPECT_LIMIT.get(name, True)
        self.name, self.cpus, self.t0 = name, cpus, time.monotonic()
        if lim is not None:
            logger.info(
                "[%s] spending limit %.6f cpu_hours%s",
                name,
                self.limit,
                "" if self.respect else " (ignored)",
            )

    def spent(self) -> float:
        return self.cpus * (time.monotonic() - self.t0) / 3600.0

    def stop_before(self, step_s: float) -> bool:
        """True if a step of ``step_s`` seconds would take this run past its limit."""
        if not self.respect or self.spent() + self.cpus * step_s / 3600.0 <= self.limit:
            return False
        logger.info("[%s] stopping early to stay within its spending limit", self.name)
        return True


def _sample_gpu_util() -> float | None:
    """Return mean GPU utilisation % across all GPUs, or None if unavailable."""
    if not _GPU_AVAILABLE:
        return None
    try:
        utils = []
        for i in range(_N_GPUS):
            h = _nvml.nvmlDeviceGetHandleByIndex(i)
            utils.append(float(_nvml.nvmlDeviceGetUtilizationRates(h).gpu))
        return sum(utils) / len(utils) if utils else None
    except Exception:
        return None


def configure(
    *,
    n_iters: int | None = None,
    iter_sleep_s: float | None = None,
    analysis_steps: int | None = None,
    respect_limit: dict[str, bool] | None = None,
) -> None:
    """Override timing constants before the campaign starts.

    Called from run_campaign.py when --n-iters / --iter-sleep flags are set.
    N_ITERS_B is auto-derived as N_ITERS // 3 whenever N_ITERS changes.
    """
    global N_ITERS, ITER_SLEEP_S, ANALYSIS_STEPS, N_ITERS_B
    if n_iters is not None:
        N_ITERS = n_iters
        N_ITERS_B = max(3, N_ITERS // 3)
    if iter_sleep_s is not None:
        ITER_SLEEP_S = iter_sleep_s
    if analysis_steps is not None:
        ANALYSIS_STEPS = analysis_steps
    if respect_limit:
        RESPECT_LIMIT.update(respect_limit)


async def sim_worker(reporter: WorkflowReporter, flow=None, context: dict | None = None) -> None:
    """Synthetic simulation — emits a score each step.

    Score drifts up or down depending on a random initial direction.
    Runs that drift low are cancelled by the ScoreController.

    With `flow`: three asyncflow tasks (init → simulate → report) form a
    linear DAG inside the block, so member-task tracking and cascade
    cancellation are exercised properly.

    BlockProfiler wraps the entire block to measure head-node overhead.
    TaskProfiler wraps _simulate to capture worker-node CPU/memory.
    """
    converging = random.random() > 0.35  # ~65 % converging
    budget = RunBudget(context, "sim_worker")

    async with BlockProfiler() as block_prof:
        if flow is not None:
            # ── asyncflow path: _init + batched _simulate_batch loop ──────────
            # Scores return through asyncflow's normal value channel (not DDict).
            # The head node emits between batches so the CM can cancel diverging
            # runs after each batch completes — no DDict.attach() from workers.

            @flow.function_task
            async def _init():
                await asyncio.sleep(0.02)
                return {"converging": converging, "score": 0.5}

            @flow.function_task
            async def _simulate_batch(state, batch_iters, iter_sleep_s):
                # Runs batch_iters steps of the random walk.
                # n_iters and iter_sleep_s are passed explicitly — Dragon workers
                # are separate processes and don't see driver-process globals.
                score = state["score"]
                c = state["converging"]
                for _ in range(batch_iters):
                    await asyncio.sleep(iter_sleep_s)
                    score += random.gauss(0.05 if c else -0.08, 0.05)
                    score = max(0.0, min(1.0, score))
                return {"score": score, "converging": c}

            batch_size = max(1, N_ITERS // 10)
            n_batches = (N_ITERS + batch_size - 1) // batch_size

            state = await _init()
            for batch_num in range(n_batches):
                if budget.stop_before(batch_size * ITER_SLEEP_S):
                    break
                state = await _simulate_batch(state, batch_size, ITER_SLEEP_S)
                # Head-node emit: goes to RadexDragonBackend → DDict (HPC) or
                # LocalDictBackend (local).  CM reads on its next policy cycle.
                await reporter.emit(
                    score=state["score"],
                    metrics={
                        "cpu_hours": budget.spent(),
                        "batch": batch_num,
                        "converging": state["converging"],
                        "hostname": socket.gethostname(),
                    },
                    iteration=(batch_num + 1) * batch_size,
                )

            # Final emit with block-level profiling metrics.
            await reporter.emit(
                score=state["score"],
                metrics={"cpu_hours": budget.spent(), "resource_usage": block_prof.metrics},
                iteration=N_ITERS + 1,
            )

        else:
            # ── pure asyncio fallback (no asyncflow dependency) ───────────────
            score = 0.5
            _t0_cpu = psutil.Process().cpu_times()
            for i in range(N_ITERS):
                if budget.stop_before(ITER_SLEEP_S):
                    break
                await asyncio.sleep(ITER_SLEEP_S)
                score += random.gauss(0.05 if converging else -0.08, 0.05)
                score = max(0.0, min(1.0, score))
                t_cpu = psutil.Process().cpu_times()
                cpu_s = (t_cpu.user + t_cpu.system) - (_t0_cpu.user + _t0_cpu.system)
                await reporter.emit(
                    score=score,
                    metrics={
                        "cpu_hours": budget.spent(),
                        "iteration": i,
                        "converging": converging,
                        "score": score,
                        "cpu_seconds": cpu_s,
                    },
                    iteration=i,
                )
            t_cpu = psutil.Process().cpu_times()
            cpu_s = (t_cpu.user + t_cpu.system) - (_t0_cpu.user + _t0_cpu.system)
            await reporter.emit(
                score=score,
                metrics={
                    "cpu_hours": budget.spent(),
                    "resource_usage": block_prof.metrics,
                    "cpu_seconds": cpu_s,
                },
                iteration=N_ITERS,
            )


async def sim_worker_b(reporter: WorkflowReporter, flow=None, context: dict | None = None) -> None:
    """Faster, more reliable simulation — 90% convergence rate, N_ITERS_B steps.

    Identical structure to sim_worker; differs only in diverge probability
    (10% vs 35%) and iteration count (N_ITERS_B ≈ N_ITERS/3).
    The bandit should learn to prefer this arm's higher throughput.
    """
    converging = random.random() > 0.10  # ~90 % converging
    budget = RunBudget(context, "sim_worker_b")

    async with BlockProfiler() as block_prof:
        if flow is not None:

            @flow.function_task
            async def _init_b():
                await asyncio.sleep(0.02)
                return {"converging": converging, "score": 0.5}

            @flow.function_task
            async def _simulate_batch_b(state, batch_iters, iter_sleep_s):
                score = state["score"]
                c = state["converging"]
                for _ in range(batch_iters):
                    await asyncio.sleep(iter_sleep_s)
                    score += random.gauss(0.05 if c else -0.08, 0.05)
                    score = max(0.0, min(1.0, score))
                return {"score": score, "converging": c}

            batch_size = max(1, N_ITERS_B // 5)
            n_batches = (N_ITERS_B + batch_size - 1) // batch_size

            state = await _init_b()
            for batch_num in range(n_batches):
                if budget.stop_before(batch_size * ITER_SLEEP_S):
                    break
                state = await _simulate_batch_b(state, batch_size, ITER_SLEEP_S)
                await reporter.emit(
                    score=state["score"],
                    metrics={
                        "cpu_hours": budget.spent(),
                        "batch": batch_num,
                        "converging": state["converging"],
                        "hostname": socket.gethostname(),
                    },
                    iteration=(batch_num + 1) * batch_size,
                )

            await reporter.emit(
                score=state["score"],
                metrics={"cpu_hours": budget.spent(), "resource_usage": block_prof.metrics},
                iteration=N_ITERS_B + 1,
            )

        else:
            score = 0.5
            _t0_cpu = psutil.Process().cpu_times()
            for i in range(N_ITERS_B):
                if budget.stop_before(ITER_SLEEP_S):
                    break
                await asyncio.sleep(ITER_SLEEP_S)
                score += random.gauss(0.05 if converging else -0.08, 0.05)
                score = max(0.0, min(1.0, score))
                t_cpu = psutil.Process().cpu_times()
                cpu_s = (t_cpu.user + t_cpu.system) - (_t0_cpu.user + _t0_cpu.system)
                await reporter.emit(
                    score=score,
                    metrics={
                        "cpu_hours": budget.spent(),
                        "iteration": i,
                        "converging": converging,
                        "score": score,
                        "cpu_seconds": cpu_s,
                    },
                    iteration=i,
                )
            t_cpu = psutil.Process().cpu_times()
            cpu_s = (t_cpu.user + t_cpu.system) - (_t0_cpu.user + _t0_cpu.system)
            await reporter.emit(
                score=score,
                metrics={
                    "cpu_hours": budget.spent(),
                    "resource_usage": block_prof.metrics,
                    "cpu_seconds": cpu_s,
                },
                iteration=N_ITERS_B,
            )


async def analysis_worker(
    reporter: WorkflowReporter, flow=None, context: dict | None = None
) -> None:
    """Post-processing step triggered after each successful simulation.

    With `flow`: two asyncflow tasks (_process → _summarise) form a
    two-node linear DAG inside the block.

    BlockProfiler wraps the entire block; for analysis work that runs locally
    on the head node this captures the actual compute cost.
    """
    budget = RunBudget(context, "analysis_worker")
    async with BlockProfiler() as block_prof:
        if flow is not None:
            # ── asyncflow path ────────────────────────────────────────────────
            @flow.function_task
            async def _process(analysis_steps, iter_sleep_s):
                # analysis_steps / iter_sleep_s passed explicitly so Dragon
                # workers see the configured values (not module defaults).
                _cpu_samples: list[float] = []
                _gpu_samples: list[float] = []
                _stop = threading.Event()
                _t0_cpu = psutil.Process().cpu_times()

                # No reporter.emit() from Dragon workers — see _simulate comment.

                def _monitor() -> None:
                    psutil.cpu_percent()  # discard first sample (always 0.0)
                    while not _stop.is_set():
                        _cpu_samples.append(psutil.cpu_percent(interval=1.0))
                        gpu = _sample_gpu_util()
                        if gpu is not None:
                            _gpu_samples.append(gpu)

                _mon = threading.Thread(target=_monitor, daemon=True)
                _mon.start()

                try:
                    with TaskProfiler() as task_prof:
                        results = []
                        steps = analysis_steps
                        sleep = iter_sleep_s
                        for step in range(steps):
                            await asyncio.sleep(sleep)
                            results.append(step)
                finally:
                    _stop.set()
                    _mon.join(timeout=3.0)
                n_cpu = len(_cpu_samples)
                n_gpu = len(_gpu_samples)
                t_cpu = psutil.Process().cpu_times()
                cpu_s = (t_cpu.user + t_cpu.system) - (_t0_cpu.user + _t0_cpu.system)

                return {
                    "steps": results,
                    "resource_usage": task_prof.metrics,
                    "profiler_cpu_avg": sum(_cpu_samples) / n_cpu if n_cpu else None,
                    "profiler_n": n_cpu,
                    "profiler_gpu_avg": sum(_gpu_samples) / n_gpu if n_gpu else None,
                    "profiler_gpu_n": n_gpu,
                    "cpu_seconds": cpu_s,
                    "gpu_seconds": sum(u / 100.0 for u in _gpu_samples) if n_gpu else None,
                }

            @flow.function_task
            async def _summarise(proc_result, iter_sleep_s):
                await asyncio.sleep(iter_sleep_s)
                # No reporter.emit() from Dragon workers — see _simulate comment.
                return {
                    "profiler_cpu_avg": proc_result.get("profiler_cpu_avg"),
                    "profiler_n": proc_result.get("profiler_n", 0),
                    "profiler_gpu_avg": proc_result.get("profiler_gpu_avg"),
                    "profiler_gpu_n": proc_result.get("profiler_gpu_n", 0),
                    "cpu_seconds": proc_result.get("cpu_seconds"),
                    "gpu_seconds": proc_result.get("gpu_seconds"),
                }

            proc = _process(ANALYSIS_STEPS, ITER_SLEEP_S)  # pass configured values explicitly
            summarise_result = await _summarise(proc, ITER_SLEEP_S)  # depends on proc Future

            await reporter.emit(
                score=1.0,
                metrics={
                    "cpu_hours": budget.spent(),
                    "score": 1.0,
                    "profiler_cpu_avg": summarise_result.get("profiler_cpu_avg"),
                    "profiler_n": summarise_result.get("profiler_n", 0),
                    "profiler_gpu_avg": summarise_result.get("profiler_gpu_avg"),
                    "profiler_gpu_n": summarise_result.get("profiler_gpu_n", 0),
                    "cpu_seconds": summarise_result.get("cpu_seconds"),
                    "gpu_seconds": summarise_result.get("gpu_seconds"),
                },
                iteration=ANALYSIS_STEPS + 1,
            )

        else:
            # ── pure asyncio fallback ─────────────────────────────────────────
            _t0_cpu = psutil.Process().cpu_times()
            for step in range(ANALYSIS_STEPS):
                await asyncio.sleep(ITER_SLEEP_S)
                t_cpu = psutil.Process().cpu_times()
                cpu_s = (t_cpu.user + t_cpu.system) - (_t0_cpu.user + _t0_cpu.system)
                await reporter.emit(
                    score=1.0,
                    metrics={
                        "cpu_hours": budget.spent(),
                        "step": step,
                        "status": "ok",
                        "cpu_seconds": cpu_s,
                    },
                    iteration=step,
                )
            t_cpu = psutil.Process().cpu_times()
            cpu_s = (t_cpu.user + t_cpu.system) - (_t0_cpu.user + _t0_cpu.system)
            await reporter.emit(
                score=1.0,
                metrics={
                    "cpu_hours": budget.spent(),
                    "score": 1.0,
                    "resource_usage": block_prof.metrics,
                    "cpu_seconds": cpu_s,
                },
                iteration=ANALYSIS_STEPS,
            )
