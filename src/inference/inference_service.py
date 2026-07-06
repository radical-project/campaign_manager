#!/usr/bin/env python3
"""
Inference Service

Multi-GPU inference service with worker pool management, batch processing,
and metrics tracking.

Contains:
- InferenceService: Base class with common infrastructure
- GPUWorker: Individual GPU worker for batch processing
"""

import asyncio
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Optional

from ..utils.logger import Logger
from .utils import ensure_dir, export_metrics

# -----------------------------------------------------------------------------
# Base Inference Service
# -----------------------------------------------------------------------------


class InferenceService(ABC):
    """
    Base class for multi-GPU async inference services.

    Provides common infrastructure:
    - Multi-GPU worker pool management
    - Batch queue management
    - Metrics collection and logging
    - Graceful shutdown handling

    Subclasses must implement:
    - _load_models(): Load model(s) onto GPU(s)
    - process_batch_sync(): Process a single batch synchronously
    - generate_batch(): Generate a batch from input queue
    - _result_writer(): Background task to handle results (optional)
    """

    def __init__(
        self,
        config: Optional[dict[str, Any]] = None,
        devices: Optional[list[str]] = None,
        rank: int = 0,
        client_mode: bool = False,
        **kwargs,
    ):
        """
        Initialize base inference service.

        Args:
            config: Configuration dictionary
            devices: List of GPU devices (e.g., ['cuda:0', 'cuda:1'])
            rank: Process rank
            client_mode: If True, skip heavy initialization (for client-side preprocessing only)
            **kwargs: Additional configuration
        """

        self.rank = rank
        self.config = config or {}
        self.client_mode = client_mode
        self.logger = Logger(use_colors=True, config=config, rank=rank, devices=devices)

        # Multi-GPU device management
        self.devices = devices or []
        if not self.devices:
            raise ValueError("No devices provided for multi-GPU inference")

        self.logger.info(
            f"[rank {self.rank}] Initializing service with {len(self.devices)} devices: {self.devices}"
        )

        # Configuration
        self.num_batches = int(self.config.get("num_batches", 100))
        self.max_batch_tokens = int(self.config.get("max_batch_tokens", 16000))
        self.num_workers_per_gpu = int(self.config.get("num_workers_per_gpu", 1))
        self.debug = self.config.get("debug", False)
        self.cancel_io = self.config.get(
            "cancel_io", True
        )  # Don't wait for all outputs to be saed to disk
        self.use_streaming = self.config.get("use_streaming", False)
        self.output_dir = Path(self.config.get("output_dir", "./"))
        if client_mode:
            self.results_dir = Path(self.config.get("results_dir", "results"))
        else:
            self.results_dir = ensure_dir(Path(self.config.get("results_dir", "results")))

        self.logger.info(
            f"[rank {self.rank}] Configuration: "
            f"max_batch_tokens={self.max_batch_tokens}, num_batches={self.num_batches}, "
            f"{len(self.devices)} GPUs, {self.num_workers_per_gpu} workers/GPU"
        )

        # Queues
        self.work_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self.seq_queue: asyncio.Queue = asyncio.Queue()
        self.input_queue: asyncio.Queue = asyncio.Queue()
        self.processed_queue: asyncio.Queue = asyncio.Queue()

        # GPU worker pool
        self.workers: list[GPUWorker] = []
        self.worker_tasks: list[asyncio.Task] = []

        # Batch storage
        self.batch_storage: dict[int, dict] = {}
        self.single_batch: Optional[dict] = None
        self.device_batches: dict[str, dict] = {}
        self.reply_store: dict[int, Any] = {}

        # Pending request futures (for server mode)
        self.pending_requests: dict[int, asyncio.Future] = {}
        self._request_counter: int = 0

        # Shutdown coordination
        self.shutting_down = asyncio.Event()
        self.shutdown_init = asyncio.Event()

        # Thread pool for async file I/O
        self.save_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="save")

    # -------------------------------------------------------------------------
    # Abstract Methods (must be implemented by subclasses)
    # -------------------------------------------------------------------------

    @abstractmethod
    def _load_models(self):
        """Load model(s) onto GPU(s). Must be implemented by subclass."""
        pass

    @abstractmethod
    def process_batch_sync(self, batch_id: int, device: str, batch_data: Optional[dict] = None):
        """
        Process a single batch synchronously.

        Args:
            batch_id: Batch identifier
            device: GPU device to use
            batch_data: Optional pre-tokenized batch data (for remote mode).
                        If provided, used directly instead of looking up from storage.

        Must be implemented by subclass.
        """
        pass

    @abstractmethod
    async def generate_batch(self) -> tuple:
        """
        Generate one batch from input_queue.

        Returns:
            Tuple of (num_tokens, batch_data)

        Must be implemented by subclass.
        """
        pass

    # -------------------------------------------------------------------------
    # Worker Management
    # -------------------------------------------------------------------------

    async def start_workers(self):
        """Start GPU worker pool and metrics logging."""
        self.logger.info(
            f"[rank {self.rank}] Starting {self.num_workers_per_gpu} worker(s) per GPU "
            f"({len(self.devices)} GPUs = {len(self.devices) * self.num_workers_per_gpu} total workers)"
        )

        worker_id = 0
        for device in self.devices:
            for _ in range(self.num_workers_per_gpu):
                worker = GPUWorker(device, worker_id, self)
                self.workers.append(worker)
                task = asyncio.create_task(worker.run())
                self.worker_tasks.append(task)
                worker_id += 1

        self.logger.start_metrics_logging(self)

        # Start result writer if subclass provides one
        if hasattr(self, "_result_writer"):
            writer_task = asyncio.create_task(self._result_writer())
            self.worker_tasks.append(writer_task)

        self.logger.info(
            f"[rank {self.rank}] All {len(self.workers)} workers started (with metrics logging)"
        )

    async def shutdown(self):
        """Stop all GPU workers and metrics logging gracefully."""
        self.logger.info(
            f"[rank {self.rank}] Stopping {len(self.workers)} workers and metrics logging..."
        )

        if self.cancel_io:
            self.logger.info(f"[rank {self.rank}] Clearing queue and signaling exit...")
            while not self.processed_queue.empty():
                _ = self.processed_queue.get_nowait()
                self.processed_queue.task_done()

        if self.debug:
            self.logger.info(f"[rank {self.rank}] Waiting for disk I/O to complete...")
        await self.processed_queue.join()
        await self.processed_queue.put(None)

        await self.work_queue.join()
        for _ in self.workers:
            await self.work_queue.put(None)

        self.shutting_down.set()

        await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        if self.logger.metrics_task:
            await asyncio.gather(self.logger.metrics_task, return_exceptions=True)
        await export_metrics(self.logger.metrics_output, self.logger.metrics)

        self.save_executor.shutdown(wait=True)
        if self.debug:
            self.logger.info(f"[rank {self.rank}] All workers and metrics logging stopped")

    async def close(self):
        """Gracefully shutdown the inference service (alias for shutdown)."""
        await self.shutdown()

    # -------------------------------------------------------------------------
    # Batch Queue Management
    # -------------------------------------------------------------------------

    async def sequence_source(self):
        """Async generator for sequences. Override in subclass if needed."""
        sequences = self.config.get("SEQUENCES", [])
        if not sequences:
            self.logger.warning(
                f"[rank {self.rank}] No SEQUENCES in config — sequence_source produces nothing"
            )
            return
        while not self.shutdown_init.is_set():
            for seq in sequences:
                yield seq
            # Cooperatively yield to the event loop after each cycle so that
            # generate_batch() can consume sequences concurrently instead of
            # waiting for sequence_producer to exhaust its entire threshold.
            await asyncio.sleep(0)

    async def sequence_producer(self):
        """Pull sequences from generator and enqueue."""
        total_tokens = 0
        try:
            async for seq in self.sequence_source():
                await self.input_queue.put(seq)
                total_tokens += len(seq)
                if total_tokens > self.num_batches * self.max_batch_tokens:
                    self.shutdown_init.set()
                    break
        finally:
            await self.input_queue.put(None)

    async def init_queue(self):
        """Initialize queue with batches."""
        t_start = time.time()
        producer_task = asyncio.create_task(self.sequence_producer())

        num_workers = len(self.devices) * self.num_workers_per_gpu
        min_batches_in_flight = num_workers * 3

        if self.debug:
            self.logger.info(
                f"[rank {self.rank}] Target: {min_batches_in_flight}+ batches in flight "
                f"to saturate {num_workers} workers"
            )

        if self.use_streaming:
            prefetch = min(int(min_batches_in_flight), int(self.num_batches))
            gen_sem = asyncio.Semaphore(prefetch)

            async def generate_and_store(batch_id):
                async with gen_sem:
                    try:
                        num_tokens, batch = await self.generate_batch()
                        self.batch_storage[batch_id] = batch
                        async with self.logger.metrics_lock:
                            self.logger.metrics["queue_tokens"] += num_tokens
                        await self.seq_queue.put(batch_id)
                    except StopAsyncIteration:
                        pass

            tasks = [
                asyncio.create_task(generate_and_store(bid)) for bid in range(self.num_batches)
            ]
            await asyncio.gather(*tasks)
        else:
            try:
                num_tokens, batch = await self.generate_batch()
            except StopAsyncIteration:
                self.logger.warning(
                    f"[rank {self.rank}] generate_batch returned no sequences "
                    f"— SEQUENCES may be empty in config; skipping init_queue"
                )
                self.shutdown_init.set()
                await asyncio.gather(producer_task)
                await self.seq_queue.put(None)
                return
            self.single_batch = batch

            async with self.logger.metrics_lock:
                self.logger.metrics["queue_tokens"] += num_tokens * self.num_batches

            if self.debug:
                self.logger.info(f"[rank {self.rank}] Enqueuing {self.num_batches} batch IDs...")
            for batch_id in range(self.num_batches):
                await self.seq_queue.put(batch_id)

            if not self.client_mode:
                if self.debug:
                    self.logger.info(
                        f"[rank {self.rank}] Pre-allocating batches for {len(self.devices)} devices..."
                    )
                for device in self.devices:
                    self.device_batches[device] = {
                        k: v.to(device, non_blocking=True) for k, v in self.single_batch.items()
                    }

        self.shutdown_init.set()
        await asyncio.gather(producer_task)
        await self.seq_queue.put(None)

        if self.debug:
            self.logger.info(
                f"[rank {self.rank}] init_queue completed: {self.num_batches} batches "
                f"in {time.time() - t_start:.2f}s"
            )

    # -------------------------------------------------------------------------
    # Direct Inference (Local Mode)
    # -------------------------------------------------------------------------

    async def submit_batch_local(self):
        """
        Process batches from seq_queue and dispatch directly to GPU workers.
        Used for local (non-client/server) mode.
        """
        self.logger.info(f"[rank {self.rank}] Starting local inference")
        batch_count = 0

        while True:
            batch_id = await self.seq_queue.get()

            try:
                if batch_id is None:
                    self.logger.info(f"[rank {self.rank}] Received shutdown sentinel")
                    break

                self.work_queue.put_nowait(
                    (batch_id, None, None)
                )  # None request_id, None batch_data for local mode
                batch_count += 1

                if self.debug and batch_count % 100 == 0:
                    self.logger.debug(
                        f"[rank {self.rank}] Dispatched {batch_count} batches, "
                        f"work_q depth: {self.work_queue.qsize()}"
                    )
            finally:
                self.seq_queue.task_done()

        self.logger.info(
            f"[rank {self.rank}] Dispatched {batch_count} batches total, "
            f"waiting for workers to complete..."
        )
        await self.work_queue.join()
        self.logger.info(f"[rank {self.rank}] All work completed")

    def submit_batch(self, batch_id: int, batch_data: Optional[dict] = None) -> asyncio.Future:
        """
        Submit a single batch for processing.

        Args:
            batch_id: Batch identifier
            batch_data: Optional pre-tokenized batch data (for remote mode).
                        If provided, passed through to process_batch_sync instead
                        of looking up from storage.

        Returns:
            Future that resolves with {"status": "success"} or {"status": "error", "error": "..."}

        This is the primary interface for the server to submit work.
        """
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        # Use unique request ID to handle same batch_id submitted multiple times
        self._request_counter += 1
        request_id = self._request_counter

        self.pending_requests[request_id] = future
        self.work_queue.put_nowait((batch_id, request_id, batch_data))

        return future

    def _resolve_request(self, request_id: int, result: dict[str, Any]):
        """Resolve a pending request future with the given result."""
        future = self.pending_requests.pop(request_id, None)
        if future is not None and not future.done():
            future.set_result(result)


# -----------------------------------------------------------------------------
# GPU Worker
# -----------------------------------------------------------------------------


class GPUWorker:
    """
    Individual GPU worker that processes batches on a specific device.

    Features:
    - Owns one GPU device
    - Processes batches from a shared work queue
    - Reports completion back to result queue
    """

    def __init__(self, device: str, worker_id: int, service: "InferenceService"):
        """
        Initialize GPU worker.

        Args:
            device: GPU device identifier (e.g., 'cuda:0')
            worker_id: Unique worker identifier
            service: Parent InferenceService instance
        """
        self.device = device
        self.worker_id = worker_id
        self.service = service
        self.logger = service.logger
        self.debug = service.debug
        self.is_busy = False
        self.processed_count = 0

    async def run(self):
        """Main worker loop - process batches from queue."""
        try:
            while True:
                item = await self.service.work_queue.get()

                try:
                    if item is None:
                        if self.debug:
                            self.logger.info(f"[Worker {self.worker_id}] Received shutdown signal")
                        break

                    batch_id, request_id, batch_data = item
                    self.is_busy = True

                    if self.debug:
                        self.logger.debug(
                            f"[Worker {self.worker_id}] Processing batch {batch_id} {self.logger.metrics['requests']}"
                        )

                    try:
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(
                            None, self.service.process_batch_sync, batch_id, self.device, batch_data
                        )
                        self.processed_count += 1

                        # Resolve pending request future (server mode)
                        if request_id is not None:
                            self.service._resolve_request(request_id, {"status": "success"})

                        self.logger.metrics["requests"] += 1
                        self.logger.metrics["gpu_stats"][self.device]["processed"] += 1

                    except Exception as e:
                        self.logger.error(
                            f"[Worker {self.worker_id}] Inference failed for batch {batch_id}: {e}"
                        )
                        if request_id is not None:
                            self.service._resolve_request(
                                request_id, {"status": "error", "error": str(e)}
                            )
                        self.logger.metrics["errors"] += 1

                finally:
                    self.is_busy = False
                    self.service.work_queue.task_done()
                    await asyncio.sleep(0)

        except asyncio.CancelledError:
            self.logger.info(f"[Worker {self.worker_id}] Cancelled")
            raise

        self.logger.info(
            f"[Worker {self.worker_id}] Stopped after processing {self.processed_count} batches"
        )
