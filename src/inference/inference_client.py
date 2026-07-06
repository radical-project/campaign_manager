#!/usr/bin/env python3
"""
InferenceClient — generic HTTP client for remote inference services.

Handles:
- Round-robin load balancing across endpoints
- Asyncflow-compatible request submission
- Retry logic with exponential backoff
- Metrics collection

Subclasses override :meth:`_get_batch_data` to prepare model-specific
payload data for each batch.  All transport, queueing, and scheduling
logic lives here.
"""

import asyncio
import itertools
from pathlib import Path
from typing import Any, Optional

import aiohttp

from ..utils.logger import Logger
from .utils import export_metrics


async def _null_coro():
    """No-op coroutine used as a placeholder for None futures in gather."""


class InferenceClient:
    """
    Generic HTTP client for remote inference.

    Subclasses must implement:
    - ``_get_batch_data(batch_id)`` — return the payload dict to send with
      each request, or ``None`` to let the server look up the batch itself.
    """

    def __init__(
        self,
        endpoints: list[str],
        rank: int = 0,
        service: Optional[Any] = None,
        config: Optional[dict[str, Any]] = None,
        asyncflow: Optional[Any] = None,
    ):
        self.config = config or {}
        self.endpoints = endpoints
        self.endpoint_cycle = itertools.cycle(endpoints) if endpoints else None
        self.rank = rank
        self.max_concurrent = self.config.get("max_concurrent", 16)
        self.timeout = self.config.get("timeout", 600)
        self.max_retries = self.config.get("max_retries", 3)
        self.service = service
        self.metrics_dir = self.config.get("metrics_dir", "outputs")
        self.workflow_id = self.config.get("workflow_id", "infern_workflow")
        self.debug = self.config.get("debug", False)

        self.flow = asyncflow
        self.logger = Logger(use_colors=True)

        self.metrics = {
            "submitted": 0,
            "successful": 0,
            "failed": 0,
            "error_msgs": [],
            "retries": 0,
        }

        if self.debug:
            self.logger.info(
                f"[Client {self.rank}] Initialized with {len(endpoints)} endpoints, "
                f"max_concurrent={self.max_concurrent}, timeout={self.timeout}s"
            )

        if self.flow:
            self._register_client()
        else:
            self.logger.critical("Unable to start client without asyncflow engine")

    # ------------------------------------------------------------------
    # Hook for subclasses
    # ------------------------------------------------------------------

    def _get_batch_data(self, batch_id: int) -> Optional[dict]:
        """
        Return the batch payload to include in the POST request body, or
        ``None`` to send only the batch ID and let the server look it up.

        Override in subclasses to attach model-specific data.
        """
        return None

    # ------------------------------------------------------------------
    # Client registration (asyncflow)
    # ------------------------------------------------------------------

    def _register_client(self):
        """Register the HTTP request function with asyncflow."""
        timeout = self.timeout
        max_retries = self.max_retries

        @self.flow.function_task
        async def client_req(
            batch_id: int,
            endpoint: str,
            request_timeout: int,
            retries: int,
            batch_data: Optional[dict] = None,
        ) -> dict[str, Any]:
            """Submit a single batch for inference via HTTP POST."""
            url = f"{endpoint}/generate"
            if batch_data is not None:
                payload = {
                    "batch_id": batch_id,
                    "batch": batch_data,
                    "timeout": request_timeout,
                }
            else:
                payload = {
                    "batch_ids": [batch_id],
                    "timeout": request_timeout,
                }

            last_error = None
            retry_count = 0

            for attempt in range(retries):
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            url,
                            json=payload,
                            timeout=aiohttp.ClientTimeout(total=request_timeout),
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                if data.get("status") == "success":
                                    return {
                                        "status": "success",
                                        "batch_id": batch_id,
                                        "successful": data.get("successful", 0),
                                        "failed": data.get("failed", 0),
                                        "retries": retry_count,
                                    }
                                else:
                                    error = data.get("message", "Unknown error")
                                    return {
                                        "status": "error",
                                        "batch_id": batch_id,
                                        "error": error,
                                        "failed": data.get("failed", 1),
                                        "retries": retry_count,
                                    }
                            else:
                                error = f"HTTP {resp.status}: {await resp.text()}"
                                last_error = error
                                if attempt < retries - 1:
                                    retry_count += 1
                                    await asyncio.sleep(0.5 * (2**attempt))
                                    continue
                                return {
                                    "status": "error",
                                    "batch_id": batch_id,
                                    "error": error,
                                    "failed": 1,
                                    "retries": retry_count,
                                }

                except asyncio.TimeoutError:
                    last_error = "Timeout"
                    if attempt < retries - 1:
                        retry_count += 1
                        await asyncio.sleep(0.5 * (2**attempt))
                        continue
                    return {
                        "status": "error",
                        "batch_id": batch_id,
                        "error": "Timeout",
                        "failed": 1,
                        "retries": retry_count,
                    }

                except Exception as e:
                    last_error = str(e)
                    if attempt < retries - 1:
                        retry_count += 1
                        await asyncio.sleep(0.5 * (2**attempt))
                        continue
                    return {
                        "status": "error",
                        "batch_id": batch_id,
                        "error": str(e),
                        "failed": 1,
                        "retries": retry_count,
                    }

            return {
                "status": "error",
                "batch_id": batch_id,
                "error": f"Max retries exceeded: {last_error}",
                "failed": 1,
                "retries": retry_count,
            }

        self._client_req_task = client_req

        def submit_request(batch_id: int, endpoint: str, batch_data: dict = None):
            return client_req(batch_id, endpoint, timeout, max_retries, batch_data)

        self.client_req = submit_request

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def health_check(self, endpoint: Optional[str] = None) -> bool:
        """Check if an endpoint is healthy."""
        if endpoint is None:
            endpoint = self.endpoints[0]
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{endpoint}/health", timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.logger.info(
                            f"[Client {self.rank}] {endpoint} - healthy "
                            f"({data.get('total_workers', 0)} workers)"
                        )
                        return True
                    self.logger.warning(
                        f"[Client {self.rank}] {endpoint} - unhealthy (HTTP {resp.status})"
                    )
                    return False
        except Exception as e:
            self.logger.warning(f"[Client {self.rank}] {endpoint} - unreachable ({e})")
            return False

    async def close(self):
        """Export metrics and shut down asyncflow."""
        self.logger.info(f"[Client {self.rank}] Closing")
        await export_metrics(Path(self.metrics_dir, f"client_{self.rank}.json"), self.metrics)

    async def init_queue(self) -> None:
        """Populate the sequence queue via the service."""
        if self.service is None:
            self.logger.error(f"[Client {self.rank}] No service provided for batch generation")
            return
        if self.debug:
            self.logger.task_started(f"[rank {self.rank}] Batch generation")
        await self.service.init_queue()
        if self.debug:
            self.logger.task_completed(f"[rank {self.rank}] Batch generation")

    async def run_inference(self) -> None:
        """Drain the sequence queue and dispatch inference requests."""
        try:
            if self.debug:
                self.logger.task_started(f"[rank {self.rank}] Remote inference")
            await self._process_queue()
            if self.debug:
                self.logger.task_completed(f"[rank {self.rank}] Remote inference")
        except asyncio.CancelledError:
            self.logger.warning(f"[Client {self.rank}] Inference was cancelled — stopping")
            raise

    async def run(self) -> None:
        """Run init_queue then run_inference (convenience wrapper)."""
        if self.debug:
            self.logger.info(f"[Client {self.rank}] Starting remote inference")
        if self.service is None:
            self.logger.error(f"[Client {self.rank}] No service provided for batch generation")
            return
        try:
            await self.init_queue()
            await self.run_inference()
        except Exception as e:
            self.logger.error(f"[Client {self.rank}] Error in run(): {e}")
            raise
        finally:
            if self.debug:
                self.logger.info(f"[Client {self.rank}] Cleanup complete")

    # ------------------------------------------------------------------
    # Queue processing
    # ------------------------------------------------------------------

    async def _process_queue(self):
        """Drain seq_queue and dispatch inference requests."""
        batch_count = 0
        tasks: list = []
        batch_ids: list = []

        while True:
            batch_id = await self.service.seq_queue.get()

            try:
                if batch_id is None:
                    if self.debug:
                        self.logger.info(f"[Client {self.rank}] Received shutdown sentinel")
                    if tasks:
                        await self._flush_tasks(tasks, batch_ids)
                    break

                batch_count += 1

                if self.endpoints:
                    endpoint = next(self.endpoint_cycle)
                    bd = self._get_batch_data(batch_id)

                    task = self.client_req(batch_id, endpoint, bd)
                    if batch_count == 1 and self.debug:
                        self.logger.debug(
                            f"[Client {self.rank}] Task type: {type(task)}, "
                            f"awaitable: {hasattr(task, '__await__')}"
                        )
                    tasks.append(task)
                    batch_ids.append(batch_id)

                    if self.debug and batch_count % 100 == 0:
                        self.logger.debug(f"[Client {self.rank}] Dispatched {batch_count} batches")

                    if len(tasks) >= self.max_concurrent:
                        await self._flush_tasks(tasks, batch_ids)
                        tasks = []
                        batch_ids = []
                else:
                    # Local mode — submit directly to the service work queue.
                    self.service.work_queue.put_nowait((batch_id, None, None))

            finally:
                self.service.seq_queue.task_done()

        if self.debug:
            self.logger.info(f"[Client {self.rank}] Dispatched {batch_count} batches total")

        # Local mode: wait for all workers to finish.
        if not self.endpoints and self.service is not None:
            if self.debug:
                self.logger.info(
                    f"[Client {self.rank}] Waiting for {self.service.work_queue.qsize()} "
                    f"batch(es) to complete..."
                )
            await self.service.work_queue.join()
            self.logger.info(f"[Client {self.rank}] All batches processed")

    # ------------------------------------------------------------------
    # Flush helpers
    # ------------------------------------------------------------------

    async def _flush_tasks(self, tasks: list, batch_ids: list = None):
        """Schedule all tasks concurrently and aggregate metrics."""
        if not tasks:
            return

        self.logger.debug(f"[Client {self.rank}] Flushing {len(tasks)} tasks")

        futs = []
        for i, task in enumerate(tasks):
            if asyncio.isfuture(task) or asyncio.iscoroutine(task):
                futs.append(asyncio.ensure_future(task))
            elif hasattr(task, "__await__"):
                t = task

                async def _wrap(t=t):
                    return await t

                futs.append(asyncio.ensure_future(_wrap()))
            elif hasattr(task, "result"):
                futs.append(
                    asyncio.ensure_future(
                        asyncio.get_event_loop().run_in_executor(None, task.result)
                    )
                )
            else:
                self.logger.error(f"[Client {self.rank}] Task {i} is not awaitable: {type(task)}")
                self.metrics["failed"] += 1
                futs.append(None)

        wrapped = [
            asyncio.wait_for(f, timeout=self.timeout) if f is not None else _null_coro()
            for f in futs
        ]
        results = await asyncio.gather(*wrapped, return_exceptions=True)

        for i, result in enumerate(results):
            if self.debug:
                self.logger.debug(f"[Client {self.rank}] Task {i}: {result!r}")
            if isinstance(result, asyncio.TimeoutError):
                self.logger.error(f"[Client {self.rank}] Task {i} timed out after {self.timeout}s")
                self.metrics["failed"] += 1
            elif isinstance(result, Exception):
                self.logger.error(
                    f"[Client {self.rank}] Task {i} failed with {type(result).__name__}: {result}"
                )
                import traceback

                self.logger.debug(f"[Client {self.rank}] Traceback: {traceback.format_exc()}")
                self.metrics["failed"] += 1
            elif isinstance(result, dict):
                self.metrics["successful"] += result.get("successful", 0)
                self.metrics["failed"] += result.get("failed", 0)
                self.metrics["retries"] += result.get("retries", 0)
                if result.get("status") == "error":
                    batch_id = result.get("batch_id", "?")
                    error = result.get("error", "Unknown")
                    self.metrics["error_msgs"].append(f"batch {batch_id}: {error}")
                    self.logger.error(f"[Client {self.rank}] Batch {batch_id}: {error}")
                self.metrics["failed"] += 1
