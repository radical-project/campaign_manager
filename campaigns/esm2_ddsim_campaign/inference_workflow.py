"""
InferenceWorkflow — async single-phase ESM2 client request workflow.

N ESM2 services are initialised exactly once (one per GPU, auto-assigned by
start_services_local based on num_services in config).  Replicas round-robin
across services:  replica_i → service (i % N).  A per-service asyncio.Lock
serialises back-to-back replicas on the same service so queue state is always
cleanly reset before the next client runs.  Workers and the asyncflow engine
stay alive for the full campaign duration.

Lifecycle
---------
  First replica  → _ensure_initialized()  (expensive: model load × N, workers)
  Every replica  → acquire service lock → processed_queue.join()
                 → _reset_service_queues() → ESM2Client.run() → release lock
  Last replica   → on_replica_done() → _teardown()
"""

import asyncio
import os
import random
import sys
from pathlib import Path
from typing import ClassVar, Optional

os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")  # suppress XLA/cuInit probe noise

_spherical_dir = os.environ.get("SPHERICAL_DIR")
if not _spherical_dir:
    raise OSError("SPHERICAL_DIR is not set. Export it before launching the campaign.")
_SPHERICAL_ROOT = Path(_spherical_dir)

if str(_SPHERICAL_ROOT) not in sys.path:
    sys.path.insert(0, str(_SPHERICAL_ROOT))

# Both campaign_manager and SPHERICAL share a top-level 'src' package.
# Python caches the first one loaded (campaign_manager's src/campaign).
# Extend its __path__ so src.inference resolves to SPHERICAL's src/inference.
import src as _src_pkg  # noqa: E402

_spherical_src = str(_SPHERICAL_ROOT / "src")
if _spherical_src not in _src_pkg.__path__:
    _src_pkg.__path__.insert(0, _spherical_src)

from src.campaign import BaseWorkflow  # noqa: E402
from src.utils.logger import Logger  # noqa: E402


class InferenceWorkflow(BaseWorkflow):
    workflow_id = "inference"

    # ------------------------------------------------------------------ #
    # Shared state — lives across all replicas in the campaign            #
    # ------------------------------------------------------------------ #

    _svc_handles: ClassVar[Optional[list]] = None  # one handle per service
    _svc_locks: ClassVar[Optional[list[asyncio.Lock]]] = None  # one lock per service
    _asyncflow: ClassVar = None  # shared WorkflowEngine
    _init_lock: ClassVar[Optional[asyncio.Lock]] = None  # one-time init guard
    _num_services: ClassVar[int] = 0
    _log: ClassVar[Logger] = Logger(name="InferenceWorkflow", use_colors=True)
    # Set to True once no-GPU is detected; suppresses subsequent per-replica warnings.
    _gpu_unavailable: ClassVar[bool] = False

    # ------------------------------------------------------------------ #
    # Replica entry point                                                 #
    # ------------------------------------------------------------------ #

    async def run(self, replica_id: str) -> None:
        cfg = self.config or {}

        # Debug mode bypasses the GPU check and runs the stub directly so that
        # downstream dummy replicas still get triggered (useful for CPU benchmarks).
        if cfg.get("debug", False):
            await self._client_request(replica_id, cfg)
            return

        # assigned_gpu_ids is empty when the ResourcePool has no GPUs to allocate.
        # In that case skip real inference entirely — stub mode would just generate
        # noise and trigger downstream replicas with no real data.
        if not cfg.get("assigned_gpu_ids"):
            if not InferenceWorkflow._gpu_unavailable:
                InferenceWorkflow._gpu_unavailable = True
                InferenceWorkflow._log.warning(
                    "no GPU allocated to inference group — ESM2 inference workflow "
                    "disabled for this run. All inference replicas will be skipped "
                    "and the dummy workflow will NOT be triggered. "
                    "Submit to a GPU partition to enable real ESM2 inference.",
                    component="workflow",
                    task_name=replica_id,
                )
            return

        await self._client_request(replica_id, cfg)

    # ------------------------------------------------------------------ #
    # Debug / real dispatch                                               #
    # ------------------------------------------------------------------ #

    async def _client_request(self, replica_id: str, cfg: dict) -> None:
        if cfg.get("debug", False):
            await self._run_stub(replica_id)
            return
        try:
            await self._run_real_inference(replica_id, cfg)
        except BaseException as exc:
            if isinstance(exc, asyncio.CancelledError):
                raise
            InferenceWorkflow._log.error(
                f"inference failed ({type(exc).__name__}: {exc}); running stub",
                component="workflow",
                task_name=replica_id,
            )
            await self._run_stub(replica_id)

    # ------------------------------------------------------------------ #
    # Real inference — round-robin across shared services                 #
    # ------------------------------------------------------------------ #

    async def _run_real_inference(self, replica_id: str, cfg: dict) -> None:
        # Probe transformers before spawning any server: if it's missing this
        # raises immediately and _ensure_initialized (which launches Dragon
        # server processes) is never reached, preventing port-conflict cascades.
        import os as _os

        _os.environ.setdefault("TRANSFORMERS_VERBOSITY", "error")
        import transformers  # noqa: F401
        from src.inference.esm2_service.esm2_client import ESM2Client  # from SPHERICAL
        from src.inference.esm2_service.esm2_service import ESM2InferenceService  # from SPHERICAL

        from src.utils.workflow import export_metrics

        await self._ensure_initialized(cfg, ESM2InferenceService, asyncflow=self.asyncflow)

        # Round-robin: replica index parsed from "group_N" replica_id.
        replica_idx = int(replica_id.split("_")[-1])
        svc_idx = replica_idx % InferenceWorkflow._num_services
        lock = InferenceWorkflow._svc_locks[svc_idx]
        handle = InferenceWorkflow._svc_handles[svc_idx]
        svc = handle.service

        async with lock:
            # Wait for all in-flight executor threads to finish first.  Worker
            # task_done on work_queue is called only after run_in_executor returns
            # (thread fully done), so this guarantees all processed_queue puts have
            # happened before we drain processed_queue.  Without this ordering,
            # processed_queue.join() can return while a slow thread is still mid-
            # flight and will later put a stale batch_id that the next replica's
            # _result_writer sees without a matching reply_store entry.
            await svc.work_queue.join()
            # Now drain the result writer — all puts are guaranteed to be in flight.
            await svc.processed_queue.join()
            # Reset per-run queue state so init_queue can repopulate.
            self._reset_service_queues(svc)

            client = ESM2Client(
                endpoints=[handle.endpoint] if handle.endpoint else [],
                rank=0,
                service=svc,
                config=cfg,
                asyncflow=InferenceWorkflow._asyncflow,
            )

            output_dir = cfg.get("output_dir", "data/outputs")
            await client.run()
            InferenceWorkflow._log.info(
                f"inference complete → {output_dir}",
                component="workflow",
                task_name=replica_id,
            )

        metrics_dir = cfg.get("metrics_dir", "outputs")
        await export_metrics(
            Path(metrics_dir, f"client_{replica_id}.json"),
            client.metrics,
        )

    # ------------------------------------------------------------------ #
    # One-time service initialisation                                     #
    # ------------------------------------------------------------------ #

    @classmethod
    async def _ensure_initialized(cls, cfg: dict, service_class, asyncflow=None) -> None:
        """Start all N services exactly once. Uses the shared asyncflow if provided."""
        if cls._init_lock is None:
            cls._init_lock = asyncio.Lock()

        async with cls._init_lock:
            if cls._svc_handles is not None:
                return

            from src.inference.orchestrator import (  # from SPHERICAL
                start_services,
                start_services_local,
            )

            mode = cfg.get("mode", "local")
            cls._log.info(f"Starting ESM2 services (mode={mode})")
            if mode == "server":
                handles = await start_services(cfg, service_class)
            else:
                handles = await start_services_local(cfg, service_class)
            if not handles:
                raise RuntimeError("Failed to initialise ESM2 inference services")

            cls._svc_handles = handles
            cls._num_services = len(handles)
            cls._svc_locks = [asyncio.Lock() for _ in range(cls._num_services)]

            cls._asyncflow = asyncflow

            if mode != "server":
                for h in cls._svc_handles:
                    await h.service.start_workers()

            cls._log.info(
                f"Initialized {cls._num_services} service(s) across "
                f"{cls._num_services} GPU(s) (asyncflow=shared)"
            )

    # ------------------------------------------------------------------ #
    # Per-replica queue reset (safe under per-service lock)               #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _reset_service_queues(svc) -> None:
        """Reset queue/event state between replicas on the same service."""
        svc.shutdown_init.clear()
        for q in (svc.seq_queue, svc.input_queue):
            while not q.empty():
                try:
                    q.get_nowait()
                    if q is svc.seq_queue:
                        q.task_done()
                except Exception:
                    break
        svc.single_batch = None
        svc.device_batches.clear()

    # ------------------------------------------------------------------ #
    # Replica-done hook                                                   #
    # ------------------------------------------------------------------ #

    async def on_replica_done(self, replica_id: str, cm, final_state: str) -> None:
        """Trigger 1 miniapps per successful inference (first n_md_runs=4); teardown on last."""
        if InferenceWorkflow._gpu_unavailable and not (self.config or {}).get("debug", False):
            return

        status = cm.status()
        g = status["groups"].get("inference", {})
        finished = g.get("replicas_finished", 0) + 1
        total = g.get("replicas_total", 1)

        InferenceWorkflow._log.info(
            f"replica finished [{final_state}] ({finished}/{total})",
            component="workflow",
            task_name=replica_id,
        )

        # miniapps is now an independent group (replicas=4, no CM dependency).
        # The policy-based scheduling benchmark requires genuine GPU contention:
        # miniapps replicas must sit in the queue while inference holds all GPU slots.
        # trigger_dependent bypassed the scheduler (GPU assigned immediately on trigger),
        # making starved=False always and the telemetry boost unreachable.
        # Miniapps handles data availability via internal file-watching (data/outputs/).

        if finished >= total:
            InferenceWorkflow._log.info(
                "all inference replicas done — tearing down ESM2 services",
                component="workflow",
                task_name=replica_id,
            )
            await InferenceWorkflow._teardown()

    # ------------------------------------------------------------------ #
    # Teardown after last replica                                         #
    # ------------------------------------------------------------------ #

    @classmethod
    async def _teardown(cls) -> None:
        """Shut down inference services and workers. Idempotent.

        NOTE: cls._asyncflow is intentionally NOT shut down here because
        other workflow groups (e.g. ddsim) may still be running and share
        the same asyncio event loop subprocess infrastructure.  Call
        _shutdown_asyncflow() explicitly after cm.wait() returns.
        """
        if cls._svc_handles is None:
            return

        for h in cls._svc_handles:
            svc = h.service
            # Drain in-flight work before shutting down.
            if hasattr(svc, "work_queue"):
                await svc.work_queue.join()
            if hasattr(svc, "processed_queue"):
                await svc.processed_queue.join()
            # h.close() terminates the Dragon subprocess (calls dragon_process.terminate()
            # + join()) so the port is released before the next policy run starts.
            # For in-process mode it calls svc.shutdown() and runner.cleanup() instead.
            await h.close()
        cls._svc_handles = None
        cls._svc_locks = None
        cls._num_services = 0

    @classmethod
    async def _shutdown_asyncflow(cls) -> None:
        """No-op: asyncflow is owned and shut down by AsyncCampaignManager."""
        pass

    # ------------------------------------------------------------------ #
    # Stub (debug / no-GPU path)                                         #
    # ------------------------------------------------------------------ #

    async def _run_stub(self, replica_id: str) -> None:
        cfg = self.config or {}
        # stub_sleep_s lets benchmark configs simulate a slow upstream stage
        # (e.g. 30 s to model real ESM2 inference time) without a GPU or server.
        sleep_s = float(cfg.get("stub_sleep_s", 0.1))
        InferenceWorkflow._log.debug(
            f"client_req starting (stub, sleep={sleep_s}s)",
            component="workflow",
            task_name=replica_id,
        )
        await asyncio.sleep(sleep_s)
        InferenceWorkflow._log.debug(
            "client_req done → requests_sent=500",
            component="workflow",
            task_name=replica_id,
        )
