#!/usr/bin/env python3
"""
HTTP Inference Server

An aiohttp-based HTTP server that exposes GPU workers for distributed
inference services.

Features:
- Health check and info endpoints
- HTTP POST endpoint for batch inference
- Multi-GPU worker pool management via InferenceService
"""

import asyncio
from typing import Any, Optional

from aiohttp import web

from ..utils.logger import Logger
from .inference_service import InferenceService

logger = Logger(use_colors=True)
inference_service: Optional[InferenceService] = None
config: dict[str, Any] = {}
server_info: dict[str, Any] = {}

DEBUG = False


async def startup_handler(app: web.Application):
    """Initialize inference service on startup."""
    global server_info

    logger.info("[Server] Starting inference service...")

    if inference_service is None:
        raise RuntimeError("Inference service not initialized. Call init_server() first.")

    logger.info("[Server] Starting GPU workers...")
    await inference_service.start_workers()
    logger.info("[Server] GPU workers started successfully")

    server_info["status"] = "ready"
    server_info["active_workers"] = len(inference_service.workers)


async def shutdown_handler(app: web.Application):
    """Cleanup on server shutdown."""
    global inference_service

    logger.info("[Server] Shutting down...")

    if inference_service:
        logger.info("[Server] Stopping GPU workers...")
        await inference_service.shutdown()
        logger.info("[Server] Cleanup complete")


async def root_handler(request: web.Request) -> web.Response:
    """Root endpoint with server info."""
    return web.json_response(
        {
            "service": "Inference Server",
            "version": "1.0",
            "endpoints": {
                "health": "/health",
                "info": "/info",
                "generate": "/generate (HTTP POST)",
            },
        }
    )


async def health_handler(request: web.Request) -> web.Response:
    """Health check endpoint."""
    if inference_service is None:
        return web.json_response(
            {"status": "unavailable", "reason": "Service not initialized"}, status=503
        )

    active_workers = sum(1 for w in inference_service.workers if w.is_busy)
    total_workers = len(inference_service.workers)

    return web.json_response(
        {
            "status": "healthy",
            "node_id": server_info.get("node_id", "unknown"),
            "devices": server_info.get("devices", []),
            "active_workers": active_workers,
            "total_workers": total_workers,
            "queue_depth": inference_service.work_queue.qsize(),
            "total_processed": inference_service.logger.metrics.get("requests", 0),
        }
    )


async def info_handler(request: web.Request) -> web.Response:
    """Get detailed server information."""
    if inference_service is None:
        return web.json_response({"error": "Service not initialized"}, status=503)

    host = server_info.get("host", "localhost")
    port = server_info.get("port", 8000)

    return web.json_response(
        {
            "node_id": server_info.get("node_id", "unknown"),
            "rank": server_info.get("rank", 0),
            "devices": server_info.get("devices", []),
            "num_workers_per_gpu": inference_service.num_workers_per_gpu,
            "total_workers": len(inference_service.workers),
            "metrics": inference_service.logger.metrics,
            "endpoint": f"http://{host}:{port}",
        }
    )


async def generate_handler(request: web.Request) -> web.Response:
    """
    HTTP POST endpoint for batch inference.

    Request (local mode — batch data pre-stored on server):
        POST /generate
        {"batch_ids": [0, 1, 2, ...], "timeout": 600}

    Request (remote mode — client sends batch data directly):
        POST /generate
        {"batch_id": 0, "batch": {"input_ids": [[...]], "attention_mask": [[...]]}, "timeout": 600}

    Response:
        {"status": "success", "total": N, "successful": M, "failed": K}
        or
        {"status": "error", "message": "..."}
    """
    try:
        data = await request.json()
        batch_data = data.get("batch", None)
        request_timeout = data.get("timeout", 600)

        # Determine batch IDs: either from "batch_ids" list or single "batch_id"
        if batch_data is not None:
            batch_id = data.get("batch_id", 0)
            batch_ids = [batch_id]
        else:
            batch_ids = data.get("batch_ids", [])

        if not batch_ids:
            return web.json_response(
                {"status": "error", "message": "batch_ids or batch_id+batch is required"},
                status=400,
            )

        if DEBUG:
            logger.info(f"[Server] Processing {len(batch_ids)} batches via HTTP POST")

        queue_depth = inference_service.work_queue.qsize()
        if DEBUG and queue_depth > 50:
            logger.warning(
                f"[Server] High queue depth: {queue_depth} batches waiting. "
                f"Consider increasing num_workers_per_gpu or reducing client concurrency."
            )

        async def process_single_batch(batch_id: int, bd: dict = None) -> dict[str, Any]:
            """Process a single batch and return result."""
            try:
                future = inference_service.submit_batch(batch_id, batch_data=bd)

                inference_timeout = config.get("inference_timeout", 600)
                timeout = min(request_timeout, inference_timeout)
                result = await asyncio.wait_for(future, timeout=timeout)

                if result.get("status") == "success":
                    if DEBUG:
                        logger.info(f"[Server] Completed batch {batch_id} via HTTP POST")
                    return {"batch_id": batch_id, "status": "success"}
                else:
                    return {
                        "batch_id": batch_id,
                        "status": "error",
                        "error": result.get("error", "Unknown error"),
                    }

            except asyncio.TimeoutError:
                logger.error(f"[Server] Inference timeout for batch {batch_id}")
                return {"batch_id": batch_id, "status": "error", "error": "Inference timeout"}
            except Exception as e:
                logger.error(f"[Server] Error processing batch {batch_id}: {e}")
                return {"batch_id": batch_id, "status": "error", "error": str(e)}

        tasks = [process_single_batch(bid, batch_data) for bid in batch_ids]
        results = await asyncio.gather(*tasks)

        failed = [r for r in results if r.get("status") == "error"]
        if failed:
            logger.warning(f"[Server] {len(failed)}/{len(results)} batches failed")

        return web.json_response(
            {
                "status": "success",
                "total": len(results),
                "successful": len(results) - len(failed),
                "failed": len(failed),
            }
        )

    except Exception as e:
        logger.error(f"[Server] Error in generate_handler: {e}")
        import traceback

        logger.error(f"[Server] Traceback: {traceback.format_exc()}")
        return web.json_response({"status": "error", "message": str(e)}, status=500)


def init_server(
    cfg: dict[str, Any],
    devices: list[str],
    rank: int,
    node_id: str,
    host: str,
    port: int,
    service_class: type = None,
) -> InferenceService:
    """
    Initialize the inference service.

    Args:
        cfg: Configuration dictionary
        devices: List of GPU devices
        rank: Node rank
        node_id: Node identifier
        host: Server host
        port: Server port
        service_class: InferenceService subclass to instantiate

    Returns:
        Initialized InferenceService instance
    """
    global inference_service, config, server_info, DEBUG

    config = cfg
    server_info = {
        "node_id": node_id,
        "rank": rank,
        "devices": devices,
        "host": host,
        "port": port,
        "status": "initializing",
    }

    DEBUG = config.get("debug", False)

    logger.info(f"[Server] Initializing inference service on devices: {devices}")

    if service_class is None:
        raise ValueError("service_class must be provided")

    inference_service = service_class(
        config=config,
        devices=devices,
        rank=rank,
    )

    logger.info("[Server] Inference service initialized")
    return inference_service


def create_app() -> web.Application:
    """Create and configure the aiohttp application."""
    app = web.Application()

    app.router.add_get("/", root_handler)
    app.router.add_get("/health", health_handler)
    app.router.add_get("/info", info_handler)
    app.router.add_post("/generate", generate_handler)

    app.on_startup.append(startup_handler)
    app.on_shutdown.append(shutdown_handler)

    return app


def get_app() -> web.Application:
    """Get or create the application instance."""
    return create_app()
