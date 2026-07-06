"""Tests for server module."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp import web

from src.inference.inference_service import InferenceService

# Stub service for init_server tests — avoids importing heavy deps.
with patch.dict("sys.modules", {"src.inference.inference_service": MagicMock()}):
    from src.inference.server import (
        create_app,
        generate_handler,
        health_handler,
        info_handler,
        init_server,
        root_handler,
    )


class _MinimalService(InferenceService):
    """Minimal concrete service for init_server tests."""

    def _load_models(self):
        self.models = {}

    def process_batch_sync(self, batch_id, device, batch_data=None):
        pass

    async def generate_batch(self):
        raise StopAsyncIteration


class TestServerHandlers:
    @pytest.mark.asyncio
    async def test_root_handler_returns_service_info(self):
        """Root endpoint returns JSON with service name and endpoint list."""
        request = MagicMock()
        response = await root_handler(request)
        assert response.status == 200
        data = json.loads(response.body)
        assert data["service"] == "Inference Server"
        assert "health" in data["endpoints"]
        assert "generate" in data["endpoints"]

    @pytest.mark.asyncio
    async def test_health_handler_no_service_returns_503(self):
        request = MagicMock()
        with patch("src.inference.server.inference_service", None):
            response = await health_handler(request)
        assert response.status == 503
        data = json.loads(response.body)
        assert data["status"] == "unavailable"

    @pytest.mark.asyncio
    async def test_health_handler_with_service_returns_200(self):
        """Initialized service returns healthy status with worker counts."""
        request = MagicMock()
        mock_svc = MagicMock()
        mock_svc.workers = [MagicMock(is_busy=False), MagicMock(is_busy=True)]
        mock_svc.work_queue.qsize.return_value = 3
        mock_svc.logger.metrics = {"requests": 50}

        with patch("src.inference.server.inference_service", mock_svc):
            response = await health_handler(request)

        assert response.status == 200
        data = json.loads(response.body)
        assert data["status"] == "healthy"
        assert data["active_workers"] == 1  # one is_busy=True
        assert data["total_workers"] == 2
        assert data["queue_depth"] == 3
        assert data["total_processed"] == 50

    @pytest.mark.asyncio
    async def test_info_handler_no_service_returns_503(self):
        request = MagicMock()
        with patch("src.inference.server.inference_service", None):
            response = await info_handler(request)
        assert response.status == 503

    @pytest.mark.asyncio
    async def test_info_handler_with_service_returns_200(self):
        """Initialized service returns detailed info including metrics."""
        request = MagicMock()
        mock_svc = MagicMock()
        mock_svc.num_workers_per_gpu = 2
        mock_svc.workers = [MagicMock(), MagicMock()]
        mock_svc.logger.metrics = {"requests": 10}

        with patch("src.inference.server.inference_service", mock_svc):
            with patch(
                "src.inference.server.server_info",
                {
                    "node_id": "n0",
                    "rank": 0,
                    "devices": ["cuda:0"],
                    "host": "localhost",
                    "port": 9000,
                },
            ):
                response = await info_handler(request)

        assert response.status == 200
        data = json.loads(response.body)
        assert data["node_id"] == "n0"
        assert data["num_workers_per_gpu"] == 2
        assert data["total_workers"] == 2

    @pytest.mark.asyncio
    async def test_generate_handler_missing_batch_ids_returns_400(self):
        """POST /generate with no batch_ids or batch returns 400."""
        request = MagicMock()
        request.json = AsyncMock(return_value={"timeout": 30})  # no batch_ids, no batch

        mock_svc = MagicMock()
        mock_svc.work_queue.qsize.return_value = 0

        with patch("src.inference.server.inference_service", mock_svc):
            response = await generate_handler(request)

        assert response.status == 400
        data = json.loads(response.body)
        assert data["status"] == "error"
        assert "batch_id" in data["message"].lower() or "batch" in data["message"].lower()

    @pytest.mark.asyncio
    async def test_generate_handler_empty_batch_ids_returns_400(self):
        """POST /generate with empty batch_ids list returns 400."""
        request = MagicMock()
        request.json = AsyncMock(return_value={"batch_ids": [], "timeout": 30})

        mock_svc = MagicMock()
        mock_svc.work_queue.qsize.return_value = 0

        with patch("src.inference.server.inference_service", mock_svc):
            response = await generate_handler(request)

        assert response.status == 400


class TestCreateApp:
    def test_create_app_returns_application(self):
        app = create_app()
        assert isinstance(app, web.Application)

    def test_create_app_has_required_routes(self):
        app = create_app()
        routes = [r.resource.canonical for r in app.router.routes() if hasattr(r, "resource")]
        assert "/" in routes
        assert "/health" in routes
        assert "/info" in routes
        assert "/generate" in routes

    def test_create_app_has_lifecycle_handlers(self):
        app = create_app()
        assert len(app.on_startup) > 0
        assert len(app.on_shutdown) > 0

    def test_init_server_creates_service(self, sample_config, temp_dir):
        """init_server instantiates the given service_class and sets module globals."""
        import src.inference.server as server_module

        sample_config["output_dir"] = str(temp_dir / "outputs")
        sample_config["metrics_dir"] = str(temp_dir / "metrics")
        sample_config["results_dir"] = str(temp_dir / "results")

        svc = init_server(
            cfg=sample_config,
            devices=["cuda:0"],
            rank=0,
            node_id="node-0",
            host="localhost",
            port=8001,
            service_class=_MinimalService,
        )

        assert svc is not None
        assert isinstance(svc, _MinimalService)
        assert server_module.inference_service is svc
        assert server_module.server_info["node_id"] == "node-0"
        assert server_module.server_info["port"] == 8001
        assert server_module.server_info["rank"] == 0

    def test_init_server_requires_service_class(self, sample_config):
        with pytest.raises(ValueError, match="service_class"):
            init_server(
                cfg=sample_config,
                devices=["cuda:0"],
                rank=0,
                node_id="n",
                host="h",
                port=8000,
                service_class=None,
            )
