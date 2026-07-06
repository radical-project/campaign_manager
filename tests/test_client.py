"""Tests for InferenceClient and ESM2Client."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.inference.esm2_service.esm2_client import ESM2Client
from src.inference.inference_client import InferenceClient

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_asyncflow():
    """Return a minimal mock asyncflow that records @function_task registrations."""
    af = MagicMock()
    # function_task is used as a decorator; return the decorated function unchanged.
    af.function_task = lambda fn: fn
    return af


def _make_client(endpoints=None, rank=0, asyncflow=None, service=None, config=None):
    af = asyncflow or _make_asyncflow()
    cfg = config or {"debug": False, "max_concurrent": 4, "timeout": 30, "max_retries": 1}
    return InferenceClient(
        endpoints=endpoints or ["http://localhost:8000"],
        rank=rank,
        service=service,
        config=cfg,
        asyncflow=af,
    )


# ---------------------------------------------------------------------------
# InferenceClient — init
# ---------------------------------------------------------------------------


class TestInferenceClientInit:
    def test_init_stores_endpoints(self):
        client = _make_client(endpoints=["http://a:8000", "http://b:8000"])
        assert client.endpoints == ["http://a:8000", "http://b:8000"]

    def test_init_without_asyncflow_logs_critical(self, output_stream):
        # asyncflow=None means _register_client is skipped; no exception raised
        client = InferenceClient(
            endpoints=["http://localhost:8000"],
            rank=0,
            config={},
            asyncflow=None,
        )
        assert client.flow is None
        assert not hasattr(client, "_client_req_task")

    def test_init_with_asyncflow_registers_task(self):
        af = _make_asyncflow()
        client = _make_client(asyncflow=af)
        assert hasattr(client, "_client_req_task")
        assert callable(client.client_req)

    def test_init_metrics_zeroed(self):
        client = _make_client()
        assert client.metrics["submitted"] == 0
        assert client.metrics["successful"] == 0
        assert client.metrics["failed"] == 0
        assert client.metrics["retries"] == 0
        assert client.metrics["error_msgs"] == []

    def test_init_empty_endpoints_no_cycle(self):
        client = InferenceClient(
            endpoints=[],
            asyncflow=_make_asyncflow(),
            config={"debug": False},
        )
        assert client.endpoint_cycle is None

    def test_config_defaults_applied(self):
        client = InferenceClient(
            endpoints=[],
            asyncflow=_make_asyncflow(),
            config={},  # no overrides → use defaults
        )
        assert client.max_concurrent == 16
        assert client.timeout == 600
        assert client.max_retries == 3


# ---------------------------------------------------------------------------
# InferenceClient — _get_batch_data
# ---------------------------------------------------------------------------


class TestGetBatchData:
    def test_base_returns_none(self):
        client = _make_client()
        assert client._get_batch_data(0) is None
        assert client._get_batch_data(42) is None


# ---------------------------------------------------------------------------
# InferenceClient — health_check
# ---------------------------------------------------------------------------


class TestHealthCheck:
    @pytest.mark.asyncio
    async def test_healthy_endpoint_returns_true(self):
        client = _make_client()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"status": "healthy", "total_workers": 2})
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await client.health_check("http://localhost:8000")

        assert result is True

    @pytest.mark.asyncio
    async def test_unhealthy_status_returns_false(self):
        client = _make_client()
        mock_resp = AsyncMock()
        mock_resp.status = 503
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await client.health_check("http://localhost:8000")

        assert result is False

    @pytest.mark.asyncio
    async def test_connection_error_returns_false(self):
        client = _make_client()
        mock_session = AsyncMock()
        mock_session.get = MagicMock(side_effect=Exception("connection refused"))
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await client.health_check("http://localhost:8000")

        assert result is False


# ---------------------------------------------------------------------------
# ESM2Client — _get_batch_data
# ---------------------------------------------------------------------------


class TestESM2ClientGetBatchData:
    def _make_esm2_client(self, service=None):
        af = _make_asyncflow()
        return ESM2Client(
            endpoints=["http://localhost:8000"],
            rank=0,
            service=service,
            config={"debug": False, "max_concurrent": 4, "timeout": 30, "max_retries": 1},
            asyncflow=af,
        )

    def test_returns_none_when_not_client_mode(self):
        svc = MagicMock()
        svc.client_mode = False
        client = self._make_esm2_client(service=svc)
        assert client._get_batch_data(0) is None

    def test_returns_none_when_no_service(self):
        client = self._make_esm2_client(service=None)
        assert client._get_batch_data(0) is None

    def test_non_streaming_serializes_single_batch(self):
        """Non-streaming: single_batch tensor dict serialized to lists."""

        svc = MagicMock()
        svc.client_mode = True
        svc.use_streaming = False

        # Simulate torch tensors with a .tolist() method
        fake_tensor = MagicMock()
        fake_tensor.tolist.return_value = [[1, 2, 3]]
        svc.single_batch = {"input_ids": fake_tensor, "attention_mask": fake_tensor}

        client = self._make_esm2_client(service=svc)
        data = client._get_batch_data(0)

        assert data is not None
        assert "input_ids" in data
        assert data["input_ids"] == [[1, 2, 3]]

    def test_non_streaming_caches_result(self):
        """_get_batch_data caches serialized batch so tolist() is called only once."""
        svc = MagicMock()
        svc.client_mode = True
        svc.use_streaming = False

        fake_tensor = MagicMock()
        fake_tensor.tolist.return_value = [[1, 2]]
        svc.single_batch = {"input_ids": fake_tensor}

        client = self._make_esm2_client(service=svc)
        _ = client._get_batch_data(0)
        _ = client._get_batch_data(1)  # second call

        # tolist should have been called exactly once (cache hit on second call)
        assert fake_tensor.tolist.call_count == 1

    def test_streaming_returns_per_batch_data(self):
        """Streaming mode: returns serialized data from batch_storage."""
        svc = MagicMock()
        svc.client_mode = True
        svc.use_streaming = True

        fake_tensor = MagicMock()
        fake_tensor.tolist.return_value = [[7, 8, 9]]
        svc.batch_storage = {3: {"input_ids": fake_tensor}}

        client = self._make_esm2_client(service=svc)
        data = client._get_batch_data(3)
        assert data == {"input_ids": [[7, 8, 9]]}

    def test_streaming_missing_batch_returns_none(self):
        svc = MagicMock()
        svc.client_mode = True
        svc.use_streaming = True
        svc.batch_storage = {}

        client = self._make_esm2_client(service=svc)
        assert client._get_batch_data(99) is None
