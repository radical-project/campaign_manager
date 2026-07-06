"""Pytest configuration and fixtures."""

import asyncio
import sys
from io import StringIO
from pathlib import Path

import pytest

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        "model_path": "facebook/esm2_t33_650M_UR50D",
        "num_services": 1,
        "num_gpus_per_service": 1,
        "num_workers_per_gpu": 2,
        "server_port": 8000,
        "max_concurrent": 64,
        "timeout": 600,
        "max_retries": 3,
        "num_batches": 10,
        "max_batch_tokens": 1000,
        "use_streaming": False,
        "output_dir": "/tmp/spherical_test_outputs",
        "results_dir": "results",
        "metrics_dir": "/tmp/spherical_test_outputs",
        "metrics_file_prefix": "test_metrics",
        "metrics_log_interval": 10,
        "debug": True,
        "SEQUENCES": [
            "MKTFFVLLLAGAGAG",
            "MASQDVKIVVLGGLG",
            "MVHLTPEEKSAVTALWGKV",
        ],
    }


@pytest.fixture
def temp_dir(tmp_path):
    """Create a temporary directory for test outputs."""
    return tmp_path


@pytest.fixture
def output_stream():
    """Capture output stream for logger tests."""
    return StringIO()


@pytest.fixture
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
