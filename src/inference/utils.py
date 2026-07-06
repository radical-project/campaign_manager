#!/usr/bin/env python3
"""
Utility functions for spherical inference framework.

Contains:
- Configuration loading
- Directory management
- GPU device utilities
- Metrics export
"""

import asyncio
import json
import os
import shutil
import socket
from pathlib import Path
from typing import Any, Optional

import yaml

from src.utils.workflow import _expand_env


def read_slurm_config(path: str | Path) -> dict[str, Any]:
    with open(path) as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError("Top-level YAML must be a mapping")

    return data


def load_config(config_path: str) -> dict[str, Any]:
    """
    Load configuration from YAML file with defaults.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Configuration dictionary with defaults merged
    """
    defaults = {
        "model_path": "facebook/esm2_t33_650M_UR50D",
        "num_services": 1,
        "num_gpus_per_service": 1,
        "num_workers_per_gpu": 2,
        "server_port": 8000,
        "max_concurrent": 64,
        "timeout": 600,
        "max_retries": 3,
        "num_batches": 100,
        "max_batch_tokens": 16000,
        "use_streaming": False,
        "output_dir": "outputs",
        "results_dir": "results",
        "metrics_dir": "outputs",
        "metrics_file_prefix": "metrics",
        "metrics_log_interval": 10,
        "debug": False,
        "engine": "concurrent",
        "SEQUENCES": [],
    }

    config = dict(defaults)

    if os.path.exists(config_path):
        with open(config_path) as f:
            user_config = _expand_env(yaml.safe_load(f) or {})
            config.update(user_config)

    return config


def ensure_dir(path: Path, clean: bool = True) -> Path:
    """
    Ensure directory exists, creating if necessary and clearing contents.

    Args:
        path: Path to directory

    Returns:
        The path that was created/cleared
    """
    path = Path(path)

    if clean and path.exists():
        # ignore_errors: multiple nodes may clean the same shared-filesystem
        # directory concurrently, so files can disappear mid-iteration.
        shutil.rmtree(path, ignore_errors=True)

    path.mkdir(parents=True, exist_ok=True)
    return path


def detect_device_type() -> str:
    """
    Detect the best available device type.

    Returns:
        'cuda' if CUDA GPUs are available, 'cpu' otherwise
    """
    try:
        import torch

        # Check that torch.cuda.is_available() returns an actual boolean
        is_available = torch.cuda.is_available()
        if isinstance(is_available, bool) and is_available:
            return "cuda"
    except (ImportError, AttributeError):
        pass
    return "cpu"


def get_available_device_count() -> int:
    """
    Get the number of available compute devices.

    Returns:
        Number of CUDA GPUs if available, else 1 (for CPU)
    """
    try:
        import torch

        if torch.cuda.is_available():
            count = torch.cuda.device_count()
            # Ensure we get an actual integer (handles mocked torch)
            if isinstance(count, int):
                return count
    except (ImportError, AttributeError):
        pass
    return 1


def get_devices_for_node(config: dict[str, Any], node_rank: int = 0) -> list[str]:
    """
    Get list of device strings for a node, auto-detecting GPU/CPU.

    Args:
        config: Configuration dictionary
        node_rank: Node rank (unused when explicit count provided)

    Returns:
        List of device strings (e.g., ['cuda:0', 'cuda:1'] or ['cpu'])
    """
    num_devices = config.get("num_gpus_per_service", 1)
    # Ensure num_devices is an integer (handles mocked config)
    if not isinstance(num_devices, int):
        num_devices = 1

    device_type = detect_device_type()

    if device_type == "cuda":
        available_gpus = get_available_device_count()
        num_devices = min(num_devices, available_gpus)
        return [f"cuda:{i}" for i in range(num_devices)]
    else:
        # CPU mode - return list of 'cpu' devices for worker distribution
        return ["cpu"] * num_devices


def get_gpus_for_node(config: dict[str, Any], node_rank: int = 0) -> list[str]:
    """
    Get list of device strings for a node (alias for get_devices_for_node).

    Args:
        config: Configuration dictionary
        node_rank: Node rank (unused when explicit count provided)

    Returns:
        List of device strings (e.g., ['cuda:0', 'cuda:1'] or ['cpu'])
    """
    return get_devices_for_node(config, node_rank)


def get_slurm_nodes(config: dict[str, Any]) -> list[str]:
    """
    Get list of nodes from SLURM environment.

    Returns:
        List of node hostnames, or ['localhost'] if not in SLURM
    """

    engine = config.get("engine", "").lower()
    if "dragon" in engine:
        # System().nodes returns huids (integer node IDs), not hostnames.
        # Node(huid).hostname gives the real string name Dragon's policy
        # evaluator expects.  Under `dragon -s` (single-node) the hostname
        # is 'localhost' which resolves to host_id=-1 and causes a ~54 s
        # scheduling timeout; substitute the real hostname in that case.
        from dragon.native.machine import Node, System

        nodes = []
        for huid in System().nodes:
            node = Node(huid)
            nodes.append(node.hostname)  # keep 'localhost' as-is; handled in _launch_servers_dragon
        return nodes

    else:
        # Non-Dragon engines: use SLURM env vars directly
        nodelist = os.environ.get("SLURM_JOB_NODELIST")

        if not nodelist:
            return [socket.getfqdn()]

        import subprocess

        try:
            result = subprocess.run(
                ["scontrol", "show", "hostnames", nodelist],
                capture_output=True,
                text=True,
                check=True,
            )
            nodes = result.stdout.strip().split("\n")
            return [n for n in nodes if n]
        except (subprocess.CalledProcessError, FileNotFoundError):
            return [socket.getfqdn()]


async def export_metrics(output_path: Path, metrics: dict[str, Any]) -> None:
    """
    Export metrics to JSON file asynchronously.

    Args:
        output_path: Path to output JSON file
        metrics: Metrics dictionary to export
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    def _write():
        with open(output_path, "w") as f:
            json.dump(metrics, f, indent=2, default=str)

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _write)


def init_collector(collector_dir: str) -> Optional[Any]:
    """
    Initialize telemetry collector if available.

    Args:
        collector_dir: Directory for telemetry output

    Returns:
        Collector instance or None if not available
    """
    try:
        from rhapsody.backends import DragonTelemetryCollector

        Path(collector_dir).mkdir(parents=True, exist_ok=True)

        collector = DragonTelemetryCollector(
            collection_rate=1.0,  # Collect every second
            checkpoint_interval=30.0,  # Checkpoint every 30 seconds
            checkpoint_dir=collector_dir,  # Save checkpoints here
            checkpoint_count=150,  # Keep last 10 checkpoints
            enable_cpu=True,
            enable_gpu=True,
            enable_memory=False,
            metric_prefix="SPHERICAL-inference",  # Prefix all metrics
        )
        return collector

    except ImportError:
        return None
    except Exception as e:
        # DragonTelemetryCollector requires Dragon runtime (e.g. GS_CD launch param).
        print(f"[WARN] Telemetry collector unavailable (Dragon runtime not active): {e}")
        return None
