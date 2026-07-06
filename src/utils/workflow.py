"""Shared workflow-launch utilities: config loading, filesystem helpers, and Dragon GPU helpers."""

import asyncio
import json
import os
import shutil
from pathlib import Path
from typing import Any, Optional

import yaml


def _expand_env(obj):
    """Recursively expand ${VAR} / $VAR references in string config values."""
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    if isinstance(obj, dict):
        return {k: _expand_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env(item) for item in obj]
    return obj


def load_config(path: str) -> dict:
    with open(path) as f:
        return _expand_env(yaml.safe_load(f) or {})


def ensure_dir(path: Path, clean: bool = True) -> Path:
    """Ensure directory exists, creating if necessary. With clean=True, removes existing contents."""
    path = Path(path)
    if clean and path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    return path


def read_slurm_config(path: str | Path) -> dict[str, Any]:
    """Load a YAML file that must be a top-level mapping."""
    with open(path) as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError("Top-level YAML must be a mapping")
    return data


async def export_metrics(output_path: Path, metrics: dict[str, Any]) -> None:
    """Export metrics dict to a JSON file asynchronously."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    def _write():
        with open(output_path, "w") as f:
            json.dump(metrics, f, indent=2, default=str)

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _write)


def detect_device_type() -> str:
    """Return 'cuda' if CUDA GPUs are available, 'cpu' otherwise."""
    try:
        import torch
        is_available = torch.cuda.is_available()
        if isinstance(is_available, bool) and is_available:
            return "cuda"
    except (ImportError, AttributeError):
        pass
    return "cpu"


def get_available_device_count() -> int:
    """Return the number of available CUDA GPUs, or 1 for CPU-only."""
    try:
        import torch
        if torch.cuda.is_available():
            count = torch.cuda.device_count()
            if isinstance(count, int):
                return count
    except (ImportError, AttributeError):
        pass
    return 1


def get_devices_for_node(config: dict[str, Any], node_rank: int = 0) -> list[str]:
    """Return device strings (e.g. ['cuda:0', 'cuda:1'] or ['cpu', 'cpu']) for a node."""
    num_devices = config.get("num_gpus_per_service", 1)
    if not isinstance(num_devices, int):
        num_devices = 1
    device_type = detect_device_type()
    if device_type == "cuda":
        available = get_available_device_count()
        num_devices = min(num_devices, available)
        return [f"cuda:{i}" for i in range(num_devices)]
    return ["cpu"] * num_devices


def get_gpus_for_node(config: dict[str, Any], node_rank: int = 0) -> list[str]:
    """Alias for get_devices_for_node."""
    return get_devices_for_node(config, node_rank)


def find_gpus():
    """Return [(hostname, gpu_id), ...] for every GPU visible to Dragon.

    Uses Dragon's native machine API to enumerate all nodes and their GPUs.
    node.gpus may be None on CPU-only nodes (e.g. login nodes), so we guard
    with `or []` to skip them safely.

    Under `dragon -s` (single-node mode) node.hostname returns 'localhost',
    which resolves to host_id=-1 and causes a ~54 s scheduling timeout per
    task.  We substitute the real hostname in that case.
    """
    import socket

    from dragon.native.machine import Node, System

    real_hostname = socket.gethostname()
    all_gpus = []
    for huid in System().nodes:
        node = Node(huid)
        hostname = node.hostname if node.hostname != "localhost" else real_hostname
        for gpu_id in node.gpus or []:
            all_gpus.append((hostname, gpu_id))
    return all_gpus


def make_policies(all_gpus, nprocs=32):
    """Create one Policy per mutation slot with round-robin GPU assignment.

    Each policy pins a worker to a specific node (HOST_NAME) and GPU
    (gpu_affinity) so that Dragon routes each mutation to the correct
    node/GPU in multi-node runs.

    HOST_NAME must be the actual compute node hostname (e.g. 'gpub001').
    Using 'localhost' resolves to host_id=-1 on single-node Dragon (-s)
    and causes a ~54 s scheduling timeout — always pass the real hostname
    returned by find_gpus().
    """
    from dragon.infrastructure.policy import Policy

    policies = []
    i = 0
    for _ in range(nprocs):
        hostname, gpu_id = all_gpus[i]
        policies.append(
            Policy(
                placement=Policy.Placement.HOST_NAME,
                host_name=hostname,
                gpu_affinity=[gpu_id],
            )
        )
        i = (i + 1) % len(all_gpus)
    return policies
