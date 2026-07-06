"""Shared workflow-launch utilities: config loading and Dragon GPU helpers."""

import os

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
