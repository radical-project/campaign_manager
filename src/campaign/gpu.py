"""
Dragon GPU discovery and policy helpers.

Both functions degrade gracefully when Dragon is not installed or when
running under the ConcurrentExecutionBackend (local testing).
"""


def detect_gpus() -> int:
    """Count CUDA-visible GPUs for concurrent-mode assignment tracking. Returns 0 when none found."""
    try:
        import torch
        return torch.cuda.device_count()
    except Exception:
        pass
    try:
        import subprocess
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=index", "--format=csv,noheader"], text=True
        )
        return len(out.strip().splitlines())
    except Exception:
        return 0


def find_gpus() -> list[tuple[str, int]]:
    """Return [(hostname, gpu_id), ...] for every GPU visible to Dragon."""
    try:
        from dragon.native.machine import Node, System

        gpus = []
        for huid in System().nodes:
            node = Node(huid)
            for gpu_id in node.gpus:
                gpus.append((node.hostname, gpu_id))
        return gpus
    except Exception:
        return []


def make_policies(gpu_pool: list[tuple[str, int]], gpu_ids: list[int]) -> list:
    """Build a single Dragon Policy covering all assigned GPU IDs.

    Returns a list with exactly one Policy whose gpu_affinity lists every
    assigned GPU.  Returns an empty list when gpu_ids is empty or Dragon is
    not available.

    Logs at WARNING when Dragon is importable but Policy construction fails
    so misconfigurations (wrong arg names after a Dragon API change, etc.)
    are diagnosable instead of silent.
    """
    if not gpu_ids or not gpu_pool:
        return []
    try:
        from dragon.infrastructure.policy import Policy
    except ImportError:
        # Concurrent backend or no Dragon installed — silent.
        return []
    try:
        hostname = gpu_pool[0][0]
        return [
            Policy(
                placement=Policy.Placement.HOST_NAME,
                host_name=hostname,
                gpu_affinity=list(gpu_ids),
            )
        ]
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning(
            "make_policies failed for gpu_ids=%s host=%s: %s: %s",
            gpu_ids, gpu_pool[0][0] if gpu_pool else "?",
            type(exc).__name__, exc,
        )
        return []
