"""Subprocess reporter for Tier 2 workflows.

Usage inside a subprocess launched by the CM via WorkflowConfig(launch_cmd=...):

    from src.aucam.subprocess_client import SubprocessReporter

    reporter = SubprocessReporter()          # reads CM_WORKFLOW_ID / CM_DATA_ENDPOINT
    reporter.emit(score=0.85, metrics={...}) # sends to the CM's data backend
    sig = reporter.poll_signal()             # non-blocking signal check (None | str)
"""

from __future__ import annotations

import os
import pickle
import time
from typing import Any


class SubprocessReporter:
    """Synchronous reporter for subprocesses.

    Reads CM_WORKFLOW_ID, CM_DATA_ENDPOINT, and CM_DATA_BACKEND from the
    environment (set by the CM before exec-ing the subprocess).

    Supported backends:
      redis  — endpoint is "host:port"; requires redis-py
      dragon — endpoint is the serialized DDict descriptor; requires radex
    """

    def __init__(self) -> None:
        self._wf_id = os.environ["CM_WORKFLOW_ID"]
        endpoint = os.environ.get("CM_DATA_ENDPOINT")
        if endpoint is None:
            raise RuntimeError(
                "CM_DATA_ENDPOINT not set — is this process running inside an AuCaM CM launch?"
            )
        self._backend = os.environ.get("CM_DATA_BACKEND", "redis")
        self._endpoint = endpoint

    def _payload(
        self,
        score: Any,
        metrics: dict | None,
        iteration: int,
    ) -> bytes:
        return pickle.dumps(
            {
                "wf_id": self._wf_id,
                "score": score,
                "metrics": metrics or {},
                "emitted_at": time.time(),
                "iteration": iteration,
            }
        )

    def emit(
        self,
        *,
        score: Any = None,
        metrics: dict | None = None,
        iteration: int = 0,
    ) -> None:
        """Push a score/metrics snapshot to the CM (synchronous)."""
        data = self._payload(score, metrics, iteration)
        if self._backend == "redis":
            import redis  # noqa: PLC0415
            host, port = self._endpoint.rsplit(":", 1)
            redis.Redis(host=host, port=int(port), protocol=2).set(self._wf_id, data)
        elif self._backend == "dragon":
            from radex.clients.core import DragonClient  # noqa: PLC0415
            DragonClient(self._endpoint, 5).put_picklable(self._wf_id, pickle.loads(data))
        else:
            raise RuntimeError(f"Unknown CM_DATA_BACKEND: {self._backend!r}")

    def poll_signal(self) -> str | None:
        """Non-blocking check for a CM signal (PAUSE / CANCEL / RESUME).

        Returns the signal string, or None if no signal is waiting.
        Consuming a signal clears it from the backend.
        """
        if self._backend == "redis":
            import redis  # noqa: PLC0415
            host, port = self._endpoint.rsplit(":", 1)
            r = redis.Redis(host=host, port=int(port), protocol=2)
            key = f"{self._wf_id}:signal"
            raw = r.get(key)
            if raw:
                r.delete(key)
            return raw.decode() if raw else None
        if self._backend == "dragon":
            from radex.clients.core import DragonClient  # noqa: PLC0415
            key = f"{self._wf_id}:signal"
            client = DragonClient(self._endpoint, 1)
            if not client.contains(key):
                return None
            val = client.get_picklable(key)
            if val is None:
                return None
            client.put_picklable(key, None)
            return val
        return None
