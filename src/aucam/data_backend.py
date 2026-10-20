"""DataBackend: read/write side of the CM ↔ workflow telemetry channel."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from .reporter import LocalDictReporter, WorkflowReport, WorkflowReporter
from .types import CampaignDecision


def _decode_report(raw: bytes | None) -> dict | None:
    """Decode a report stored in Redis.

    Command-line workflows write UTF-8 JSON (the language-neutral protocol of
    ``src.aucam_client``); Python reporters write a pickled dict.
    """
    if not raw:
        return None
    if raw[:1] == b"\x80":                 # pickle protocol header
        import pickle  # noqa: PLC0415
        return pickle.loads(raw)
    import json  # noqa: PLC0415
    return json.loads(raw.decode("utf-8"))


@runtime_checkable
class DataBackend(Protocol):
    """Protocol satisfied by all concrete data-backend implementations."""

    # ── CM → workflow signal channel ──────────────────────────────────────────

    async def write_signal(self, wf_id: str, decision: CampaignDecision) -> None:
        """Deliver a CM decision (PAUSE / RESUME) to the workflow."""
        ...

    async def poll_signal(self, wf_id: str) -> CampaignDecision | None:
        """Peek at the pending signal for wf_id without consuming it."""
        ...

    async def clear_signal(self, wf_id: str) -> None:
        """Remove the pending signal (called by the reporter after consumption)."""
        ...

    # ── Workflow → CM report channel ──────────────────────────────────────────

    def make_reporter(self, wf_id: str) -> WorkflowReporter:
        """Create a reporter the workflow uses to push scores and poll signals."""
        ...

    async def get_report(self, wf_id: str) -> WorkflowReport | None:
        """Return the latest report for wf_id, or None if none yet."""
        ...

    async def delete_report(self, wf_id: str) -> None:
        """Remove the report entry when a workflow finishes or is cancelled."""
        ...

    # ── Connection descriptor ─────────────────────────────────────────────────

    def endpoint(self) -> str | None:
        """Return the connection string for launch_cmd workflows (None = in-process)."""
        ...


class LocalDictBackend:
    """
    In-process backend backed by plain dicts.

    Safe for asyncio (single event-loop, no threads).  Used as the automatic
    fallback in tests and single-node runs where Dragon/Redis are unavailable.
    """

    def __init__(self) -> None:
        self._store: dict[str, WorkflowReport] = {}
        self._signals: dict[str, CampaignDecision] = {}

    def endpoint(self) -> None:
        return None

    # ── Signal channel ────────────────────────────────────────────────────────

    async def write_signal(self, wf_id: str, decision: CampaignDecision) -> None:
        self._signals[wf_id] = decision

    async def poll_signal(self, wf_id: str) -> CampaignDecision | None:
        return self._signals.get(wf_id)

    async def clear_signal(self, wf_id: str) -> None:
        self._signals.pop(wf_id, None)

    # ── Report channel ────────────────────────────────────────────────────────

    def make_reporter(self, wf_id: str) -> WorkflowReporter:
        return LocalDictReporter(wf_id=wf_id, store=self._store, signals=self._signals)

    def sync_get_report(self, wf_id: str) -> WorkflowReport | None:
        """Synchronous read for use in done-callbacks (in-process only)."""
        return self._store.get(wf_id)

    async def get_report(self, wf_id: str) -> WorkflowReport | None:
        return self._store.get(wf_id)

    async def delete_report(self, wf_id: str) -> None:
        self._store.pop(wf_id, None)


# ---------------------------------------------------------------------------
# HPC data backends — CM ↔ workflow communication on HPC nodes.
# ---------------------------------------------------------------------------

class RadexDragonBackend:
    """
    CM ↔ workflow communication channel backed by radex DragonClient.

    Usage (head node):
        rdb = await DragonDataBackend(...)         # Rhapsody owns the DDict
        descriptor = rdb.endpoints[0].serialize()
        backend = RadexDragonBackend(descriptor)
        spec = CampaignSpec(..., data_backend=backend)

    Radex has no delete for picklable items; clear_signal and delete_report
    overwrite with None as a consumed sentinel.
    """

    def __init__(self, descriptor: str) -> None:
        import threading  # noqa: PLC0415
        self._descriptor = descriptor
        self._lock = threading.Lock()
        self._client: Any = None

    def _get_client(self) -> Any:
        if self._client is None:
            from radex.clients.core import DragonClient  # noqa: PLC0415
            self._client = DragonClient(self._descriptor, 5)
        return self._client

    def endpoint(self) -> str:
        return self._descriptor

    def close(self) -> None:
        """Release the persistent DragonClient before DDict teardown.

        Must be called before the backing DDict is destroyed (i.e. before
        DragonDataBackend.shutdown()), otherwise the C++ destructor tries to
        detach from an already-destroyed DDict and prints DRAGON_OBJECT_DESTROYED.
        """
        with self._lock:
            self._client = None

    # ── Signal channel (CM → workflow) ────────────────────────────────────────

    async def write_signal(self, wf_id: str, decision: CampaignDecision) -> None:
        import asyncio  # noqa: PLC0415

        lock, value = self._lock, decision.value

        def _write() -> None:
            with lock:
                self._get_client().put_picklable(f"{wf_id}:signal", value)

        await asyncio.to_thread(_write)

    async def poll_signal(self, wf_id: str) -> CampaignDecision | None:
        import asyncio  # noqa: PLC0415

        lock, key = self._lock, f"{wf_id}:signal"

        def _peek():
            with lock:
                client = self._get_client()
                if not client.contains(key):
                    return None
                return client.get_picklable(key)

        raw = await asyncio.to_thread(_peek)
        return CampaignDecision(raw) if raw else None

    async def clear_signal(self, wf_id: str) -> None:
        import asyncio  # noqa: PLC0415

        lock, key = self._lock, f"{wf_id}:signal"

        def _clear() -> None:
            with lock:
                client = self._get_client()
                if client.contains(key):
                    client.put_picklable(key, None)

        await asyncio.to_thread(_clear)

    # ── Report channel (workflow → CM) ────────────────────────────────────────

    def make_reporter(self, wf_id: str) -> "RadexDragonReporter":
        return RadexDragonReporter(wf_id=wf_id, descriptor=self._descriptor)

    async def get_report(self, wf_id: str) -> WorkflowReport | None:
        import asyncio  # noqa: PLC0415

        lock = self._lock

        def _read():
            with lock:
                client = self._get_client()
                if not client.contains(wf_id):
                    return None
                return client.get_picklable(wf_id)

        raw = await asyncio.to_thread(_read)
        if raw is None:
            return None
        return WorkflowReport(**raw)

    async def delete_report(self, wf_id: str) -> None:
        import asyncio  # noqa: PLC0415

        lock, sig_key = self._lock, f"{wf_id}:signal"

        def _del() -> None:
            with lock:
                client = self._get_client()
                if client.contains(wf_id):
                    client.put_picklable(wf_id, None)
                if client.contains(sig_key):
                    client.put_picklable(sig_key, None)

        await asyncio.to_thread(_del)


DragonDictBackend = RadexDragonBackend


class RadexDragonReporter(WorkflowReporter):
    """Reporter using radex DragonClient inside Dragon worker processes.

    Synchronous calls only — no asyncio.to_thread: Dragon worker processes
    are dedicated, and to_thread doesn't work in Dragon's function_task env.
    """

    def __init__(self, wf_id: str, descriptor: str) -> None:
        self._wf_id = wf_id
        self._descriptor = descriptor

    async def emit(
        self,
        *,
        score: Any = None,
        metrics: dict | None = None,
        iteration: int = 0,
    ) -> None:
        import time as _time  # noqa: PLC0415
        from radex.clients.core import DragonClient  # noqa: PLC0415

        payload = {
            "wf_id": self._wf_id,
            "score": score,
            "metrics": metrics or {},
            "emitted_at": _time.time(),
            "iteration": iteration,
        }
        DragonClient(self._descriptor, 5).put_picklable(self._wf_id, payload)

    async def poll(self) -> CampaignDecision | None:
        from radex.clients.core import DragonClient  # noqa: PLC0415

        key = f"{self._wf_id}:signal"
        client = DragonClient(self._descriptor, 1)
        if not client.contains(key):
            return None
        val = client.get_picklable(key)
        if val is None:
            return None
        client.put_picklable(key, None)
        return CampaignDecision(val)


DragonDictReporter = RadexDragonReporter


class RhapsodyRedisBackend:
    """
    DataBackend backed by a Redis instance managed by Rhapsody.

    Usage:
        descriptor = redis_data_backend.endpoints[0].serialize()  # "host:port"
        backend = RhapsodyRedisBackend(descriptor)
        spec = CampaignSpec(..., data_backend=backend)
    """

    def __init__(self, descriptor: str) -> None:
        host, port = descriptor.split(":")
        self._host = host
        self._port = int(port)

    def endpoint(self) -> str:
        return f"{self._host}:{self._port}"

    # ── Signal channel ────────────────────────────────────────────────────────

    async def write_signal(self, wf_id: str, decision: CampaignDecision) -> None:
        import asyncio  # noqa: PLC0415
        host, port = self._host, self._port

        def _write():
            import redis  # noqa: PLC0415
            redis.Redis(host=host, port=port, protocol=2).set(f"{wf_id}:signal", decision.value)

        await asyncio.to_thread(_write)

    async def poll_signal(self, wf_id: str) -> CampaignDecision | None:
        import asyncio  # noqa: PLC0415
        host, port = self._host, self._port

        def _peek():
            import redis  # noqa: PLC0415
            raw = redis.Redis(host=host, port=port, protocol=2).get(f"{wf_id}:signal")
            return raw.decode() if raw else None

        raw = await asyncio.to_thread(_peek)
        return CampaignDecision(raw) if raw else None

    async def clear_signal(self, wf_id: str) -> None:
        import asyncio  # noqa: PLC0415
        host, port = self._host, self._port

        def _del():
            import redis  # noqa: PLC0415
            redis.Redis(host=host, port=port, protocol=2).delete(f"{wf_id}:signal")

        await asyncio.to_thread(_del)

    # ── Report channel ────────────────────────────────────────────────────────

    def make_reporter(self, wf_id: str) -> "RhapsodyRedisReporter":
        return RhapsodyRedisReporter(
            wf_id=wf_id, host=self._host, port=self._port
        )

    async def get_report(self, wf_id: str) -> WorkflowReport | None:
        import asyncio  # noqa: PLC0415

        host, port = self._host, self._port

        def _read():
            import redis  # noqa: PLC0415
            client = redis.Redis(host=host, port=port, protocol=2)
            return _decode_report(client.get(wf_id))

        data = await asyncio.to_thread(_read)
        if data is None:
            return None
        return WorkflowReport(**data)

    async def delete_report(self, wf_id: str) -> None:
        import asyncio  # noqa: PLC0415

        host, port = self._host, self._port

        def _del():
            import redis  # noqa: PLC0415
            r = redis.Redis(host=host, port=port, protocol=2)
            r.delete(wf_id)
            r.delete(f"{wf_id}:signal")

        await asyncio.to_thread(_del)


class RhapsodyRedisReporter(WorkflowReporter):
    """Reporter that reads/writes Redis via redis-py (blocking, wrapped in to_thread)."""

    def __init__(self, wf_id: str, host: str, port: int) -> None:
        self._wf_id = wf_id
        self._host = host
        self._port = port

    async def emit(
        self,
        *,
        score: Any = None,
        metrics: dict | None = None,
        iteration: int = 0,
    ) -> None:
        import asyncio  # noqa: PLC0415
        import pickle  # noqa: PLC0415
        import time as _time  # noqa: PLC0415

        payload = {
            "wf_id": self._wf_id,
            "score": score,
            "metrics": metrics or {},
            "emitted_at": _time.time(),
            "iteration": iteration,
        }
        host, port, wf_id = self._host, self._port, self._wf_id

        def _write():
            import redis  # noqa: PLC0415
            redis.Redis(host=host, port=port, protocol=2).set(wf_id, pickle.dumps(payload))

        await asyncio.to_thread(_write)

    async def poll(self) -> CampaignDecision | None:
        import asyncio  # noqa: PLC0415
        host, port, wf_id = self._host, self._port, self._wf_id

        def _pop():
            import redis  # noqa: PLC0415
            r = redis.Redis(host=host, port=port, protocol=2)
            key = f"{wf_id}:signal"
            raw = r.get(key)
            if raw:
                r.delete(key)
            return raw.decode() if raw else None

        raw = await asyncio.to_thread(_pop)
        return CampaignDecision(raw) if raw else None
