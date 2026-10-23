"""WorkflowReporter: mid-run telemetry channel from workflow to CM."""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .types import CampaignDecision


@dataclass
class WorkflowReport:
    """Snapshot emitted by a running workflow.

    `score` is the primary payload: a scalar float for simple policies, or a
    dict for richer data (e.g. {"loss": 0.23, "energy": -142.7, "converged": False}).
    `metrics` carries supplementary telemetry (timing, resource usage, etc.).
    """

    wf_id: str
    score: Any  # float, dict, numpy array, custom object — anything the workflow produces
    metrics: dict = field(default_factory=dict)
    emitted_at: float = field(default_factory=time.time)
    iteration: int = 0


class WorkflowReporter(ABC):
    """Injected into a block function so it can stream scores and receive signals."""

    @property
    def wf_id(self) -> str:
        return self._wf_id  # type: ignore[attr-defined]

    @abstractmethod
    async def emit(
        self,
        *,
        score: Any = None,
        metrics: dict | None = None,
        iteration: int = 0,
    ) -> None:
        """Push a score/metrics snapshot to the CM."""
        ...

    @abstractmethod
    async def poll(self) -> CampaignDecision | None:
        """Non-blocking check for a pending CM signal.

        Consumes and returns the signal if one is waiting, or returns None.
        The workflow should call this at the start of each iteration and act
        on CampaignDecision.PAUSE by checkpointing then calling wait_for_resume().
        """
        ...

    async def wait_for_resume(self) -> None:
        """Block until the CM sends a RESUME signal.

        Yields to the event loop every 50 ms so the policy cycle can run.
        Subclasses may override for a backend-native wait (e.g. Redis BLPOP).
        Only meaningful after poll() returned PAUSE.
        """
        from .types import CampaignDecision  # noqa: PLC0415

        while True:
            sig = await self.poll()
            if sig == CampaignDecision.RESUME:
                return
            await asyncio.sleep(0.05)


class LocalDictReporter(WorkflowReporter):
    """In-process reporter backed by shared dicts — for tests and local runs."""

    def __init__(
        self,
        wf_id: str,
        store: dict[str, WorkflowReport],
        signals: dict[str, Any],
    ) -> None:
        self._wf_id = wf_id
        self._store = store
        self._signals = signals

    async def emit(
        self,
        *,
        score: Any = None,
        metrics: dict | None = None,
        iteration: int = 0,
    ) -> None:
        self._store[self._wf_id] = WorkflowReport(
            wf_id=self._wf_id,
            score=score,
            metrics=metrics or {},
            emitted_at=time.time(),
            iteration=iteration,
        )

    async def poll(self) -> CampaignDecision | None:
        return self._signals.pop(self._wf_id, None)
