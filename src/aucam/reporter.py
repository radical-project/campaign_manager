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
    # Final report of a command-line program (aucam-report done): it has
    # finished with this exit code.  Lets a restarted AuCaM finish a workflow
    # it adopted (R1), whose task it no longer has a handle to.
    done: bool = False
    returncode: int | None = None


@dataclass(frozen=True)
class Message:
    """A message the controller sent to a running workflow (``WorkflowMessage``).

    ``seq`` increases by one per message sent to this workflow; ``payload`` is
    whatever the controller sent (JSON: numbers, strings, lists, dicts).
    """

    name: str
    payload: Any = None
    seq: int = 0
    sent_at: float = 0.0

    @classmethod
    def from_dict(cls, d: dict) -> Message:
        return cls(
            name=str(d.get("name", "")),
            payload=d.get("payload"),
            seq=int(d.get("seq", 0)),
            sent_at=float(d.get("sent_at", 0.0)),
        )


def unread(stored: list | None, cursor: int) -> tuple[list[Message], int]:
    """Messages in ``stored`` (dicts, oldest first) newer than ``cursor``, and the new cursor."""
    new = [Message.from_dict(m) for m in (stored or []) if int(m.get("seq", 0)) > cursor]
    return new, max([cursor] + [m.seq for m in new])


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
        on CampaignDecision.PAUSE by checkpointing then calling wait_for_resume(),
        and on CampaignDecision.CANCEL by saving what it needs and returning
        (it then has its type's cancel_grace_s before AuCaM cancels it).
        """
        ...

    async def messages(self) -> list[Message]:
        """Messages the controller sent since the last call, oldest first.

        Non-blocking; returns [] when there are none.  Call it where the
        workflow can act on them, e.g. at the start of each iteration.  A
        reporter that does not support messages always returns [].
        """
        return []

    async def wait_for_resume(self) -> CampaignDecision:
        """Block until the CM sends RESUME or CANCEL; return which one.

        A paused workflow that gets CANCEL should save what it needs and return.
        Yields to the event loop every 50 ms so the policy cycle can run.
        Subclasses may override for a backend-native wait (e.g. Redis BLPOP).
        Only meaningful after poll() returned PAUSE.
        """
        from .types import CampaignDecision  # noqa: PLC0415

        while True:
            sig = await self.poll()
            if sig in (CampaignDecision.RESUME, CampaignDecision.CANCEL):
                return sig
            await asyncio.sleep(0.05)


class LocalDictReporter(WorkflowReporter):
    """In-process reporter backed by shared dicts — for tests and local runs."""

    def __init__(
        self,
        wf_id: str,
        store: dict[str, WorkflowReport],
        signals: dict[str, Any],
        messages: dict[str, list] | None = None,
    ) -> None:
        self._wf_id = wf_id
        self._store = store
        self._signals = signals
        self._messages = messages if messages is not None else {}

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

    async def messages(self) -> list[Message]:
        # Same process and loop as AuCaM: take the whole list.
        return [Message.from_dict(m) for m in self._messages.pop(self._wf_id, [])]
