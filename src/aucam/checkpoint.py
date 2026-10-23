"""Campaign checkpoint: serializable state written after each policy cycle.

Only serializable state lives here — queue, active workflow metadata, counters,
and spent budget.  The CampaignSpec (which carries callables and the controller
object) is NOT serialized; the driver must provide the same spec on recovery.

Atomic write strategy: write to <path>.tmp then os.replace() to <path>.
os.replace() is atomic on POSIX (rename(2)) so a partial write is never
visible to the reader.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

CHECKPOINT_VERSION = 1


@dataclass
class ActiveWorkflowRecord:
    """Metadata for one workflow that was running when the checkpoint was written."""

    wf_id: str
    workflow_type: str
    started_at: float
    # Declared spending of the workflow (its in-flight spending estimate).
    declared_spending: dict[str, float] = field(default_factory=dict)


@dataclass
class QueuedRequestRecord:
    """Serializable snapshot of one queued LaunchRequest."""

    workflow_type: str
    priority: str  # LaunchPriority.value
    enqueued_at: float
    context: dict
    requester: str


@dataclass
class CampaignCheckpoint:
    """Snapshot of a campaign's mutable runtime state."""

    campaign_name: str
    written_at: float = field(default_factory=time.time)
    version: int = CHECKPOINT_VERSION

    # Queue state
    queue: list[QueuedRequestRecord] = field(default_factory=list)

    # Active workflows at checkpoint time
    active: list[ActiveWorkflowRecord] = field(default_factory=list)

    # wf_ids that were paused (PAUSE signal sent, RESUME not yet sent)
    paused_wf_ids: list[str] = field(default_factory=list)

    # DAG dependency unlock state: target workflow types whose lock was already
    # released by a successful source completion.  Restored before re-pushing
    # queued requests so enqueue() admits them immediately on recovery.
    dep_satisfied: list[str] = field(default_factory=list)

    # Lifetime spending
    spent: dict[str, float] = field(default_factory=dict)

    # Observability counters (cumulative at checkpoint time)
    instances_run: int = 0
    completed: int = 0
    cancelled: int = 0
    failed: int = 0
    stale_cancels: int = 0
    walltime_cancels: int = 0
    policy_cancels: int = 0
    policy_cycles: int = 0


# ── Serialization ─────────────────────────────────────────────────────────────


def _to_json(ckpt: CampaignCheckpoint) -> dict[str, Any]:
    d = asdict(ckpt)
    return d


def _from_json(d: dict[str, Any]) -> CampaignCheckpoint:
    queue = [QueuedRequestRecord(**r) for r in d.get("queue", [])]
    active = []
    for r in d.get("active", []):
        r = dict(r)
        # Checkpoints written before the spending-only budget model stored a
        # derived concurrency "resources" dict; it is no longer used.
        r.pop("resources", None)
        active.append(ActiveWorkflowRecord(**r))
    return CampaignCheckpoint(
        campaign_name=d["campaign_name"],
        written_at=d.get("written_at", 0.0),
        version=d.get("version", CHECKPOINT_VERSION),
        queue=queue,
        active=active,
        paused_wf_ids=d.get("paused_wf_ids", []),
        dep_satisfied=d.get("dep_satisfied", []),
        spent=d.get("spent", {}),
        instances_run=d.get("instances_run", 0),
        completed=d.get("completed", 0),
        cancelled=d.get("cancelled", 0),
        failed=d.get("failed", 0),
        stale_cancels=d.get("stale_cancels", 0),
        walltime_cancels=d.get("walltime_cancels", 0),
        policy_cancels=d.get("policy_cancels", 0),
        policy_cycles=d.get("policy_cycles", 0),
    )


# ── I/O ───────────────────────────────────────────────────────────────────────


def checkpoint_path(checkpoint_dir: str, campaign_name: str) -> Path:
    return Path(checkpoint_dir) / f"{campaign_name}.json"


def write_checkpoint(path: Path | str, ckpt: CampaignCheckpoint) -> None:
    """Atomically write checkpoint to disk.  Silently logs errors; never raises.

    Each call creates a unique .tmp file so concurrent writes from the policy-
    cycle thread pool and the shutdown path don't race on the same temp file.
    """
    import tempfile  # noqa: PLC0415

    path = Path(path)
    tmp: Path | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_str = tempfile.mkstemp(dir=path.parent, prefix=path.stem + ".", suffix=".tmp")
        tmp = Path(tmp_str)
        with os.fdopen(fd, "w") as fh:
            json.dump(_to_json(ckpt), fh, indent=2)
        os.replace(tmp, path)
        logger.debug(f"[CKPT] wrote {path} (cycles={ckpt.policy_cycles})")
    except Exception:
        logger.warning(f"[CKPT] write failed: {path}", exc_info=True)
        if tmp is not None:
            with contextlib.suppress(OSError):
                tmp.unlink(missing_ok=True)


def read_checkpoint(path: Path | str) -> CampaignCheckpoint | None:
    """Read checkpoint from disk.  Returns None if file missing or corrupt."""
    path = Path(path)
    if not path.exists():
        return None
    try:
        with path.open() as fh:
            d = json.load(fh)
        ckpt = _from_json(d)
        logger.info(
            f"[CKPT] loaded {path} "
            f"(cycles={ckpt.policy_cycles} "
            f"queued={len(ckpt.queue)} "
            f"active={len(ckpt.active)})"
        )
        return ckpt
    except Exception:
        logger.warning(f"[CKPT] read failed (corrupt?): {path}", exc_info=True)
        return None
