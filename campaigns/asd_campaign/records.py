"""The corpus: one JSONL record per labelled design.

The handoff between the label workflow (which writes a record per design) and the
train workflow (which trains on the ones not yet used).  A file rather than a
Dragon ``DDict`` on purpose: the prototype proves the control flow first, and
`README.md` says what moving to a DDict costs.

Two files side by side, each written by one side only, so nothing needs a lock
beyond the append:

* ``records.jsonl`` — the records, one JSON object per line, written by `label`
* ``records.consumed`` — the ids already trained on, written by `train`
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

RECORDS = "records.jsonl"
CONSUMED = "records.consumed"


def _paths(corpus_dir: str | Path) -> tuple[Path, Path]:
    root = Path(corpus_dir)
    root.mkdir(parents=True, exist_ok=True)
    return root / RECORDS, root / CONSUMED


def append(corpus_dir: str | Path, record: dict[str, Any]) -> dict[str, Any]:
    """Add one record.  ``id`` identifies it for the consumed set."""
    records, _ = _paths(corpus_dir)
    if not record.get("id"):
        raise ValueError("a corpus record needs an 'id' (the design's workflow id)")
    line = json.dumps(record, default=str)
    # One open-append-close per record, and a single write of a line below the
    # pipe buffer: concurrent label workflows can append without a lock.
    with records.open("a") as fh:
        fh.write(line + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    return record


def read_all(corpus_dir: str | Path) -> list[dict[str, Any]]:
    records, _ = _paths(corpus_dir)
    if not records.exists():
        return []
    out: list[dict[str, Any]] = []
    for line in records.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except ValueError:
            continue  # a torn final line: the next read picks it up
    return out


def consumed_ids(corpus_dir: str | Path) -> set[str]:
    _, consumed = _paths(corpus_dir)
    if not consumed.exists():
        return set()
    try:
        return set(json.loads(consumed.read_text()))
    except ValueError:
        return set()


def unconsumed(corpus_dir: str | Path) -> list[dict[str, Any]]:
    """Records no training round has used yet, oldest first."""
    used = consumed_ids(corpus_dir)
    return [r for r in read_all(corpus_dir) if str(r.get("id")) not in used]


def mark_consumed(corpus_dir: str | Path, ids: list[str]) -> int:
    """Record that these ids have been trained on; returns the new total."""
    _, consumed = _paths(corpus_dir)
    used = consumed_ids(corpus_dir) | {str(i) for i in ids}
    tmp = consumed.with_suffix(".consumed.tmp")
    tmp.write_text(json.dumps(sorted(used)))
    tmp.replace(consumed)
    return len(used)
