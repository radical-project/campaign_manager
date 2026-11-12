"""Lightweight reporting client for command-line workflows.

A program launched by CM through ``WorkflowConfig.launch_cmd`` can be written
in any language.  It reports to CM and receives PAUSE / RESUME / CANCEL signals through
the campaign's DataBackend (Redis or Dragon), using CM's language-neutral
protocol:

* **Report** — key ``<CM_WORKFLOW_ID>``, value a UTF-8 JSON object::

      {"wf_id": "...", "score": <any JSON>, "metrics": {...},
       "iteration": <int>, "emitted_at": <unix time, float>}

  Each write replaces the previous report; CM reads the latest one every
  policy cycle and once more when the workflow ends.
* **Signal** — key ``<CM_WORKFLOW_ID>:signal``, value the string ``PAUSE``,
  ``RESUME`` or ``CANCEL``.  The program reads it and deletes the key.  CANCEL
  is sent only to workflow types with a grace period (``cancel_grace_s``): the
  program should save what it needs and exit; when the grace period ends, or
  without one, CM stops the task through AsyncFlow.

CM exports ``CM_WORKFLOW_ID``, ``CM_DATA_ENDPOINT`` (``host:port`` for Redis,
the serialized DDict descriptor for Dragon) and ``CM_DATA_BACKEND``
(``redis`` | ``dragon``) into the program's environment.

Three ways to use it:

* any language with a Redis client — follow the protocol above directly;
* any language, Redis or Dragon — call the ``aucam-report`` command
  (``aucam-report emit --score 0.8 --metric cpu_hours=1.5``,
  ``aucam-report signal``, ``aucam-report wait-resume``, ``aucam-report done``);
* Python — ``from src.aucam_client import Reporter``.

This package imports only the standard library plus ``redis`` or ``radex``
(lazily), so a compute node does not need the rest of AuCaM.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

__all__ = ["Reporter", "ReporterConfigError", "SIGNAL_SUFFIX", "encode_report"]

SIGNAL_SUFFIX = ":signal"
_SIGNALS = ("PAUSE", "RESUME", "CANCEL")


class ReporterConfigError(RuntimeError):
    """The CM_* environment is missing or incomplete."""


def encode_report(
    wf_id: str,
    *,
    score: Any = None,
    metrics: dict | None = None,
    iteration: int = 0,
    emitted_at: float | None = None,
    done: bool = False,
    returncode: int | None = None,
) -> dict:
    """Build the report object; raise TypeError if it is not JSON-serializable."""
    report = {
        "wf_id": wf_id,
        "score": score,
        "metrics": dict(metrics or {}),
        "iteration": int(iteration),
        "emitted_at": time.time() if emitted_at is None else float(emitted_at),
    }
    if done:
        report["done"] = True
        report["returncode"] = int(returncode or 0)
    try:
        json.dumps(report)
    except (TypeError, ValueError) as exc:
        raise TypeError(
            f"report is not JSON-serializable ({exc}); score and metrics of a "
            "command-line workflow must be numbers, strings, lists or dicts"
        ) from exc
    return report


class Reporter:
    """Report to CM and poll signals from a command-line workflow.

    By default every setting comes from the environment CM exports; pass
    ``wf_id``, ``endpoint`` or ``backend`` to override.
    """

    def __init__(
        self,
        wf_id: str | None = None,
        endpoint: str | None = None,
        backend: str | None = None,
    ) -> None:
        self.wf_id = wf_id or os.environ.get("CM_WORKFLOW_ID")
        self.endpoint = endpoint or os.environ.get("CM_DATA_ENDPOINT")
        self.backend = (backend or os.environ.get("CM_DATA_BACKEND") or "redis").lower()
        if not self.wf_id:
            raise ReporterConfigError(
                "CM_WORKFLOW_ID is not set — is this program running as a CM workflow?"
            )
        if not self.endpoint:
            raise ReporterConfigError(
                "CM_DATA_ENDPOINT is not set — command-line workflows can report "
                "only when the campaign uses a Redis or Dragon DataBackend"
            )
        if self.backend not in ("redis", "dragon"):
            raise ReporterConfigError(f"unknown CM_DATA_BACKEND: {self.backend!r}")
        self._client: Any = None

    # ── backend access ───────────────────────────────────────────────────────

    def _redis(self) -> Any:
        if self._client is None:
            import redis  # noqa: PLC0415

            host, port = self.endpoint.rsplit(":", 1)
            self._client = redis.Redis(host=host, port=int(port), protocol=2)
        return self._client

    def _dragon(self) -> Any:
        if self._client is None:
            from radex.clients.core import DragonClient  # noqa: PLC0415

            self._client = DragonClient(self.endpoint, 5)
        return self._client

    # ── public API ───────────────────────────────────────────────────────────

    def emit(self, *, score: Any = None, metrics: dict | None = None, iteration: int = 0) -> dict:
        """Send a report (replaces the previous one).  Returns the report sent."""
        report = encode_report(self.wf_id, score=score, metrics=metrics, iteration=iteration)
        # Redis: UTF-8 JSON; Dragon stores Python objects (the same dict is read back by CM).
        self._write_report(report)
        return report

    def done(self, returncode: int = 0) -> dict:
        """Send the final report: the program has finished with ``returncode``.

        Keeps the score and metrics of the last report (so its spending is
        kept).  Call it last, e.g. ``aucam-report done --rc $?`` at the end of
        a shell script.  It lets AuCaM finish this workflow after AuCaM itself
        was restarted while the program was running (it then adopts the
        program instead of starting it again).
        """
        last = self._read_report() or {}
        report = encode_report(
            self.wf_id,
            score=last.get("score"),
            metrics=last.get("metrics"),
            iteration=int(last.get("iteration", 0) or 0),
            done=True,
            returncode=returncode,
        )
        self._write_report(report)
        return report

    def _read_report(self) -> dict | None:
        if self.backend == "redis":
            raw = self._redis().get(self.wf_id)
            if not raw:
                return None
            try:
                return json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
            except (ValueError, UnicodeDecodeError):
                return None
        client = self._dragon()
        if not client.contains(self.wf_id):
            return None
        value = client.get_picklable(self.wf_id)
        return value if isinstance(value, dict) else None

    def _write_report(self, report: dict) -> None:
        if self.backend == "redis":
            self._redis().set(self.wf_id, json.dumps(report).encode("utf-8"))
        else:
            self._dragon().put_picklable(self.wf_id, report)

    def poll_signal(self) -> str | None:
        """Return ``"PAUSE"``, ``"RESUME"`` or ``"CANCEL"`` if one is pending (and clear it), else None."""
        key = self.wf_id + SIGNAL_SUFFIX
        if self.backend == "redis":
            r = self._redis()
            raw = r.get(key)
            if not raw:
                return None
            r.delete(key)
            sig = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        else:
            client = self._dragon()
            if not client.contains(key):
                return None
            sig = client.get_picklable(key)
            if sig is None:
                return None
            client.put_picklable(key, None)
        sig = str(sig).strip().upper()
        return sig if sig in _SIGNALS else None

    def wait_for_resume(self, interval: float = 1.0) -> str:
        """Block until RESUME or CANCEL arrives; return which (call after PAUSE).

        On CANCEL, save what you need and exit: AuCaM cancels the program once
        its grace period (WorkflowConfig.cancel_grace_s) ends.
        """
        while True:
            sig = self.poll_signal()
            if sig in ("RESUME", "CANCEL"):
                return sig
            time.sleep(interval)
