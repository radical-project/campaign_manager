"""run_supervised and CampaignAbortedError — no radical.adr dependency.

Kept separate from operator.py so tests can import these without needing
radical.adr installed in the environment.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

log = logging.getLogger(__name__)


class CampaignAbortedError(RuntimeError):
    """Raised by run_supervised when consecutive all-failed ticks exceed max_failed_ticks.

    Signals transient infrastructure failure (e.g. SLURM preemption) rather than
    a campaign goal or a programming error.  Callers can catch this specifically to
    log or alert differently from an unexpected crash.
    """


async def run_supervised(
    cm,
    operator: Any,
    tick_s: float = 1.0,
    max_failed_ticks: Optional[int] = None,
) -> None:
    """Run a CampaignOperator's decision loop alongside a running CM.

    The CM must already be started by the caller.  This drives the operator one
    cycle per ``tick_s`` until the CM completes naturally OR the operator's goal
    fires — whichever comes first.  When the goal fires, ``cm.stop()`` is called
    to signal the CM to stop accepting new work.

    Raises ValueError if the operator has no stopping condition (no @goals and
    no max_cycles).  This catches the common mistake of forgetting to call
    _validate_stopping_condition() in the subclass __init__.

    max_failed_ticks: if set, counts consecutive ticks where every replica that
        finished did so with failure (sum delta_failed > 0, sum delta_succeeded == 0
        across all stages).  When the count reaches max_failed_ticks, cm.stop() is
        called and CampaignAbortedError is raised.  Ticks where nothing finishes at
        all do not increment the counter.  Disabled (None) by default.
    """
    operator._validate_stopping_condition()

    async def _drive() -> None:
        prev_finished: dict[str, int] = {}
        prev_n_failed: dict[str, int] = {}
        consecutive_bad = 0

        async for _snapshot in operator.run():
            await asyncio.sleep(tick_s)

            if max_failed_ticks is not None:
                obs = operator.view.observe()
                stages = obs.get("stages", {})

                delta_finished = 0
                delta_failed = 0
                for name, info in stages.items():
                    cur_fin = int(info.get("finished", 0))
                    cur_fail = int(info.get("n_failed", 0))
                    delta_finished += cur_fin - prev_finished.get(name, 0)
                    delta_failed += cur_fail - prev_n_failed.get(name, 0)
                    prev_finished[name] = cur_fin
                    prev_n_failed[name] = cur_fail

                delta_succeeded = delta_finished - delta_failed

                if delta_failed > 0 and delta_succeeded == 0:
                    consecutive_bad += 1
                    log.warning(
                        "run_supervised: all-failed tick %d/%d "
                        "(delta_failed=%d, delta_succeeded=0) — "
                        "possible SLURM preemption",
                        consecutive_bad, max_failed_ticks, delta_failed,
                    )
                    if consecutive_bad >= max_failed_ticks:
                        log.error(
                            "run_supervised: %d consecutive all-failed ticks — "
                            "aborting campaign",
                            consecutive_bad,
                        )
                        if hasattr(cm, "stop") and callable(cm.stop):
                            try:
                                await cm.stop()
                            except Exception as exc:  # noqa: BLE001
                                log.warning("run_supervised: cm.stop() raised %s", exc)
                        raise CampaignAbortedError(
                            f"{consecutive_bad} consecutive all-failed ticks "
                            f"(max_failed_ticks={max_failed_ticks})"
                        )
                elif delta_finished > 0:
                    consecutive_bad = 0

        if hasattr(cm, "stop") and callable(cm.stop):
            try:
                await cm.stop()
                log.info("run_supervised: ADR goal reached — CM stop() signalled")
            except Exception as exc:  # noqa: BLE001
                log.warning("run_supervised: cm.stop() raised %s", exc)

    drive_task = asyncio.ensure_future(_drive())
    try:
        await cm.wait()
    finally:
        await operator.shutdown()
        if not drive_task.done():
            drive_task.cancel()
            try:
                await drive_task
            except asyncio.CancelledError:
                pass

    # Re-raise any exception from _drive() (e.g. CampaignAbortedError).
    if not drive_task.cancelled():
        exc = drive_task.exception()
        if exc is not None:
            raise exc
