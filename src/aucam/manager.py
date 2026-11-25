"""CampaignManager: runs one campaign and returns its outcome."""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from typing import Any

from contextlib import suppress

from .budget import BudgetAllocator
from .campaign import _CampaignInstance
from .spec import CampaignSpec
from .types import CampaignOutcome

logger = logging.getLogger(__name__)


def _registered_handler(loop: asyncio.AbstractEventLoop, sig: signal.Signals):
    """The callback and args already registered with `loop` for `sig`.

    asyncio has no getter for this, so it reads the loop's own bookkeeping;
    returns None where that is not available, and the handler is then left
    as it was before: dropped.
    """
    handlers = getattr(loop, "_signal_handlers", None)
    handle = handlers.get(sig) if isinstance(handlers, dict) else None
    callback = getattr(handle, "_callback", None)
    if callback is None:
        return None
    return callback, tuple(getattr(handle, "_args", ()) or ())


class CampaignManager:
    """
    Runs one campaign and returns its outcome::

        cm = CampaignManager()
        outcome = asyncio.run(cm.run(spec))
        print(outcome.exit_reason, outcome.completed, outcome.spent)

    An agent or a script that plans campaigns calls ``run()`` again with the
    next spec; one ``run()`` at a time.

    For Dragon on HPC, pass the pre-started backend; it is reused across runs
    and not shut down by AuCaM::

        backend = await DragonExecutionBackend()
        cm = CampaignManager(backend=backend)

    By default ``run()`` handles SIGTERM / SIGINT itself, and puts back any
    handler the host had for them when the run ends.  A host that embeds
    AuCaM and owns its own shutdown passes ``install_signal_handlers=False``
    and calls :meth:`stop` instead.
    """

    def __init__(
        self,
        backend: Any = None,
        enable_telemetry: bool = False,
        telemetry_checkpoint_dir: str | None = None,
        install_signal_handlers: bool = True,
    ) -> None:
        self._backend = backend
        self._install_handlers = install_signal_handlers
        # Handlers the host had for the signals we take over, put back after.
        self._prev_handlers: dict[signal.Signals, tuple] = {}
        self._enable_telemetry = enable_telemetry
        self._telemetry_checkpoint_dir = telemetry_checkpoint_dir
        self._running = False
        self._signalled = False
        # The running campaign and its task (None between runs).
        self._instance: _CampaignInstance | None = None
        self._task: asyncio.Task | None = None

    async def run(self, spec: CampaignSpec) -> CampaignOutcome:
        """Run the campaign until it ends and return its :class:`CampaignOutcome`.

        Creates the AsyncFlow WorkflowEngine (and a local RHAPSODY backend if
        none was given), runs the campaign, and shuts the engine down.  On
        SIGTERM / SIGINT (unless ``install_signal_handlers=False``) or on
        :meth:`stop`, the campaign shuts down gracefully and the outcome has
        ``exit_reason="signal"``.
        """
        if self._running:
            raise RuntimeError("CampaignManager runs one campaign at a time")
        self._running = True
        self._signalled = False

        from radical.asyncflow import WorkflowEngine
        from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend

        backend = self._backend if self._backend is not None else await ConcurrentExecutionBackend()
        flow = await WorkflowEngine.create(backend=backend)
        logger.info("[CM] WorkflowEngine ready")
        try:
            if self._enable_telemetry:
                await flow.start_telemetry(
                    checkpoint_path=self._telemetry_checkpoint_dir,
                    resource_poll_interval=1.0,
                )
                logger.info("[CM] asyncflow telemetry started")

            allocator = BudgetAllocator()
            inst = _CampaignInstance(spec=spec, allocator=allocator, flow=flow)
            self._instance = inst
            start = time.monotonic()
            task = asyncio.create_task(inst.run(), name=f"campaign-{spec.name}")
            self._task = task
            installed = self._install_signal_handlers() if self._install_handlers else []
            try:
                await task
            except asyncio.CancelledError:
                if not self._signalled:
                    raise  # run() itself was cancelled: propagate
                logger.info("[CM] %s: stopped by signal", spec.name)
            finally:
                self._remove_signal_handlers(installed)

            outcome = self._outcome(
                spec, inst, time.monotonic() - start, allocator.close(spec.name)
            )
            logger.info("[CM] %s: complete (exit=%s)", spec.name, outcome.exit_reason)
            return outcome
        finally:
            telemetry = getattr(flow, "_telemetry", None)
            if telemetry is not None:
                await telemetry.stop()
                logger.info("[CM] asyncflow telemetry stopped")
            await flow.shutdown(skip_execution_backend=self._backend is not None)
            logger.info("[CM] WorkflowEngine shut down")
            self._instance = None
            self._task = None
            self._running = False

    def stop(self) -> None:
        """Stop the running campaign gracefully, as SIGTERM does.

        Running workflows are cancelled, a final checkpoint is written, and
        ``run()`` returns the outcome with ``exit_reason="signal"``.  No-op
        when no campaign is running.
        """
        if self._task is not None and not self._task.done():
            self._signalled = True
            self._task.cancel()

    # ── Internals ──────────────────────────────────────────────────────────────

    @staticmethod
    def _outcome(
        spec: CampaignSpec, inst: _CampaignInstance, elapsed_s: float, spent: dict[str, float]
    ) -> CampaignOutcome:
        ctrl = spec.controller

        def counts(attr: str) -> dict[str, int]:
            # A controller that skips CampaignController.__init__, or reuses the
            # name for something else, has no per-type counters.
            value = getattr(ctrl, attr, None)
            return dict(value) if isinstance(value, dict) else {}

        try:
            goal_check = bool(ctrl.is_goal_reached())
        except AttributeError:  # same: no goal_type / goal_n attributes
            goal_check = False
        # The goal is reached when all declared goals were satisfied, or when the
        # controller's own goal check (e.g. goal_type / goal_n) is met.
        return CampaignOutcome(
            campaign_name=spec.name,
            completed=counts("completed"),
            cancelled=counts("cancelled"),
            failed=counts("failed"),
            elapsed_s=elapsed_s,
            goal_reached=inst.exit_reason == "goals_satisfied" or goal_check,
            exit_reason=inst.exit_reason,
            spent=spent,
            summary=ctrl.get_summary() if hasattr(ctrl, "get_summary") else {},
            budget_cancels=getattr(inst, "budget_cancels", 0),
            resource_cancels=getattr(inst, "resource_cancels", 0),
            unfit_dropped=getattr(inst, "unfit_dropped", 0),
            controller_errors=getattr(inst, "controller_errors", 0),
            model_usage=getattr(inst, "model_usage", {}),
            goals=list(getattr(inst, "goals", [])),
        )

    def _install_signal_handlers(self) -> list[signal.Signals]:
        """On SIGTERM / SIGINT, stop the campaign gracefully.

        A handler the host already had for one of these signals is noted and
        put back by _remove_signal_handlers, rather than dropped.
        """
        loop = asyncio.get_running_loop()
        self._prev_handlers.clear()
        installed = []
        for sig in (signal.SIGTERM, signal.SIGINT):
            previous = _registered_handler(loop, sig)
            try:
                loop.add_signal_handler(sig, self.stop)
            except (NotImplementedError, OSError, RuntimeError, ValueError):
                continue
            installed.append(sig)
            if previous is not None:
                self._prev_handlers[sig] = previous
        return installed

    def _remove_signal_handlers(self, installed: list[signal.Signals]) -> None:
        loop = asyncio.get_running_loop()
        for sig in installed:
            loop.remove_signal_handler(sig)
            previous = self._prev_handlers.pop(sig, None)
            if previous is None:
                continue
            callback, args = previous
            with suppress(NotImplementedError, OSError, RuntimeError, ValueError):
                loop.add_signal_handler(sig, callback, *args)
