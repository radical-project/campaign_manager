"""CampaignManager: top-level AuCaM service."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import time
from typing import Any

from .budget import BudgetAllocator
from .campaign import _CampaignInstance
from .spec import CampaignSpec
from .types import CampaignOutcome, GlobalBudget, SubmitResult

logger = logging.getLogger(__name__)


class CampaignManager:
    """
    Runs many campaigns concurrently under a shared budget.

    **Legacy usage** (all campaigns submitted before run)::

        cm = CampaignManager()
        cm.submit_campaign(spec_a)
        cm.submit_campaign(spec_b)
        asyncio.run(cm.run())

    **Driver usage** (campaigns submitted dynamically at runtime)::

        async def my_driver(cm: CampaignManager) -> None:
            for spec in generate_specs():
                result = await cm.submit(spec)
                if result.accepted:
                    print(f"{result.campaign_name}: accepted")
                else:
                    print(f"{result.campaign_name}: rejected — {result.reason}")
                await asyncio.sleep(10.0)
            cm.close()             # signal CM: no more campaigns coming

        async def main():
            cm = CampaignManager(global_budget=GlobalBudget(max_concurrent_campaigns=3))
            asyncio.create_task(my_driver(cm))
            await cm.run()         # blocks until driver closes + all campaigns done

    For Dragon on HPC, pass the pre-started backend::

        backend = await DragonExecutionBackend()
        cm = CampaignManager(backend=backend)
    """

    def __init__(
        self,
        backend: Any = None,
        total_budget: dict[str, float] | None = None,
        global_budget: GlobalBudget | None = None,
        telemetry_interval_s: float = 300.0,
        slurm_account: str | None = None,
        qos: str | None = None,
        enable_telemetry: bool = False,
        telemetry_checkpoint_dir: str | None = None,
    ) -> None:
        self._backend = backend
        self._total_budget = total_budget or {}
        self._global_budget = global_budget
        self._telemetry_interval_s = telemetry_interval_s
        self._slurm_account = slurm_account
        self._qos = qos
        self._enable_telemetry = enable_telemetry
        self._telemetry_checkpoint_dir = telemetry_checkpoint_dir

        self._pending: list[CampaignSpec] = []
        self._allocator = BudgetAllocator(self._total_budget)
        self._flow: Any = None
        self._shutdown_event = asyncio.Event()

        # Dynamic-submission state
        self._submission_queue: asyncio.Queue[CampaignSpec | None] = asyncio.Queue()
        self._pending_drivers: int = 0   # incremented by expect_driver(); decremented by driver_done()
        # _reserved_specs: admitted by submit() but not yet dequeued by _start_campaign().
        # Declared spending is reserved immediately so rapid back-to-back submit() calls
        # can't both pass the admission check before either is actually started.
        self._reserved_specs: dict[str, CampaignSpec] = {}
        self._active_specs: dict[str, CampaignSpec] = {}        # name → spec, while running
        self._active_instances: dict[str, "_CampaignInstance"] = {}  # name → instance, for reading _spent
        self._campaign_futures: dict[str, asyncio.Future[CampaignOutcome]] = {}
        self._campaign_start_times: dict[str, float] = {}
        # Cumulative actual spending across all completed campaigns.
        self._global_spent: dict[str, float] = {}
        # Auto-close counters (GlobalBudget.n_campaigns / max_rejections).
        self._completed_campaign_count: int = 0
        self._budget_rejection_count: int = 0
        self._auto_closed: bool = False

    # ── Public API ─────────────────────────────────────────────────────────────

    def submit_campaign(self, spec: CampaignSpec) -> None:
        """Enqueue a campaign for the next ``run()`` call (legacy API).

        All campaigns submitted this way start immediately when ``run()``
        begins.  Prefer ``submit()`` + ``close()`` for dynamic workflows.
        """
        self._pending.append(spec)

    async def submit(self, spec: CampaignSpec) -> SubmitResult:
        """Submit a campaign dynamically — safe to call before or during ``run()``.

        Performs an admission check against ``global_budget`` before enqueuing.
        Returns immediately with the decision; ``run()`` picks it up
        asynchronously.

        Call ``close()`` when no more campaigns will be submitted so that
        ``run()`` knows when to exit.
        """
        if self._pending_drivers == 0:
            self._pending_drivers = 1   # implicit expect_driver() for drivers that skip it
        reason = self._admission_check(spec)
        if reason:
            if "spending" in reason:
                self._budget_rejection_count += 1
                self._maybe_auto_close()
            logger.warning("[CM] %s: rejected — %s", spec.name, reason)
            return SubmitResult(accepted=False, campaign_name=spec.name, reason=reason)

        loop = asyncio.get_running_loop()
        future: asyncio.Future[CampaignOutcome] = loop.create_future()
        self._campaign_futures[spec.name] = future

        # Reserve declared budget immediately so concurrent submit() calls
        # can't both pass the spending check before either campaign starts.
        self._reserved_specs[spec.name] = spec

        await self._submission_queue.put(spec)
        logger.info("[CM] %s: queued (budget reserved)", spec.name)

        result = SubmitResult(accepted=True, campaign_name=spec.name)
        result.outcome_future = future
        return result

    def expect_driver(self) -> None:
        """Register a driver that will call ``submit()`` / ``driver_done()`` during ``run()``.

        Call this before ``run()`` for each driver task that will submit campaigns.
        ``run()`` will not auto-close until all registered drivers call ``driver_done()``.

        ``submit()`` auto-registers an implicit driver if none was registered, so
        this is only needed when the task is created in the same turn as ``run()``
        and hasn't had a chance to execute yet.
        """
        self._pending_drivers += 1

    def driver_done(self) -> None:
        """Signal that this driver has finished submitting campaigns.

        When all registered drivers call ``driver_done()``, the CM closes the
        submission queue and ``run()`` exits once all running campaigns finish.
        Safe to call from a ``finally`` block.
        """
        self._pending_drivers = max(0, self._pending_drivers - 1)
        if self._pending_drivers == 0:
            self._submission_queue.put_nowait(None)
            logger.info("[CM] all drivers done — no more submissions")

    def close(self) -> None:
        """Directly close the submission queue (legacy / single-driver shorthand).

        Prefer ``driver_done()`` when multiple drivers may be active.
        Safe to call more than once.
        """
        self._submission_queue.put_nowait(None)
        logger.info("[CM] submission queue closed")

    # ── Run loop ───────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """
        Create the shared WorkflowEngine, start campaigns, and run until all
        finish or SIGTERM is received.

        *Legacy mode*: if only ``submit_campaign()`` was used (no calls to
        ``submit()``), the method behaves exactly as before — all pending
        campaigns start immediately and ``run()`` exits when they all complete.

        *Driver mode*: ``run()`` also monitors the submission queue fed by
        ``submit()``; it exits only after ``close()`` is called **and** all
        active campaigns have finished.
        """
        from radical.asyncflow import WorkflowEngine
        from rhapsody.backends.execution.concurrent import ConcurrentExecutionBackend

        if self._flow is None:
            if self._backend is None:
                backend = await ConcurrentExecutionBackend()
            else:
                backend = self._backend
            self._flow = await WorkflowEngine.create(backend=backend)
            logger.info("[CM] WorkflowEngine ready")

        if self._enable_telemetry:
            await self._flow.start_telemetry(
                checkpoint_path=self._telemetry_checkpoint_dir,
                resource_poll_interval=1.0,
            )
            logger.info("[CM] asyncflow telemetry started")

        self._install_signal_handlers()

        # Pre-seed queue from legacy submit_campaign() calls.
        for spec in self._pending:
            self._submission_queue.put_nowait(spec)

        # Legacy mode: no driver registered → automatically close after pending batch.
        if self._pending_drivers == 0:
            self._submission_queue.put_nowait(None)

        active: dict[str, asyncio.Task] = {}
        done_submitting = False
        q_t: asyncio.Task | None = None
        shutdown_t = asyncio.create_task(self._shutdown_event.wait(), name="cm-shutdown")

        try:
            while not (done_submitting and not active):
                watch: set[asyncio.Task] = set(active.values()) | {shutdown_t}

                if not done_submitting:
                    if q_t is None or q_t.done():
                        q_t = asyncio.create_task(
                            self._submission_queue.get(), name="cm-queue"
                        )
                    watch.add(q_t)

                done_set, _ = await asyncio.wait(watch, return_when=asyncio.FIRST_COMPLETED)

                # ── Shutdown signal ────────────────────────────────────────────
                if shutdown_t in done_set:
                    logger.info(
                        "[CM] shutdown signal — cancelling %d campaign(s)", len(active)
                    )
                    for t in active.values():
                        t.cancel()
                    await asyncio.gather(*active.values(), return_exceptions=True)
                    # Resolve any pending driver futures so waiters don't hang.
                    for name in list(self._campaign_futures):
                        elapsed_s = time.monotonic() - self._campaign_start_times.pop(name, time.monotonic())
                        self._resolve_future(name, elapsed_s=elapsed_s, task_cancelled=True)
                    if q_t and not q_t.done():
                        q_t.cancel()
                    return

                # ── New submission from driver ─────────────────────────────────
                if q_t and q_t in done_set:
                    item: CampaignSpec | None = q_t.result()
                    q_t = None
                    if item is None:
                        done_submitting = True
                        logger.info(
                            "[CM] no more submissions — waiting for %d campaign(s)",
                            len(active),
                        )
                    else:
                        t = self._start_campaign(item)
                        if t is not None:
                            active[item.name] = t

                # ── Completed campaign tasks ───────────────────────────────────
                finished = done_set - ({q_t} if q_t else set()) - {shutdown_t}
                for t in finished:
                    name = t.get_name().removeprefix("campaign-")
                    elapsed_s = time.monotonic() - self._campaign_start_times.pop(name, time.monotonic())
                    # Resolve driver future before removing the spec (outcome needs controller).
                    self._resolve_future(name, elapsed_s=elapsed_s, task_cancelled=t.cancelled())
                    self._active_specs.pop(name, None)
                    active.pop(name, None)
                    self._allocator.release(name)
                    exc = t.exception() if not t.cancelled() else None
                    if exc:
                        logger.error("[CM] campaign %s raised: %s", name, exc)
                    else:
                        logger.info("[CM] campaign %s complete", name)
                    if not t.cancelled():
                        self._completed_campaign_count += 1
                    # Check auto-close conditions after each campaign finishes and
                    # after _active_specs is updated so remaining-capacity math is correct.
                    self._maybe_auto_close()

        finally:
            shutdown_t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await shutdown_t
            if q_t and not q_t.done():
                q_t.cancel()

            if self._flow is not None:
                telemetry = getattr(self._flow, "_telemetry", None)
                if telemetry is not None:
                    await telemetry.stop()
                    logger.info("[CM] asyncflow telemetry stopped")
                await self._flow.shutdown(skip_execution_backend=True)
                logger.info("[CM] WorkflowEngine shut down")

        logger.info("[CM] all campaigns complete")

    # ── Internals ──────────────────────────────────────────────────────────────

    def _start_campaign(self, spec: CampaignSpec) -> asyncio.Task | None:
        """Reserve budget and create an asyncio Task for the campaign. Returns None on failure."""
        # Move from reserved → active (clears the admit-to-start gap).
        self._reserved_specs.pop(spec.name, None)

        ok = self._allocator.reserve(spec.name, spec.budget.budget, spec.budget.spending)
        if not ok:
            logger.error(
                "[CM] %s: budget request denied (insufficient shared budget)", spec.name
            )
            # Reject: resolve any waiting driver future immediately.
            self._resolve_future(spec.name, elapsed_s=0.0, task_cancelled=False)
            return None
        self._active_specs[spec.name] = spec
        self._campaign_start_times[spec.name] = time.monotonic()
        inst = _CampaignInstance(spec=spec, allocator=self._allocator, flow=self._flow)
        self._active_instances[spec.name] = inst
        logger.info("[CM] %s: started", spec.name)
        return asyncio.create_task(inst.run(), name=f"campaign-{spec.name}")

    def _resolve_future(self, name: str, elapsed_s: float, task_cancelled: bool) -> None:
        """Build a CampaignOutcome from the controller and resolve the driver future, if any.

        Also updates the global spending ledger from the campaign instance's
        actual ``_spent`` dict so that subsequent admission checks account for
        resources already consumed by completed campaigns.
        """
        fut = self._campaign_futures.pop(name, None)
        if fut is None or fut.done():
            return

        spec = self._active_specs.get(name)
        ctrl = spec.controller if spec is not None else None

        # Read actual spending and update the cross-campaign ledger.
        inst = self._active_instances.pop(name, None)
        actual_spent: dict[str, float] = dict(inst._spent) if inst is not None else {}
        for key, amount in actual_spent.items():
            self._global_spent[key] = self._global_spent.get(key, 0.0) + amount

        # Log remaining budget for each tracked dimension.
        if self._global_budget and self._global_budget.spending:
            for key, limit in self._global_budget.spending.items():
                used = self._global_spent.get(key, 0.0)
                # committed = declared limits of all still-admitted campaigns (name is being resolved)
                committed = sum(
                    s.budget.spending.get(key, 0.0)
                    for n, s in {**self._reserved_specs, **self._active_specs}.items()
                    if n != name
                )
                remaining = limit - used - committed
                logger.info(
                    "[CM] global_budget[%s]: used=%.5f committed=%.5f remaining=%.5f / %.5f",
                    key, used, committed, remaining, limit,
                )

        outcome = CampaignOutcome(
            campaign_name=name,
            completed=dict(ctrl.completed) if ctrl else {},
            cancelled=dict(ctrl.cancelled) if ctrl else {},
            failed=dict(ctrl.failed) if ctrl else {},
            elapsed_s=elapsed_s,
            goal_reached=(ctrl.is_goal_reached() if ctrl else False),
            spent=actual_spent,
            summary=ctrl.get_summary() if ctrl else {},
        )
        fut.set_result(outcome)

    def _admission_check(self, spec: CampaignSpec) -> str | None:
        """Return a rejection reason string, or None if the spec is admissible."""
        gb = self._global_budget
        if gb is None:
            return None

        # All specs that have declared their budget: admitted-but-not-started
        # (_reserved_specs) + currently running (_active_specs).
        all_committed = {**self._reserved_specs, **self._active_specs}

        n_admitted = len(all_committed)
        if gb.max_concurrent_campaigns is not None and n_admitted >= gb.max_concurrent_campaigns:
            return (
                f"max_concurrent_campaigns={gb.max_concurrent_campaigns} already reached "
                f"({n_admitted} admitted)"
            )

        if gb.max_total_concurrent_workflows is not None:
            current_wf = sum(
                s.budget.max_concurrent_workflows for s in all_committed.values()
            )
            incoming = spec.budget.max_concurrent_workflows
            if current_wf + incoming > gb.max_total_concurrent_workflows:
                return (
                    f"max_total_concurrent_workflows={gb.max_total_concurrent_workflows} "
                    f"would be exceeded (current={current_wf}, requesting={incoming})"
                )

        for key, limit in gb.spending.items():
            already_spent = self._global_spent.get(key, 0.0)
            committed = sum(
                s.budget.spending.get(key, 0.0) for s in all_committed.values()
            )
            incoming = spec.budget.spending.get(key, 0.0)
            total = already_spent + committed + incoming
            if total > limit:
                return (
                    f"global spending[{key}]={limit} would be exceeded "
                    f"(spent={already_spent:.5f}, committed={committed:.5f}, "
                    f"requesting={incoming:.5f}, total={total:.5f})"
                )

        return None

    def _maybe_auto_close(self) -> None:
        """Trigger auto-close when a GlobalBudget termination condition is met."""
        if self._auto_closed:
            return
        gb = self._global_budget
        if gb is None:
            return

        if gb.n_campaigns is not None and self._completed_campaign_count >= gb.n_campaigns:
            self._do_auto_close(f"n_campaigns={gb.n_campaigns} reached")
            return

        if gb.spending:
            for key, limit in gb.spending.items():
                used = self._global_spent.get(key, 0.0)
                committed = sum(
                    s.budget.spending.get(key, 0.0)
                    for s in {**self._reserved_specs, **self._active_specs}.values()
                )
                if limit - used - committed <= 0.0:
                    self._do_auto_close(
                        f"global_budget[{key}] exhausted "
                        f"(used={used:.5f} committed={committed:.5f} limit={limit:.5f})"
                    )
                    return

        if gb.max_rejections is not None and self._budget_rejection_count > gb.max_rejections:
            self._do_auto_close(
                f"budget rejections={self._budget_rejection_count} "
                f"> max_rejections={gb.max_rejections}"
            )

    def _do_auto_close(self, reason: str) -> None:
        self._auto_closed = True
        logger.info("[CM] auto-close: %s", reason)
        self.close()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, lambda: self._shutdown_event.set())
            except (NotImplementedError, OSError):
                pass
