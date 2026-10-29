"""CampaignManager: top-level AuCaM orchestrator."""

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

    **Fixed set of campaigns** (all submitted before run)::

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
        global_budget: GlobalBudget | None = None,
        enable_telemetry: bool = False,
        telemetry_checkpoint_dir: str | None = None,
    ) -> None:
        self._backend = backend
        self._owns_backend = backend is None  # CM created it; CM must shut it down
        self._global_budget = global_budget
        self._enable_telemetry = enable_telemetry
        self._telemetry_checkpoint_dir = telemetry_checkpoint_dir

        self._pending: list[CampaignSpec] = []
        # Spending ledger for all campaigns (per-campaign and global spending).
        self._allocator = BudgetAllocator()
        self._flow: Any = None
        self._shutdown_event = asyncio.Event()

        # Dynamic-submission state
        self._submission_queue: asyncio.Queue[CampaignSpec | None] = asyncio.Queue()
        self._pending_drivers: int = (
            0  # incremented by expect_driver(); decremented by driver_done()
        )
        # _reserved_specs: admitted by submit() but not yet dequeued by _start_campaign().
        # Declared spending is reserved immediately so rapid back-to-back submit() calls
        # can't both pass the admission check before either is actually started.
        self._reserved_specs: dict[str, CampaignSpec] = {}
        self._active_specs: dict[str, CampaignSpec] = {}  # name → spec, while running
        self._active_instances: dict[
            str, _CampaignInstance
        ] = {}  # name → instance, for reading _spent
        self._campaign_futures: dict[str, asyncio.Future[CampaignOutcome]] = {}
        self._campaign_start_times: dict[str, float] = {}
        # Auto-close counters (GlobalBudget.n_campaigns / max_rejections).
        self._completed_campaign_count: int = 0
        self._budget_rejection_count: int = 0
        self._auto_closed: bool = False
        # Scheduling-policy instances already given to a campaign (id → (policy,
        # campaign)).  A policy instance belongs to one campaign only, so
        # campaigns never share learned scheduling state.
        self._used_policies: dict[int, tuple[Any, str]] = {}

    # ── Public API ─────────────────────────────────────────────────────────────

    def submit_campaign(self, spec: CampaignSpec) -> SubmitResult:
        """Enqueue a campaign for the next ``run()`` call (no outcome future).

        Applies the same admission check as ``submit()`` (campaign and
        workflow counts, global spending); a rejected campaign is logged and
        not started.  Accepted campaigns start when ``run()`` begins.  The
        result has no outcome future — use ``submit()`` to wait for a
        CampaignOutcome.
        """
        reason = self._admit(spec)
        if reason:
            return SubmitResult(accepted=False, campaign_name=spec.name, reason=reason)
        self._reserved_specs[spec.name] = spec
        self._pending.append(spec)
        logger.info("[CM] %s: accepted (starts with run())", spec.name)
        return SubmitResult(accepted=True, campaign_name=spec.name)

    def _admit(self, spec: CampaignSpec) -> str | None:
        """Admission check shared by submit() and submit_campaign().

        Returns a rejection reason, or None — then the campaign is open in the
        ledger and its spending limit counts as committed.
        """
        reason = self._policy_check(spec) or self._admission_check(spec)
        if not reason:
            # Spending admission: global spent + committed + incoming <= cap.
            reason = self._allocator.admit(
                spec.name,
                spec.budget.spending,
                self._global_budget.spending if self._global_budget else None,
            )
        if reason:
            if "spending" in reason:
                self._budget_rejection_count += 1
                self._maybe_auto_close()
            logger.warning("[CM] %s: rejected — %s", spec.name, reason)
        elif spec.scheduling_policy is not None:
            pol = spec.scheduling_policy
            self._used_policies[id(pol)] = (pol, spec.name)
        return reason

    def _policy_check(self, spec: CampaignSpec) -> str | None:
        """Reject a scheduling-policy instance already used by another campaign."""
        pol = spec.scheduling_policy
        if pol is None:
            return None
        used = self._used_policies.get(id(pol))
        if used is not None and used[0] is pol:
            return (
                f"scheduling_policy instance is already used by campaign "
                f"'{used[1]}'; give each campaign its own policy instance"
            )
        return None

    async def submit(self, spec: CampaignSpec) -> SubmitResult:
        """Submit a campaign dynamically — safe to call before or during ``run()``.

        Performs an admission check against ``global_budget`` before enqueuing.
        Returns immediately with the decision; ``run()`` picks it up
        asynchronously.

        Call ``close()`` when no more campaigns will be submitted so that
        ``run()`` knows when to exit.
        """
        if self._pending_drivers == 0:
            self._pending_drivers = 1  # implicit expect_driver() for drivers that skip it
        reason = self._admit(spec)
        if reason:
            return SubmitResult(accepted=False, campaign_name=spec.name, reason=reason)

        loop = asyncio.get_running_loop()
        future: asyncio.Future[CampaignOutcome] = loop.create_future()
        self._campaign_futures[spec.name] = future

        # Track the admitted spec so concurrent submit() calls see it in the
        # campaign / workflow-count admission checks before it starts.
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
        """Directly close the submission queue (single-driver shorthand).

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

        *Fixed set*: if only ``submit_campaign()`` was used (no calls to
        ``submit()``), all pending campaigns start immediately and ``run()``
        exits when they all complete.

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

        # Pre-seed the queue with campaigns from submit_campaign().
        for spec in self._pending:
            self._submission_queue.put_nowait(spec)

        # No driver registered → automatically close after pending batch.
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
                        q_t = asyncio.create_task(self._submission_queue.get(), name="cm-queue")
                    watch.add(q_t)

                done_set, _ = await asyncio.wait(watch, return_when=asyncio.FIRST_COMPLETED)

                # ── Shutdown signal ────────────────────────────────────────────
                if shutdown_t in done_set:
                    logger.info("[CM] shutdown signal — cancelling %d campaign(s)", len(active))
                    for t in active.values():
                        t.cancel()
                    await asyncio.gather(*active.values(), return_exceptions=True)
                    # Resolve any pending driver futures so waiters don't hang.
                    for name in list(self._campaign_futures):
                        elapsed_s = time.monotonic() - self._campaign_start_times.pop(
                            name, time.monotonic()
                        )
                        self._resolve_future(name, elapsed_s=elapsed_s)
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
                    elapsed_s = time.monotonic() - self._campaign_start_times.pop(
                        name, time.monotonic()
                    )
                    # Resolve driver future before removing the spec (outcome needs controller).
                    self._resolve_future(name, elapsed_s=elapsed_s)
                    self._active_specs.pop(name, None)
                    active.pop(name, None)
                    self._allocator.close(name)  # no-op if already closed
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
                await self._flow.shutdown(skip_execution_backend=not self._owns_backend)
                logger.info("[CM] WorkflowEngine shut down")

        logger.info("[CM] all campaigns complete")

    # ── Internals ──────────────────────────────────────────────────────────────

    def _all_task_counts(self) -> dict[str, int]:
        """AsyncFlow task counts per state, summed over all running campaigns."""
        total: dict[str, int] = {}
        for inst in list(self._active_instances.values()):
            for state, n in inst.task_counts().items():
                total[state] = total.get(state, 0) + n
        return total

    def _start_campaign(self, spec: CampaignSpec) -> asyncio.Task | None:
        """Reserve budget and create an asyncio Task for the campaign. Returns None on failure."""
        # Move from reserved → active (clears the admit-to-start gap).
        self._reserved_specs.pop(spec.name, None)

        # Admitted campaigns are already open in the ledger (_admit); open() is
        # then a no-op.
        self._allocator.open(spec.name, spec.budget.spending)
        self._active_specs[spec.name] = spec
        self._campaign_start_times[spec.name] = time.monotonic()
        inst = _CampaignInstance(
            spec=spec,
            allocator=self._allocator,
            flow=self._flow,
            all_task_counts=self._all_task_counts,
        )
        self._active_instances[spec.name] = inst
        logger.info("[CM] %s: started", spec.name)
        return asyncio.create_task(inst.run(), name=f"campaign-{spec.name}")

    def _resolve_future(self, name: str, elapsed_s: float) -> None:
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

        # Close the campaign in the ledger: its actual spending moves into the
        # global total and its spending limit stops counting as committed.
        inst = self._active_instances.pop(name, None)
        actual_spent = self._allocator.close(name)
        # The goal is reached when the campaign ended because all its goals were
        # satisfied, or when the controller's goal_type / goal_n count is met.
        goals_met = inst is not None and inst.exit_reason == "goals_satisfied"

        # Log remaining budget for each tracked dimension.
        if self._global_budget and self._global_budget.spending:
            global_spent = self._allocator.global_spent()
            for key, limit in self._global_budget.spending.items():
                used = global_spent.get(key, 0.0)
                committed = self._allocator.committed(key)
                remaining = limit - used - committed
                logger.info(
                    "[CM] global_budget[%s]: used=%.5f committed=%.5f remaining=%.5f / %.5f",
                    key,
                    used,
                    committed,
                    remaining,
                    limit,
                )

        outcome = CampaignOutcome(
            campaign_name=name,
            completed=dict(ctrl.completed) if ctrl else {},
            cancelled=dict(ctrl.cancelled) if ctrl else {},
            failed=dict(ctrl.failed) if ctrl else {},
            elapsed_s=elapsed_s,
            goal_reached=goals_met or (ctrl.is_goal_reached() if ctrl else False),
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
            current_wf = sum(s.budget.max_concurrent_workflows for s in all_committed.values())
            incoming = spec.budget.max_concurrent_workflows
            if current_wf + incoming > gb.max_total_concurrent_workflows:
                return (
                    f"max_total_concurrent_workflows={gb.max_total_concurrent_workflows} "
                    f"would be exceeded (current={current_wf}, requesting={incoming})"
                )

        # Spending admission is done by BudgetAllocator.admit() in submit().
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
                used = self._allocator.global_spent().get(key, 0.0)
                committed = self._allocator.committed(key)
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
