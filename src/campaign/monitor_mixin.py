"""
MonitorMixin — active periodic monitoring loop for AsyncCampaignManager.

Mixed into AsyncCampaignManager alongside SchedulerMixin and ExecutorMixin.

Two monitoring paths
--------------------
Reactive (executor.py):  fires after every replica completion — low-latency,
                          per-event drift check.
Periodic (this module):  background task at monitor_interval_s cadence —
                          logs a full health table, catches stalls (stages not
                          finishing), and uses a richer pass-through calculation
                          that includes the shard buffer.

Pass-through formula (periodic)
--------------------------------
  observed = (downstream.replicas + sharder.buffered) / upstream.finished

Including the buffer gives the "true" pipeline pass-through rate.  Without it,
strict-stratify batching makes the ratio appear 0 until the first full batch
dispatches — a false positive in the reactive path.
"""

import asyncio

from .executor import _campaign_complete


class MonitorMixin:
    def _start_monitor_loop(self, interval_s: float) -> "asyncio.Task":
        """Spawn the background monitor loop; return the Task."""
        self._monitor_interval_s: float = interval_s
        task = asyncio.get_running_loop().create_task(self._run_monitor_loop())
        return task

    async def _run_monitor_loop(self) -> None:
        """Periodic health check — runs until all campaign groups are done."""
        while not self._all_done.is_set():
            try:
                # No asyncio.shield here — we want the Event.wait() cancelled
                # when the timeout fires.  shield() would leave an orphaned Task
                # pending on _all_done for every tick, producing hundreds of
                # "Task was destroyed but it is pending!" warnings at shutdown.
                await asyncio.wait_for(
                    self._all_done.wait(),
                    timeout=self._monitor_interval_s,
                )
                break  # campaign finished while we were waiting
            except asyncio.TimeoutError:
                pass  # normal — interval elapsed, run a tick
            await self._tick_monitor()

    async def _tick_monitor(self) -> None:
        """Snapshot all groups and run health checks. Acquires the lock."""
        async with self._lock:
            for name, g in self._workflows.items():
                if g.replicas == 0:
                    continue  # not yet activated
                if g.replicas > 0 and g.finished_replicas >= g.replicas and g.running_count == 0:
                    continue  # group fully finished — skip to avoid log spam

                completion_pct = g.finished_replicas / g.replicas * 100 if g.replicas else 0
                sharder = self._sharders.get(name)
                extra = ""
                if sharder and sharder.buffered:
                    extra += f"  buffered={sharder.buffered}"
                self._log.info(
                    f"  {name}: {g.finished_replicas}/{g.replicas} done "
                    f"({completion_pct:.0f}%)  running={g.running_count}{extra}"
                )

                if not self._monitor or g.finished_replicas == 0:
                    continue

                grp_cfg = g.workflow_config or {}

                # ── Pass-through: include shard buffer in downstream count ─────
                # Skip until enough upstream completions for a stable ratio.
                # Uses the shared helper on ExecutorMixin so the periodic path
                # agrees with the reactive path in executor.py.
                _min_passthrough_sample = 10
                trigger_name = grp_cfg.get("trigger_downstream")
                expected_frac = float(grp_cfg.get("trigger_fraction", 1.0))
                if (
                    trigger_name
                    and trigger_name in self._workflows
                    and expected_frac < 1.0
                    and g.finished_replicas >= _min_passthrough_sample
                ):
                    observed_frac = self._compute_passthrough(name, trigger_name)
                    if observed_frac is not None:
                        ev = self._monitor.check_passthrough(name, observed_frac, expected_frac)
                        if ev:
                            tag = " [ESCALATING]" if self._monitor.is_escalating(ev) else ""
                            self._log.warning(
                                f"  Monitor [{name}] pass_through drift{tag}: "
                                f"observed={observed_frac:.3f}  expected={expected_frac:.3f}"
                                f"  dev={ev.deviation_pct:.1f}%  breach={ev.breach_count}"
                            )

                # ── Budget burn ───────────────────────────────────────────────
                budget = float(grp_cfg.get("budget_node_hours") or 0)
                if budget > 0 and g.replicas > 0:
                    pilot = grp_cfg.get("pilot", {})
                    nodes = int(pilot.get("nodes", 1))
                    walltime_h = float(pilot.get("walltime_h", 1))
                    spent_actual = nodes * walltime_h * g.finished_replicas / g.replicas
                    expected_so_far = budget * g.finished_replicas / g.replicas
                    ev = self._monitor.check_budget(name, spent_actual, expected_so_far)
                    if ev:
                        self._log.warning(
                            f"  Monitor [{name}] budget drift: "
                            f"spent={spent_actual:.1f}  expected={expected_so_far:.1f} node-hours"
                            f"  dev={ev.deviation_pct:.1f}%"
                        )

            # Fallback: if everything is done but _all_done was never set
            # (status-propagation chain stalled), detect it here.
            if _campaign_complete(self._workflows, self._sharders):
                self._all_done.set()
                self._log.info("Monitor tick: all groups done — signalling campaign complete")
