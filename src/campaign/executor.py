"""
ExecutorMixin — replica launch, completion, and monitor logic for AsyncCampaignManager.

Mixed into AsyncCampaignManager; all methods use ``self`` to access shared
state (``_groups``, ``_resources``, ``_sharders``, ``_monitor``, ``_log``).
"""

import asyncio
from typing import TYPE_CHECKING, Optional

from .gpu import make_policies
from .types import _WorkflowInfo

if TYPE_CHECKING:
    from .base_workflow import BaseWorkflow


def _campaign_complete(groups: dict, sharders: dict) -> bool:
    """True when every group has finished all its replicas and all sharder buffers are empty.

    Deliberately does NOT use group.status so it works even when the
    deps_done status-propagation chain stalls (e.g. a downstream stage
    finishes all replicas before its upstream is marked 'done').
    """
    if not groups:
        return False
    if not any(g.replicas > 0 for g in groups.values()):
        return False  # nothing has started yet
    for g in groups.values():
        if g.replicas == 0:
            continue  # not yet activated
        if g.running_count > 0:
            return False
        if g.finished_replicas < g.replicas:
            return False
    if any(s.buffered > 0 for s in sharders.values()):
        return False
    return True


class ExecutorMixin:
    async def _run_replica(self, group: _WorkflowInfo, replica_idx: int) -> None:
        """Execute one replica of a workflow group.

        Concurrency ceiling: group._semaphore (asyncio.Semaphore) gates entry so
        at most concurrency_cap replicas execute simultaneously.  Tasks are created
        eagerly by the scheduler (limited only by resources and replicas quota) and
        block here until a slot opens.  Within-group cycling no longer requires a
        scheduler round-trip: a finishing replica releases the semaphore and the
        next waiting task wakes directly.

        started_count is incremented AFTER semaphore.acquire() so that
        running_count = started_count - finished_replicas counts only executing
        replicas, not tasks waiting in the semaphore queue.

        If cancelled while waiting for the semaphore (shutdown path), the queued
        reservation is undone and pre-allocated resources are returned.
        """
        replica_id = f"{group.name}_{replica_idx}"
        _sem = group._semaphore

        # Acquire execution slot — may block if concurrency_cap replicas are running.
        # Cancellation here means the campaign is shutting down; undo the allocation.
        if _sem is not None:
            try:
                await _sem.acquire()
            except asyncio.CancelledError:
                async with self._lock:
                    group._queued_count -= 1
                    self._resources.release(
                        group.required_cpus, group.required_gpus, group.required_memory_gb
                    )
                    _freed = self._replica_gpu_assignments.pop(replica_id, [])
                    self._free_gpu_ids.extend(_freed)
                for _gid in _freed:
                    try:
                        group.running_gpu_ids.remove(_gid)
                    except ValueError:
                        pass
                raise

        # Now executing — mark as started.  Safe without the lock: asyncio is
        # single-threaded; no await between semaphore acquire and this increment.
        group.started_count += 1
        self._stats[group.name].replicas_started = group.started_count

        final_state = "done"
        wf: Optional[BaseWorkflow] = None

        try:
            gpu_ids = self._replica_gpu_assignments.get(replica_id, [])
            policies = make_policies(self._gpu_pool, gpu_ids)

            res_tag = ""
            if group.required_cpus > 0 or group.required_gpus > 0 or group.required_memory_gb > 0:
                res_tag = (
                    f" [cpus={group.required_cpus} gpus={group.required_gpus}"
                    + (f" mem={group.required_memory_gb}GB" if group.required_memory_gb > 0 else "")
                    + "]"
                )
            if gpu_ids:
                host = self._gpu_pool[0][0] if self._gpu_pool else "?"
                res_tag += f" [gpu_affinity={gpu_ids} host={host}]"
            self._log.info(f"  starting replica {replica_id!r}{res_tag}")

            # Build per-replica config: start from group config, layer in GPU and candidate info.
            replica_config = group.workflow_config
            if gpu_ids:
                replica_config = {
                    **(replica_config or {}),
                    "assigned_gpu_ids": gpu_ids,
                    "group_gpu_ids": list(group.running_gpu_ids),
                }
            candidate_id = self._replica_candidate_assignments.pop(replica_id, None)
            score = None
            if candidate_id and self._candidate_log:
                h = self._candidate_log.get(candidate_id)
                if h:
                    score = h.latest_score
                    # ADVANCE flag: Triage stamped the latest StageResult
                    # decision="triaged_advance" when this candidate's
                    # surrogate prediction cleared advance_threshold at low
                    # uncertainty.  Workflows that honour the flag skip the
                    # expensive computation and pass through with the
                    # predicted score (dreamer skips its simulated sleep).
                    triage_advance = bool(h.results and h.results[-1].decision == "triaged_advance")
                    replica_config = {
                        **(replica_config or {}),
                        "candidate_id": candidate_id,
                        "candidate_score": h.latest_score,
                        "candidate_surr": h.latest_surrogate_pred,
                        "candidate_surr_unc": h.latest_surrogate_unc,
                        "candidate_scaffold": h.scaffold_class,
                        "candidate_triage_advance": triage_advance,
                    }
                else:
                    replica_config = {**(replica_config or {}), "candidate_id": candidate_id}
            lineage = group._retry_lineage.pop(replica_id, None)
            if lineage:
                retry_of, attempt, root_id = lineage
            else:
                retry_of, attempt, root_id = None, 0, replica_id
            self._metrics.record_replica_start(
                group.name, replica_id, candidate_id=candidate_id, score=score,
                retry_of=retry_of, attempt=attempt,
            )

            wf = group.workflow_class(
                config=replica_config,
                _cm=self,
                _group_name=group.name,
                asyncflow=self._engine,
                policies=policies,
                engine_dragon=self._engine_dragon,
            )
            entry = getattr(wf, group.entry_point)

            try:
                if asyncio.iscoroutinefunction(entry):
                    await entry(replica_id)
                else:
                    await asyncio.to_thread(entry, replica_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                self._log.error(f"Replica {replica_id!r} raised: {type(exc).__name__}: {exc}")
                final_state = "failed"
        except asyncio.CancelledError:
            final_state = "failed"
            raise
        except BaseException as exc:
            # Setup, construction, or getattr(entry) failed before the entry
            # point ran.  Mark failed and fall through to the finally block
            # so resources still get released.
            self._log.error(f"Replica {replica_id!r} setup failed: {type(exc).__name__}: {exc}")
            final_state = "failed"
        finally:
            # Release the semaphore slot before cleanup so the next queued task
            # can start executing without waiting for _handle_replica_done.
            if _sem is not None:
                _sem.release()
            try:
                await self._handle_replica_done(wf, group, replica_id, replica_idx, final_state, root_id=root_id)
            except Exception as exc:
                import sys as _sys
                import traceback as _tb

                _tb.print_exc(file=_sys.stderr)
                _sys.stderr.flush()
                self._log.error(
                    f"Replica {replica_id!r} cleanup raised: {type(exc).__name__}: {exc}"
                )

    async def _handle_replica_done(
        self,
        wf: "Optional[BaseWorkflow]",
        group: _WorkflowInfo,
        replica_id: str,
        replica_idx: int,
        final_state: str,
        *,
        root_id: Optional[str] = None,
    ) -> None:
        """Call workflow hook, then update group state and re-schedule.

        wf is None when workflow construction failed before the instance was
        built; in that case hooks are skipped and we go straight to resource
        release via _on_replica_finished.

        For failed replicas the sequence is:
          1. on_replica_failed  — dedicated failure hook; returns True to suppress
                                  automatic retry, False to let executor retry
          2. Automatic retry    — if on_replica_failed returned False, retries < max_retries,
                                  and the failure is not a CancelledError; increments
                                  group.replicas so the scheduler picks up the retry slot
          3. on_replica_done    — fires on final outcome (success or exhausted retries);
                                  skipped for intermediate retries
          4. _on_completion     — DAG routing; skipped for intermediate retries
        """
        retry_scheduled = False
        # root_id tracks the original replica in a retry chain so _retry_counts
        # uses a stable key regardless of how many retries have been attempted.
        if root_id is None:
            root_id = replica_id

        if wf is not None and final_state == "failed":
            # Step 1: dedicated failure hook
            hook_handled = False
            try:
                fail_hook = wf.on_replica_failed
                if asyncio.iscoroutinefunction(fail_hook):
                    hook_handled = bool(await fail_hook(replica_id, self))
                else:
                    hook_handled = bool(fail_hook(replica_id, self))
            except Exception as exc:
                self._log.error(f"Replica {replica_id!r} on_replica_failed raised: {exc}")

            # Step 2: automatic executor retry.
            # Count under root_id (the original replica in this chain) so that
            # each retry doesn't get a fresh zero count — which would cause
            # infinite retries when max_retries >= 1.
            # _queued_count and _retry_lineage are written inside self._lock to
            # prevent a race when two replicas fail concurrently.
            if not hook_handled and group.max_retries > 0:
                attempts = group._retry_counts.get(root_id, 0)
                if attempts < group.max_retries:
                    group._retry_counts[root_id] = attempts + 1
                    async with self._lock:
                        new_replica_id = f"{group.name}_{group._queued_count}"
                        group._retry_lineage[new_replica_id] = (replica_id, attempts + 1, root_id)
                        group.replicas += 1
                    retry_scheduled = True
                    self._log.warning(
                        f"Replica {replica_id!r} failed "
                        f"(attempt {attempts + 1}/{group.max_retries + 1}) "
                        f"— retry scheduled as {new_replica_id!r}"
                    )
                else:
                    self._log.error(
                        f"Replica {replica_id!r} failed after {attempts + 1} attempts "
                        f"(max_retries={group.max_retries}) — giving up"
                    )
                    group._retry_counts.pop(root_id, None)

        # Only count terminal failures (not intermediate retry attempts) so that
        # fail_rate in the view reflects actual unrecoverable failures.
        if final_state == "failed" and not retry_scheduled:
            async with self._lock:
                group.failed_replicas += 1

        # Steps 3 & 4: run on_replica_done and _on_completion only for the
        # final outcome — skip for intermediate retries so downstream stages
        # are not triggered prematurely.
        if wf is not None and not retry_scheduled:
            try:
                hook = wf.on_replica_done
                if asyncio.iscoroutinefunction(hook):
                    await hook(replica_id, self, final_state)
                else:
                    hook(replica_id, self, final_state)
            except Exception as exc:
                self._log.error(f"Replica {replica_id!r} on_replica_done raised: {exc}")

            try:
                completion_result = wf._on_completion(replica_id, self, final_state)
                if asyncio.iscoroutine(completion_result):
                    completion_result = await completion_result
            except Exception as exc:
                self._log.error(f"Replica {replica_id!r} _on_completion raised: {exc}")
                completion_result = None

            if completion_result is not None:
                await self._apply_completion_routing(replica_id, completion_result)
                self._log.debug(f"Replica {replica_id!r}: next-step routing via _on_completion")
            else:
                self._log.debug(
                    f"Replica {replica_id!r}: _on_completion returned None — "
                    "using config-based dependency resolution"
                )

        self._metrics.record_replica_finish(group.name, replica_id, final_state)
        await self._on_replica_finished(group, replica_id)

    async def _apply_completion_routing(self, replica_id: str, result) -> None:
        """Trigger next workflow groups from an _on_completion return value.

        Normalises all supported shapes to list[dict] then calls
        trigger_dependent for each entry.
        """
        # Normalise to list[dict]
        if isinstance(result, str):
            specs = [{"name": result}]
        elif isinstance(result, dict):
            specs = [result]
        elif isinstance(result, (list, tuple)):
            specs = []
            for item in result:
                if isinstance(item, str):
                    specs.append({"name": item})
                elif isinstance(item, dict):
                    specs.append(item)
                else:
                    self._log.warning(
                        f"_on_completion [{replica_id!r}]: unrecognised item {item!r} — skipping"
                    )
        else:
            self._log.warning(
                f"_on_completion [{replica_id!r}]: unrecognised return type "
                f"{type(result).__name__!r} — ignoring, falling back to config"
            )
            return

        for spec in specs:
            name = spec.get("name")
            if not name:
                self._log.warning(
                    f"_on_completion [{replica_id!r}]: spec missing 'name' — skipping {spec!r}"
                )
                continue
            replicas = int(spec.get("replicas", 1))
            kwargs = {k: v for k, v in spec.items() if k not in ("name", "replicas")}
            self._log.info(
                f"_on_completion [{replica_id!r}]: → {name!r} "
                f"replicas={replicas}" + (f" {kwargs}" if kwargs else "")
            )
            await self.trigger_dependent(name, replicas=replicas, **kwargs)

    def _propagate_status_done_locked(self) -> list[str]:
        """Mark all groups whose replicas are complete AND deps are done.

        Iterates until no more transitions happen, so a single upstream
        completion can cascade status="done" through any number of
        downstream groups that were waiting only on that upstream.

        Returns the list of groups that transitioned to "done" in this call,
        in topological (upstream-first) order.
        """
        newly_done: list[str] = []
        changed = True
        while changed:
            changed = False
            for g in self._workflows.values():
                if g.status == "done":
                    continue
                if g.replicas == 0:
                    continue
                if g.finished_replicas < g.replicas:
                    continue
                if g.running_count > 0:
                    continue
                deps_done = not g.dependencies or all(
                    self._workflows.get(d) is not None and self._workflows[d].status == "done"
                    for d in g.dependencies
                )
                if deps_done:
                    g.status = "done"
                    newly_done.append(g.name)
                    changed = True
        return newly_done

    def _compute_passthrough(
        self,
        upstream_name: str,
        downstream_name: str,
    ) -> Optional[float]:
        """Observed pass-through fraction for upstream → downstream.

        Counts downstream.replicas (dispatched count) PLUS sharder buffer
        (pending dispatches) so strict-stratify accumulation doesn't
        falsely report a near-zero pass-through during normal batching.

        Returns None when the upstream hasn't finished any replicas yet
        (no signal) or either group is missing.
        """
        upstream = self._workflows.get(upstream_name)
        downstream = self._workflows.get(downstream_name)
        if upstream is None or downstream is None:
            return None
        if upstream.finished_replicas <= 0:
            return None
        sharder = self._sharders.get(downstream_name)
        buffered = sharder.buffered if sharder else 0
        return (downstream.replicas + buffered) / upstream.finished_replicas

    async def _on_replica_finished(self, group: _WorkflowInfo, replica_id: str) -> None:
        """Update group counters, notify sharders, run monitor, then re-schedule."""
        newly_done: list[str] = []
        async with self._lock:
            # running_count is derived from started_count - finished_replicas;
            # incrementing finished_replicas implicitly decrements running_count.
            group.finished_replicas += 1
            self._resources.release(
                group.required_cpus, group.required_gpus, group.required_memory_gb
            )
            self._stats[group.name].replicas_finished = group.finished_replicas
            # Drop this replica's candidate-tracking entry so subsequent
            # sharder dispatches see an accurate "running scaffolds" set.
            self._running_candidates.pop(replica_id, None)

            # Propagate status="done" through the cascade.  Handles the
            # current group transitioning AND any downstream group that
            # was waiting only on this group's completion.
            newly_done = self._propagate_status_done_locked()

            # When ReplanningController is DRAINING, signal completion
            # the moment all in-flight work has finished.  is_paused()
            # is true for any non-NORMAL state; we only signal drain on
            # the DRAINING branch.
            if self._replanning is not None and self._replanning.is_paused():
                total_running = sum(w.running_count for w in self._workflows.values())
                if total_running == 0:
                    self._replanning.drained()

            # (The in-loop scheduling bandit was removed; adaptive priority is
            # now driven by the ADR layer's BanditSchedulingPolicy, which feeds
            # its own reward from the observation each cycle.)

            freed_gpu_ids = self._replica_gpu_assignments.pop(replica_id, [])
            self._free_gpu_ids.extend(freed_gpu_ids)

        for gid in freed_gpu_ids:
            try:
                group.running_gpu_ids.remove(gid)
            except ValueError:
                pass

        if freed_gpu_ids:
            if self._replica_gpu_assignments:
                asgn_str = ", ".join(
                    f"{rid}→{gids}" for rid, gids in sorted(self._replica_gpu_assignments.items())
                )
                self._log.info(
                    f"  GPU freed: {replica_id!r} released {freed_gpu_ids}"
                    f"  | active: [{asgn_str}]"
                    f"  | free: {sorted(self._free_gpu_ids)}"
                )
            else:
                self._log.info(
                    f"  GPU freed: {replica_id!r} released {freed_gpu_ids}"
                    f"  | active: (none)"
                    f"  | free: {sorted(self._free_gpu_ids)}"
                )

        release_tag = ""
        if group.required_cpus > 0 or group.required_gpus > 0 or group.required_memory_gb > 0:
            release_tag = (
                f" | released cpus={group.required_cpus} gpus={group.required_gpus}"
                + (f" mem={group.required_memory_gb}GB" if group.required_memory_gb > 0 else "")
                + f" | available: {self._resources.available_str()}"
            )
        if freed_gpu_ids:
            release_tag += (
                f" [freed gpu_affinity={freed_gpu_ids} | free_gpus={sorted(self._free_gpu_ids)}]"
            )
        self._log.info(f"Replica {replica_id!r} finished{release_tag}")

        # Notify downstream sharders for every group that just transitioned
        # to "done" (the current group AND any downstream group that
        # propagated through _propagate_status_done_locked).
        for done_name in newly_done:
            self._log.info(f"Workflow group {done_name!r} completed")
            for sh_name, sharder in self._sharders.items():
                sh_group = self._workflows.get(sh_name)
                if sh_group and done_name in sh_group.dependencies:
                    sharder.mark_upstream_done()
                    self._log.info(
                        f"Sharder [{sh_name}]: upstream {done_name!r} done "
                        f"— partial tail ({sharder.buffered}) will flush next cycle"
                    )

        # ── Monitor: pass-through and budget drift checks ─────────────────────
        if self._monitor and group.finished_replicas > 0:
            trigger_name = (group.workflow_config or {}).get("trigger_downstream")
            expected_frac = float((group.workflow_config or {}).get("trigger_fraction", 1.0))
            _min_passthrough_sample = 10
            if (
                trigger_name
                and trigger_name in self._workflows
                and expected_frac < 1.0
                and group.finished_replicas >= _min_passthrough_sample
            ):
                # Use the shared buffer-aware helper so the reactive path
                # agrees with the periodic monitor (monitor_mixin.py).
                observed_frac = self._compute_passthrough(group.name, trigger_name)
                if observed_frac is not None:
                    ev = self._monitor.check_passthrough(group.name, observed_frac, expected_frac)
                    if ev:
                        tag = " [ESCALATING]" if self._monitor.is_escalating(ev) else ""
                        self._log.warning(
                            f"Monitor [{group.name}] pass_through drift{tag}: "
                            f"observed={observed_frac:.3f}  expected={expected_frac:.3f}"
                            f"  dev={ev.deviation_pct:.1f}%  breach={ev.breach_count}"
                        )

        # ── BudgetController tick — independent of monitor.  Runs whenever
        # a BudgetController is registered for the stage, regardless of
        # whether features.monitor is enabled.  Previously this was nested
        # under the monitor guard and silently disabled when monitor=False.
        if group.finished_replicas > 0:
            # ── BudgetController tick — feeds back into Triage cutoffs ────
            # Runs on every finish; the controller's internal warmup ensures
            # it doesn't act on early-stage noise.  Bound-locked outcomes
            # escalate to a BUDGET_LOCKED DriftEvent for the replan path.
            bc = self._budget_controllers.get(group.name)
            if bc is not None:
                # Compute spend from measured wall-time so ADVANCE-skipped
                # replicas actually register as ~zero cost.  Falls back to
                # the pilot reservation when no duration data is available.
                stage_wall_s = self._metrics.stage_wall_s(group.name)
                if stage_wall_s > 0:
                    spend_actual = stage_wall_s * bc.pilot_nodes / 3600.0
                else:
                    spend_actual = bc.pilot_nodes * bc.pilot_walltime_h * group.finished_replicas
                bev = bc.evaluate(
                    finished_replicas=group.finished_replicas,
                    actual_node_hours=spend_actual,
                )
                if bev is not None:
                    # Record every tick (in_band, nudged, bound_locked) so
                    # plot_budget_control.py has a full trajectory.
                    self._metrics.record_budget(
                        stage_id=group.name,
                        kind=bev.kind,
                        burn_ratio=bev.burn_ratio,
                        progress=bev.progress,
                        spend_node_hours=spend_actual,
                        finished=group.finished_replicas,
                        score_cutoff=bev.score_cutoff,
                        uncertainty_cutoff=bev.uncertainty_cutoff,
                        score_at_bound=bev.score_at_bound,
                        unc_at_bound=bev.unc_at_bound,
                        consecutive_hits=bev.consecutive_hits,
                        frozen=bev.frozen,
                    )
                if bev is not None and bev.kind != "in_band":
                    if bev.kind == "bound_locked":
                        self._log.warning(
                            f"BudgetController [{group.name}] BOUND-LOCKED  "
                            f"burn_ratio={bev.burn_ratio:.2f}  "
                            f"score_cutoff={bev.score_cutoff:.3f} (at_bound={bev.score_at_bound})  "
                            f"unc_cutoff={bev.uncertainty_cutoff:.3f} (at_bound={bev.unc_at_bound})  "
                            f"consecutive={bev.consecutive_hits} → replan recommended"
                        )
                        # Convert to a DriftEvent and route through the
                        # ReplanningController (when configured).  Fire and
                        # forget: the handshake runs asynchronously while
                        # _on_replica_finished proceeds with cleanup.
                        if self._replanning is not None:
                            from .monitor import DriftEvent, DriftKind

                            ev = DriftEvent(
                                kind=DriftKind.BUDGET_LOCKED,
                                stage_id=group.name,
                                observed=bev.burn_ratio,
                                expected=1.0,
                                deviation_pct=abs(bev.burn_ratio - 1.0) * 100,
                                breach_count=bev.consecutive_hits,
                            )
                            policy = (
                                self._plan.replan.on_drift if self._plan is not None else "log_only"
                            )
                            asyncio.get_running_loop().create_task(
                                self._replanning.on_drift(ev, policy=policy)
                            )
                    else:
                        self._log.info(
                            f"BudgetController [{group.name}] nudged  "
                            f"burn_ratio={bev.burn_ratio:.2f}  "
                            f"progress={bev.progress:.1%}  "
                            f"score_cutoff={bev.score_cutoff:.3f}  "
                            f"unc_cutoff={bev.uncertainty_cutoff:.3f}"
                        )

            # Monitor's budget drift check — only runs when monitor is enabled.
            if self._monitor:
                _wfcfg = group.workflow_config or {}
                budget = float(_wfcfg.get("budget_node_hours") or 0)
                if budget > 0 and group.replicas > 0:
                    pilot = _wfcfg.get("pilot", {})
                    nodes = int(pilot.get("nodes", 1))
                    walltime_h = float(pilot.get("walltime_h", 1))
                    spent_actual = nodes * walltime_h * group.finished_replicas / group.replicas
                    expected_so_far = budget * group.finished_replicas / group.replicas
                    ev = self._monitor.check_budget(group.name, spent_actual, expected_so_far)
                    if ev:
                        self._log.warning(
                            f"Monitor [{group.name}] budget drift: "
                            f"spent={spent_actual:.1f}  expected={expected_so_far:.1f} node-hours"
                            f"  dev={ev.deviation_pct:.1f}%"
                        )

        # ── Early termination: campaign_target ───────────────────────────────
        # Only fires when campaign_target > 0 (explicitly set in config).
        # downstream_input_target is the BudgetController denominator only and
        # must NOT trigger early stopping — plan configs always set it for every
        # stage even when early stop is not intended.
        if not self._all_done.is_set():
            for gname, g in self._workflows.items():
                target = int((g.workflow_config or {}).get("campaign_target") or 0)
                if target > 0 and g.finished_replicas >= target:
                    self._all_done.set()
                    self._log.info(
                        f"Campaign target reached: {gname!r} finished "
                        f"{g.finished_replicas}/{target} replicas — stopping early"
                    )

        await self._schedule()

        async with self._lock:
            all_done = _campaign_complete(self._workflows, self._sharders)

        if all_done:
            self._all_done.set()
            self._log.info("All campaign workflow groups finished")
