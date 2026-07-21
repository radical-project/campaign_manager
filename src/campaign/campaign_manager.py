"""
AsyncCampaignManager — async-native campaign orchestrator.

Each workflow replica is an asyncio Task.  Supports both
``async def run(replica_id)`` and sync ``def run(replica_id)`` entry points
(sync ones run via ``asyncio.to_thread``).

Scheduling model
----------------
Two-pass greedy scheduler on every state change (see scheduler.py):
  Pass 1 — guarantee ``concurrency_floor`` for all eligible groups (highest priority).
  Pass 2 — fill remaining capacity up to ``concurrency_cap`` (highest priority).

A group becomes eligible either via ``trigger_dependent()`` (explicit) or when
each dependency has ``dep_threshold`` finished replicas (count-based fallback).

Usage
-----
    cm = AsyncCampaignManager.from_config(config, WORKFLOW_REGISTRY)
    await cm.start()
    await cm.wait()
    await cm.close()
"""

import asyncio
import itertools
from typing import Optional

from ..utils.logger import Logger
from .backpressure import BackpressureNegotiator, BPState  # noqa: F401 (re-exported)

# (No bandit import: cross-stage scheduling priority is driven by the ADR
# layer's BanditSchedulingPolicy, not an in-CM bandit.)
from .base_workflow import BaseWorkflow
from .budget_controller import BudgetController, BudgetEvent  # noqa: F401
from .candidate_log import CandidateHistory, CandidateLog, StageResult  # noqa: F401
from .executor import ExecutorMixin
from .metrics import CampaignMetrics
from .monitor import DriftKind, Monitor  # noqa: F401 (re-exported)
from .monitor_mixin import MonitorMixin
from .plan import CampaignPlan, load_plan, plan_to_workflows_dict
from .replanning import ReplanningController, ReplanningState  # noqa: F401
from .scheduler import SchedulerMixin
from .sharder import Sharder, ShardingSpec
from .surrogate import Surrogate, build_default_surrogate
from .triage import Triage, TriageDecision  # noqa: F401 (re-exported)
from .types import CampaignState, ResourcePool, WorkflowStats, _WorkflowInfo


class AsyncCampaignManager(SchedulerMixin, ExecutorMixin, MonitorMixin):
    """
    Async campaign manager that orchestrates multiple replicas of one
    or more :class:`BaseWorkflow` subclasses.
    """

    def __init__(
        self,
        max_workers: Optional[int] = None,
        engine: str = "concurrent",
        total_cpus: int = 0,
        total_gpus: int = 0,
        total_memory_gb: float = 0.0,
        num_workers: Optional[int] = None,
        debug: bool = False,
        asyncflow=None,
        engine_dragon=None,
        features: Optional[dict] = None,
    ) -> None:
        self._log = Logger(name="AsyncCampaignManager", use_colors=True)
        self._seq = itertools.count()
        self._lock = asyncio.Lock()
        self._engine_type = engine
        self._num_workers = num_workers
        self._debug = debug
        self._asyncflow = asyncflow
        self._engine_dragon = engine_dragon
        self._gpu_pool: list[tuple[str, int]] = []
        self._free_gpu_ids: list[int] = []
        self._replica_gpu_assignments: dict[str, list[int]] = {}
        self._resources = ResourcePool(
            total_cpus=total_cpus,
            total_gpus=total_gpus,
            total_memory_gb=total_memory_gb,
        )

        self._workflows: dict[str, _WorkflowInfo] = {}
        self._stats: dict[str, WorkflowStats] = {}
        self._all_done = asyncio.Event()
        # Live replica tasks — tracked so close() can cancel any still in flight
        # (e.g. after an early-termination target or a wait() timeout) before the
        # asyncflow backend is torn down.  Without this, pending tasks trigger
        # "Task was destroyed but it is pending!" warnings at shutdown.
        self._replica_tasks: set[asyncio.Task] = set()
        # Set by close(); the scheduler stops launching new replicas once true,
        # so cancelling in-flight replicas during shutdown can't race the
        # scheduler into spawning fresh (uncancelled) ones.
        self._closing: bool = False

        self._features: dict[str, bool] = features or {}
        self._bp: dict[str, BackpressureNegotiator] = {}
        self._sharders: dict[str, Sharder] = {}
        self._monitor: Optional[Monitor] = None
        self._monitor_interval_s: float = 30.0  # overwritten by from_config
        self._monitor_task: Optional[asyncio.Task] = None
        self._candidate_log: Optional[CandidateLog] = None
        self._cand_seq: itertools.count = itertools.count()
        self._replica_candidate_assignments: dict[str, str] = {}
        # Candidate IDs of currently-running replicas (populated in
        # _allocate_locked, cleared in _on_replica_finished).  Read by
        # _flush_sharders_locked for diversity scoring against the set of
        # scaffolds actually executing right now (as opposed to
        # _replica_candidate_assignments which only covers the brief
        # window between allocation and entry into _run_replica).
        self._running_candidates: dict[str, str] = {}
        # Per-stage Triage and BudgetController populated by from_config when
        # the plan provides a SurrogateSpec with cutoffs+bounds and a
        # budget_node_hours target.  Used at trigger time (Triage gate) and
        # on the monitor tick (BudgetController nudge).
        self._triages: dict[str, Triage] = {}
        self._budget_controllers: dict[str, BudgetController] = {}
        # Per-stage Surrogate instances.  When a stage has one and the
        # workflow author didn't supply surrogate_pred / surrogate_unc at
        # trigger time, trigger_dependent fills them in.  After each replica
        # finishes, the surrogate's RecallTracker observes (predicted, actual)
        # and triggers BudgetController.freeze when recall drifts below the
        # plan's surrogate_recall_floor.
        self._surrogates: dict[str, Surrogate] = {}
        # Active campaign plan — None for legacy flat configs without plan_id.
        self._plan: Optional[CampaignPlan] = None
        # ReplanningController orchestrates the drain → replan → resume
        # handshake when drift escalates beyond the in-band envelope.
        # Built only when the plan opts in via replan.on_drift="drain_and_replan".
        self._replanning: Optional[ReplanningController] = None
        self._metrics: CampaignMetrics = CampaignMetrics()

        feat_summary = ", ".join(f"{k}={'on' if v else 'off'}" for k, v in self._features.items())
        self._log.info(
            f"AsyncCampaignManager initialised (engine={engine})"
            + (f"  features: [{feat_summary}]" if feat_summary else "")
        )

    # ------------------------------------------------------------------
    # Alternative constructor
    # ------------------------------------------------------------------

    @classmethod
    def from_config(
        cls,
        config: dict,
        workflow_registry: dict[str, type[BaseWorkflow]],
        asyncflow=None,
        engine_dragon=None,
    ) -> "AsyncCampaignManager":
        """Build an AsyncCampaignManager from a config dict + workflow registry.

        Accepts both shapes:
          - structured plan (top-level ``plan_id`` + ``stages`` + ``edges``)
          - legacy flat config (top-level ``workflows`` dict)
        Structured plans are validated by the schema in src/campaign/plan/
        and then flattened to the same workflows-dict shape the rest of
        from_config consumes.  See plan/loader.py for the conversion.
        """
        # Detect structured plan.  When the caller (e.g., run_campaign.py)
        # has already flattened the plan into a ``workflows`` dict with
        # workflow-specific keys we don't recognise, keep their dict and
        # use the typed plan only for Triage / BudgetController wiring.
        # When workflows is absent, render the plan ourselves via
        # plan_to_workflows_dict so the rest of from_config sees the
        # flat shape it expects.
        plan: Optional[CampaignPlan] = None
        if "plan_id" in config and "stages" in config:
            plan = load_plan(config)
            if "workflows" not in config:
                config = plan_to_workflows_dict(plan)
        res_cfg = config.get("resources", {})
        num_workers = config.get("num_workers")
        cm_cfg = config.get("cm", {})
        features = cm_cfg.get("features", {}) or config.get("features", {})

        cm = cls(
            max_workers=config.get("max_workers"),
            engine=config.get("engine", "concurrent"),
            total_cpus=int(res_cfg.get("total_cpus", 0)),
            total_gpus=int(res_cfg.get("total_gpus", 0)),
            total_memory_gb=float(res_cfg.get("total_memory_gb", 0.0)),
            num_workers=int(num_workers) if num_workers is not None else None,
            debug=bool(config.get("debug", False)),
            asyncflow=asyncflow,
            engine_dragon=engine_dragon,
            features=dict(features) if features else {},
        )

        # Both new (concurrency_floor / concurrency_cap) and legacy
        # (min_replicas / max_replicas) YAML keys are accepted; legacy keys
        # are stripped from the config dict passed to the workflow so they
        # don't accidentally leak through as workflow-level config.
        _cm_keys = {
            "replicas",
            "dependencies",
            "dependency_threshold",
            "concurrency_floor",
            "concurrency_cap",
            "min_replicas",
            "max_replicas",  # legacy aliases
            "priority",
            "required_cpus",
            "required_gpus",
            "required_memory_gb",
            "sharding",
        }

        for name, wf_cfg in config.get("workflows", {}).items():
            wf_class = workflow_registry.get(name)
            if wf_class is None:
                cm._log.warning(f"from_config: no class registered for {name!r} — skipping")
                continue

            has_deps = bool(wf_cfg.get("dependencies", []))
            default_replicas = 0 if has_deps else 1
            # Prefer new key; fall back to legacy alias.
            concurrency_cap = int(wf_cfg.get("concurrency_cap") or wf_cfg.get("max_replicas") or 0)
            concurrency_floor = int(
                wf_cfg.get("concurrency_floor") or wf_cfg.get("min_replicas") or 0
            )
            cm.register_workflow(
                name=name,
                workflow_class=wf_class,
                replicas=int(wf_cfg.get("replicas", default_replicas)),
                dependencies=list(wf_cfg.get("dependencies", [])),
                dep_threshold=int(wf_cfg.get("dependency_threshold", 1)),
                concurrency_floor=concurrency_floor,
                concurrency_cap=concurrency_cap,
                priority=int(wf_cfg.get("priority", 0)),
                required_cpus=int(wf_cfg.get("required_cpus", 0)),
                required_gpus=int(wf_cfg.get("required_gpus", 0)),
                required_memory_gb=float(wf_cfg.get("required_memory_gb", 0.0)),
                config={k: v for k, v in wf_cfg.items() if k not in _cm_keys} or None,
            )

        # ── Feature: Backpressure ─────────────────────────────────────────────
        if features.get("backpressure"):
            for name, wf_cfg in config.get("workflows", {}).items():
                hi = int(wf_cfg.get("backpressure_high") or 0)
                lo = int(wf_cfg.get("backpressure_low") or 0)
                if hi > 0 and lo > 0 and hi > lo:
                    # score_slack > 0 lets a high-quality upstream buffer raise the
                    # effective high-water mark (see BackpressureNegotiator.step()).
                    # Requires backpressure + sharder features to both be enabled;
                    # safe to configure regardless (0.0 = feature off).
                    score_slack = float(wf_cfg.get("backpressure_score_slack") or 0.0)
                    cm._bp[name] = BackpressureNegotiator(
                        edge_name=f"*_to_{name}",
                        high_water=hi,
                        low_water=lo,
                        score_slack=score_slack,
                    )
                    cm._log.info(
                        f"Backpressure [{name}]: high_water={hi}  low_water={lo}"
                        + (f"  score_slack={score_slack}" if score_slack else "")
                    )

        # ── Feature: Sharder ─────────────────────────────────────────────────
        if features.get("sharder"):
            for name, wf_cfg in config.get("workflows", {}).items():
                sh_raw = wf_cfg.get("sharding")
                if sh_raw and isinstance(sh_raw, dict):
                    spec = ShardingSpec.from_dict(sh_raw)
                    sharder = Sharder(name=name, spec=spec)
                    sharder._log_fn = cm._log.info
                    sharder._metrics_fn = lambda sid, n, sc, pr, _name=name: (
                        cm._metrics.record_shard(_name, sid, n, sc, pr)
                    )
                    cm._sharders[name] = sharder
                    cm._log.info(
                        f"Sharder [{name}]: target={spec.target_size} "
                        f"[{spec.min_size}, {spec.max_size}] stratify={spec.stratify}"
                    )

        # ── Candidate log (always enabled when any sharder exists) ───────────
        if any(wf_cfg.get("sharding") for wf_cfg in config.get("workflows", {}).values()):
            cm._candidate_log = CandidateLog()
            cm._log.info("CandidateLog enabled")

        # ── Feature: Monitor ──────────────────────────────────────────────────
        if features.get("monitor"):
            replan = config.get("replan", {})
            cm._monitor = Monitor(
                burn_dev_pct=float(replan.get("budget_burn_deviation_pct", 20.0)),
                passthrough_dev_pct=float(replan.get("pass_through_deviation_pct", 25.0)),
                recall_floor=float(replan.get("surrogate_recall_floor", 0.90)),
                breaches_to_escalate=2,
            )
            cm._monitor_interval_s = float(config.get("cm", {}).get("monitor_interval_s", 30.0))
            cm._log.info(
                f"Monitor enabled: pass_through_dev={cm._monitor.passthrough_dev_pct}%  "
                f"budget_dev={cm._monitor.burn_dev_pct}%  "
                f"escalate_after={cm._monitor.breaches_to_escalate} breaches  "
                f"interval={cm._monitor_interval_s}s"
            )

        # NOTE: the in-loop scheduling bandit was removed. Adaptive cross-stage
        # scheduling priority is now driven by the ADR layer (src/campaign/adr)
        # via the group.priority lever — wrap the SchedulingBandit as an ADR
        # BanditSchedulingPolicy to get the same behaviour. The scheduler orders
        # eligible groups purely by group.priority.

        # Store the parsed plan regardless of features so external callers
        # can inspect it via cm.state.plan.
        if plan is not None:
            cm._plan = plan

        # ── Flat-config Triage (no plan, triage: key in workflow config) ──────
        # When features.budget is set and a workflow config has a triage: block,
        # create a Triage without a BudgetController so the ADR view exposes
        # score_cutoff / score_cutoff_bounds in budget_controllers obs.
        if features.get("budget_control") or features.get("budget"):
            for name, wf_cfg in config.get("workflows", {}).items():
                if name in cm._triages:
                    continue  # already set by plan-based path below
                triage_cfg = wf_cfg.get("triage")
                if triage_cfg and isinstance(triage_cfg, dict):
                    bounds = triage_cfg.get("score_cutoff_bounds", [0.0, 1.0])
                    t = Triage(
                        stage_id=name,
                        score_cutoff=float(triage_cfg.get("score_cutoff", 0.5)),
                        score_cutoff_bounds=tuple(bounds),
                        uncertainty_cutoff=float(triage_cfg.get("uncertainty_cutoff", 1.0)),
                        uncertainty_cutoff_bounds=(0.0, 1.0),
                    )
                    cm._triages[name] = t
                    cm._log.info(
                        f"Triage [{name}]: score_cutoff={t.score_cutoff:.2f}"
                        f"  bounds={list(t.score_cutoff_bounds)}"
                        f"  unc_cutoff={t.uncertainty_cutoff:.2f}"
                    )

        # ── Triage + BudgetController per stage ──────────────────────────────
        # Gated by features.budget_control so other benchmark configurations
        # (sharding+bp, all_optimizations) stay unaffected
        # even if the plan defines surrogate specs.  When the flag is off, no
        # surrogates, no triages, no controllers, no replanning controller —
        # the CM behaves like the legacy flat-config path.
        if plan is not None and (features.get("budget_control") or features.get("budget")):
            for stage in plan.stages:
                if stage.surrogate is None:
                    continue
                triage = Triage.from_surrogate_spec(
                    stage.id,
                    stage.surrogate,
                    advance_threshold=stage.surrogate.advance_threshold,
                )
                cm._triages[stage.id] = triage
                if stage.budget_node_hours > 0 and stage.downstream_input_target > 0:
                    bc = BudgetController.from_stage_spec(
                        stage,
                        triage,
                        kp=stage.budget_kp,
                        warmup_min_finished=stage.budget_warmup_min,
                    )
                    cm._budget_controllers[stage.id] = bc
                    cm._log.info(
                        f"BudgetController [{stage.id}]: "
                        f"budget={stage.budget_node_hours} node-h  "
                        f"target={stage.downstream_input_target}  "
                        f"band=±{stage.burn_rate_band}  "
                        f"score_cutoff={stage.surrogate.score_cutoff} "
                        f"in {list(stage.surrogate.score_cutoff_nudge_bounds)}  "
                        f"unc_cutoff={stage.surrogate.uncertainty_cutoff} "
                        f"in {list(stage.surrogate.uncertainty_cutoff_nudge_bounds)}"
                    )
                else:
                    cm._log.info(
                        f"Triage [{stage.id}]: gate-only (no budget or no target) "
                        f"score_cutoff={stage.surrogate.score_cutoff} "
                        f"unc_cutoff={stage.surrogate.uncertainty_cutoff}"
                    )

                # Per-stage Surrogate — only when surrogate spec exists.
                # Recall drift on this surrogate FREEZES the BudgetController
                # (when one is configured for the same stage) so cutoff
                # nudging doesn't compound errors from a degraded model.
                bc_for_stage = cm._budget_controllers.get(stage.id)

                def _make_freeze_callback(_bc):
                    def _on_drift(recall: float, breaches: int) -> None:
                        if _bc is None:
                            return
                        # breaches=0 is the "recovered" signal from the
                        # RecallTracker — unfreeze and resume nudging.
                        if breaches == 0:
                            if _bc.frozen:
                                _bc.freeze(False)
                                cm._log.info(
                                    f"BudgetController [{_bc.stage_id}] unfrozen "
                                    f"(surrogate recall recovered to {recall:.2f})"
                                )
                        else:
                            if not _bc.frozen:
                                _bc.freeze(True)
                                cm._log.warning(
                                    f"BudgetController [{_bc.stage_id}] FROZEN "
                                    f"(surrogate recall {recall:.2f} < floor for "
                                    f"{breaches} consecutive observations)"
                                )

                    return _on_drift

                cm._surrogates[stage.id] = build_default_surrogate(
                    stage_id=stage.id,
                    spec=stage.surrogate,
                    seed=hash(stage.id) & 0xFFFF,
                    enable_recall=True,
                    on_recall_drift=_make_freeze_callback(bc_for_stage),
                    recall_floor=plan.replan.surrogate_recall_floor,
                )
                cm._log.info(
                    f"Surrogate [{stage.id}]: "
                    f"{type(cm._surrogates[stage.id]).__name__}  "
                    f"recall_floor={plan.replan.surrogate_recall_floor}"
                )

            # ── ReplanningController (opt-in via plan.replan.on_drift) ────
            # When the plan asks for "drain_and_replan", build the controller
            # so escalating drift events trigger the formal hand-off.  The
            # request_sink / response_source are populated by the caller via
            # cm.set_replan_io(sink, source) before start().
            if plan.replan.on_drift == "drain_and_replan":
                cm._replanning = ReplanningController(
                    plan_id=plan.plan_id,
                    plan_version=plan.plan_version,
                    log=cm._log,
                    snapshot_fn=lambda cm_ref=cm: cm_ref._replan_snapshot(),
                )
                cm._log.info(
                    f"ReplanningController enabled (on_drift={plan.replan.on_drift}, "
                    f"plan {plan.plan_id}@v{plan.plan_version})"
                )

        return cm

    # ------------------------------------------------------------------
    # Group registration
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_entry_point(workflow_class: type[BaseWorkflow]) -> str:
        """Detect which method the user defined as the workflow entry point.

        ``run`` is checked by comparing against BaseWorkflow.run (which is a
        NotImplementedError stub).  ``start`` is detected by walking the MRO
        from the workflow class upward, stopping at BaseWorkflow — so a
        ``start`` method inherited from a framework class above BaseWorkflow
        in the MRO (e.g. threading.Thread.start) is NOT mistaken for a
        user-defined entry point.
        """
        has_run = workflow_class.run is not BaseWorkflow.run

        has_start = False
        for cls in workflow_class.__mro__:
            if cls is BaseWorkflow or cls is object:
                break
            attr = cls.__dict__.get("start")
            if attr is not None and callable(attr):
                has_start = True
                break

        if has_run and has_start:
            raise ValueError(
                f"{workflow_class.__name__} defines both 'run' and 'start' — "
                "choose exactly one as the workflow entry point"
            )
        if not has_run and not has_start:
            raise ValueError(f"{workflow_class.__name__} must define either 'run' or 'start'")
        return "run" if has_run else "start"

    def register_workflow(
        self,
        name: str,
        workflow_class: type[BaseWorkflow],
        replicas: int = 1,
        dependencies: Optional[list[str]] = None,
        dep_threshold: int = 1,
        concurrency_floor: int = 0,
        concurrency_cap: int = 0,
        priority: int = 0,
        required_cpus: int = 0,
        required_gpus: int = 0,
        required_memory_gb: float = 0.0,
        config: Optional[dict] = None,
        # Legacy aliases — accepted for backward compatibility.
        min_replicas: Optional[int] = None,
        max_replicas: Optional[int] = None,
    ) -> None:
        # Honour legacy kwargs if the new ones weren't provided.
        if min_replicas is not None and concurrency_floor == 0:
            concurrency_floor = min_replicas
        if max_replicas is not None and concurrency_cap == 0:
            concurrency_cap = max_replicas

        entry_point = self._resolve_entry_point(workflow_class)
        effective_max = concurrency_cap if concurrency_cap > 0 else replicas

        self._workflows[name] = _WorkflowInfo(
            name=name,
            workflow_class=workflow_class,
            replicas=replicas,
            dependencies=list(dependencies or []),
            workflow_config=config,
            configured_replicas=replicas,
            concurrency_floor=concurrency_floor,
            concurrency_cap=effective_max,
            priority=priority,
            required_cpus=required_cpus,
            required_gpus=required_gpus,
            required_memory_gb=required_memory_gb,
            dep_threshold=dep_threshold,
            entry_point=entry_point,
        )
        self._stats[name] = WorkflowStats()
        self._log.info(
            f"Registered workflow {name!r}: replicas={replicas} "
            f"min={concurrency_floor} max={effective_max} "
            f"deps={dependencies or []} dep_threshold={dep_threshold} "
            f"resources=(cpus={required_cpus}, gpus={required_gpus}, "
            f"mem={required_memory_gb}GB)"
        )

    # Deprecated alias retained for backward compatibility — prefer
    # register_workflow.  Forwards all kwargs (including legacy
    # min_replicas / max_replicas aliases).
    register_group = register_workflow

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _setup_resources(self) -> None:
        """Sync CM resource state against the pre-built asyncflow engine.

        Called once from start(). The caller (run_campaign.py) is responsible
        for creating the backend and WorkflowEngine before passing asyncflow=
        to from_config() / __init__. This method only does CM-side setup:
        debug logging, GPU pool discovery, and ResourcePool cap correction.

        Raises RuntimeError if asyncflow was not provided.
        """
        if self._asyncflow is None:
            raise RuntimeError(
                "asyncflow engine not provided — create the backend and "
                "WorkflowEngine in your run script and pass asyncflow= to from_config()"
            )

        if self._debug:
            try:
                from rhapsody import enable_logging

                enable_logging(level="DEBUG")
                import logging as _logging

                _logging.getLogger("radical.asyncflow").setLevel(_logging.WARNING)
                _logging.getLogger("asyncio").setLevel(_logging.WARNING)
                self._log.warning("rhapsody.enable_logging active")
            except ImportError:
                self._log.warning("rhapsody.enable_logging not available — skipping")

        from .gpu import detect_gpus, find_gpus

        if self._engine_type == "dragon":
            self._gpu_pool = find_gpus()
            self._free_gpu_ids = [gid for _, gid in self._gpu_pool]
            actual_gpus = len(self._free_gpu_ids)
            if actual_gpus != self._resources.total_gpus:
                self._log.warning(
                    f"config total_gpus={self._resources.total_gpus} "
                    f"!= discovered GPUs={actual_gpus} — "
                    f"capping ResourcePool to {actual_gpus}"
                )
                self._resources.total_gpus = actual_gpus
                self._resources.available_gpus = actual_gpus
            self._log.info(
                f"GPU pool: {len(self._gpu_pool)} GPU(s) — "
                + (", ".join(f"{h}:{g}" for h, g in self._gpu_pool) or "none found")
            )
        else:
            # Concurrent mode: auto-detect CUDA GPUs for assignment tracking.
            if not self._free_gpu_ids:
                n = detect_gpus()
                self._free_gpu_ids = list(range(n))
                if n:
                    self._log.info(f"Concurrent mode: auto-detected {n} GPU(s) for assignment")

        self._log.info(f"CM ready (engine={self._engine_type})")

    async def start(self) -> None:
        """Kick off the campaign — schedule all eligible groups."""
        if not self._workflows:
            self._all_done.set()
            return
        await self._setup_resources()
        res = self._resources
        if res.total_cpus > 0 or res.total_gpus > 0 or res.total_memory_gb > 0:
            self._log.info(
                f"Resource pool: total_cpus={res.total_cpus}  "
                f"total_gpus={res.total_gpus}  "
                f"total_memory_gb={res.total_memory_gb}"
            )
        if self._monitor is not None:
            self._monitor_task = self._start_monitor_loop(self._monitor_interval_s)
        await self._schedule()

    async def wait(self, timeout: Optional[float] = None) -> bool:
        """Block (async) until all workflow groups have finished."""
        if timeout is not None:
            # Wrap in an explicit Task so we can cancel the shielded waiter on
            # timeout — asyncio.shield() leaves an orphaned pending Task if we
            # just let wait_for discard it, causing "Task was destroyed but it
            # is pending!" warnings at shutdown.
            inner: asyncio.Task = asyncio.ensure_future(self._all_done.wait())
            try:
                await asyncio.wait_for(asyncio.shield(inner), timeout=timeout)
                return True
            except asyncio.TimeoutError:
                inner.cancel()
                try:
                    await inner
                except (asyncio.CancelledError, Exception):
                    pass
                return False
        await self._all_done.wait()
        return True

    async def close(self) -> None:
        """Release CM resources (asyncflow shutdown left to caller)."""
        # Stop the scheduler first so cancelling in-flight replicas (below) can't
        # free resources and race the scheduler into launching fresh, uncancelled
        # ones — that race is what leaks "Task was destroyed but it is pending!".
        self._closing = True

        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

        # Cancel any replica tasks still in flight (early-termination target hit
        # or wait() timeout) so the asyncflow backend isn't torn down underneath
        # them.  Loop until quiescent: a cancelled replica's done-callbacks run
        # during the gather and may enqueue more work before _closing takes hold.
        for _ in range(5):
            pending = [t for t in self._replica_tasks if not t.done()]
            if not pending:
                break
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        self._replica_tasks.clear()
        self._asyncflow = None
        self._metrics.finish()
        self._log.info("AsyncCampaignManager closed")

    # ------------------------------------------------------------------
    # Public control API
    # ------------------------------------------------------------------

    async def stop(self) -> None:
        """Signal the campaign to stop early (e.g. an ADR goal was reached).

        Sets the internal completion event so that ``wait()`` returns immediately.
        In-flight replicas are cancelled by the subsequent ``close()`` call.
        """
        if not self._all_done.is_set():
            self._all_done.set()
            self._log.info("Campaign stopped by external signal (ADR goal reached)")

    async def signal_done(self, group_name: str) -> None:
        """Signal that *group_name* has produced output; queue 1 replica in each dependent."""
        async with self._lock:
            group = self._workflows.get(group_name)
            if group is None:
                return
            group.ready = True
            dependents = [g for g in self._workflows.values() if group_name in g.dependencies]
            for dep in dependents:
                dep.replicas += 1
                dep.configured_replicas += 1
                if dep.status == "done":
                    dep.status = "pending"
            if dependents:
                self._log.info(
                    f"{group_name!r} signaled done → +1 replica for {[d.name for d in dependents]}"
                )
        await self._schedule()

    async def trigger_dependent(
        self,
        name: str,
        replicas: int = 1,
        config: Optional[dict] = None,
        candidate_id: Optional[str] = None,
        score: float = 0.0,
        surrogate_pred: float = 0.0,
        surrogate_unc: float = 0.0,
        scaffold_class: str = "",
        source_stage: str = "",
    ) -> None:
        """Queue replicas of the dependent group *name*.

        When candidate signals are supplied (candidate_id, score, …) the call
        is treated as a single candidate trigger:
          1. The result is recorded in CandidateLog for *source_stage*.
          2. threshold_top_fraction of *source_stage* gates whether the candidate
             enters the sharder buffer.
          3. The sharder ranks the buffer by profile-weighted priority on dispatch.

        When candidate_id is None the call is a count-based trigger (legacy API):
        *replicas* anonymous entries are added to the buffer with score=0.
        Routes directly to group.replicas when no sharder is registered.
        """
        async with self._lock:
            group = self._workflows.get(name)
            if group is None:
                self._log.warning(f"trigger_dependent: group {name!r} not registered — ignoring")
                return
            if config:
                group.workflow_config = {**(group.workflow_config or {}), **config}

            sharder = self._sharders.get(name)
            if sharder is not None:
                if candidate_id is not None:
                    # ── Surrogate fill-in (when caller didn't supply) ────────
                    # surrogate_pred=0 and surrogate_unc=0 are the defaults;
                    # treat that as "missing" and ask the new-stage surrogate
                    # to predict.  Workflows that already inline their own
                    # predictions (legacy dreamer path) pass real values and
                    # this branch is skipped.
                    dst_surrogate = self._surrogates.get(name)
                    if dst_surrogate is not None and surrogate_pred == 0.0 and surrogate_unc == 0.0:
                        surrogate_pred, surrogate_unc = dst_surrogate.predict(
                            candidate_id,
                            score=score,
                            scaffold_class=scaffold_class,
                        )

                    # ── Candidate-aware single-trigger path ──────────────────
                    enqueue_time = None
                    if self._candidate_log and source_stage:
                        # Capture the prior record (if any) BEFORE recording
                        # the new one — the prior holds the prediction made
                        # for source_stage's output, which we now know.
                        existing_history = self._candidate_log.get(candidate_id)
                        prior_pred_for_source = None
                        if existing_history is not None and existing_history.results:
                            prior_pred_for_source = existing_history.results[-1].surrogate_pred

                        result = self._candidate_log.record(
                            candidate_id,
                            source_stage,
                            score,
                            surrogate_pred,
                            surrogate_unc,
                            scaffold_class,
                        )
                        enqueue_time = self._candidate_log.get(candidate_id).enqueue_time

                        # ── Surrogate recall update for source_stage ────────
                        # Feed (prior_pred, actual=score) back to source's
                        # surrogate so its RecallTracker can detect drift.
                        if prior_pred_for_source is not None:
                            src_surrogate = self._surrogates.get(source_stage)
                            if src_surrogate is not None:
                                src_surrogate.update_with_results(
                                    [(candidate_id, prior_pred_for_source, score)]
                                )

                        src_group = self._workflows.get(source_stage)
                        top_frac = (
                            float(
                                (src_group.workflow_config or {}).get("threshold_top_fraction", 1.0)
                            )
                            if src_group
                            else 1.0
                        )
                        if not self._candidate_log.passes_threshold(
                            candidate_id, source_stage, top_frac
                        ):
                            cutoff = self._candidate_log.threshold_cutoff(source_stage, top_frac)
                            self._log.info(
                                f"  Filtered {candidate_id!r} at {source_stage!r}: "
                                f"score={score:.4f} < cutoff={cutoff:.4f} "
                                f"(top-{top_frac:.0%})"
                            )
                            result.decision = "filtered"
                            return
                        result.decision = "passed"
                    # ── Triage gate (budget-adaptive surrogate cutoffs) ──────
                    # Runs when the downstream stage has a Triage configured.
                    # The Triage's cutoffs are nudged by BudgetController over
                    # time, so this gate tightens/loosens automatically as the
                    # campaign progresses.
                    triage = self._triages.get(name)
                    if triage is not None:
                        decision = triage.decide(score, surrogate_pred, surrogate_unc)
                        if decision is TriageDecision.DISCARD:
                            self._log.info(
                                f"  Triaged {candidate_id!r} → DISCARD at {name!r}: "
                                f"score={score:.3f} surrogate_pred={surrogate_pred:.3f} "
                                f"surrogate_unc={surrogate_unc:.3f}  "
                                f"(score_cutoff={triage.score_cutoff:.3f} "
                                f"unc_cutoff={triage.uncertainty_cutoff:.3f})"
                            )
                            if self._candidate_log and source_stage:
                                self._candidate_log.get(candidate_id).results[
                                    -1
                                ].decision = "triaged_discard"
                            return
                        # ADVANCE: confident high-quality candidate — mark
                        # the lineage so the executor can short-circuit the
                        # expensive computation when the workflow honours the
                        # candidate_triage_advance flag.  The candidate still
                        # enters the queue; the workflow decides what to skip.
                        if decision is TriageDecision.ADVANCE:
                            # Per-candidate ADVANCE log was a wall-time killer
                            # in benchmarks (thousands of formatted INFO lines).
                            # The decision is stamped on CandidateLog below and
                            # surfaces in replica_events' duration_s (~0 for
                            # skipped) for plot_budget_control.py to count.
                            if self._candidate_log and source_stage:
                                self._candidate_log.get(candidate_id).results[
                                    -1
                                ].decision = "triaged_advance"
                    sharder.receive(
                        candidate_id=candidate_id,
                        score=score,
                        surrogate_pred=surrogate_pred,
                        surrogate_unc=surrogate_unc,
                        scaffold_class=scaffold_class,
                        enqueue_time=enqueue_time,
                    )
                    # Per-candidate trigger log silenced: with ADVANCE-heavy
                    # workloads this fires thousands of times per run and
                    # dominates wall-clock time.  buffer depth still visible
                    # via sharder dispatch logs and CampaignMetrics events.
                else:
                    # ── Anonymous count-based path (legacy) ──────────────────
                    for _ in range(replicas):
                        anon_id = f"cand_{name}_{next(self._cand_seq):06d}"
                        sharder.receive(
                            candidate_id=anon_id,
                            score=score,
                            surrogate_pred=surrogate_pred,
                            surrogate_unc=surrogate_unc,
                            scaffold_class=scaffold_class,
                        )
                    self._log.info(
                        f"trigger_dependent: {name!r} +{replicas} anonymous → shard buffer "
                        f"(buffered={sharder.buffered})"
                    )
            else:
                group.replicas += replicas
                group.configured_replicas += replicas
                if group.status == "done":
                    group.status = "pending"
                self._log.info(
                    f"trigger_dependent: {name!r} +{replicas} replicas (total={group.replicas})"
                )
        await self._schedule()

    async def trigger_candidate(
        self,
        name: str,
        candidate_id: str,
        score: float = 0.0,
        surrogate_pred: float = 0.0,
        surrogate_unc: float = 0.0,
        scaffold_class: str = "",
        source_stage: str = "",
        config: Optional[dict] = None,
    ) -> None:
        """Convenience wrapper for a single named-candidate trigger.

        Equivalent to trigger_dependent(name, replicas=1, candidate_id=...).
        Workflows prefer this over trigger_dependent when they have scored results.
        """
        await self.trigger_dependent(
            name=name,
            replicas=1,
            config=config,
            candidate_id=candidate_id,
            score=score,
            surrogate_pred=surrogate_pred,
            surrogate_unc=surrogate_unc,
            scaffold_class=scaffold_class,
            source_stage=source_stage,
        )

    async def trigger_batch(
        self,
        name: str,
        candidates: list[dict],
    ) -> None:
        """Add N candidates to the sharder buffer in one lock acquisition.

        Each dict in *candidates* must contain ``candidate_id`` and may include
        ``score``, ``scaffold_class``, ``surrogate_pred``, ``surrogate_unc``.

        Unlike N sequential trigger_dependent() calls (each of which calls
        _schedule() after releasing the lock), this method holds the lock through
        all sharder.receive() calls and calls _schedule() exactly once.  The
        sharder buffer therefore accumulates N candidates before the first
        dispatch() runs, enabling meaningful priority ranking across the batch.

        Routes directly to group.replicas when no sharder is registered.
        """
        async with self._lock:
            group = self._workflows.get(name)
            if group is None:
                self._log.warning(f"trigger_batch: group {name!r} not registered — ignoring")
                return
            sharder = self._sharders.get(name)
            n_added = 0
            for cand in candidates:
                candidate_id = str(cand["candidate_id"])
                score = float(cand.get("score", 0.0))
                scaffold_class = str(cand.get("scaffold_class", ""))
                surrogate_pred = float(cand.get("surrogate_pred", 0.0))
                surrogate_unc = float(cand.get("surrogate_unc", 0.0))
                if sharder is not None:
                    sharder.receive(
                        candidate_id=candidate_id,
                        score=score,
                        surrogate_pred=surrogate_pred,
                        surrogate_unc=surrogate_unc,
                        scaffold_class=scaffold_class,
                    )
                else:
                    group.replicas += 1
                    group.configured_replicas += 1
                    if group.status == "done":
                        group.status = "pending"
                n_added += 1
            self._log.info(
                f"trigger_batch: {name!r} +{n_added} candidates "
                f"(buffered={sharder.buffered if sharder else '—'})"
            )
        await self._schedule()

    # ------------------------------------------------------------------
    # Status / stats
    # ------------------------------------------------------------------

    def status(self) -> dict:
        return {
            "resources": self._resources.as_dict(),
            "groups": {
                name: {
                    "status": g.status,
                    "replicas_total": g.replicas,
                    "replicas_configured": g.configured_replicas,
                    "replicas_started": g.started_count,
                    "replicas_running": g.running_count,
                    "replicas_finished": g.finished_replicas,
                    "concurrency_floor": g.concurrency_floor,
                    "concurrency_cap": g.concurrency_cap,
                    "required_cpus": g.required_cpus,
                    "required_gpus": g.required_gpus,
                    "required_memory_gb": g.required_memory_gb,
                    "dep_threshold": g.dep_threshold,
                    "ready": g.ready,
                    "dependencies": g.dependencies,
                }
                for name, g in self._workflows.items()
            },
        }

    def stats(self) -> dict[str, WorkflowStats]:
        return {name: WorkflowStats(**vars(s)) for name, s in self._stats.items()}

    def metrics(self) -> CampaignMetrics:
        """Return the live metrics recorder for this campaign run."""
        return self._metrics

    def set_score_cutoff(self, stage: str, value: float) -> bool:
        """Absolute-set the triage score cutoff for a stage, clamped to its bounds.

        Convenience method for non-ADR callers (tests, scripts, REPL).
        Within an ADR operator prefer view.set_score_cutoff() directly.

        Note: BudgetController nudges the cutoff every scheduler cycle, so this
        must be called repeatedly to maintain the target (last write wins).
        Returns False if the stage has no active triage.
        """
        triage = self._triages.get(stage)
        if triage is None:
            return False
        triage.nudge_cutoffs(value - triage.score_cutoff, 0.0)
        return True

    def set_replan_io(
        self,
        request_sink=None,
        response_source=None,
    ) -> None:
        """Configure the I/O endpoints the ReplanningController will use.

        request_sink:    async callable(ReplanRequest) → None
        response_source: async callable(ReplanRequest) → CampaignPlan
        Either may be None — sink-only mode logs requests; missing
        response_source leaves the campaign in AWAITING_PLAN until
        the user supplies a new plan manually.
        """
        if self._replanning is None:
            self._log.warning(
                "set_replan_io: no ReplanningController active "
                "(plan.replan.on_drift != 'drain_and_replan')"
            )
            return
        self._replanning.request_sink = request_sink
        self._replanning.response_source = response_source
        self._replanning.on_resume = self._apply_new_plan_for_resume

    def _replan_snapshot(self) -> dict:
        """Snapshot of the current campaign state, attached to ReplanRequest."""
        return {
            "workflows": {
                name: {
                    "status": w.status,
                    "replicas": w.replicas,
                    "started_count": w.started_count,
                    "running_count": w.running_count,
                    "finished_replicas": w.finished_replicas,
                }
                for name, w in self._workflows.items()
            },
            "resources": self._resources.as_dict(),
            "triages": {sid: t.state() for sid, t in self._triages.items()},
            "budget_controllers": {sid: bc.state() for sid, bc in self._budget_controllers.items()},
        }

    async def _apply_new_plan_for_resume(self, new_plan: CampaignPlan) -> None:
        """Refresh per-stage Triages and BudgetControllers from a new plan.

        Called by ReplanningController on RESUMING.  Existing in-memory
        candidate state and bandit posteriors are preserved — only the
        thresholds / budgets / bounds get swapped.  Per-stage triage cutoffs
        are reset to the new plan's initial values.
        """
        from .budget_controller import BudgetController
        from .triage import Triage

        async with self._lock:
            self._plan = new_plan
            # Rebuild triages and controllers for every stage with surrogate
            new_triages: dict[str, Triage] = {}
            new_controllers: dict[str, BudgetController] = {}
            for stage in new_plan.stages:
                if stage.surrogate is None:
                    continue
                triage = Triage.from_surrogate_spec(stage.id, stage.surrogate)
                new_triages[stage.id] = triage
                if stage.budget_node_hours > 0 and stage.downstream_input_target > 0:
                    new_controllers[stage.id] = BudgetController.from_stage_spec(
                        stage,
                        triage,
                        kp=stage.budget_kp,
                        warmup_min_finished=stage.budget_warmup_min,
                    )
            self._triages = new_triages
            self._budget_controllers = new_controllers
        self._log.info(
            f"Applied new plan {new_plan.plan_id}@v{new_plan.plan_version}: "
            f"{len(new_triages)} triages, {len(new_controllers)} budget controllers refreshed"
        )

    @property
    def state(self) -> CampaignState:
        """Structured view of the CM's cross-mixin shared state.

        Returns a CampaignState whose fields are references to the live
        underlying objects (no copy).  Use this in tests and external
        introspection instead of poking at private attributes — the
        attribute names are stable across refactors that may rearrange
        the underlying storage.
        """
        return CampaignState(
            lock=self._lock,
            workflows=self._workflows,
            resources=self._resources,
            sharders=self._sharders,
            bp=self._bp,
            candidate_log=self._candidate_log,
            monitor=self._monitor,
            running_candidates=self._running_candidates,
            replica_candidate_assignments=self._replica_candidate_assignments,
            replica_gpu_assignments=self._replica_gpu_assignments,
            free_gpu_ids=self._free_gpu_ids,
            gpu_pool=self._gpu_pool,
            all_done=self._all_done,
            metrics=self._metrics,
            stats=self._stats,
            features=self._features,
            plan=self._plan,
            triages=self._triages,
            budget_controllers=self._budget_controllers,
            surrogates=self._surrogates,
            replanning=self._replanning,
            log=self._log,
        )

    # ------------------------------------------------------------------
    # Internal — schedule dispatch (called outside the lock)
    # ------------------------------------------------------------------

    async def _schedule(self) -> None:
        async with self._lock:
            to_start = self._schedule_locked()
        for group, replica_idx in to_start:
            task = asyncio.get_running_loop().create_task(self._run_replica(group, replica_idx))
            self._replica_tasks.add(task)
            task.add_done_callback(self._on_replica_task_done)

    def _on_replica_task_done(self, task: "asyncio.Task") -> None:
        """Surface unhandled exceptions from replica tasks.

        _run_replica wraps its body in try/finally so the normal cleanup path
        always runs, but a pathological exception escaping the finally (or
        a bug in the cleanup itself) would otherwise be silently swallowed
        by the task object.
        """
        self._replica_tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None and not isinstance(exc, asyncio.CancelledError):
            self._log.error(f"Replica task raised unhandled exception: {type(exc).__name__}: {exc}")
