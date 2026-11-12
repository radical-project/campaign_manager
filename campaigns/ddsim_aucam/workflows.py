"""
MiniApps AuCaM adapter — thin wrapper around MiniAppsWorkflow.

One monolithic block (returned by make_miniapps_worker) wraps the full pipeline:
  simulation → train_model → evaluate_simulations (repeat until total_num_sim).

Score: mean per-sim MSE from predictions.yaml (lower = better model fit).
Scores are emitted after each evaluate_simulations() cycle, giving AuCaM a
mid-run signal it can use to cancel underperforming campaigns early.

Adapter design (thin layer):
  - _AuCaMAdapter mixin overrides evaluate_simulations() to fire a score
    callback after each prediction cycle.  All workflow logic stays in
    MiniAppsWorkflow; this file adds only the CM-required code.
  - make_miniapps_worker(cfg) returns a closure-bound block function.
    The config is captured in the closure, so concurrent campaigns with
    different configs never share or overwrite each other's state.
  - The asyncflow engine (flow=) is shared with MiniAppsWorkflow so that
    sim/train/predict tasks are registered on the same engine.  AuCaM
    passes flow= because the block declares a ``flow`` parameter.
"""

from __future__ import annotations

import logging
import math
import os
import socket
import sys
import time
from pathlib import Path

from src.aucam import WorkflowReporter

logger = logging.getLogger(__name__)


# ── MiniApps import (lazy: DDSIM_DIR must be set before first call) ───────────


def _ddsim_root() -> Path:
    d = os.environ.get("DDSIM_DIR")
    if not d:
        raise OSError("DDSIM_DIR is not set. Export it before launching the campaign.")
    root = Path(d)
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    return root


_MiniAppsWorkflow = None  # cached after first import


def _get_miniapps_class():
    global _MiniAppsWorkflow
    if _MiniAppsWorkflow is None:
        root = _ddsim_root()
        # Use importlib to load by file path — avoids sys.modules['workflows']
        # collision with this file (ddsim_aucam/workflows.py).
        import importlib.util  # noqa: PLC0415

        fpath = root / "workflows" / "miniapps_workflow" / "miniapps_workflow.py"
        spec = importlib.util.spec_from_file_location("_ddsim_miniapps_workflow", fpath)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _MiniAppsWorkflow = mod.MiniAppsWorkflow
    return _MiniAppsWorkflow


# ── Scoring mixin ─────────────────────────────────────────────────────────────


class _AuCaMAdapter:
    """Mixin that hooks evaluate_simulations() to fire an async score callback.

    Placed first in the MRO so its evaluate_simulations() wraps MiniAppsWorkflow's.
    The callback (_score_cb) is set as an instance attribute by the subclass below.
    """

    async def evaluate_simulations(self):
        await super().evaluate_simulations()
        cb = getattr(self, "_score_cb", None)
        if cb is None:
            return
        predictions = getattr(self, "sim_predictions", None) or {}
        if not predictions:
            logger.debug("[miniapps:%s] evaluate_simulations — no predictions yet", self.name)
            return
        score = sum(predictions.values()) / len(predictions)
        n_done = len(self.completed_sims)
        logger.info(
            "[miniapps:%s] prediction cycle done  score=%.4f  n_pred=%d  n_done=%d",
            self.name,
            score,
            len(predictions),
            n_done,
        )
        await cb(score, n_done)


_AdaptedClass = None  # singleton subclass, created once


def _get_adapted_class():
    global _AdaptedClass
    if _AdaptedClass is None:
        miniapps_cls = _get_miniapps_class()

        class _Adapted(_AuCaMAdapter, miniapps_cls):
            def __init__(self, *args, score_cb=None, **kwargs):
                super().__init__(*args, **kwargs)
                self._score_cb = score_cb

        _AdaptedClass = _Adapted
    return _AdaptedClass


# ── Block factory ─────────────────────────────────────────────────────────────


def _cpu_rate(declared: dict) -> float:
    """CPUs implied by declared_spending (cpu_hours per wall hour); 1 if not declared."""
    wall = declared.get("wall_hours", 0.0)
    return declared.get("cpu_hours", wall) / wall if wall > 0 else 1.0


def fit_to_limit(cfg: dict, limit_cpu_h: float, declared: dict) -> int:
    """How many simulations of this arm fit in ``limit_cpu_h``.

    The MiniApps pipeline runs as one call, so a run stays within its limit by
    sizing its work up front: declared cpu_hours is the cost of total_num_sim
    simulations, so each simulation costs that / total_num_sim.
    """
    total = int(cfg.get("total_num_sim", 1))
    cost = declared.get("cpu_hours")
    if not cost or math.isinf(limit_cpu_h):
        return total
    per_sim = cost / max(1, total)
    return max(1, min(total, int(limit_cpu_h // per_sim)))


def make_miniapps_worker(
    cfg: dict,
    arm_label: str = "wf",
    engine: str = "concurrent",
    declared_spending: dict | None = None,
    respect_limit: bool = True,
):
    """Return an AuCaM block function bound to the given MiniApps config dict.

    AuCaM passes each run its spending limit (context["spending_limit"]).  A run
    that respects it runs only as many simulations as fit (fit_to_limit); every
    report carries the run's cpu_hours so far (wall time × the declared CPU
    rate) and the CPUs it uses (cpus_in_use, the same rate), so AuCaM can
    cancel a run that spends past its limit or uses more CPUs than it was
    given (WorkflowConfig.resources).  The MiniApps pipeline sets its own
    parallelism, so the run does not resize its tasks to the resources.

    Using a factory (instead of a module-level configure()) ensures that
    concurrent campaigns with different configs each get their own closure
    and never clobber each other's settings.

    The returned coroutine declares ``flow``, so AuCaM passes it the shared
    asyncflow engine.
    """

    declared = dict(declared_spending or {})
    cpus = _cpu_rate(declared)

    async def miniapps_worker(
        reporter: WorkflowReporter, flow=None, context: dict | None = None
    ) -> None:
        root = _ddsim_root()
        adapted_cls = _get_adapted_class()
        t0 = time.monotonic()

        def spent() -> float:
            return cpus * (time.monotonic() - t0) / 3600.0

        worker_cfg = dict(cfg)
        limit = float((context or {}).get("spending_limit", {}).get("cpu_hours", math.inf))
        if not math.isinf(limit):
            if respect_limit:
                n = fit_to_limit(worker_cfg, limit, declared)
                logger.info(
                    "[miniapps_worker] %s: spending limit %.4f cpu_hours → %d of %s simulations",
                    arm_label,
                    limit,
                    n,
                    worker_cfg.get("total_num_sim"),
                )
                worker_cfg["total_num_sim"] = n
            else:
                logger.info(
                    "[miniapps_worker] %s: spending limit %.4f cpu_hours (ignored)",
                    arm_label,
                    limit,
                )
        if not worker_cfg.get("src_dir"):
            worker_cfg["src_dir"] = str(root / "workflows/miniapps_workflow")
        worker_cfg["engine"] = engine

        # Each workflow instance needs a unique filesystem-safe name: MiniAppsWorkflow
        # creates and cleans home_base/name/ on init, so clashes would delete live data.
        wf_tail = reporter.wf_id.split(".")[-1][:8] or reporter.wf_id[:8]
        safe_name = f"{arm_label}_{wf_tail}"
        home_base = Path(worker_cfg.get("home_dir") or Path.home() / "MiniApps").expanduser()

        async def _on_score(score: float, n_done: int) -> None:
            logger.info(
                "[miniapps_worker] emit  id=%s  score=%.4f  n_done=%d",
                reporter.wf_id,
                score,
                n_done,
            )
            await reporter.emit(
                score=score,
                metrics={
                    "score": score,
                    "n_completed": n_done,
                    "status": "evaluating",
                    "cpu_hours": spent(),
                    "cpus_in_use": cpus,
                },
            )

        logger.info(
            "[miniapps_worker] start  id=%s  num_step=%s  total_num_sim=%s",
            reporter.wf_id,
            worker_cfg.get("num_step", "?"),
            worker_cfg.get("total_num_sim", "?"),
        )

        workflow = adapted_cls(
            config=worker_cfg,
            asyncflow=flow,
            name=safe_name,
            home_dir=str(home_base),
            score_cb=_on_score,
        )

        await workflow.start()

        # Final emit — guarantees at least one score even when no
        # evaluate_simulations() cycle ran (e.g. early shutdown).
        predictions = getattr(workflow, "sim_predictions", None) or {}
        n_done = len(workflow.completed_sims)
        if predictions:
            score = sum(predictions.values()) / len(predictions)
        else:
            total = getattr(workflow, "total_num_sim", 1)
            score = 1.0 - n_done / max(1, total)

        logger.info(
            "[miniapps_worker] complete  id=%s  score=%.4f  n_done=%d  n_preds=%d",
            reporter.wf_id,
            score,
            n_done,
            len(predictions),
        )

        await reporter.emit(
            score=score,
            metrics={
                "score": score,
                "n_completed": n_done,
                "status": "complete",
                "node": socket.gethostname(),
                "cpu_hours": spent(),
                "cpus_in_use": cpus,
            },
        )

        await workflow.close()

    return miniapps_worker
