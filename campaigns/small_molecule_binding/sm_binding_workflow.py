"""
SmMolBindingWorkflow — Campaign Manager workflow that wraps one complete
SmallMoleculeBindingPipeline run (black-box A1 integration).

Each CM replica is one full IMPRESS pipeline execution with its own
ImpressManager and LocalExecutionBackend.  On completion, the replica
writes {replica_id}_result.json so the ADR operator can read results.

Environment variables (required at runtime):
  SM_BINDING_EXAMPLES_DIR  — path to IMPRESS/examples/small_molecule_binding/
  IMPRESS_SRC              — path to IMPRESS/src/ (for `impress` package)

Config keys (per workflow group in config.yaml):
  input_dir    str    path to input PDB directory, relative to SM_BINDING_EXAMPLES_DIR
                      (e.g. "p1_in/input_pdbs")
  work_dir     str    base dir for pipeline output dirs  (default: cwd)
  min_plddt    float  pLDDT pass threshold               (default: 70.0)
  mock         bool   use SmallMoleculeBindingPipeline mock mode (default: False)
  backbone_max_ca_deviation  float  (default 1.0)
  backbone_min_ss_fraction   float  (default 0.5)
  fastrelax_max_fa_rep       float  (default 100.0)
  fastrelax_max_total_score  float  (default 0.0)
  interface_min_sc           float  (default 0.35)
  fold_min_plddt             float  (default 70.0)
"""

from __future__ import annotations

import json
import logging
import os
from typing import ClassVar

from src.campaign import BaseWorkflow

log = logging.getLogger(__name__)


class SmMolBindingWorkflow(BaseWorkflow):
    """One complete SmallMoleculeBindingPipeline run per CM replica."""

    workflow_id = "sm_binding"

    # Campaign-level result accumulator (ClassVar — shared across all replicas).
    _results: ClassVar[list] = []

    @classmethod
    def reset_state(cls) -> None:
        cls._results = []

    # ── Compute ────────────────────────────────────────────────────────────────

    async def run(self, replica_id: str) -> None:
        cfg = self.config or {}

        _wd = cfg.get("work_dir", "")
        work_dir = os.path.abspath(_wd) if _wd else os.getcwd()
        os.makedirs(work_dir, exist_ok=True)
        input_dir = cfg.get("input_dir", "")
        mock      = bool(cfg.get("mock", False))

        examples_dir = os.environ.get("SM_BINDING_EXAMPLES_DIR", "")
        foundry_sif_path = cfg.get("foundry_sif_path", os.environ.get("FOUNDRY_SIF_PATH", ""))
        mpnn_path = cfg.get("mpnn_dir", os.environ.get("MPNN_PATH", ""))
        colabfold_path = cfg.get("colabfold_path", os.environ.get("COLABFOLD_PATH", ""))
        scripts_path = cfg.get(
            "scripts_path",
            os.path.join(examples_dir, "scripts") if examples_dir else "",
        )

        pipeline_kwargs = {
            "base_path":                  work_dir,
            "mock":                       mock,
            # Quality thresholds
            "backbone_max_ca_deviation":  float(cfg.get("backbone_max_ca_deviation", 1.0)),
            "backbone_min_ss_fraction":   float(cfg.get("backbone_min_ss_fraction",  0.5)),
            "fastrelax_max_fa_rep":       float(cfg.get("fastrelax_max_fa_rep",      100.0)),
            "fastrelax_max_total_score":  float(cfg.get("fastrelax_max_total_score", 0.0)),
            "fastrelax_max_interact":     float(cfg.get("fastrelax_max_interact",    0.0)),
            "interface_min_sc":           float(cfg.get("interface_min_sc",          0.35)),
            "fold_min_plddt":             float(cfg.get("fold_min_plddt",            70.0)),
            # Ensemble sizes / iteration counts
            "diffusion_batch_size":       int(cfg.get("diffusion_batch_size",        2)),
            "num_refine_cycles":          int(cfg.get("num_refine_cycles",           3)),
            "num_seqs":                   int(cfg.get("num_seqs",                    4)),
            "mpnn_ensemble_size":         int(cfg.get("mpnn_ensemble_size",          1)),
            "max_tasks":                  int(cfg.get("max_tasks",                   300)),
        }
        if foundry_sif_path:
            pipeline_kwargs["foundry_sif_path"] = foundry_sif_path
        if mpnn_path:
            pipeline_kwargs["mpnn_dir"] = mpnn_path
        if colabfold_path:
            pipeline_kwargs["colabfold_path"] = colabfold_path
        if scripts_path:
            pipeline_kwargs["scripts_path"] = scripts_path
        if self.policies:
            pipeline_kwargs["policy"] = self.policies[0]

        # Resolve input directory.
        # The pipeline reads its inputs from {base_path}/{name}_in/.
        # We symlink {replica_id}_in/ → the configured input_dir so all
        # explore/exploit replicas share the same starting PDB inputs.
        if input_dir:
            src_input_dir = os.path.join(examples_dir, input_dir) if examples_dir else input_dir
            src_input_dir = os.path.abspath(src_input_dir)
        else:
            src_input_dir = os.path.join(examples_dir, "p1_in", "input_pdbs") if examples_dir else ""

        link_path = os.path.join(work_dir, f"{replica_id}_in")
        if src_input_dir:
            if os.path.lexists(link_path) and os.readlink(link_path) != src_input_dir:
                os.unlink(link_path)
            if not os.path.lexists(link_path):
                os.symlink(src_input_dir, link_path)
                log.info("symlinked %s → %s", link_path, src_input_dir)

        log.info("starting IMPRESS pipeline %s (mock=%s)", replica_id, mock)

        # Import here so CM workers that don't have IMPRESS installed can
        # still import this module (imports fail only when run() is called).
        from impress import ImpressManager, PipelineSetup
        from small_molecule_binding import SmallMoleculeBindingPipeline
        from run_small_molecule_binding import adaptive_decision

        # All shell tasks use local_task=True + asyncio.create_subprocess_shell(),
        # so there is no Dragon channel routing. We can safely share the CM's
        # Dragon backend across replicas without task-result cross-contamination.
        if self.engine_dragon is not None:
            backend = self.engine_dragon
        else:
            from concurrent.futures import ProcessPoolExecutor
            from radical.asyncflow import LocalExecutionBackend
            backend = await LocalExecutionBackend(ProcessPoolExecutor())

        manager = ImpressManager(execution_backend=backend)

        setup = PipelineSetup(
            name=replica_id,
            type=SmallMoleculeBindingPipeline,
            adaptive_fn=adaptive_decision,
            kwargs=pipeline_kwargs,
        )

        await manager.start(pipeline_setups=[setup])
        await manager.flow.shutdown()

        log.info("IMPRESS pipeline %s complete", replica_id)

    # ── Completion hook ────────────────────────────────────────────────────────

    async def on_replica_done(self, replica_id: str, cm, final_state: str) -> None:
        cfg = self.config or {}
        work_dir = cfg.get("work_dir", os.getcwd())

        result = {
            "replica_id": replica_id,
            "group":      self._group_name,
            "status":     final_state,
        }

        # Collect best mean pLDDT from AF2 scores JSONs produced by the pipeline.
        # Files live at: {work_dir}/{replica_id}/*_alphafold/out/binder_scores_*.json
        try:
            import glob
            pattern = os.path.join(work_dir, replica_id, "*_alphafold", "out", "binder_scores_*.json")
            score_files = glob.glob(pattern)
            best = 0.0
            for sf in score_files:
                with open(sf) as f:
                    arr = json.load(f).get("plddt", [])
                if arr:
                    best = max(best, sum(arr) / len(arr))
            if best > 0.0:
                result["best_plddt"] = best
        except Exception as e:
            log.warning("could not extract pLDDT for %s: %s", replica_id, e)

        out_path = os.path.join(work_dir, f"{replica_id}_result.json")
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2)

        SmMolBindingWorkflow._results.append(result)
        log.info(
            "  %s  status=%s  group=%s  result → %s",
            replica_id, final_state, self._group_name, out_path,
        )
