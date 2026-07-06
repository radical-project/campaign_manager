"""
DDMdWrapperWorkflow — wraps the real DDMdWorkflow from DeepDriveSim.

DAG routing via _on_completion:
  md (done) ──→ miniapps
Each finished MD replica triggers one MiniApps replica for ML analysis of the
produced trajectories.  Routing lives here, not in config dependencies.
"""

import importlib.util
import os
import sys
import tempfile
import traceback
from functools import lru_cache
from pathlib import Path

import yaml

_ddsim_dir = os.environ.get("DDSIM_DIR")
if not _ddsim_dir:
    raise OSError("DDSIM_DIR is not set. Export it before launching the campaign.")
_DDSIM_ROOT = Path(_ddsim_dir)
if str(_DDSIM_ROOT) not in sys.path:
    sys.path.insert(0, str(_DDSIM_ROOT))

from src.campaign import BaseWorkflow  # noqa: E402


@lru_cache(maxsize=1)
def _get_workflow_class():
    spec = importlib.util.spec_from_file_location(
        "ddmd_workflow",
        _DDSIM_ROOT / "workflows/ddmd_workflow/ddmd_workflow.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.DDMdWorkflow


# Pre-load at module import time so the lru_cache is warm before Dragon's
# worker pool starts.  Calling _get_workflow_class() inside an active Dragon
# event loop causes an import-lock deadlock.
_get_workflow_class()


class DDMdWrapperWorkflow(BaseWorkflow):
    """Async wrapper that runs one replica of the DDMd pipeline.

    DAG routing: _on_completion triggers "miniapps" when the replica succeeds.
    On failure, returns None so the scheduler falls back to config dependencies
    (if any) — which here means no downstream is started for failed replicas.
    """

    workflow_id = "ddmd"

    async def run(self, replica_id: str) -> None:
        workflow_class = _get_workflow_class()

        asyncflow = self.asyncflow
        cfg = self.config or {}
        ddsim_config = cfg.get("ddsim_config")
        if not ddsim_config:
            raise ValueError(f"[{replica_id}] 'ddsim_config' missing from workflow config.")

        experiment_dir = cfg.get("experiment_dir", "")
        replica_config_path = self._make_replica_config(ddsim_config, replica_id, experiment_dir)
        name = replica_id.replace("_", "")

        # Delta A40 TF wrapper: sets correct CUDA_HOME and nvidia lib paths.
        # Falls back gracefully when not on Delta (e.g. local testing).
        _delta_wrapper = _DDSIM_ROOT / "workflows/ddmd_workflow/delta_tf_gpu_wrapper.sh"
        tf_gpu_wrapper = str(_delta_wrapper) if _delta_wrapper.exists() else None

        # Dragon sets CUDA_VISIBLE_DEVICES=-1 inside function tasks; asyncio
        # subprocesses (TF training) inherit the parent env, so we must
        # override it explicitly via SPHERICAL_TRAINING_GPU.
        gpu_id = None
        if self.policies:
            aff = getattr(self.policies[0], "gpu_affinity", [])
            if aff:
                gpu_id = aff[0]
        if gpu_id is not None:
            os.environ["SPHERICAL_TRAINING_GPU"] = str(gpu_id)
        elif "SPHERICAL_TRAINING_GPU" in os.environ:
            del os.environ["SPHERICAL_TRAINING_GPU"]

        try:
            workflow = workflow_class(
                asyncflow=asyncflow,
                config=replica_config_path,
                name=name,
                # on_ready intentionally omitted: mid-run signalling is replaced
                # by _on_completion which fires after the full replica completes.
                policies=self.policies,
                engine_dragon=self.engine_dragon,
                **({"tf_gpu_wrapper": tf_gpu_wrapper} if tf_gpu_wrapper else {}),
            )
        except Exception:
            print(
                f"[{replica_id}] DDMdWorkflow.__init__ raised:\n" + traceback.format_exc(),
                flush=True,
            )
            raise

        try:
            await workflow.start()
        finally:
            Path(replica_config_path).unlink(missing_ok=True)

    def _on_completion(self, replica_id: str, cm, final_state: str):
        """DAG edge: md ──→ miniapps.

        Triggers one MiniApps replica per completed MD replica so ML analysis
        of trajectories runs immediately after each MD finishes.  Failed
        replicas do not propagate downstream.
        """
        if final_state != "done":
            return None
        return {"name": "miniapps", "replicas": 1}

    @staticmethod
    def _make_replica_config(
        base_config_path: str, replica_id: str, experiment_dir: str = ""
    ) -> str:
        with open(base_config_path) as f:
            cfg = yaml.safe_load(f)

        if cfg.get("node_local_path"):
            cfg["node_local_path"] = str(Path(cfg["node_local_path"]) / replica_id)

        if experiment_dir:
            cfg["experiment_directory"] = str(Path(experiment_dir).expanduser().resolve())

        tmp = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".yaml",
            delete=False,
            prefix=f"ddmd_{replica_id}_",
        )
        yaml.dump(cfg, tmp)
        tmp.close()
        return tmp.name
