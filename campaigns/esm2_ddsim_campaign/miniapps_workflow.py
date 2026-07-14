"""
MiniAppsWorkflow — wraps MiniAppsWorkflow from DeepDriveSim.
"""

import importlib.util
import os
import sys
import traceback
from functools import lru_cache
from pathlib import Path

# Make DeepDriveSim importable — honour $DDSIM_DIR set by the sbatch script.
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
        "miniapps_workflow",
        _DDSIM_ROOT / "workflows/miniapps_workflow/miniapps_workflow.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.MiniAppsWorkflow


# Pre-load at module import time to warm the cache before Dragon workers start.
_get_workflow_class()


class MiniAppsWrapperWorkflow(BaseWorkflow):
    """Async wrapper that runs one replica of the MiniApps pipeline."""

    workflow_id = "miniapps"

    async def run(self, replica_id: str) -> None:
        # Workaround: concurrent asyncflow backend does not apply process_template.env
        # to subprocess children, so CUDA_VISIBLE_DEVICES must be set in os.environ
        # before any subprocesses are spawned by the workflow.
        if self.policies:
            gpu_id = str(self.policies[0].gpu_affinity[0])
            os.environ["CUDA_VISIBLE_DEVICES"] = gpu_id
        elif self.config and self.config.get("assigned_gpu_ids"):
            gpu_id = str(self.config["assigned_gpu_ids"][0])
            os.environ["CUDA_VISIBLE_DEVICES"] = gpu_id

        workflow_class = _get_workflow_class()
        asyncflow = self.asyncflow
        cfg = self.config or {}

        if not cfg.get("src_dir"):
            cfg = {**cfg, "src_dir": str(_DDSIM_ROOT / "workflows/miniapps_workflow")}

        name = replica_id.replace("_", "")
        home_base = Path(cfg.get("home_dir", Path.home() / "MiniApps")).expanduser()

        try:
            workflow = workflow_class(
                config=cfg,
                asyncflow=asyncflow,
                home_dir=str(home_base),
                name=name,
                _cm=self._cm,
                _group_name=self._group_name,
                policies=self.policies,
            )
        except Exception:
            print(
                f"[{replica_id}] MiniAppsWorkflow.__init__ raised:\n" + traceback.format_exc(),
                flush=True,
            )
            raise

        await workflow.start()

    async def on_replica_done(self, replica_id: str, cm, final_state: str) -> None:
        """DAG edge: miniapps ──→ dummy.

        Each finished MiniApps replica (ML analysis complete) triggers one
        Dummy replica for downstream scoring/selection.  Failed replicas do
        not propagate downstream.
        """
        if final_state != "done":
            return
        await self._trigger_dependent("dummy", replicas=1)
