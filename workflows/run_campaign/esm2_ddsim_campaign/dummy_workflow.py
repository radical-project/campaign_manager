"""
DDSimWorkflow — wraps DummyWorkflow from DeepDriveSim in-process.
"""

import importlib.util
import os
import sys
import traceback
from functools import lru_cache
from pathlib import Path

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
        "dummy_workflow_mod",
        _DDSIM_ROOT / "workflows/dummy_workflow/dummy_workflow.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.DummyWorkflow


# Pre-load before Dragon's worker pool starts to avoid import-lock deadlock.
_get_workflow_class()


class DDSimWorkflow(BaseWorkflow):
    """Async wrapper that runs one replica of the DummyWorkflow pipeline."""

    workflow_id = "dummy"

    async def run(self, replica_id: str) -> None:
        workflow_class = _get_workflow_class()

        asyncflow = self.asyncflow
        cfg = self.config or {}

        if not cfg.get("src_dir"):
            cfg = {**cfg, "src_dir": str(_DDSIM_ROOT / "workflows/dummy_workflow")}

        name = replica_id.replace("_", "")
        home_base = Path(cfg.get("home_dir", Path.home() / "Dummy")).expanduser()

        try:
            workflow = workflow_class(
                config=cfg,
                name=name,
                asyncflow=asyncflow,
                home_dir=str(home_base),
                _cm=self._cm,
                _group_name=self._group_name,
                policies=self.policies,
            )
        except Exception:
            print(
                f"[{replica_id}] DummyWorkflow.__init__ raised:\n" + traceback.format_exc(),
                flush=True,
            )
            raise

        await workflow.start()
