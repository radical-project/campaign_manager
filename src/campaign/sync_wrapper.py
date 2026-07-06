"""
CampaignManager — synchronous shim around AsyncCampaignManager.

Runs a dedicated event loop in a background thread so callers without an
async context can orchestrate workflows with plain blocking calls.

Notes
-----
- The sync wrapper stubs out AsyncCampaignManager._setup_resources because
  the background-loop startup path can't run the async resource discovery
  used by the production flow.  GPU auto-detection and Dragon pool
  discovery therefore do not happen via this wrapper; pass total_cpus /
  total_gpus explicitly.
- ``from_config`` delegates to AsyncCampaignManager.from_config so all
  feature flags (backpressure, sharder, monitor) are wired through.
"""

import asyncio
from typing import Optional

from .base_workflow import BaseWorkflow
from .campaign_manager import AsyncCampaignManager
from .types import WorkflowStats


class CampaignManager:
    """Synchronous campaign manager — thin wrapper around AsyncCampaignManager."""

    def __init__(
        self,
        max_workers: Optional[int] = None,
        engine: str = "concurrent",
        total_cpus: int = 0,
        total_gpus: int = 0,
        num_workers: Optional[int] = None,
        debug: bool = False,
        asyncflow=None,
        engine_dragon=None,
        features: Optional[dict] = None,
        _acm: Optional[AsyncCampaignManager] = None,
    ) -> None:
        import threading

        # Allow callers (notably from_config) to supply a pre-built async CM
        # so feature wiring done by AsyncCampaignManager.from_config isn't lost.
        if _acm is None:
            self._acm = AsyncCampaignManager(
                max_workers=max_workers,
                engine=engine,
                total_cpus=total_cpus,
                total_gpus=total_gpus,
                num_workers=num_workers,
                debug=debug,
                asyncflow=asyncflow,
                engine_dragon=engine_dragon,
                features=features,
            )
        else:
            self._acm = _acm

        async def _noop_init() -> None:
            pass

        # Sync wrapper has no usable async startup path for resource discovery
        # — stub it out.  See module docstring.
        self._acm._setup_resources = _noop_init

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, daemon=True, name="CampaignManagerLoop"
        )
        self._thread.start()

    def register_workflow(self, *args, **kwargs) -> None:
        self._acm.register_workflow(*args, **kwargs)

    # Deprecated alias — prefer register_workflow.
    register_group = register_workflow

    def start(self) -> None:
        future = asyncio.run_coroutine_threadsafe(self._acm.start(), self._loop)
        future.result()

    def wait(self, timeout: Optional[float] = None) -> bool:
        future = asyncio.run_coroutine_threadsafe(self._acm.wait(timeout=timeout), self._loop)
        outer_timeout = (timeout + 2.0) if timeout is not None else None
        try:
            return bool(future.result(timeout=outer_timeout))
        except Exception:
            return False

    def close(self) -> None:
        try:
            future = asyncio.run_coroutine_threadsafe(self._acm.close(), self._loop)
            future.result(timeout=5.0)
        except Exception:
            pass
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5.0)

    def status(self) -> dict:
        return self._acm.status()

    def stats(self) -> dict[str, WorkflowStats]:
        return self._acm.stats()

    @classmethod
    def from_config(
        cls,
        config: dict,
        workflow_registry: dict[str, type[BaseWorkflow]],
        **kwargs,
    ) -> "CampaignManager":
        """Build a sync CampaignManager from a config dict.

        Delegates to AsyncCampaignManager.from_config so every feature
        (backpressure, sharder, monitor, candidate log)
        is wired up identically to the async path.  Without this delegation,
        the sync wrapper silently dropped all features keyed under
        ``features:`` in the config.
        """
        async_cm = AsyncCampaignManager.from_config(
            config, workflow_registry, **kwargs
        )
        return cls(_acm=async_cm)
