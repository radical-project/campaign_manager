#!/usr/bin/env python3
"""
run_noDDSim_campaign.py — orbit campaign with /bin/echo + /bin/sleep tasks.

Runs the same two-stage search → refine pipeline as run_campaign.py but
replaces DDSim simulation tasks with lightweight shell commands:
  search  — /bin/echo "search <replica_id>"   (random score)
  refine  — /bin/sleep <duration>

Useful for testing the ORBIT + rhapsody plumbing without a full simulation
environment.

Endpoint auto-discovery (new):
  1. Tries rhapsody.get_backend("orbit", ...) — works when $RADICAL_ORBIT_BROKER_URL
     is set and a matching endpoint is already registered with the broker.
  2. Falls back to EndpointRuntime + topology inspection — same as the original
     orbit_workflow.py before the rhapsody Session API was introduced.

Usage
-----
    export RADICAL_ORBIT_BROKER_URL=wss://dt-login03.delta.ncsa.illinois.edu:8020
    python run_noDDSim_campaign.py --config config.yaml

Prerequisites
-------------
    # Terminal 1 (login node)
    ./bin/radical-orbit-broker.py --port 8020

    # Terminal 2 (compute node, inside allocation)
    ./bin/radical-orbit-endpoint.py -p rhapsody
"""

from __future__ import annotations

import asyncio
import random
import sys
from pathlib import Path
from typing import ClassVar

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

import argparse  # noqa: E402

from src.campaign import AsyncCampaignManager as CampaignManager  # noqa: E402
from src.campaign import BaseWorkflow  # noqa: E402
from src.utils.workflow import load_config  # noqa: E402


class NoDDSimWorkflow(BaseWorkflow):
    """
    Submits /bin/echo (search) and /bin/sleep (refine) via ORBIT + rhapsody.

    Endpoint discovery uses rhapsody.get_backend("orbit") when available,
    falling back to EndpointRuntime + topology inspection.  Both connection
    paths share a single class-level handle reused across all replicas.
    """

    workflow_id = "orbit"

    # Shared connection — one of (_session) or (_rt + _rh) is set after
    # the first _ensure_connected call; the other pair remains None.
    _session: ClassVar = None
    _rt: ClassVar = None
    _rh: ClassVar = None
    _lock: ClassVar[asyncio.Lock | None] = None

    # Campaign-level stats
    _best_score: ClassVar[float] = float("inf")
    _n_evaluated: ClassVar[int] = 0
    _refine_scores: ClassVar[list] = []

    @classmethod
    def reset_state(cls) -> None:
        cls._best_score = float("inf")
        cls._n_evaluated = 0
        cls._refine_scores = []

    @classmethod
    async def _ensure_connected(cls, config: dict) -> None:
        """Initialize the shared connection (idempotent).

        Path 1 — rhapsody.get_backend("orbit"):
            Requires $RADICAL_ORBIT_BROKER_URL.  Auto-discovers the first
            registered endpoint that advertises the rhapsody plugin.

        Path 2 — EndpointRuntime fallback:
            Creates an EndpointRuntime, fetches the live topology, and
            picks the first endpoint whose plugin list contains "rhapsody".
            Optionally narrowed by config keys orbit_broker_url /
            orbit_endpoint.
        """
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        async with cls._lock:
            if cls._session is not None or cls._rh is not None:
                return

            import rhapsody

            batch_window = config.get("batch_window", 0.05)
            batch_limit  = config.get("batch_limit", 1024)

            try:
                backend = await rhapsody.get_backend(
                    "orbit",
                    backends=["concurrent"],
                    batch_window=batch_window,
                    batch_limit=batch_limit,
                )
                cls._session = rhapsody.Session(backends=[backend])
                print(
                    f"[NoDDSimWorkflow] Connected (rhapsody) → "
                    f"broker='{backend._broker_url}'  "
                    f"endpoint='{backend._endpoint_name}'"
                )
            except (ValueError, RuntimeError) as e:
                import warnings

                warnings.warn(
                    f"rhapsody 'orbit' backend not available ({e}) — "
                    "falling back to EndpointRuntime.",
                    stacklevel=2,
                )

                from radical.orbit import EndpointRuntime

                broker_url = config.get("orbit_broker_url") or None
                endpoint   = config.get("orbit_endpoint") or None

                rt = EndpointRuntime(broker_url=broker_url)
                await asyncio.to_thread(rt.start, True)
                topology = rt.topology()

                eids = [
                    n
                    for n, info in topology.items()
                    if n != "broker" and "rhapsody" in info.get("plugins", [])
                ]
                if not eids:
                    raise RuntimeError(
                        "No endpoint with rhapsody plugin found — "
                        "is the endpoint running with '-p rhapsody'?"
                    )

                eid = endpoint if (endpoint and endpoint in topology) else eids[0]
                cls._rh = await asyncio.to_thread(
                    rt.get_plugin, eid, "rhapsody", backends=["concurrent"]
                )
                cls._rt = rt
                print(
                    f"[NoDDSimWorkflow] Connected (EndpointRuntime) → endpoint='{eid}'"
                )

    @classmethod
    async def close_connection(cls, timeout: float = 5.0) -> None:
        """Shut down the shared rhapsody session or EndpointRuntime."""
        if cls._session is not None:
            try:
                await asyncio.wait_for(cls._session.close(), timeout=timeout)
            except (asyncio.TimeoutError, Exception):
                pass
            cls._session = None
        if cls._rh is not None:
            try:
                await asyncio.wait_for(asyncio.to_thread(cls._rh.close), timeout=timeout)
            except (asyncio.TimeoutError, Exception):
                pass
            cls._rh = None
        if cls._rt is not None:
            try:
                await asyncio.wait_for(asyncio.to_thread(cls._rt.stop), timeout=timeout)
            except (asyncio.TimeoutError, Exception):
                pass
            cls._rt = None

    # ── Compute ────────────────────────────────────────────────────────────────

    async def run(self, replica_id: str) -> None:
        cfg = self.config or {}
        await self._ensure_connected(cfg)

        if self._group_name == "search":
            executable = "/bin/echo"
            arguments  = [f"search {replica_id}"]
        else:
            duration   = float(cfg.get("duration", 0.3))
            executable = "/bin/sleep"
            arguments  = [str(duration)]

        if self._session is not None:
            import rhapsody

            ct = rhapsody.ComputeTask(executable=executable, arguments=arguments)
            await self._session.submit_tasks([ct])
            await self._session.wait_tasks([ct])
            if ct.get("exit_code", 0) != 0:
                raise RuntimeError(
                    f"[{replica_id}] task failed "
                    f"(exit={ct.get('exit_code')}): {ct.get('stderr', '').strip()}"
                )
        else:
            task = {"executable": executable, "arguments": arguments}
            submitted = await asyncio.to_thread(self._rh.submit_tasks, [task])
            uids      = [t["uid"] for t in submitted]
            completed = await asyncio.to_thread(self._rh.wait_tasks, uids)
            for t in completed:
                if t.get("exit_code", 0) != 0:
                    raise RuntimeError(
                        f"[{replica_id}] task {t['uid']} failed "
                        f"(exit={t.get('exit_code')}): {t.get('stderr', '').strip()}"
                    )

    # ── Completion hook ────────────────────────────────────────────────────────

    async def on_replica_done(self, replica_id: str, cm, final_state: str) -> None:
        if final_state != "done":
            return

        cfg = self.config or {}

        if self._group_name == "search":
            score = random.random()
            NoDDSimWorkflow._n_evaluated += 1
            is_best = score < NoDDSimWorkflow._best_score
            if is_best:
                NoDDSimWorkflow._best_score = score
            print(
                f"[NoDDSimWorkflow] {replica_id}  score={score:.4f}"
                f"  best={NoDDSimWorkflow._best_score:.4f}"
                + ("  ★" if is_best else "")
            )

            threshold     = float(cfg.get("refine_threshold", 0.5))
            trigger_group = cfg.get("trigger_refine", "refine")
            if score < threshold:
                NoDDSimWorkflow._refine_scores.append(score)
                await self._trigger_dependent(trigger_group, replicas=1)
        else:
            init_score = (
                NoDDSimWorkflow._refine_scores.pop(0)
                if NoDDSimWorkflow._refine_scores
                else 0.3
            )
            print(
                f"[NoDDSimWorkflow] {replica_id} refine complete "
                f"(init_score={init_score:.4f})"
            )


# ── Campaign runner ────────────────────────────────────────────────────────────


def _build_registry(config: dict) -> dict:
    import importlib

    registry: dict = {}
    for name, cls_path in config.get("workflow_registry", {}).items():
        module_name, cls_name = cls_path.rsplit(".", 1)
        module = importlib.import_module(module_name)
        registry[name] = getattr(module, cls_name)
    return registry


async def main(config_file: str) -> None:
    config_path = Path(config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_file}")

    config = load_config(config_file)

    from radical.asyncflow import WorkflowEngine
    from rhapsody.backends import ConcurrentExecutionBackend

    backend   = await ConcurrentExecutionBackend()
    asyncflow = await WorkflowEngine.create(backend)
    print("ConcurrentExecutionBackend started (asyncflow)")

    # Override every group with NoDDSimWorkflow regardless of what the config
    # registry points to (group names are "search" and "refine").
    registry = {name: NoDDSimWorkflow for name in config.get("workflows", {})}

    cm = CampaignManager.from_config(config, registry, asyncflow=asyncflow)

    groups = config.get("workflows", {})
    print(
        "Campaign: "
        + ", ".join(
            f"{name}: {cfg.get('replicas', 0)} replica(s) "
            f"cap={cfg.get('concurrency_cap', '—')} "
            f"target={cfg.get('campaign_target', '—')}"
            for name, cfg in groups.items()
        )
    )

    try:
        await cm.start()
        await cm.wait()
    finally:
        await cm.close()
        await NoDDSimWorkflow.close_connection()
        await asyncflow.shutdown()

    gs = cm.status()["groups"]
    search_done = gs.get("search", {}).get("replicas_finished", 0)
    refine_done = gs.get("refine", {}).get("replicas_finished", 0)

    print("\n── Campaign complete ──")
    print(f"  best_score   = {NoDDSimWorkflow._best_score:.4f}")
    print(f"  n_evaluated  = {NoDDSimWorkflow._n_evaluated}")
    print(f"  search done  = {search_done}")
    print(f"  refine done  = {refine_done}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="orbit campaign with /bin/echo + /bin/sleep tasks (no DDSim)"
    )
    parser.add_argument(
        "--config", default="config.yaml",
        help="Path to YAML config (default: config.yaml)",
    )
    args = parser.parse_args()
    asyncio.run(main(args.config))
