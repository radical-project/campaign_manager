"""
OrbitWorkflow — submits tasks to a remote HPC node via ORBIT + rhapsody.

Both search and refine stages use this class; _group_name distinguishes them.

search  — submits /bin/echo to the orbit endpoint, generates a random score.
          Candidates below refine_threshold trigger a refine replica.

refine  — triggered by search, submits /bin/sleep 0.3 to the endpoint.
          Campaign stops when campaign_target refine replicas finish.

The EndpointRuntime and RhapsodyClient are shared across all replicas via
class variables, initialized once on first use.  Broker URL is read from
$RADICAL_ORBIT_BROKER_URL (or ~/.radical/orbit/broker.url).
"""

from __future__ import annotations

import asyncio
import random
from typing import ClassVar

from src.campaign import BaseWorkflow


class OrbitWorkflow(BaseWorkflow):
    workflow_id = "orbit"

    # Shared connection — initialized once, reused by all replicas.
    _rt: ClassVar = None
    _rh: ClassVar = None
    _lock: ClassVar[asyncio.Lock | None] = None

    # Campaign-level stats.
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
        """Initialize the shared EndpointRuntime + RhapsodyClient (idempotent)."""
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        async with cls._lock:
            if cls._rh is not None:
                return

            from radical.orbit import EndpointRuntime

            broker_url = config.get("orbit_broker_url") or None
            endpoint   = config.get("orbit_endpoint")   or None

            rt = EndpointRuntime(broker_url=broker_url)
            await asyncio.to_thread(rt.start, True)

            topology = rt.topology()
            eids = [
                n for n, info in topology.items()
                if n != "broker" and "rhapsody" in info.get("plugins", [])
            ]
            if not eids:
                raise RuntimeError(
                    "No endpoint with rhapsody plugin found — "
                    "is the endpoint running with '-p rhapsody'?"
                )

            eid = endpoint if (endpoint and endpoint in topology) else eids[0]
            rh  = await asyncio.to_thread(
                rt.get_plugin, eid, "rhapsody", backends=["concurrent"]
            )

            cls._rt = rt
            cls._rh = rh
            print(f"[OrbitWorkflow] Connected → endpoint='{eid}'")

    @classmethod
    async def close_connection(cls, timeout: float = 5.0) -> None:
        """Shutdown the shared rhapsody session and EndpointRuntime."""
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
        rh = self._rh

        if self._group_name == "search":
            task = {
                "executable": "/bin/echo",
                "arguments":  [f"search {replica_id}"],
            }
        else:
            duration = float(cfg.get("duration", 0.3))
            task = {
                "executable": "/bin/sleep",
                "arguments":  [str(duration)],
            }

        submitted = await asyncio.to_thread(rh.submit_tasks, [task])
        uids      = [t["uid"] for t in submitted]
        completed = await asyncio.to_thread(rh.wait_tasks, uids)

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
            OrbitWorkflow._n_evaluated += 1
            if score < OrbitWorkflow._best_score:
                OrbitWorkflow._best_score = score

            threshold     = float(cfg.get("refine_threshold", 0.5))
            trigger_group = cfg.get("trigger_refine", "refine")
            if score < threshold:
                OrbitWorkflow._refine_scores.append(score)
                await self._trigger_dependent(trigger_group, replicas=1)

        else:
            init_score = (
                OrbitWorkflow._refine_scores.pop(0)
                if OrbitWorkflow._refine_scores else 0.3
            )
            decay = float(cfg.get("score_decay", 0.6))
            noise = float(cfg.get("score_noise", 0.05))
            score = init_score * decay * max(0.1, 1.0 + random.gauss(0.0, noise))
            OrbitWorkflow._n_evaluated += 1
            if score < OrbitWorkflow._best_score:
                OrbitWorkflow._best_score = score
