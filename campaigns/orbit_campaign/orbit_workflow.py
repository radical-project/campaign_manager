"""
OrbitWorkflow — submits dummy_workflow simulation tasks to a remote HPC node
via the ORBIT broker + rhapsody plugin.

Each replica submits num_sims independent simulation.py tasks to orbit in a
single batch.  Orbit runs them all in parallel on the endpoint, so per-replica
wall time ≈ one simulation's duration (~15s) regardless of num_sims.

Score = mean(|y|) across all .npz output files, computed after all tasks land.

Candidates below refine_threshold trigger a refine replica.  Campaign stops
when campaign_target refine replicas finish.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import ClassVar

import numpy as np

from src.campaign import ArtifactManifest, BaseWorkflow


class OrbitWorkflow(BaseWorkflow):
    workflow_id = "orbit"

    # Shared connection — initialized once, reused by all replicas.
    # _session is set when the rhapsody 'orbit' backend is available;
    # _rt/_rh are set when falling back to EndpointRuntime.
    _session: ClassVar = None
    _rt: ClassVar = None
    _rh: ClassVar = None
    _lock: ClassVar[asyncio.Lock | None] = None

    # Endpoint ID captured at connection time; written into manifests so
    # refine replicas know which endpoint holds the search output files.
    _endpoint_id: ClassVar[str] = ""

    # Campaign-level stats.
    _best_score: ClassVar[float] = float("inf")
    _n_evaluated: ClassVar[int] = 0
    _refine_scores: ClassVar[list] = []

    # Manifest registry: candidate_id (= search replica_id) → ArtifactManifest.
    # Search populates this in on_replica_done; refine looks it up by
    # self.config["candidate_id"] to find the endpoint + path for its task.
    _manifests: ClassVar[dict[str, ArtifactManifest]] = {}

    @classmethod
    def reset_state(cls) -> None:
        cls._best_score = float("inf")
        cls._n_evaluated = 0
        cls._refine_scores = []
        cls._manifests = {}

    @classmethod
    async def _ensure_connected(cls, config: dict) -> None:
        """Initialize the shared rhapsody Session (idempotent)."""
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
                cls._endpoint_id = getattr(backend, "_endpoint_name", "")
                print(
                    f"[OrbitWorkflow] Connected (rhapsody) → "
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
                rt = EndpointRuntime()
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
                eid = eids[0]
                cls._rh = await asyncio.to_thread(
                    rt.get_plugin, eid, "rhapsody", backends=["concurrent"]
                )
                cls._rt = rt
                cls._endpoint_id = eid
                print(f"[OrbitWorkflow] Connected (EndpointRuntime) → endpoint='{eid}'")

    @classmethod
    async def close_connection(cls, timeout: float = 5.0) -> None:
        """Shutdown the shared connection (rhapsody session or EndpointRuntime)."""
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
        import rhapsody

        cfg = self.config or {}
        await self._ensure_connected(cfg)

        python_exe  = cfg["python_exe"]
        work_dir    = cfg["work_dir"]
        num_sims    = int(cfg.get("num_sims", 20))
        sim_out_dir = str(Path(cfg["sim_output_dir"]) / replica_id / "sim_output")
        Path(sim_out_dir).mkdir(parents=True, exist_ok=True)

        # One ComputeTask per simulation — orbit runs them all in parallel.
        # Per-replica wall time ≈ single sim duration regardless of num_sims.
        def _make_args(i: int) -> list[str]:
            return [
                f"{work_dir}/simulation.py",
                "--output_dir", sim_out_dir,
                "--sim_tag",    f"sim_{i}",
                "--filename",   f"config_{i}.npz",
            ]

        # Submit in small batches so the endpoint isn't flooded, but wait for
        # all tasks together so sims still run in parallel on the endpoint.
        sub_batch = int(cfg.get("sim_batch_size", 5))

        if self._session is not None:
            tasks = [
                rhapsody.ComputeTask(executable=python_exe, arguments=_make_args(i))
                for i in range(num_sims)
            ]
            for i in range(0, num_sims, sub_batch):
                await self._session.submit_tasks(tasks[i : i + sub_batch])
            await self._session.wait_tasks(tasks)
            failed = [t for t in tasks if t.get("exit_code", 0) != 0]
            if failed:
                codes = [t.get("exit_code") for t in failed]
                raise RuntimeError(
                    f"[{replica_id}] {len(failed)}/{num_sims} sims failed "
                    f"(exit codes: {codes})"
                )
        else:
            task_dicts = [
                {"executable": python_exe, "arguments": _make_args(i)}
                for i in range(num_sims)
            ]
            all_uids = []
            for i in range(0, num_sims, sub_batch):
                submitted = await asyncio.to_thread(
                    self._rh.submit_tasks, task_dicts[i : i + sub_batch]
                )
                all_uids.extend(t["uid"] for t in submitted)
            completed = await asyncio.to_thread(self._rh.wait_tasks, all_uids)
            failed = [t for t in completed if t.get("exit_code", 0) != 0]
            if failed:
                raise RuntimeError(
                    f"[{replica_id}] {len(failed)}/{num_sims} sims failed"
                )

    # ── Score ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _compute_score(replica_dir: str) -> float:
        """Return mean |y| across all simulation .npz files in sim_output/."""
        ys = []
        for f in (Path(replica_dir) / "sim_output").rglob("*.npz"):
            arr = np.load(f)
            if "y" in arr:
                ys.append(arr["y"].ravel())
        if not ys:
            return float("inf")
        return float(np.mean(np.abs(np.concatenate(ys))))

    # ── Completion hook ────────────────────────────────────────────────────────

    async def on_replica_done(self, replica_id: str, cm, final_state: str) -> None:
        if final_state != "done":
            return

        cfg = self.config or {}

        if self._group_name == "search":
            replica_dir = str(Path(cfg["sim_output_dir"]) / replica_id)

            score = await asyncio.to_thread(self._compute_score, replica_dir)

            OrbitWorkflow._n_evaluated += 1
            is_best = score < OrbitWorkflow._best_score
            if is_best:
                OrbitWorkflow._best_score = score
            print(
                f"[OrbitWorkflow] {replica_id}  score={score:.4f}"
                f"  best={OrbitWorkflow._best_score:.4f}"
                + ("  ★" if is_best else "")
            )

            # Build manifest for this replica's output files.
            manifest = ArtifactManifest(
                artifact_id=f"artifact_{replica_id}",
                created_by=replica_id,
                endpoint_id=OrbitWorkflow._endpoint_id,
                path=replica_dir,
                metadata={
                    "score": score,
                    "num_sims": int(cfg.get("num_sims", 20)),
                    "group": self._group_name,
                },
            )
            manifest.validate()
            OrbitWorkflow._manifests[replica_id] = manifest
            cm.metrics().record_manifest(manifest)

            threshold = float(cfg.get("refine_threshold", 0.5))
            trigger_group = cfg.get("trigger_refine", "refine")
            if score < threshold:
                OrbitWorkflow._refine_scores.append(score)
                await self._trigger_dependent(
                    trigger_group,
                    replicas=1,
                    candidate_id=replica_id,
                    score=score,
                )

        else:
            # Refine group: look up the search manifest to find remote files.
            candidate_id = (self.config or {}).get("candidate_id", "")
            manifest = OrbitWorkflow._manifests.pop(candidate_id, None)
            if manifest is not None:
                # Build a child manifest recording lineage of the refine output.
                refine_manifest = ArtifactManifest(
                    artifact_id=f"artifact_{replica_id}",
                    created_by=replica_id,
                    endpoint_id=manifest.endpoint_id,
                    path=str(Path(manifest.path) / "refined"),
                    parent_ids=[manifest.artifact_id],
                    metadata={"source_score": manifest.metadata.get("score")},
                )
                refine_manifest.validate()
                cm.metrics().record_manifest(refine_manifest)
                # TODO: submit refine analysis orbit task to manifest.endpoint_id
                # using manifest.path as input directory.
                print(
                    f"[OrbitWorkflow] {replica_id} refine stub — "
                    f"endpoint={manifest.endpoint_id!r}  path={manifest.path!r}"
                )
            if OrbitWorkflow._refine_scores:
                OrbitWorkflow._refine_scores.pop(0)
