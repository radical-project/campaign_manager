"""Rule-based scheduling policy: DownstreamFirstPolicy."""

from __future__ import annotations

import asyncio
import logging

from radical.adr import Decision, Policy, decide

from ._helpers import _batch_for_bp, _stage_depth

log = logging.getLogger(__name__)


class DownstreamFirstPolicy(Policy):
    """Deterministic downstream-first scheduling — the rule the bandit learns.

    Every cycle it assigns descending priorities by dependency depth (deepest =
    highest), so the CM's two-pass scheduler always feeds the most-downstream
    stage first and holds the screening root lowest.  This is the fixed schedule
    the bandit converges to — making it the natural deterministic baseline for
    a learned vs. hand-coded comparison.

    Note: this is *proactive* (it ranks every cycle regardless of visible queue
    depth).  An earlier reactive variant gated boosts on ``queue_depth > 0``,
    but the emulation's sharder buffers drain between ticks, so that gate
    effectively never fired — the policy did nothing and the campaign stalled.

    Telemetry-aware starvation boost (``use_telemetry_boost=True``):
        Any stage with ``starved=True`` in obs receives an additional priority
        boost scaled inversely by live ``cpu_util`` from TelemetrySubscriber.
        ``boost = starved_boost * (0.5 + 0.5 * (1 - cpu_util / 100))``
        Rationale: low cpu_util means no dummies are running — GPUs are fully
        occupied by inference and miniapps is maximally starved.  The boost is
        therefore STRONGEST when cpu_util≈0 (boost=starved_boost) and weakest
        when cpu_util≈100 (boost=starved_boost/2).  This is the semantically
        correct direction: the signal's magnitude matches the starvation severity.
        Degrades gracefully to half-strength when cpu_util is high (lots of CPU
        work = inference is completing, dummies running, starvation easing).
        GPU-utilisation throttling already operates via batch sizing (see below).
    """

    def __init__(
        self,
        op,
        base_priority: int = 100,
        batch_base: int = 50,
        batch_lo: int = 1,
        batch_hi: int = 1000,
        use_telemetry_boost: bool = False,
        starved_boost: int = 4,
        source_first: bool = False,
    ) -> None:
        super().__init__()
        self._act = op.get_actions()
        self._base = base_priority
        self._batch_base = batch_base
        self._batch_lo = batch_lo
        self._batch_hi = batch_hi
        self._use_telemetry_boost = use_telemetry_boost
        self._starved_boost = starved_boost
        # source_first=True: source stages (depth=0) get highest priority;
        # downstream stages get lower. Telemetry boost then flips the order
        # for starved downstream stages when use_telemetry_boost=True.
        # source_first=False (default): classical downstream-first ordering.
        self._source_first = source_first
        # Readable by RuleCorrectionsPolicy wrapper after each run.
        self.starved_cycles: int = 0
        self._boost_sum: float = 0.0
        self._boost_count: int = 0

    @property
    def mean_boost(self) -> float:
        return self._boost_sum / self._boost_count if self._boost_count > 0 else 0.0

    @decide
    async def run(self, obs: dict) -> Decision:
        stages = obs.get("stages", {})
        if not stages:
            return Decision()

        # Real hardware utilisation from TelemetrySubscriber (0.0 when absent).
        gpu_util = float(obs.get("gpu_util", 0.0))
        cpu_util = float(obs.get("cpu_util", 0.0))

        # Run in a thread so Lustre cold-page-fault stalls don't block the event loop.
        depth = await asyncio.to_thread(_stage_depth, stages)
        # source_first=True: shallowest stages get highest priority (inference-first baseline).
        # source_first=False: deepest stages get highest priority (downstream-first, default).
        order = sorted(stages, key=lambda s: depth[s], reverse=not self._source_first)
        n = len(order)
        priorities = {s: self._base + (n - i) for i, s in enumerate(order)}

        # Telemetry-aware starvation boost: scale priority bump inversely by cpu_util.
        # cpu_factor ∈ [0.5, 1.0]:
        #   cpu_util=0   (GPU-bound, dummies dry, miniapps maximally starved) → factor=1.0
        #   cpu_util=100 (CPU saturated, dummies running, starvation easing)  → factor=0.5
        # When telemetry is absent, cpu_util=0 → full-strength boost (safe default).
        if self._use_telemetry_boost:
            cpu_factor = 0.5 + 0.5 * (1.0 - cpu_util / 100.0)
            boost = int(self._starved_boost * cpu_factor)
            boosted_any = False
            for s, info in stages.items():
                if info.get("starved") and boost > 0:
                    priorities[s] += boost
                    boosted_any = True
            if boosted_any:
                self.starved_cycles += 1
                self._boost_sum += boost
                self._boost_count += 1
                log.debug("[telemetry boost] cpu_util=%.1f%% boost=%d", cpu_util, boost)

        actions = [
            self._act.set_priority(stage=s, priority=p) for s, p in priorities.items()
        ]

        # Batch sizing: BP state is the primary signal; hardware utilisation
        # clamps or stretches it when real telemetry is flowing (gpu_util > 0).
        #   GPU > 85% → clamp to base (don't over-dispatch to saturated GPUs)
        #   GPU < 40% → double the BP-derived size (fill idle hardware)
        #   CPU > 90% → halve the size (CPU is the bottleneck, ease pressure)
        # GPU-util adjustments apply ONLY to stages that actually use GPU slots;
        # CPU-only stages (requires_gpu=False) are unaffected by GPU saturation.
        for s, info in stages.items():
            new_batch = _batch_for_bp(
                info["bp_state"],
                self._batch_base,
                lo=self._batch_lo,
                hi=self._batch_hi,
            )
            if info.get("requires_gpu", True):
                if gpu_util > 85.0:
                    new_batch = min(new_batch, self._batch_base)
                elif gpu_util > 0.0 and gpu_util < 40.0:
                    new_batch = min(new_batch * 2, info.get("cap", new_batch) * 4)
            if cpu_util > 90.0:
                new_batch = max(1, new_batch // 2)
            if new_batch != self._batch_base:
                actions.append(self._act.set_batch_size(stage=s, size=new_batch))

        return Decision(actions=actions)
