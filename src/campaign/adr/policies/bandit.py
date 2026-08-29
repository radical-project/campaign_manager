"""Thompson-sampling bandit policy."""

from __future__ import annotations

import logging

from radical.adr import Decision, Policy, decide

from ...bandit import SchedulingBandit
from ._helpers import _stage_depth

log = logging.getLogger(__name__)

# BP state → reward, matching SchedulingBandit's documented signal.
_BP_REWARD = {"WIDEN": 0.8, "HOLD": 0.7, "THROTTLE": 0.2}
_TERMINAL_REWARD = 0.5  # terminal stage / no downstream BP → neutral


def _downstream_bp(stage: str, stages: dict) -> str | None:
    """BP state of the stage that *stage* feeds (its first downstream consumer)."""
    for _other, info in stages.items():
        if stage in info.get("deps", []):
            return info.get("bp_state", "HOLD")
    return None  # no downstream → terminal


class _NamedStage:
    """Minimal object with a ``.name`` for SchedulingBandit.rank()."""

    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name


class BanditSchedulingPolicy(Policy):
    """Thompson-sampling SchedulingBandit exposed as an ADR Policy.

    Reproduces the in-CM bandit's behaviour through the operator's levers so it
    can be A/B-compared against the rule and LLM policies on equal footing:

      1. Reward: each new replica completion feeds the bandit a reward derived
         from that stage's *downstream* backpressure (WIDEN 0.8 / HOLD 0.7 /
         THROTTLE 0.2; terminal stage 0.5) — the same signal the CM uses.
      2. Decide: rank all stages by a fresh Thompson sample and emit descending
         ``set_priority`` actions, so the CM's two-pass scheduler tries the
         bandit's most-promising stage first.

    ``warmstart=True`` seeds depth-based priors (Beta(depth+1, 1)), matching the
    CM's ``bandit_warmstart`` flag.
    """

    def __init__(
        self,
        op,
        seed: int | None = 0,
        warmstart: bool = False,
        base_priority: int = 100,
        long_task_threshold_s: float | None = None,
    ) -> None:
        super().__init__()
        self._act = op.get_actions()
        self._seed = seed
        self._warmstart = warmstart
        self._base = base_priority
        # When set, rewards are discounted 20% for stages whose mean task duration
        # exceeds this threshold (signals stall / resource starvation).
        # Default None = disabled.  Set campaign-side when typical replica runtime
        # is known (e.g. long_task_threshold_s = 5 × expected_mean_s).
        self._long_task_threshold_s = long_task_threshold_s
        self._bandit: SchedulingBandit | None = None
        self._prev_finished: dict[str, int] = {}

    def _ensure_bandit(self, stages: dict) -> SchedulingBandit:
        if self._bandit is None:
            priors = None
            if self._warmstart:
                depth = _stage_depth(stages)
                priors = {s: (float(depth[s] + 1), 1.0) for s in stages}
            self._bandit = SchedulingBandit(list(stages), seed=self._seed, stage_priors=priors)
        return self._bandit

    @decide
    async def run(self, obs: dict) -> Decision:
        stages = obs.get("stages", {})
        if not stages:
            return Decision()
        bandit = self._ensure_bandit(stages)

        # Real hardware signals from TelemetrySubscriber (0.0 / None when absent).
        fail_rate = float(obs.get("task_fail_rate", 0.0))
        avg_dur = obs.get("avg_task_duration_s")  # None when no completions yet

        # 1. Reward: feed the bandit for each new completion since last cycle.
        #    Base reward: downstream BP state (same as the in-CM bandit).
        #    Telemetry modulation:
        #      - High fail_rate discounts the reward (hardware/config problem).
        #      - avg_task_duration unusually long (> 5× nominal) also discounts,
        #        modelling a stalled or resource-starved stage.
        for s, info in stages.items():
            delta = info["finished"] - self._prev_finished.get(s, 0)
            if delta > 0:
                ds_bp = _downstream_bp(s, stages)
                reward = _TERMINAL_REWARD if ds_bp is None else _BP_REWARD.get(ds_bp, 0.5)
                # Discount by failure rate (capped at 50% discount so bandit
                # doesn't collapse when transient errors spike).
                if fail_rate > 0.0:
                    reward *= 1.0 - min(fail_rate, 0.5)
                # Discount when average task duration is pathologically long
                # relative to the campaign's expected baseline (campaign-configured).
                if (
                    avg_dur is not None
                    and self._long_task_threshold_s is not None
                    and avg_dur > self._long_task_threshold_s
                ):
                    reward *= 0.8
                for _ in range(min(delta, 64)):  # cap pathological catch-up
                    bandit.update(s, reward)
            self._prev_finished[s] = info["finished"]

        # 2. Decide: rank by Thompson sample, emit descending priorities.
        ranked = bandit.rank([_NamedStage(s) for s in stages])
        n = len(ranked)
        actions = [
            self._act.set_priority(stage=g.name, priority=self._base + (n - i))
            for i, g in enumerate(ranked)
        ]
        return Decision(actions=actions)

    @property
    def summary(self) -> dict:
        """Posterior mean per stage — for comparison logging."""
        return self._bandit.summary() if self._bandit is not None else {}
