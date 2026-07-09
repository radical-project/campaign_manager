"""
Thompson-sampling scheduling bandit.

`SchedulingBandit` (one `BanditArm` per pipeline stage) ranks eligible stages
by a Beta-posterior sample so the most-promising stage is scheduled first.

It is **not** wired into the CM scheduler — the scheduler orders eligible
groups by ``group.priority``.  The bandit is consumed by the ADR layer's
``BanditSchedulingPolicy`` (``src/campaign/adr/policies.py``), which drives
that priority lever.  This module therefore only provides the learning
primitive; the in-loop shard / resource / scheduling bandits were removed.

Algorithm
---------
Beta-Bernoulli Thompson sampling with continuous reward:

  Prior:  Beta(α=1, β=1)  — uniform, no preference
  Update: α += reward      (reward ∈ [0, 1])
          β += 1 - reward
  Select: sample each arm from Beta(α, β); choose the highest sample
"""

import random
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class BanditArm:
    """One arm of a Beta-Bernoulli bandit."""

    label: Any
    alpha: float = 1.0  # successes + prior
    beta: float = 1.0  # failures  + prior

    def sample(self, rng: random.Random) -> float:
        """Draw a Thompson sample from Beta(alpha, beta)."""
        return rng.betavariate(self.alpha, self.beta)

    def update(self, reward: float) -> None:
        """Update with *reward* ∈ [0, 1].  Values outside are clamped."""
        reward = max(0.0, min(1.0, reward))
        self.alpha += reward
        self.beta += 1.0 - reward

    def reset(self) -> None:
        """Return to uninformative uniform prior."""
        self.alpha = 1.0
        self.beta = 1.0

    @property
    def mean(self) -> float:
        """Current posterior mean estimate."""
        return self.alpha / (self.alpha + self.beta)

    @property
    def pulls(self) -> int:
        """Effective number of updates (alpha + beta - 2 initial prior units)."""
        return max(0, round(self.alpha + self.beta - 2))

    def __repr__(self) -> str:
        return (
            f"BanditArm({self.label!r}  "
            f"mean={self.mean:.3f}  pulls={self.pulls}  "
            f"α={self.alpha:.2f}  β={self.beta:.2f})"
        )


class SchedulingBandit:
    """Thompson-sampling bandit for cross-stage scheduling priority.

    One BanditArm per pipeline stage. When multiple stages are eligible
    simultaneously, rank() returns them sorted by Thompson-sampled Beta value
    so the scheduler tries the most-promising stage first.

    Reward signal (fed at replica completion via update()):
      WIDEN    → 0.8  downstream hungry — this stage's output is needed, keep going
      HOLD     → 0.7  balanced — good scheduling rate
      THROTTLE → 0.2  downstream flooded — back off this stage
      none     → 0.5  terminal stage or no BP tracking — neutral

    stage_priors: optional per-stage (alpha, beta) warm-start values.  Use to
    give CPU-only source stages a head start so they are not starved during the
    cold-start window before the bandit has accumulated enough observations.
    Example: {"s1_ligand_filter": (2.0, 1.0)} → initial mean 0.67 vs 0.5 default.
    """

    def __init__(
        self,
        stage_names: list[str],
        seed: Optional[int] = None,
        stage_priors: Optional[dict[str, tuple[float, float]]] = None,
    ) -> None:
        self._arms: dict[str, BanditArm] = {}
        for n in stage_names:
            arm = BanditArm(label=n)
            if stage_priors and n in stage_priors:
                arm.alpha, arm.beta = stage_priors[n]
            self._arms[n] = arm
        self._rng = random.Random(seed)

    def rank(self, eligible: list) -> list:
        """Return eligible groups sorted by Thompson-sampled priority (highest first).

        Groups not in the bandit's arm set (e.g. added dynamically) fall back
        to a neutral 0.5 sample so they're still scheduled fairly.
        """
        if len(eligible) <= 1:
            return eligible
        return sorted(
            eligible,
            key=lambda g: self._arms[g.name].sample(self._rng) if g.name in self._arms else 0.5,
            reverse=True,
        )

    def update(self, stage_name: str, reward: float) -> None:
        """Update the arm for *stage_name* with *reward* ∈ [0, 1]."""
        arm = self._arms.get(stage_name)
        if arm is not None:
            arm.update(reward)

    def summary(self) -> dict[str, float]:
        """Posterior mean per stage — for logging."""
        return {name: arm.mean for name, arm in self._arms.items()}

    def best(self) -> Optional[str]:
        """Stage name with highest posterior mean."""
        if not self._arms:
            return None
        return max(self._arms, key=lambda n: self._arms[n].mean)

    def __repr__(self) -> str:
        arms_str = "  ".join(f"{n}:{arm.mean:.3f}" for n, arm in self._arms.items())
        return f"SchedulingBandit(best={self.best()!r}  [{arms_str}])"
