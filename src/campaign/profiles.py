"""
Named priority profiles for candidate dispatch ordering.

Each profile is a weight vector over five signals:
  score       — most recent upstream stage score (primary quality signal)
  surrogate   — surrogate model prediction
  uncertainty — surrogate uncertainty (drives active learning)
  age         — time since enqueue (anti-starvation bonus)
  diversity   — novelty bonus for under-represented scaffold classes (MMR-style)

Higher weight → that signal contributes more to the candidate's priority score
when the sharder ranks its buffer for dispatch.

Ported from cm-prototype/src/cm/plan/profiles.py; weights are unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ProfileWeights:
    score: float
    surrogate: float
    uncertainty: float
    age: float
    diversity: float

    def as_dict(self) -> dict[str, float]:
        return {
            "score": self.score,
            "surrogate": self.surrogate,
            "uncertainty": self.uncertainty,
            "age": self.age,
            "diversity": self.diversity,
        }


PROFILES: dict[str, ProfileWeights] = {
    # Greedy: rank purely by score; tiny age bonus prevents starvation.
    "pure_promise": ProfileWeights(
        score=1.0, surrogate=0.0, uncertainty=0.0, age=0.05, diversity=0.0
    ),
    # Active learning: maximize uncertainty reduction; ignore score.
    "active_learning": ProfileWeights(
        score=0.0, surrogate=0.0, uncertainty=1.0, age=0.05, diversity=0.0
    ),
    # Balanced: score + surrogate + uncertainty (good for exploration with a model).
    "explore_exploit": ProfileWeights(
        score=0.5, surrogate=0.3, uncertainty=0.4, age=0.05, diversity=0.0
    ),
    # Score-weighted with scaffold diversity to avoid chemical echo chambers.
    "diverse_top": ProfileWeights(
        score=0.6, surrogate=0.0, uncertainty=0.1, age=0.05, diversity=0.3
    ),
    # Pure diversity: round-robin across scaffold classes.
    "round_robin": ProfileWeights(
        score=0.0, surrogate=0.0, uncertainty=0.0, age=0.05, diversity=1.0
    ),
}


def get_profile(name: str) -> ProfileWeights:
    if name not in PROFILES:
        raise KeyError(f"unknown profile {name!r}; known: {sorted(PROFILES)}")
    return PROFILES[name]
