"""Surrogate models: per-stage prediction + uncertainty + recall tracking.

A Surrogate is a per-stage object that predicts what a stage will produce
for a given candidate (without running the candidate) and reports its own
uncertainty.  The CM uses these signals two ways:

  1. At trigger time, ``trigger_dependent`` calls the downstream stage's
     surrogate when the caller didn't supply ``surrogate_pred`` /
     ``surrogate_unc`` itself — the model fills them in.
  2. After a replica finishes, ``update_with_results`` feeds the actually-
     measured score back into the surrogate so it can update its internal
     state.  Surrogates that maintain rolling recall statistics use this
     to detect when their predictions are diverging from reality; the
     ``check_recall`` callback signals the BudgetController to freeze
     while the surrogate is unreliable.

This module provides:
  - ``Surrogate`` — abstract base class
  - ``NullSurrogate`` — no-op default (high uncertainty, neutral pred)
  - ``RandomSurrogate`` — stochastic predictions for tests
  - ``CorrelatedSurrogate`` — score-correlated predictions with configurable
    bias/noise; useful for benchmark workflows that don't have a real model
    but still want non-trivial surrogate signal

Recall accounting
-----------------
``RecallTracker`` is a tiny helper Surrogates can compose with — it keeps
a rolling window of (predicted, actual) pairs and computes recall@k or
mean absolute error.  When recall drops below ``floor``, the tracker
triggers the registered freeze callback.  This is the production wiring
point for the ``surrogate_recall_floor`` field in ReplanThresholds.
"""

from __future__ import annotations

import math
import random
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Iterable, Optional


# ── Recall tracking ─────────────────────────────────────────────────────────

@dataclass
class RecallTracker:
    """Rolling-window recall computation for surrogate drift detection.

    Records (predicted, actual) pairs and exposes ``recall_at_k`` —
    the fraction of the surrogate's top-k predictions that turn out to
    be in the actual top-k.  ``floor`` is the plan-set
    ``surrogate_recall_floor``; falling below it for
    ``breaches_to_escalate`` consecutive observations triggers
    ``on_recall_drift`` (typically wired to ``BudgetController.freeze``).
    """
    window_size:           int   = 50
    floor:                 float = 0.90
    breaches_to_escalate:  int   = 2
    on_recall_drift:       Optional[Callable[[float, int], None]] = None

    _pairs: deque = field(default_factory=lambda: deque(maxlen=50), init=False)
    _consecutive_low: int = field(default=0, init=False)
    _drift_active:    bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        self._pairs = deque(maxlen=self.window_size)

    def observe(self, predicted: float, actual: float) -> None:
        """Record one (predicted, actual) pair and update recall state."""
        self._pairs.append((predicted, actual))
        if len(self._pairs) < self.window_size:
            return
        recall = self.recall_at_k(k=max(5, self.window_size // 5))
        if recall < self.floor:
            self._consecutive_low += 1
            if (self._consecutive_low >= self.breaches_to_escalate
                    and not self._drift_active):
                self._drift_active = True
                if self.on_recall_drift is not None:
                    try:
                        self.on_recall_drift(recall, self._consecutive_low)
                    except Exception:
                        pass
        else:
            self._consecutive_low = 0
            # Recovery → clear drift flag and let the callback know it can
            # unfreeze.  Signal recovery with a positive recall reading.
            if self._drift_active:
                self._drift_active = False
                if self.on_recall_drift is not None:
                    try:
                        self.on_recall_drift(recall, 0)
                    except Exception:
                        pass

    def recall_at_k(self, k: int) -> float:
        """Fraction of the top-k predicted candidates that are also in the
        top-k actuals over the current window.  Returns 1.0 with too little
        data to be meaningful."""
        if len(self._pairs) < k:
            return 1.0
        preds   = sorted(range(len(self._pairs)),
                         key=lambda i: -self._pairs[i][0])[:k]
        actuals = sorted(range(len(self._pairs)),
                         key=lambda i: -self._pairs[i][1])[:k]
        return len(set(preds) & set(actuals)) / k

    def mean_absolute_error(self) -> Optional[float]:
        if not self._pairs:
            return None
        return sum(abs(p - a) for p, a in self._pairs) / len(self._pairs)

    def state(self) -> dict:
        return {
            "window_size":      self.window_size,
            "samples":          len(self._pairs),
            "floor":            self.floor,
            "recall_at_k":      self.recall_at_k(max(5, self.window_size // 5)),
            "mae":              self.mean_absolute_error(),
            "consecutive_low":  self._consecutive_low,
            "drift_active":     self._drift_active,
        }


# ── Surrogate ABC ───────────────────────────────────────────────────────────

class Surrogate(ABC):
    """Per-stage prediction model + uncertainty estimate.

    A Surrogate predicts the score a stage will produce for a given
    candidate without actually running it.  ``predict`` returns
    (prediction, uncertainty) for one candidate; ``predict_batch`` is
    the bulk equivalent.

    Update protocol:
      - ``update_with_results`` is called after each replica finishes
        with (predicted, actual) pairs.  Surrogates that learn online
        use this to refine their model; static surrogates ignore it.
      - ``redeploy`` returns a new model version string when the
        surrogate has been retrained or replaced.  The CM uses this as
        a signal to re-prioritise queued candidates.
    """
    def __init__(
        self,
        stage_id:        str,
        recall_tracker:  Optional[RecallTracker] = None,
    ) -> None:
        self.stage_id       = stage_id
        self.recall_tracker = recall_tracker
        self.version        = "1"

    @abstractmethod
    def predict(self, candidate_id: str, **features) -> tuple[float, float]:
        """Return (predicted_score, uncertainty) for one candidate."""
        ...

    def predict_batch(
        self,
        candidates: Iterable[tuple[str, dict]],
    ) -> list[tuple[float, float]]:
        """Bulk prediction.  Default falls through to ``predict``."""
        return [self.predict(cid, **feats) for cid, feats in candidates]

    def update_with_results(
        self,
        observations: Iterable[tuple[str, float, float]],
    ) -> None:
        """Record (candidate_id, predicted, actual) tuples.

        Default implementation only feeds the recall tracker (if any).
        Override to learn from observations.
        """
        if self.recall_tracker is None:
            return
        for _cid, predicted, actual in observations:
            self.recall_tracker.observe(predicted, actual)

    def redeploy(self) -> str:
        """Bump version and return the new identifier.  No-op for static
        surrogates; learning surrogates use this to checkpoint."""
        self.version = str(int(self.version) + 1) if self.version.isdigit() else self.version + "+1"
        return self.version

    def state(self) -> dict:
        out = {
            "stage_id": self.stage_id,
            "class":    type(self).__name__,
            "version":  self.version,
        }
        if self.recall_tracker is not None:
            out["recall"] = self.recall_tracker.state()
        return out


# ── Concrete implementations ────────────────────────────────────────────────

class NullSurrogate(Surrogate):
    """No-op surrogate: maximum uncertainty, neutral prediction.

    Used as the default when no model is configured.  The Triage's
    uncertainty cutoff will gate everything (because unc=1.0 > cutoff)
    unless the cutoff itself is raised to ≥ 1.0 in the plan.
    """
    def predict(self, candidate_id: str, **features) -> tuple[float, float]:
        return (0.0, 1.0)


class RandomSurrogate(Surrogate):
    """Stochastic predictions — useful for tests and noise sensitivity studies."""

    def __init__(
        self,
        stage_id:        str,
        seed:            int = 0,
        pred_range:      tuple[float, float] = (0.0, 1.0),
        unc_range:       tuple[float, float] = (0.0, 1.0),
        recall_tracker:  Optional[RecallTracker] = None,
    ) -> None:
        super().__init__(stage_id, recall_tracker=recall_tracker)
        self._rng = random.Random(seed)
        self._pred_lo, self._pred_hi = pred_range
        self._unc_lo,  self._unc_hi  = unc_range

    def predict(self, candidate_id: str, **features) -> tuple[float, float]:
        return (
            self._rng.uniform(self._pred_lo, self._pred_hi),
            self._rng.uniform(self._unc_lo,  self._unc_hi),
        )


class CorrelatedSurrogate(Surrogate):
    """Predictions correlated with the upstream score, plus configurable noise.

    A pragmatic default for benchmark workflows that don't have a real
    learned model but want non-trivial surrogate signal.  Given a
    candidate's ``score`` feature, returns:

        predicted_score    = clip(score × decay + noise, 0..1)
        uncertainty        = max(min_unc, base_unc × (1 - score))

    Uncertainty shrinks as the upstream score grows (we're more confident
    about good leads, more uncertain about marginal ones).  ``decay`` < 1
    models the fact that downstream stages tend to refine scores
    downward; tune to taste.

    Online recall tracking is supported via the ``RecallTracker`` plugin.
    """
    def __init__(
        self,
        stage_id:        str,
        decay:           float = 0.90,
        noise_std:       float = 0.05,
        base_unc:        float = 0.40,
        min_unc:         float = 0.05,
        seed:            int   = 0,
        recall_tracker:  Optional[RecallTracker] = None,
    ) -> None:
        super().__init__(stage_id, recall_tracker=recall_tracker)
        self.decay     = decay
        self.noise_std = noise_std
        self.base_unc  = base_unc
        self.min_unc   = min_unc
        self._rng      = random.Random(seed)

    def predict(self, candidate_id: str, **features) -> tuple[float, float]:
        score = float(features.get("score", 0.0))
        noise = self._rng.gauss(0.0, self.noise_std)
        pred  = max(0.0, min(1.0, score * self.decay + noise))
        unc   = max(self.min_unc, self.base_unc * (1.0 - score))
        return pred, unc


# ── Factory helpers ─────────────────────────────────────────────────────────

def build_default_surrogate(
    stage_id:           str,
    spec:               Optional["SurrogateSpec"] = None,  # type: ignore[name-defined]  # noqa: F821
    seed:               int = 0,
    enable_recall:      bool = True,
    on_recall_drift:    Optional[Callable[[float, int], None]] = None,
    recall_floor:       float = 0.90,
) -> Surrogate:
    """Construct a CorrelatedSurrogate (or NullSurrogate when no spec).

    Used by AsyncCampaignManager.from_config when the caller doesn't
    supply a surrogate via the plan registry — gives a sane default so
    Triage gates have something to work with in prototype runs.
    """
    if spec is None:
        return NullSurrogate(stage_id=stage_id)
    tracker = None
    if enable_recall:
        tracker = RecallTracker(
            window_size=50, floor=recall_floor,
            breaches_to_escalate=2, on_recall_drift=on_recall_drift,
        )
    return CorrelatedSurrogate(
        stage_id=stage_id,
        seed=seed,
        recall_tracker=tracker,
    )
