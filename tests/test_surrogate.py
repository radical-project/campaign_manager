"""Unit tests for src.campaign.surrogate — surrogate models + RecallTracker."""

import pytest

from src.campaign import (
    CorrelatedSurrogate,
    NullSurrogate,
    RandomSurrogate,
    RecallTracker,
)


class TestNullSurrogate:
    def test_returns_neutral_max_uncertainty(self):
        s = NullSurrogate("s1")
        pred, unc = s.predict("c1")
        assert pred == 0.0
        assert unc == 1.0


class TestCorrelatedSurrogate:
    def test_prediction_tracks_score(self):
        s = CorrelatedSurrogate("s1", noise_std=0.0, decay=0.9)
        pred, _ = s.predict("c1", score=0.8)
        assert pred == pytest.approx(0.72)  # 0.8 * 0.9, no noise

    def test_uncertainty_shrinks_with_score(self):
        s = CorrelatedSurrogate("s1", noise_std=0.0, base_unc=0.4, min_unc=0.05)
        _, unc_low = s.predict("c1", score=0.2)
        _, unc_high = s.predict("c2", score=0.9)
        assert unc_high < unc_low  # more confident about good leads

    def test_prediction_clipped_to_unit_interval(self):
        s = CorrelatedSurrogate("s1", noise_std=0.0, decay=2.0)
        pred, _ = s.predict("c1", score=0.9)
        assert 0.0 <= pred <= 1.0

    def test_deterministic_with_seed(self):
        a = CorrelatedSurrogate("s1", seed=42).predict("c1", score=0.5)
        b = CorrelatedSurrogate("s1", seed=42).predict("c1", score=0.5)
        assert a == b


class TestRandomSurrogate:
    def test_within_configured_ranges(self):
        s = RandomSurrogate("s1", seed=1, pred_range=(0.2, 0.4), unc_range=(0.0, 0.1))
        for _ in range(20):
            pred, unc = s.predict("c")
            assert 0.2 <= pred <= 0.4
            assert 0.0 <= unc <= 0.1


class TestRecallTracker:
    def test_recall_one_with_too_little_data(self):
        rt = RecallTracker(window_size=50)
        rt.observe(0.9, 0.9)
        assert rt.recall_at_k(k=5) == 1.0  # < k samples → 1.0

    def test_drift_fires_when_recall_below_floor(self):
        fired = []
        rt = RecallTracker(
            window_size=10,
            floor=0.90,
            breaches_to_escalate=1,
            on_recall_drift=lambda r, n: fired.append((r, n)),
        )
        # Feed perfectly anti-correlated pairs so top-k preds miss top-k actuals.
        for i in range(20):
            rt.observe(predicted=float(i), actual=float(-i))
        assert fired, "expected on_recall_drift to fire on low recall"

    def test_perfect_correlation_no_drift(self):
        fired = []
        rt = RecallTracker(
            window_size=10,
            floor=0.90,
            breaches_to_escalate=1,
            on_recall_drift=lambda r, n: fired.append((r, n)),
        )
        for i in range(20):
            rt.observe(predicted=float(i), actual=float(i))
        assert not fired

    def test_mae_tracks_error(self):
        rt = RecallTracker(window_size=50)
        rt.observe(0.5, 0.7)
        rt.observe(0.5, 0.3)
        assert rt.mean_absolute_error() == pytest.approx(0.2)


def test_update_with_results_feeds_recall_tracker():
    rt = RecallTracker(window_size=50)
    s = CorrelatedSurrogate("s1", recall_tracker=rt)
    s.update_with_results([("c1", 0.5, 0.6), ("c2", 0.4, 0.4)])
    assert rt.mean_absolute_error() is not None
