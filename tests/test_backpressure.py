"""Tests for score-aware BackpressureNegotiator and Sharder.buffer_score_quality().

Covers items #8:
  - BackpressureNegotiator.score_slack raises effective high_water proportionally
    to the upstream buffer quality signal, while low_water stays fixed.
  - Sharder tracks _max_score_seen and computes buffer_score_quality() as
    mean(buf scores) / max_seen, returning 0.5 (neutral) when the buffer is
    empty or all scores are zero.
"""

import pytest

from src.campaign.backpressure import BackpressureNegotiator, BPState
from src.campaign.sharder import Sharder, ShardingSpec

# ── BackpressureNegotiator score_slack ─────────────────────────────────────────


class TestBackpressureScoreSlack:
    def _bp(self, high=10, low=3, slack=0.3):
        return BackpressureNegotiator("edge", high_water=high, low_water=low, score_slack=slack)

    def test_default_score_slack_is_zero(self):
        bp = BackpressureNegotiator("e", high_water=10, low_water=3)
        assert bp.score_slack == 0.0

    def test_no_slack_behaves_exactly_as_original(self):
        # score_slack=0.0 and any quality: effective_high == high_water
        bp = self._bp(high=10, slack=0.0)
        assert bp.step(10, score_quality=1.0) == BPState.THROTTLE
        bp2 = self._bp(high=10, slack=0.0)
        assert bp2.step(9, score_quality=1.0) == BPState.HOLD

    def test_high_quality_raises_effective_threshold(self):
        # slack=0.3, quality=1.0 → effective_high = int(10*(1+0.3)) = 13
        bp = self._bp(high=10, slack=0.3)
        assert bp.step(12, score_quality=1.0) == BPState.HOLD     # 12 < 13
        assert bp.step(13, score_quality=1.0) == BPState.THROTTLE  # 13 >= 13

    def test_low_quality_uses_nominal_threshold(self):
        # slack=0.3, quality=0.0 → effective_high = int(10*(1+0)) = 10
        bp = self._bp(high=10, slack=0.3)
        assert bp.step(10, score_quality=0.0) == BPState.THROTTLE
        bp2 = self._bp(high=10, slack=0.3)
        assert bp2.step(9, score_quality=0.0) == BPState.HOLD

    def test_partial_quality_partial_slack(self):
        # slack=0.3, quality=0.5 → effective_high = int(10*(1+0.15)) = int(11.5) = 11
        bp = self._bp(high=10, slack=0.3)
        assert bp.step(11, score_quality=0.5) == BPState.THROTTLE  # 11 >= 11
        bp2 = self._bp(high=10, slack=0.3)
        assert bp2.step(10, score_quality=0.5) == BPState.HOLD     # 10 < 11

    def test_low_water_is_unaffected_by_score_quality(self):
        # Low water must not scale — draining is always aggressive once throttled.
        bp = self._bp(high=10, low=3, slack=0.5)
        bp.step(15, score_quality=1.0)      # → THROTTLE
        assert bp.step(3, score_quality=1.0) == BPState.WIDEN  # drains to fixed low_water
        bp2 = self._bp(high=10, low=3, slack=0.5)
        bp2.step(15)
        assert bp2.step(4, score_quality=1.0) == BPState.HOLD  # above low_water → HOLD

    def test_default_quality_param_is_neutral(self):
        # step(depth) without score_quality defaults to 0.5; for slack=0.0 this
        # must be identical to the original behaviour.
        bp = self._bp(high=10, slack=0.0)
        bp_orig = BackpressureNegotiator("orig", high_water=10, low_water=3)
        assert bp.step(10) == bp_orig.step(10) == BPState.THROTTLE
        bp2 = self._bp(high=10, slack=0.0)
        bp2_orig = BackpressureNegotiator("orig2", high_water=10, low_water=3)
        assert bp2.step(5) == bp2_orig.step(5) == BPState.HOLD

    def test_post_init_validation_still_rejects_bad_watermarks(self):
        with pytest.raises(ValueError):
            BackpressureNegotiator("bad", high_water=5, low_water=5, score_slack=0.3)

    def test_repr_includes_edge_name_and_state(self):
        bp = self._bp()
        assert "edge" in repr(bp)
        assert "hold" in repr(bp)


# ── Sharder.buffer_score_quality ───────────────────────────────────────────────


class TestSharderScoreQuality:
    def _sharder(self):
        return Sharder(name="test", spec=ShardingSpec())

    def test_empty_buffer_returns_neutral(self):
        sh = self._sharder()
        assert sh.buffer_score_quality() == pytest.approx(0.5)

    def test_zero_scores_returns_neutral(self):
        # max_score_seen < 1e-9 → numerically zero → return neutral 0.5
        sh = self._sharder()
        sh.receive("c1", score=0.0)
        sh.receive("c2", score=0.0)
        assert sh.buffer_score_quality() == pytest.approx(0.5)

    def test_quality_is_mean_over_max(self):
        sh = self._sharder()
        sh.receive("c1", score=0.8)
        sh.receive("c2", score=0.6)
        sh.receive("c3", score=1.0)
        # mean = 0.8, max_seen = 1.0 → quality = 0.8
        assert sh.buffer_score_quality() == pytest.approx(0.8)

    def test_quality_clamped_to_one(self):
        sh = self._sharder()
        sh.receive("c1", score=1.0)
        assert sh.buffer_score_quality() <= 1.0
        assert sh.buffer_score_quality() == pytest.approx(1.0)

    def test_quality_clamped_to_zero(self):
        sh = self._sharder()
        sh.receive("c1", score=1.0)
        sh._max_score_seen = 2.0  # force a scenario where mean/max < 0
        sh._buf[0].score = -0.5   # edge: negative score
        assert sh.buffer_score_quality() >= 0.0

    def test_max_score_seen_preserved_after_clear(self):
        sh = self._sharder()
        sh.receive("c1", score=0.9)
        sh.clear()
        sh.receive("c2", score=0.3)
        # buffer has only 0.3 but max_seen is still 0.9 → quality = 0.3/0.9
        assert sh.buffer_score_quality() == pytest.approx(0.3 / 0.9)

    def test_max_score_seen_updated_incrementally(self):
        sh = self._sharder()
        sh.receive("c1", score=0.5)
        assert sh._max_score_seen == pytest.approx(0.5)
        sh.receive("c2", score=0.8)
        assert sh._max_score_seen == pytest.approx(0.8)
        sh.receive("c3", score=0.3)
        assert sh._max_score_seen == pytest.approx(0.8)  # stays at peak

    def test_quality_reflects_only_current_buffer(self):
        # After dispatching high-quality entries the remaining buffer
        # should show lower quality even though max_seen is still high.
        # Use target_size=1 so only the top-score entry is dispatched.
        sh = Sharder(name="test", spec=ShardingSpec(target_size=1))
        sh.receive("c1", score=1.0)
        sh.receive("c2", score=0.2)
        sh.dispatch(bp=None, occupancy=0.5)  # dispatches 1 item: c1 (highest score)
        # remaining: only c2 with score=0.2; max_seen=1.0 → quality≈0.2
        assert sh.buffer_score_quality() == pytest.approx(0.2)
