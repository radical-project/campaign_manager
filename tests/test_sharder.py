"""Unit tests for src.campaign.sharder — _percentile_rank, ShardingSpec, and Sharder."""

import pytest

from src.campaign.backpressure import BackpressureNegotiator, BPState
from src.campaign.sharder import ShardingSpec, Sharder, _percentile_rank


# ── _percentile_rank ──────────────────────────────────────────────────────────


class TestPercentileRank:
    def test_empty_input_returns_empty(self):
        assert _percentile_rank([]) == []

    def test_single_value_returns_one(self):
        assert _percentile_rank([42.0]) == [1.0]

    def test_two_values_returns_zero_and_one(self):
        result = _percentile_rank([0.0, 1.0])
        assert len(result) == 2
        assert min(result) == pytest.approx(0.0)
        assert max(result) == pytest.approx(1.0)

    def test_larger_value_gets_higher_rank(self):
        result = _percentile_rank([1.0, 2.0, 3.0])
        assert result[0] < result[1] < result[2]

    def test_ranks_span_zero_to_one(self):
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        result = _percentile_rank(values)
        assert min(result) == pytest.approx(0.0)
        assert max(result) == pytest.approx(1.0)

    def test_tied_values_get_identical_ranks(self):
        result = _percentile_rank([5.0, 5.0, 5.0])
        assert result[0] == result[1] == result[2]

    def test_output_length_matches_input(self):
        for n in (3, 7, 10):
            assert len(_percentile_rank(list(range(n)))) == n


# ── ShardingSpec ──────────────────────────────────────────────────────────────


class TestShardingSpecFromDict:
    def test_defaults_when_empty_dict(self):
        spec = ShardingSpec.from_dict({})
        assert spec.target_size == 50
        assert spec.min_size == 1
        assert spec.max_size == 200
        assert spec.stratify == "soft"
        assert spec.top_fraction == pytest.approx(1.0)
        assert spec.profile == "diverse_top"

    def test_values_from_dict_override_defaults(self):
        spec = ShardingSpec.from_dict(
            {"target_size": 10, "min_size": 2, "max_size": 100,
             "stratify": "strict", "top_fraction": 0.5, "profile": "pure_promise"}
        )
        assert spec.target_size == 10
        assert spec.min_size == 2
        assert spec.max_size == 100
        assert spec.stratify == "strict"
        assert spec.top_fraction == pytest.approx(0.5)
        assert spec.profile == "pure_promise"

    def test_partial_override_keeps_remaining_defaults(self):
        spec = ShardingSpec.from_dict({"target_size": 20})
        assert spec.target_size == 20
        assert spec.stratify == "soft"


# ── Sharder helpers ───────────────────────────────────────────────────────────


def _sharder(stratify="soft", target_size=5, min_size=1, max_size=100, profile="diverse_top"):
    spec = ShardingSpec(
        target_size=target_size, min_size=min_size, max_size=max_size,
        stratify=stratify, profile=profile,
    )
    return Sharder(name="test", spec=spec)


def _fill(sh, n, base_score=1.0):
    for i in range(n):
        sh.receive(f"c{i}", score=base_score)


# ── Sharder.receive ───────────────────────────────────────────────────────────


class TestSharderReceive:
    def test_buffer_grows_on_receive(self):
        sh = _sharder()
        sh.receive("c1", score=0.5)
        sh.receive("c2", score=0.8)
        assert sh.buffered == 2

    def test_max_score_seen_updated(self):
        sh = _sharder()
        sh.receive("c1", score=0.3)
        assert sh._max_score_seen == pytest.approx(0.3)
        sh.receive("c2", score=0.9)
        assert sh._max_score_seen == pytest.approx(0.9)

    def test_max_score_seen_never_decreases(self):
        sh = _sharder()
        sh.receive("c1", score=0.9)
        sh.receive("c2", score=0.1)
        assert sh._max_score_seen == pytest.approx(0.9)

    def test_receive_stores_candidate_id(self):
        sh = _sharder()
        sh.receive("my_candidate", score=0.5)
        assert sh._buf[0].candidate_id == "my_candidate"

    def test_receive_stores_all_fields(self):
        sh = _sharder()
        sh.receive("c1", score=0.7, surrogate_pred=0.6, surrogate_unc=0.2,
                   scaffold_class="typeA", enqueue_time=1000.0)
        e = sh._buf[0]
        assert e.score == pytest.approx(0.7)
        assert e.surrogate_pred == pytest.approx(0.6)
        assert e.surrogate_unc == pytest.approx(0.2)
        assert e.scaffold_class == "typeA"
        assert e.enqueue_time == pytest.approx(1000.0)

    def test_receive_uses_now_fn_when_no_enqueue_time(self):
        sh = _sharder()
        sh.set_now_fn(lambda: 9999.0)
        sh.receive("c1", score=0.5)
        assert sh._buf[0].enqueue_time == pytest.approx(9999.0)


# ── Sharder.dispatch — empty / THROTTLE / strict-undersized ──────────────────


class TestSharderDispatchEmpty:
    def test_dispatch_empty_buffer_returns_empty_list(self):
        sh = _sharder()
        assert sh.dispatch(bp=None, occupancy=0.5) == []


class TestSharderDispatchThrottle:
    def _throttled_bp(self):
        bp = BackpressureNegotiator("e", high_water=5, low_water=2)
        bp.step(10)  # push into THROTTLE
        assert bp.state == BPState.THROTTLE
        return bp

    def test_dispatch_returns_empty_on_throttle(self):
        sh = _sharder()
        _fill(sh, 10)
        bp = self._throttled_bp()
        assert sh.dispatch(bp=bp, occupancy=0.5) == []

    def test_buffer_unchanged_after_throttle(self):
        sh = _sharder()
        _fill(sh, 5)
        bp = self._throttled_bp()
        sh.dispatch(bp=bp, occupancy=0.5)
        assert sh.buffered == 5


class TestSharderDispatchStrictUndersized:
    def test_strict_returns_empty_when_buffer_below_target(self):
        sh = _sharder(stratify="strict", target_size=10)
        _fill(sh, 5)  # 5 < target_size=10
        assert sh.dispatch(bp=None, occupancy=0.5) == []

    def test_strict_dispatches_when_buffer_equals_target(self):
        sh = _sharder(stratify="strict", target_size=5)
        _fill(sh, 5)
        result = sh.dispatch(bp=None, occupancy=0.5)
        assert len(result) == 5

    def test_soft_dispatches_tail_smaller_than_target(self):
        sh = _sharder(stratify="soft", target_size=10)
        _fill(sh, 3)
        result = sh.dispatch(bp=None, occupancy=0.5)
        assert len(result) == 3


# ── Sharder.dispatch — upstream_done + strict flush ───────────────────────────


class TestSharderDispatchUpstreamDone:
    def test_strict_flushes_all_when_upstream_done(self):
        sh = _sharder(stratify="strict", target_size=10)
        _fill(sh, 4)  # below target
        sh.mark_upstream_done()
        result = sh.dispatch(bp=None, occupancy=0.5)
        assert len(result) == 4
        assert sh.buffered == 0

    def test_soft_ignores_upstream_done(self):
        sh = _sharder(stratify="soft", target_size=10)
        _fill(sh, 4)
        sh.mark_upstream_done()
        # soft: tail truncation still applies, dispatches min(buf, computed_n)
        result = sh.dispatch(bp=None, occupancy=0.5)
        # Should still dispatch (tail truncation kicks in for soft)
        assert len(result) > 0

    def test_strict_flush_returns_candidates_in_priority_order(self):
        sh = _sharder(stratify="strict", target_size=10, profile="pure_promise")
        sh.set_now_fn(lambda: 0.0)
        sh.receive("low", score=0.1, enqueue_time=0.0)
        sh.receive("high", score=0.9, enqueue_time=0.0)
        sh.receive("mid", score=0.5, enqueue_time=0.0)
        sh.mark_upstream_done()
        result = sh.dispatch(bp=None, occupancy=0.5)
        assert result[0] == "high"


# ── Sharder.adaptive_size ────────────────────────────────────────────────────


class TestSharderAdaptiveSize:
    def test_high_occupancy_reduces_size(self):
        sh = _sharder(target_size=100)
        _fill(sh, 50)
        size = sh.adaptive_size(bp=None, occupancy=0.90)
        assert size <= 75  # × 0.75

    def test_low_occupancy_increases_size(self):
        sh = _sharder(target_size=100)
        _fill(sh, 200)
        size = sh.adaptive_size(bp=None, occupancy=0.20)
        assert size >= 100  # ≥ target (× 1.25)

    def test_normal_occupancy_keeps_target(self):
        sh = _sharder(target_size=50)
        _fill(sh, 100)
        size = sh.adaptive_size(bp=None, occupancy=0.60)
        assert size == 50

    def test_size_clamped_to_min(self):
        sh = _sharder(target_size=1, min_size=1, max_size=100)
        _fill(sh, 10)
        size = sh.adaptive_size(bp=None, occupancy=0.90)
        assert size >= 1

    def test_size_clamped_to_max(self):
        sh = _sharder(target_size=200, min_size=1, max_size=50)
        _fill(sh, 300)
        size = sh.adaptive_size(bp=None, occupancy=0.20)
        assert size <= 50

    def test_tail_truncation_for_soft_stratify(self):
        sh = _sharder(stratify="soft", target_size=50)
        _fill(sh, 3)
        size = sh.adaptive_size(bp=None, occupancy=0.5)
        assert size == 3  # buf=3 < computed_n → truncate to 3

    def test_no_tail_truncation_for_strict_stratify(self):
        sh = _sharder(stratify="strict", target_size=50)
        _fill(sh, 3)
        size = sh.adaptive_size(bp=None, occupancy=0.5)
        assert size >= 50  # strict: floor at target


# ── Sharder._score_entries ────────────────────────────────────────────────────


class TestSharderScoreEntries:
    def test_empty_returns_empty(self):
        sh = _sharder()
        assert sh._score_entries([], now=0.0) == []

    def test_higher_score_candidate_gets_higher_priority(self):
        sh = _sharder(profile="pure_promise")
        sh.set_now_fn(lambda: 0.0)
        sh.receive("low", score=0.1, enqueue_time=0.0)
        sh.receive("high", score=0.9, enqueue_time=0.0)
        scores = sh._score_entries(sh._buf, now=0.0)
        # high-score entry (index 1) should score higher
        assert scores[1] > scores[0]

    def test_scaffold_in_running_set_gets_zero_diversity(self):
        sh = _sharder(profile="round_robin")
        sh.set_now_fn(lambda: 0.0)
        sh.receive("c1", scaffold_class="typeA", enqueue_time=0.0)
        sh.receive("c2", scaffold_class="typeB", enqueue_time=0.0)
        scores = sh._score_entries(sh._buf, now=0.0, running_scaffolds={"typeA"})
        # typeA gets diversity=0, typeB gets diversity>0 → typeB scores higher
        assert scores[1] > scores[0]

    def test_dispatch_respects_priority_order(self):
        sh = _sharder(stratify="soft", target_size=1, profile="pure_promise")
        sh.set_now_fn(lambda: 0.0)
        sh.receive("low", score=0.1, enqueue_time=0.0)
        sh.receive("high", score=0.9, enqueue_time=0.0)
        result = sh.dispatch(bp=None, occupancy=0.5)
        assert result[0] == "high"

    def test_dispatched_candidates_removed_from_buffer(self):
        sh = _sharder(stratify="soft", target_size=2)
        _fill(sh, 5)
        sh.dispatch(bp=None, occupancy=0.5)
        assert sh.buffered == 3


# ── Sharder.mark_upstream_done ────────────────────────────────────────────────


class TestSharderMarkUpstreamDone:
    def test_flag_set_on_mark(self):
        sh = _sharder()
        sh.mark_upstream_done()
        assert sh._upstream_done is True

    def test_flag_starts_false(self):
        sh = _sharder()
        assert sh._upstream_done is False
