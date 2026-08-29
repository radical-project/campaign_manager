"""Tests for plan/schema.py dataclass __post_init__ validators."""

import pytest

from src.campaign.plan.schema import (
    BackpressureEdge,
    EdgeSpec,
    RetryPolicy,
    StageSpec,
    SurrogateSpec,
)


class TestRetryPolicy:
    def test_defaults_valid(self):
        RetryPolicy()  # must not raise

    def test_max_attempts_zero_raises(self):
        with pytest.raises(ValueError, match="max_attempts"):
            RetryPolicy(max_attempts=0)

    def test_max_attempts_negative_raises(self):
        with pytest.raises(ValueError, match="max_attempts"):
            RetryPolicy(max_attempts=-1)

    def test_max_attempts_one_valid(self):
        RetryPolicy(max_attempts=1)

    def test_backoff_negative_raises(self):
        with pytest.raises(ValueError, match="backoff_s"):
            RetryPolicy(backoff_s=-0.1)

    def test_backoff_zero_valid(self):
        RetryPolicy(backoff_s=0.0)


class TestBackpressureEdge:
    def test_valid_watermarks(self):
        BackpressureEdge(high_water=100, low_water=50)

    def test_high_water_zero_raises(self):
        with pytest.raises(ValueError, match="high_water"):
            BackpressureEdge(high_water=0, low_water=0)

    def test_high_water_negative_raises(self):
        with pytest.raises(ValueError, match="high_water"):
            BackpressureEdge(high_water=-1, low_water=0)

    def test_low_water_negative_raises(self):
        with pytest.raises(ValueError, match="low_water"):
            BackpressureEdge(high_water=10, low_water=-1)

    def test_low_water_equals_high_water_raises(self):
        with pytest.raises(ValueError, match="low_water"):
            BackpressureEdge(high_water=10, low_water=10)

    def test_low_water_greater_than_high_water_raises(self):
        with pytest.raises(ValueError, match="low_water"):
            BackpressureEdge(high_water=5, low_water=8)


class TestStageSpec:
    def test_valid_stage(self):
        StageSpec(id="sim")

    def test_empty_id_raises(self):
        with pytest.raises(ValueError, match="id"):
            StageSpec(id="")

    def test_threshold_above_one_raises(self):
        with pytest.raises(ValueError, match="threshold_top_fraction"):
            StageSpec(id="sim", threshold_top_fraction=1.5)

    def test_threshold_zero_raises(self):
        with pytest.raises(ValueError, match="threshold_top_fraction"):
            StageSpec(id="sim", threshold_top_fraction=0.0)

    def test_threshold_one_valid(self):
        StageSpec(id="sim", threshold_top_fraction=1.0)

    def test_budget_node_hours_negative_raises(self):
        with pytest.raises(ValueError, match="budget_node_hours"):
            StageSpec(id="sim", budget_node_hours=-1.0)

    def test_concurrency_floor_above_cap_raises(self):
        with pytest.raises(ValueError, match="concurrency_floor"):
            StageSpec(id="sim", concurrency_floor=10, concurrency_cap=5)

    def test_floor_equals_cap_valid(self):
        StageSpec(id="sim", concurrency_floor=4, concurrency_cap=4)


class TestSurrogateSpec:
    def test_defaults_valid(self):
        SurrogateSpec()

    def test_score_cutoff_within_bounds_valid(self):
        SurrogateSpec(score_cutoff=0.5, score_cutoff_nudge_bounds=(0.0, 1.0))

    def test_score_cutoff_outside_bounds_raises(self):
        with pytest.raises(ValueError, match="score_cutoff"):
            SurrogateSpec(score_cutoff=1.5, score_cutoff_nudge_bounds=(0.0, 1.0))

    def test_inverted_score_nudge_bounds_raises(self):
        with pytest.raises(ValueError, match="score_cutoff_nudge_bounds"):
            SurrogateSpec(score_cutoff_nudge_bounds=(1.0, 0.0))

    def test_uncertainty_cutoff_outside_bounds_raises(self):
        with pytest.raises(ValueError, match="uncertainty_cutoff"):
            SurrogateSpec(uncertainty_cutoff=2.0, uncertainty_cutoff_nudge_bounds=(0.0, 1.0))


class TestEdgeSpec:
    def test_valid_edge(self):
        EdgeSpec(upstream="a", downstream="b")

    def test_empty_upstream_raises(self):
        with pytest.raises(ValueError, match="upstream"):
            EdgeSpec(upstream="", downstream="b")

    def test_empty_downstream_raises(self):
        with pytest.raises(ValueError, match="downstream"):
            EdgeSpec(upstream="a", downstream="")

    def test_invalid_profile_raises(self):
        with pytest.raises(ValueError, match="profile"):
            EdgeSpec(upstream="a", downstream="b", profile="nonexistent_profile")
