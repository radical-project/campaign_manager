"""Unit tests for src.campaign.profiles — ProfileWeights and named profiles."""

import pytest

from src.campaign.profiles import PROFILES, ProfileWeights, get_profile


# ── ProfileWeights ────────────────────────────────────────────────────────────


class TestProfileWeights:
    def test_as_dict_returns_all_five_signals(self):
        pw = ProfileWeights(score=0.6, surrogate=0.1, uncertainty=0.2, age=0.05, diversity=0.3)
        d = pw.as_dict()
        assert set(d.keys()) == {"score", "surrogate", "uncertainty", "age", "diversity"}

    def test_as_dict_values_match_fields(self):
        pw = ProfileWeights(score=0.6, surrogate=0.1, uncertainty=0.2, age=0.05, diversity=0.3)
        d = pw.as_dict()
        assert d["score"] == pytest.approx(0.6)
        assert d["surrogate"] == pytest.approx(0.1)
        assert d["uncertainty"] == pytest.approx(0.2)
        assert d["age"] == pytest.approx(0.05)
        assert d["diversity"] == pytest.approx(0.3)

    def test_frozen_rejects_mutation(self):
        pw = ProfileWeights(score=0.5, surrogate=0.0, uncertainty=0.0, age=0.0, diversity=0.0)
        with pytest.raises((AttributeError, TypeError)):
            pw.score = 1.0  # type: ignore[misc]


# ── Named profiles ────────────────────────────────────────────────────────────


class TestGetProfile:
    def test_returns_profile_weights_instance(self):
        pw = get_profile("pure_promise")
        assert isinstance(pw, ProfileWeights)

    def test_raises_key_error_for_unknown_name(self):
        with pytest.raises(KeyError):
            get_profile("does_not_exist")

    def test_key_error_message_includes_known_profiles(self):
        with pytest.raises(KeyError, match="known"):
            get_profile("mystery_profile")

    def test_all_five_profiles_accessible(self):
        for name in ("pure_promise", "active_learning", "explore_exploit", "diverse_top", "round_robin"):
            pw = get_profile(name)
            assert isinstance(pw, ProfileWeights)

    def test_profiles_dict_contains_exactly_five(self):
        assert len(PROFILES) == 5


class TestProfileSemantics:
    def test_pure_promise_has_zero_uncertainty_and_diversity(self):
        pw = get_profile("pure_promise")
        assert pw.uncertainty == pytest.approx(0.0)
        assert pw.diversity == pytest.approx(0.0)
        assert pw.score > 0.0

    def test_active_learning_maximises_uncertainty(self):
        pw = get_profile("active_learning")
        assert pw.uncertainty == pytest.approx(1.0)
        assert pw.score == pytest.approx(0.0)
        assert pw.surrogate == pytest.approx(0.0)

    def test_explore_exploit_has_all_three_primary_signals(self):
        pw = get_profile("explore_exploit")
        assert pw.score > 0.0
        assert pw.surrogate > 0.0
        assert pw.uncertainty > 0.0

    def test_diverse_top_has_diversity_weight(self):
        pw = get_profile("diverse_top")
        assert pw.diversity > 0.0
        assert pw.score > 0.0

    def test_round_robin_maximises_diversity(self):
        pw = get_profile("round_robin")
        assert pw.diversity == pytest.approx(1.0)
        assert pw.score == pytest.approx(0.0)
        assert pw.surrogate == pytest.approx(0.0)
        assert pw.uncertainty == pytest.approx(0.0)

    def test_all_profiles_have_positive_age_bonus(self):
        for name in PROFILES:
            pw = get_profile(name)
            assert pw.age > 0.0, f"profile {name!r} has zero age weight"
