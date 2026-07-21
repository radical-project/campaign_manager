"""Unit tests for src.campaign.bandit — BanditArm and SchedulingBandit."""

import random

import pytest

from src.campaign.bandit import BanditArm, SchedulingBandit


# ── BanditArm ─────────────────────────────────────────────────────────────────


class TestBanditArmDefaults:
    def test_default_alpha_and_beta(self):
        arm = BanditArm(label="s1")
        assert arm.alpha == pytest.approx(1.0)
        assert arm.beta == pytest.approx(1.0)

    def test_initial_mean_is_half(self):
        arm = BanditArm(label="s1")
        assert arm.mean == pytest.approx(0.5)

    def test_initial_pulls_is_zero(self):
        arm = BanditArm(label="s1")
        assert arm.pulls == 0

    def test_label_stored(self):
        arm = BanditArm(label="stage_A")
        assert arm.label == "stage_A"


class TestBanditArmUpdate:
    def test_full_reward_increments_alpha(self):
        arm = BanditArm(label="s1")
        arm.update(1.0)
        assert arm.alpha == pytest.approx(2.0)
        assert arm.beta == pytest.approx(1.0)

    def test_zero_reward_increments_beta(self):
        arm = BanditArm(label="s1")
        arm.update(0.0)
        assert arm.alpha == pytest.approx(1.0)
        assert arm.beta == pytest.approx(2.0)

    def test_half_reward_increments_both_equally(self):
        arm = BanditArm(label="s1")
        arm.update(0.5)
        assert arm.alpha == pytest.approx(1.5)
        assert arm.beta == pytest.approx(1.5)

    def test_reward_clamped_above_one(self):
        arm = BanditArm(label="s1")
        arm.update(5.0)
        assert arm.alpha == pytest.approx(2.0)
        assert arm.beta == pytest.approx(1.0)

    def test_reward_clamped_below_zero(self):
        arm = BanditArm(label="s1")
        arm.update(-1.0)
        assert arm.alpha == pytest.approx(1.0)
        assert arm.beta == pytest.approx(2.0)

    def test_multiple_updates_accumulate(self):
        arm = BanditArm(label="s1")
        arm.update(1.0)
        arm.update(0.0)
        arm.update(0.5)
        assert arm.alpha == pytest.approx(3.5)
        assert arm.beta == pytest.approx(2.5)

    def test_pulls_counts_updates(self):
        arm = BanditArm(label="s1")
        arm.update(0.8)
        arm.update(0.2)
        assert arm.pulls == 2

    def test_mean_rises_after_high_rewards(self):
        arm = BanditArm(label="s1")
        for _ in range(10):
            arm.update(1.0)
        assert arm.mean > 0.8

    def test_mean_falls_after_low_rewards(self):
        arm = BanditArm(label="s1")
        for _ in range(10):
            arm.update(0.0)
        assert arm.mean < 0.2


class TestBanditArmReset:
    def test_reset_restores_uniform_prior(self):
        arm = BanditArm(label="s1")
        arm.update(1.0)
        arm.update(0.0)
        arm.reset()
        assert arm.alpha == pytest.approx(1.0)
        assert arm.beta == pytest.approx(1.0)

    def test_reset_clears_pulls(self):
        arm = BanditArm(label="s1")
        arm.update(0.9)
        arm.reset()
        assert arm.pulls == 0

    def test_reset_restores_mean_to_half(self):
        arm = BanditArm(label="s1")
        for _ in range(5):
            arm.update(1.0)
        arm.reset()
        assert arm.mean == pytest.approx(0.5)


class TestBanditArmSample:
    def test_sample_returns_float_in_unit_interval(self):
        rng = random.Random(42)
        arm = BanditArm(label="s1")
        for _ in range(50):
            s = arm.sample(rng)
            assert 0.0 <= s <= 1.0

    def test_high_alpha_arm_samples_higher_on_average(self):
        rng = random.Random(0)
        good = BanditArm(label="good", alpha=50.0, beta=1.0)
        bad = BanditArm(label="bad", alpha=1.0, beta=50.0)
        good_mean = sum(good.sample(rng) for _ in range(1000)) / 1000
        bad_mean = sum(bad.sample(rng) for _ in range(1000)) / 1000
        assert good_mean > bad_mean

    def test_repr_includes_label_and_mean(self):
        arm = BanditArm(label="search")
        r = repr(arm)
        assert "search" in r
        assert "mean=" in r


# ── SchedulingBandit ──────────────────────────────────────────────────────────


class _MockGroup:
    def __init__(self, name):
        self.name = name


class TestSchedulingBanditInit:
    def test_arms_created_for_all_stages(self):
        bandit = SchedulingBandit(["s1", "s2", "s3"])
        assert set(bandit._arms.keys()) == {"s1", "s2", "s3"}

    def test_default_prior_is_uniform(self):
        bandit = SchedulingBandit(["s1"])
        arm = bandit._arms["s1"]
        assert arm.alpha == pytest.approx(1.0)
        assert arm.beta == pytest.approx(1.0)

    def test_stage_priors_applied(self):
        bandit = SchedulingBandit(["s1", "s2"], stage_priors={"s1": (3.0, 1.0)})
        assert bandit._arms["s1"].alpha == pytest.approx(3.0)
        assert bandit._arms["s1"].beta == pytest.approx(1.0)
        assert bandit._arms["s2"].alpha == pytest.approx(1.0)

    def test_seed_makes_deterministic(self):
        b1 = SchedulingBandit(["s1", "s2"], seed=99)
        b2 = SchedulingBandit(["s1", "s2"], seed=99)
        groups = [_MockGroup("s1"), _MockGroup("s2")]
        assert [g.name for g in b1.rank(groups)] == [g.name for g in b2.rank(groups)]


class TestSchedulingBanditRank:
    def test_single_eligible_returned_unchanged(self):
        bandit = SchedulingBandit(["s1", "s2"])
        groups = [_MockGroup("s1")]
        assert bandit.rank(groups) == groups

    def test_empty_eligible_returned_unchanged(self):
        bandit = SchedulingBandit(["s1"])
        assert bandit.rank([]) == []

    def test_high_alpha_stage_ranked_first_on_average(self):
        bandit = SchedulingBandit(["weak", "strong"])
        bandit._arms["strong"].alpha = 100.0
        bandit._arms["strong"].beta = 1.0
        groups = [_MockGroup("weak"), _MockGroup("strong")]
        wins = sum(
            bandit.rank(groups)[0].name == "strong" for _ in range(100)
        )
        assert wins > 80

    def test_unknown_group_gets_neutral_sample(self):
        bandit = SchedulingBandit(["known"])
        groups = [_MockGroup("known"), _MockGroup("unknown")]
        # Should not raise; unknown group uses 0.5 fallback
        result = bandit.rank(groups)
        assert len(result) == 2


class TestSchedulingBanditUpdate:
    def test_update_known_stage_changes_arm(self):
        bandit = SchedulingBandit(["s1"])
        bandit.update("s1", 1.0)
        assert bandit._arms["s1"].alpha == pytest.approx(2.0)

    def test_update_unknown_stage_is_silent(self):
        bandit = SchedulingBandit(["s1"])
        bandit.update("s_nonexistent", 0.9)  # must not raise

    def test_reward_widen_signal(self):
        bandit = SchedulingBandit(["s1"])
        bandit.update("s1", 0.8)
        assert bandit._arms["s1"].mean > 0.5

    def test_reward_throttle_signal(self):
        bandit = SchedulingBandit(["s1"])
        bandit.update("s1", 0.2)
        assert bandit._arms["s1"].mean < 0.5


class TestSchedulingBanditBest:
    def test_best_returns_none_for_empty_bandit(self):
        bandit = SchedulingBandit([])
        assert bandit.best() is None

    def test_best_returns_highest_mean_stage(self):
        bandit = SchedulingBandit(["s1", "s2", "s3"])
        bandit._arms["s2"].alpha = 10.0
        bandit._arms["s2"].beta = 1.0
        assert bandit.best() == "s2"

    def test_best_after_updates(self):
        bandit = SchedulingBandit(["s1", "s2"], seed=7)
        for _ in range(5):
            bandit.update("s1", 1.0)
        assert bandit.best() == "s1"


class TestSchedulingBanditSummary:
    def test_summary_returns_dict_of_means(self):
        bandit = SchedulingBandit(["s1", "s2"])
        summary = bandit.summary()
        assert set(summary.keys()) == {"s1", "s2"}
        assert summary["s1"] == pytest.approx(0.5)
        assert summary["s2"] == pytest.approx(0.5)

    def test_summary_reflects_updates(self):
        bandit = SchedulingBandit(["s1", "s2"])
        bandit.update("s1", 1.0)
        summary = bandit.summary()
        assert summary["s1"] > 0.5

    def test_repr_includes_best(self):
        bandit = SchedulingBandit(["s1", "s2"])
        r = repr(bandit)
        assert "best=" in r
