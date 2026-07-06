"""Unit tests for src.campaign.budget_controller — burn-ratio feedback on Triage cutoffs."""

import pytest

from src.campaign import BudgetController, Triage


def _triage(score_cutoff=0.5, bounds=(0.0, 1.0)):
    return Triage(
        stage_id="s1",
        score_cutoff=score_cutoff,
        score_cutoff_bounds=bounds,
        uncertainty_cutoff=0.5,
        uncertainty_cutoff_bounds=(0.0, 1.0),
    )


def _controller(triage=None, **kw):
    defaults = dict(
        stage_id="s1",
        triage=triage or _triage(),
        budget_node_hours=100.0,
        pilot_nodes=1,
        pilot_walltime_h=1.0,
        downstream_target=100,
        kp=0.05,
        warmup_min_finished=3,
    )
    defaults.update(kw)
    return BudgetController(**defaults)


class TestWarmupGating:
    def test_returns_none_before_min_finished(self):
        c = _controller(warmup_min_finished=10)
        assert c.evaluate(finished_replicas=5) is None

    def test_returns_none_below_warmup_progress(self):
        # finished ≥ min but progress < warmup_progress (0.10)
        c = _controller(warmup_min_finished=3, downstream_target=1000)
        assert c.evaluate(finished_replicas=5) is None  # 5/1000 = 0.5% < 10%

    def test_returns_none_without_budget(self):
        c = _controller(budget_node_hours=0.0)
        assert c.evaluate(finished_replicas=50) is None


class TestControlLaw:
    def test_in_band_no_nudge(self):
        # actual == expected → burn_ratio 1.0 → in band
        # expected = budget * progress = 100 * (50/100) = 50
        # actual   = nodes*walltime*finished = 1*1*50 = 50
        c = _controller()
        before = c.triage.score_cutoff
        ev = c.evaluate(finished_replicas=50)
        assert ev.kind == "in_band"
        assert ev.burn_ratio == pytest.approx(1.0)
        assert c.triage.score_cutoff == before  # unchanged

    def test_over_budget_tightens_score_cutoff(self):
        # Make actual >> expected: walltime 4h → actual=200, expected=50 → ratio 4
        c = _controller(pilot_walltime_h=4.0)
        before = c.triage.score_cutoff
        ev = c.evaluate(finished_replicas=50)
        assert ev.kind in ("nudged", "bound_locked")
        assert ev.burn_ratio > 1.0
        assert c.triage.score_cutoff > before   # raised the bar

    def test_under_budget_loosens_score_cutoff(self):
        # actual << expected: walltime 0.25h → actual=12.5, expected=50 → ratio 0.25
        c = _controller(pilot_walltime_h=0.25)
        before = c.triage.score_cutoff
        ev = c.evaluate(finished_replicas=50)
        assert ev.burn_ratio < 1.0
        assert c.triage.score_cutoff < before   # lowered the bar


class TestEscalation:
    def test_repeated_bound_hits_lock(self):
        # Start near the high bound so the first nudge clamps immediately.
        t = _triage(score_cutoff=0.99, bounds=(0.0, 1.0))
        c = _controller(triage=t, pilot_walltime_h=4.0, consecutive_bound_threshold=2)
        ev1 = c.evaluate(finished_replicas=50)
        ev2 = c.evaluate(finished_replicas=60)
        assert ev1.score_at_bound is True
        assert ev2.kind == "bound_locked"
        assert ev2.consecutive_hits >= 2


class TestFreeze:
    def test_frozen_controller_does_not_nudge(self):
        c = _controller(pilot_walltime_h=4.0)
        c.freeze(True)
        before = c.triage.score_cutoff
        ev = c.evaluate(finished_replicas=50)
        assert ev.frozen is True
        assert c.triage.score_cutoff == before


def test_invalid_params_raise():
    with pytest.raises(ValueError):
        _controller(kp=0.0)
    with pytest.raises(ValueError):
        _controller(burn_rate_band=2.0)
