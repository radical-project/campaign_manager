"""Unit tests for src.campaign.triage — the per-candidate RUN/DISCARD/ADVANCE gate."""

import pytest

from src.campaign import Triage, TriageDecision


def _triage(score_cutoff=0.5, unc_cutoff=0.5, advance_threshold=float("inf")):
    return Triage(
        stage_id="s1",
        score_cutoff=score_cutoff,
        score_cutoff_bounds=(0.0, 1.0),
        uncertainty_cutoff=unc_cutoff,
        uncertainty_cutoff_bounds=(0.0, 1.0),
        advance_threshold=advance_threshold,
    )


class TestTriageDecide:
    def test_run_when_score_clears_floor(self):
        t = _triage(score_cutoff=0.5)
        assert t.decide(score=0.8) is TriageDecision.RUN

    def test_discard_when_score_and_pred_below_floor(self):
        t = _triage(score_cutoff=0.5)
        assert t.decide(score=0.2, surrogate_pred=0.1) is TriageDecision.DISCARD

    def test_high_score_saves_low_surrogate_pred(self):
        # Either signal clearing the floor is enough to RUN.
        t = _triage(score_cutoff=0.5)
        assert t.decide(score=0.9, surrogate_pred=0.0) is TriageDecision.RUN

    def test_high_surrogate_pred_saves_low_score(self):
        t = _triage(score_cutoff=0.5)
        assert t.decide(score=0.1, surrogate_pred=0.9) is TriageDecision.RUN

    def test_high_uncertainty_low_signal_discards(self):
        t = _triage(score_cutoff=0.5, unc_cutoff=0.3)
        # unc above cutoff AND both score/pred below floor → DISCARD
        assert t.decide(score=0.2, surrogate_pred=0.2, surrogate_unc=0.9) is TriageDecision.DISCARD

    def test_advance_disabled_by_default(self):
        # default advance_threshold is +inf → never ADVANCE
        t = _triage(score_cutoff=0.1)
        assert t.decide(score=0.99, surrogate_pred=0.99, surrogate_unc=0.0) is TriageDecision.RUN

    def test_advance_when_confident_and_above_threshold(self):
        t = _triage(score_cutoff=0.1, unc_cutoff=0.5, advance_threshold=0.9)
        assert (
            t.decide(score=0.95, surrogate_pred=0.95, surrogate_unc=0.1) is TriageDecision.ADVANCE
        )

    def test_no_advance_when_uncertain(self):
        # prediction high enough but uncertainty above cutoff → not ADVANCE
        t = _triage(score_cutoff=0.1, unc_cutoff=0.2, advance_threshold=0.9)
        assert t.decide(score=0.95, surrogate_pred=0.95, surrogate_unc=0.5) is TriageDecision.RUN


class TestTriageNudge:
    def test_nudge_raises_score_cutoff(self):
        t = _triage(score_cutoff=0.5)
        t.nudge_cutoffs(score_delta=0.1, unc_delta=0.0)
        assert t.score_cutoff == pytest.approx(0.6)

    def test_nudge_clamps_to_bounds_and_reports_at_bound(self):
        t = _triage(score_cutoff=0.95)
        score_at_bound, _ = t.nudge_cutoffs(score_delta=0.5, unc_delta=0.0)
        assert t.score_cutoff == pytest.approx(1.0)  # clamped to high bound
        assert score_at_bound is True

    def test_reset_restores_initial(self):
        t = _triage(score_cutoff=0.5, unc_cutoff=0.5)
        t.nudge_cutoffs(score_delta=0.3, unc_delta=-0.2)
        t.reset()
        assert t.score_cutoff == pytest.approx(0.5)
        assert t.uncertainty_cutoff == pytest.approx(0.5)


def test_invalid_bounds_raise():
    with pytest.raises(ValueError):
        Triage(
            stage_id="s",
            score_cutoff=0.5,
            score_cutoff_bounds=(1.0, 0.0),
            uncertainty_cutoff=0.5,
            uncertainty_cutoff_bounds=(0.0, 1.0),
        )
