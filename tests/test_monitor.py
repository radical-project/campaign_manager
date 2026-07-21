"""Unit tests for src.campaign.monitor — Monitor drift detection."""

import pytest

from src.campaign.monitor import DriftEvent, DriftKind, Monitor


# ── Helpers ───────────────────────────────────────────────────────────────────


def _monitor(**kwargs) -> Monitor:
    return Monitor(**kwargs)


# ── _check / check_passthrough ────────────────────────────────────────────────


class TestCheckPassthrough:
    def test_no_event_within_threshold(self):
        m = _monitor(passthrough_dev_pct=25.0)
        ev = m.check_passthrough("s1", observed=0.75, expected=0.8)
        assert ev is None

    def test_event_fired_when_deviation_exceeds_threshold(self):
        m = _monitor(passthrough_dev_pct=25.0)
        # |0.4 - 0.8| / 0.8 * 100 = 50% > 25%
        ev = m.check_passthrough("s1", observed=0.4, expected=0.8)
        assert ev is not None
        assert ev.kind == DriftKind.PASS_THROUGH
        assert ev.stage_id == "s1"

    def test_deviation_pct_computed_correctly(self):
        m = _monitor(passthrough_dev_pct=25.0)
        ev = m.check_passthrough("s1", observed=0.4, expected=0.8)
        assert ev.deviation_pct == pytest.approx(50.0)

    def test_no_event_when_observed_equals_expected(self):
        m = _monitor(passthrough_dev_pct=10.0)
        ev = m.check_passthrough("s1", observed=0.5, expected=0.5)
        assert ev is None

    def test_event_observed_and_expected_stored(self):
        m = _monitor(passthrough_dev_pct=10.0)
        ev = m.check_passthrough("s1", observed=0.1, expected=0.5)
        assert ev.observed == pytest.approx(0.1)
        assert ev.expected == pytest.approx(0.5)

    def test_returns_none_when_expected_is_zero(self):
        m = _monitor()
        ev = m.check_passthrough("s1", observed=0.5, expected=0.0)
        assert ev is None

    def test_breach_count_increments_on_consecutive_breaches(self):
        m = _monitor(passthrough_dev_pct=10.0)
        ev1 = m.check_passthrough("s1", observed=0.0, expected=1.0)
        ev2 = m.check_passthrough("s1", observed=0.0, expected=1.0)
        assert ev1.breach_count == 1
        assert ev2.breach_count == 2

    def test_breach_count_resets_when_back_in_bounds(self):
        m = _monitor(passthrough_dev_pct=10.0)
        m.check_passthrough("s1", observed=0.0, expected=1.0)  # breach
        m.check_passthrough("s1", observed=0.95, expected=1.0)  # back in bounds
        ev = m.check_passthrough("s1", observed=0.0, expected=1.0)  # breach again
        assert ev.breach_count == 1  # counter was reset

    def test_different_stages_tracked_independently(self):
        m = _monitor(passthrough_dev_pct=10.0)
        m.check_passthrough("s1", observed=0.0, expected=1.0)
        m.check_passthrough("s1", observed=0.0, expected=1.0)
        ev_s2 = m.check_passthrough("s2", observed=0.0, expected=1.0)
        assert ev_s2.breach_count == 1  # s2 starts fresh


# ── check_budget ─────────────────────────────────────────────────────────────


class TestCheckBudget:
    def test_no_event_within_threshold(self):
        m = _monitor(burn_dev_pct=20.0)
        ev = m.check_budget("s1", spent=10.0, expected=11.0)
        assert ev is None

    def test_event_fired_when_over_budget(self):
        m = _monitor(burn_dev_pct=20.0)
        # |15 - 10| / 10 * 100 = 50% > 20%
        ev = m.check_budget("s1", spent=15.0, expected=10.0)
        assert ev is not None
        assert ev.kind == DriftKind.BUDGET_BURN

    def test_budget_event_stores_spent_and_expected(self):
        m = _monitor(burn_dev_pct=20.0)
        ev = m.check_budget("s1", spent=15.0, expected=10.0)
        assert ev.observed == pytest.approx(15.0)
        assert ev.expected == pytest.approx(10.0)

    def test_no_event_when_expected_zero(self):
        m = _monitor()
        ev = m.check_budget("s1", spent=5.0, expected=0.0)
        assert ev is None


# ── check_recall ─────────────────────────────────────────────────────────────


class TestCheckRecall:
    def test_no_event_when_recall_above_floor(self):
        m = _monitor(recall_floor=0.90)
        ev = m.check_recall("s1", recall=0.95)
        assert ev is None

    def test_no_event_when_recall_equals_floor(self):
        m = _monitor(recall_floor=0.90)
        ev = m.check_recall("s1", recall=0.90)
        assert ev is None

    def test_event_fired_when_recall_below_floor(self):
        m = _monitor(recall_floor=0.90)
        ev = m.check_recall("s1", recall=0.80)
        assert ev is not None
        assert ev.kind == DriftKind.SURROGATE_RECALL

    def test_deviation_pct_computed_correctly(self):
        m = _monitor(recall_floor=0.90)
        ev = m.check_recall("s1", recall=0.80)
        # (0.90 - 0.80) / 0.90 * 100 ≈ 11.11%
        assert ev.deviation_pct == pytest.approx((0.90 - 0.80) / 0.90 * 100, rel=1e-4)

    def test_recall_event_stores_observed_and_expected(self):
        m = _monitor(recall_floor=0.90)
        ev = m.check_recall("s1", recall=0.70)
        assert ev.observed == pytest.approx(0.70)
        assert ev.expected == pytest.approx(0.90)

    def test_recall_breach_count_increments(self):
        m = _monitor(recall_floor=0.90)
        ev1 = m.check_recall("s1", recall=0.80)
        ev2 = m.check_recall("s1", recall=0.80)
        assert ev1.breach_count == 1
        assert ev2.breach_count == 2

    def test_recall_counter_reset_when_back_above_floor(self):
        m = _monitor(recall_floor=0.90)
        m.check_recall("s1", recall=0.80)  # breach
        m.check_recall("s1", recall=0.95)  # back above floor → reset
        ev = m.check_recall("s1", recall=0.80)  # breach again
        assert ev.breach_count == 1


# ── is_escalating ─────────────────────────────────────────────────────────────


class TestIsEscalating:
    def test_not_escalating_below_threshold(self):
        m = _monitor(breaches_to_escalate=3)
        ev = DriftEvent(DriftKind.PASS_THROUGH, "s1", 0.1, 0.5, 80.0, breach_count=2)
        assert m.is_escalating(ev) is False

    def test_escalating_at_threshold(self):
        m = _monitor(breaches_to_escalate=3)
        ev = DriftEvent(DriftKind.PASS_THROUGH, "s1", 0.1, 0.5, 80.0, breach_count=3)
        assert m.is_escalating(ev) is True

    def test_escalating_above_threshold(self):
        m = _monitor(breaches_to_escalate=2)
        ev = DriftEvent(DriftKind.PASS_THROUGH, "s1", 0.1, 0.5, 80.0, breach_count=5)
        assert m.is_escalating(ev) is True

    def test_escalation_check_on_consecutive_real_checks(self):
        m = _monitor(passthrough_dev_pct=10.0, breaches_to_escalate=2)
        ev1 = m.check_passthrough("s1", observed=0.0, expected=1.0)
        ev2 = m.check_passthrough("s1", observed=0.0, expected=1.0)
        assert m.is_escalating(ev1) is False
        assert m.is_escalating(ev2) is True


# ── reset / active_alerts ────────────────────────────────────────────────────


class TestReset:
    def test_reset_clears_breach_count(self):
        m = _monitor(passthrough_dev_pct=10.0)
        m.check_passthrough("s1", observed=0.0, expected=1.0)
        m.reset("s1", DriftKind.PASS_THROUGH)
        ev = m.check_passthrough("s1", observed=0.0, expected=1.0)
        assert ev.breach_count == 1  # restarted from 0

    def test_reset_unknown_stage_is_silent(self):
        m = _monitor()
        m.reset("nonexistent_stage", DriftKind.BUDGET_BURN)  # must not raise


class TestActiveAlerts:
    def test_empty_when_no_breaches(self):
        m = _monitor()
        assert m.active_alerts() == {}

    def test_breaching_stage_appears_in_active_alerts(self):
        m = _monitor(passthrough_dev_pct=10.0)
        m.check_passthrough("s1", observed=0.0, expected=1.0)
        alerts = m.active_alerts()
        assert "s1" in alerts
        assert DriftKind.PASS_THROUGH.value in alerts["s1"]

    def test_alert_cleared_when_back_in_bounds(self):
        m = _monitor(passthrough_dev_pct=10.0)
        m.check_passthrough("s1", observed=0.0, expected=1.0)  # breach
        m.check_passthrough("s1", observed=0.99, expected=1.0)  # resolved
        assert m.active_alerts() == {}

    def test_multiple_stages_and_kinds(self):
        m = _monitor(passthrough_dev_pct=10.0, burn_dev_pct=10.0)
        m.check_passthrough("s1", observed=0.0, expected=1.0)
        m.check_budget("s2", spent=5.0, expected=1.0)
        alerts = m.active_alerts()
        assert "s1" in alerts
        assert "s2" in alerts

    def test_multiple_kinds_for_same_stage(self):
        m = _monitor(passthrough_dev_pct=10.0, burn_dev_pct=10.0)
        m.check_passthrough("s1", observed=0.0, expected=1.0)
        m.check_budget("s1", spent=5.0, expected=1.0)
        alerts = m.active_alerts()
        assert len(alerts["s1"]) == 2
