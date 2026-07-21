"""Unit tests for src.campaign.candidate_log — StageResult, CandidateHistory, and CandidateLog."""

import pytest

from src.campaign.candidate_log import CandidateHistory, CandidateLog, StageResult


# ---------------------------------------------------------------------------
# StageResult
# ---------------------------------------------------------------------------


class TestStageResult:
    def test_required_fields_stored(self):
        r = StageResult(stage_id="s1", score=0.7)
        assert r.stage_id == "s1"
        assert r.score == pytest.approx(0.7)

    def test_surrogate_pred_defaults_to_zero(self):
        r = StageResult(stage_id="s1", score=0.5)
        assert r.surrogate_pred == pytest.approx(0.0)

    def test_surrogate_unc_defaults_to_zero(self):
        r = StageResult(stage_id="s1", score=0.5)
        assert r.surrogate_unc == pytest.approx(0.0)

    def test_scaffold_class_defaults_to_empty_string(self):
        r = StageResult(stage_id="s1", score=0.5)
        assert r.scaffold_class == ""

    def test_decision_defaults_to_empty_string(self):
        r = StageResult(stage_id="s1", score=0.5)
        assert r.decision == ""

    def test_explicit_optional_fields_stored(self):
        r = StageResult(
            stage_id="s2",
            score=0.9,
            surrogate_pred=0.8,
            surrogate_unc=0.1,
            scaffold_class="typeA",
            decision="RUN",
        )
        assert r.surrogate_pred == pytest.approx(0.8)
        assert r.surrogate_unc == pytest.approx(0.1)
        assert r.scaffold_class == "typeA"
        assert r.decision == "RUN"

    def test_timestamp_is_set(self):
        r = StageResult(stage_id="s1", score=0.5)
        assert r.timestamp > 0.0


# ---------------------------------------------------------------------------
# CandidateHistory
# ---------------------------------------------------------------------------


class TestCandidateHistory:
    def test_latest_score_empty_results_returns_zero(self):
        h = CandidateHistory(candidate_id="c1")
        assert h.latest_score == pytest.approx(0.0)

    def test_latest_score_returns_last_result(self):
        h = CandidateHistory(candidate_id="c1")
        h.results.append(StageResult(stage_id="s1", score=0.3))
        h.results.append(StageResult(stage_id="s2", score=0.7))
        assert h.latest_score == pytest.approx(0.7)

    def test_latest_surrogate_pred_empty_returns_zero(self):
        h = CandidateHistory(candidate_id="c1")
        assert h.latest_surrogate_pred == pytest.approx(0.0)

    def test_latest_surrogate_pred_returns_last_value(self):
        h = CandidateHistory(candidate_id="c1")
        h.results.append(StageResult(stage_id="s1", score=0.5, surrogate_pred=0.4))
        h.results.append(StageResult(stage_id="s2", score=0.6, surrogate_pred=0.9))
        assert h.latest_surrogate_pred == pytest.approx(0.9)

    def test_latest_surrogate_unc_empty_returns_zero(self):
        h = CandidateHistory(candidate_id="c1")
        assert h.latest_surrogate_unc == pytest.approx(0.0)

    def test_latest_surrogate_unc_returns_last_value(self):
        h = CandidateHistory(candidate_id="c1")
        h.results.append(StageResult(stage_id="s1", score=0.5, surrogate_unc=0.2))
        h.results.append(StageResult(stage_id="s2", score=0.6, surrogate_unc=0.05))
        assert h.latest_surrogate_unc == pytest.approx(0.05)

    def test_score_at_returns_none_when_no_results(self):
        h = CandidateHistory(candidate_id="c1")
        assert h.score_at("s1") is None

    def test_score_at_returns_none_when_stage_not_present(self):
        h = CandidateHistory(candidate_id="c1")
        h.results.append(StageResult(stage_id="s1", score=0.5))
        assert h.score_at("s_other") is None

    def test_score_at_returns_score_when_found(self):
        h = CandidateHistory(candidate_id="c1")
        h.results.append(StageResult(stage_id="s1", score=0.42))
        assert h.score_at("s1") == pytest.approx(0.42)

    def test_score_at_returns_most_recent_for_duplicate_stage(self):
        h = CandidateHistory(candidate_id="c1")
        h.results.append(StageResult(stage_id="s1", score=0.3))
        h.results.append(StageResult(stage_id="s1", score=0.8))
        assert h.score_at("s1") == pytest.approx(0.8)

    def test_score_at_works_across_multiple_stages(self):
        h = CandidateHistory(candidate_id="c1")
        h.results.append(StageResult(stage_id="s1", score=0.1))
        h.results.append(StageResult(stage_id="s2", score=0.5))
        h.results.append(StageResult(stage_id="s3", score=0.9))
        assert h.score_at("s1") == pytest.approx(0.1)
        assert h.score_at("s2") == pytest.approx(0.5)
        assert h.score_at("s3") == pytest.approx(0.9)


# ---------------------------------------------------------------------------
# CandidateLog
# ---------------------------------------------------------------------------


class TestCandidateLogRegister:
    def test_register_creates_history_with_correct_fields(self):
        log = CandidateLog()
        h = log.register("c1", scaffold_class="typeA")
        assert h.candidate_id == "c1"
        assert h.scaffold_class == "typeA"

    def test_register_is_idempotent(self):
        log = CandidateLog()
        h1 = log.register("c1", scaffold_class="typeA")
        h1.results.append(StageResult(stage_id="s1", score=0.5))
        h2 = log.register("c1", scaffold_class="typeB")
        assert h1 is h2
        assert len(h2.results) == 1  # not reset by second call

    def test_register_stores_explicit_enqueue_time(self):
        log = CandidateLog()
        t = 1_000_000.0
        h = log.register("c1", enqueue_time=t)
        assert h.enqueue_time == pytest.approx(t)


class TestCandidateLogGet:
    def test_get_returns_none_for_unknown_candidate(self):
        log = CandidateLog()
        assert log.get("unknown") is None

    def test_get_returns_history_for_known_candidate(self):
        log = CandidateLog()
        log.register("c1")
        h = log.get("c1")
        assert h is not None
        assert h.candidate_id == "c1"


class TestCandidateLogRecord:
    def test_record_auto_registers_unknown_candidate(self):
        log = CandidateLog()
        log.record("c_new", stage_id="s1", score=0.5)
        assert log.get("c_new") is not None

    def test_record_appends_stage_result(self):
        log = CandidateLog()
        log.record("c1", stage_id="s1", score=0.5)
        h = log.get("c1")
        assert len(h.results) == 1
        assert h.results[0].stage_id == "s1"
        assert h.results[0].score == pytest.approx(0.5)

    def test_record_updates_stage_scores(self):
        log = CandidateLog()
        log.record("c1", stage_id="s1", score=0.6)
        assert 0.6 in log._stage_scores["s1"]

    def test_record_returns_stage_result(self):
        log = CandidateLog()
        r = log.record("c1", stage_id="s1", score=0.7)
        assert isinstance(r, StageResult)
        assert r.score == pytest.approx(0.7)

    def test_record_stores_correct_optional_fields(self):
        log = CandidateLog()
        r = log.record(
            "c1",
            stage_id="s1",
            score=0.5,
            surrogate_pred=0.4,
            surrogate_unc=0.1,
            scaffold_class="typeB",
            decision="DISCARD",
        )
        assert r.surrogate_pred == pytest.approx(0.4)
        assert r.surrogate_unc == pytest.approx(0.1)
        assert r.scaffold_class == "typeB"
        assert r.decision == "DISCARD"

    def test_record_twice_appends_not_replaces(self):
        log = CandidateLog()
        log.record("c1", stage_id="s1", score=0.3)
        log.record("c1", stage_id="s1", score=0.8)
        h = log.get("c1")
        assert len(h.results) == 2
        scores = log._stage_scores["s1"]
        assert len(scores) == 2
        assert pytest.approx(0.3) in scores
        assert pytest.approx(0.8) in scores


class TestCandidateLogThresholdCutoff:
    def test_returns_neg_inf_when_no_scores(self):
        log = CandidateLog()
        assert log.threshold_cutoff("s_unknown", top_fraction=0.5) == float("-inf")

    def test_returns_neg_inf_with_exactly_one_score(self):
        log = CandidateLog()
        log.record("c1", stage_id="s1", score=0.9)
        assert log.threshold_cutoff("s1", top_fraction=0.5) == float("-inf")

    def test_correct_quantile_with_two_or_more_scores(self):
        log = CandidateLog()
        for i, score in enumerate([0.1, 0.3, 0.5, 0.7, 0.9]):
            log.record(f"c{i}", stage_id="s1", score=score)
        cutoff = log.threshold_cutoff("s1", top_fraction=0.4)
        # top 40 % → quantile at 0.60 of [0.1, 0.3, 0.5, 0.7, 0.9] = 0.58
        assert cutoff == pytest.approx(0.58, abs=1e-6)

    def test_top_fraction_one_returns_min_score(self):
        log = CandidateLog()
        for i, score in enumerate([0.2, 0.5, 0.8]):
            log.record(f"c{i}", stage_id="s1", score=score)
        cutoff = log.threshold_cutoff("s1", top_fraction=1.0)
        assert cutoff == pytest.approx(0.2)

    def test_top_fraction_zero_returns_max_score(self):
        log = CandidateLog()
        for i, score in enumerate([0.2, 0.5, 0.8]):
            log.record(f"c{i}", stage_id="s1", score=score)
        cutoff = log.threshold_cutoff("s1", top_fraction=0.0)
        assert cutoff == pytest.approx(0.8)

    def test_top_fraction_half_returns_median(self):
        log = CandidateLog()
        for i, score in enumerate([0.2, 0.4, 0.6, 0.8]):
            log.record(f"c{i}", stage_id="s1", score=score)
        cutoff = log.threshold_cutoff("s1", top_fraction=0.5)
        assert cutoff == pytest.approx(0.5)


class TestCandidateLogPassesThreshold:
    def test_always_true_when_top_fraction_ge_one(self):
        log = CandidateLog()
        log.record("c1", stage_id="s1", score=0.0)
        assert log.passes_threshold("c1", "s1", top_fraction=1.0) is True
        assert log.passes_threshold("c1", "s1", top_fraction=2.0) is True

    def test_true_for_unknown_candidate(self):
        log = CandidateLog()
        log.record("c1", stage_id="s1", score=0.9)
        log.record("c2", stage_id="s1", score=0.1)
        assert log.passes_threshold("c_unknown", "s1", top_fraction=0.5) is True

    def test_true_when_candidate_has_no_score_at_stage(self):
        log = CandidateLog()
        log.register("c1")
        log.record("c2", stage_id="s1", score=0.9)
        log.record("c3", stage_id="s1", score=0.1)
        assert log.passes_threshold("c1", "s1", top_fraction=0.5) is True

    def test_true_when_only_one_score_in_log(self):
        log = CandidateLog()
        log.record("c1", stage_id="s1", score=0.1)
        assert log.passes_threshold("c1", "s1", top_fraction=0.5) is True

    def test_passes_when_score_above_cutoff(self):
        log = CandidateLog()
        log.record("c1", stage_id="s1", score=0.9)
        log.record("c2", stage_id="s1", score=0.1)
        assert log.passes_threshold("c1", "s1", top_fraction=0.5) is True

    def test_fails_when_score_below_cutoff(self):
        log = CandidateLog()
        log.record("c1", stage_id="s1", score=0.9)
        log.record("c2", stage_id="s1", score=0.1)
        assert log.passes_threshold("c2", "s1", top_fraction=0.5) is False


class TestCandidateLogStageSummary:
    def test_returns_n_zero_for_unknown_stage(self):
        log = CandidateLog()
        assert log.stage_summary("s_unknown") == {"n": 0}

    def test_correct_stats_for_known_stage(self):
        log = CandidateLog()
        scores = [0.2, 0.4, 0.6, 0.8, 1.0]
        for i, s in enumerate(scores):
            log.record(f"c{i}", stage_id="s1", score=s)
        summary = log.stage_summary("s1")
        assert summary["n"] == 5
        assert summary["mean"] == pytest.approx(0.6, abs=1e-4)
        assert summary["p50"] == pytest.approx(0.6, abs=1e-4)
        assert summary["p90"] == pytest.approx(0.92, abs=1e-4)
        assert summary["max"] == pytest.approx(1.0, abs=1e-4)

    def test_single_score_all_stats_equal_that_score(self):
        log = CandidateLog()
        log.record("c1", stage_id="s1", score=0.75)
        summary = log.stage_summary("s1")
        assert summary["n"] == 1
        assert summary["mean"] == pytest.approx(0.75)
        assert summary["p50"] == pytest.approx(0.75)
        assert summary["p90"] == pytest.approx(0.75)
        assert summary["max"] == pytest.approx(0.75)
