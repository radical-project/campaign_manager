"""Unit tests for CampaignView.observe() score_p50 computation from shard_events.

score_p50 (and score_p90) must be:
  - None when fewer than 5 scored candidates have been dispatched for a stage.
  - The correct p50 (index n//2 of sorted scores) once ≥5 samples exist.

Tests use a fake CM whose state provides just enough structure for observe()
to reach the score_p50 computation path.
"""

import sys
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.campaign.adr.view import CampaignView
from src.campaign.metrics import ShardEvent

# ── Fake CM state ─────────────────────────────────────────────────────────────


@dataclass
class _FakeWorkflow:
    status: str = "running"
    priority: int = 0
    started_count: int = 0
    finished_replicas: int = 0
    concurrency_cap: int = 4
    replicas: int = 0
    dependencies: list = field(default_factory=list)
    ready: bool = True
    required_gpus: int = 0
    _consecutive_stalls: int = 0
    failed_replicas: int = 0
    workflow_config: dict = field(default_factory=dict)


@dataclass
class _FakeMetrics:
    shard_events: list = field(default_factory=list)

    def stage_wall_s(self, name: str) -> float:
        return 0.0


class _FakeState:
    def __init__(self, workflows: dict, shard_events: list):
        self.workflows = workflows
        self.sharders = {}
        self.bp = {}
        self.resources = MagicMock()
        self.metrics = _FakeMetrics(shard_events=shard_events)


class _FakeCM:
    def __init__(self, workflows: dict, shard_events: list):
        self.state = _FakeState(workflows, shard_events)


def _make_shard_event(group: str, scores: list) -> ShardEvent:
    return ShardEvent(
        group=group,
        shard_id=0,
        n=len(scores),
        scores=scores,
        priorities=[1.0] * len(scores),
        timestamp=0.0,
    )


def _view_with_events(stage_name: str, all_scores: list[float]) -> CampaignView:
    """Return a CampaignView whose metrics contain one ShardEvent per score."""
    wfs = {stage_name: _FakeWorkflow()}
    # Each call to trigger_dependent creates one ShardEvent with one score.
    events = [_make_shard_event(stage_name, [s]) for s in all_scores]
    cm = _FakeCM(wfs, events)
    return CampaignView(cm, terminal=None)


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestScoreP50FromShardEvents:
    def _observe_stage(self, stage: str, scores: list[float]) -> dict:
        view = _view_with_events(stage, scores)
        return view.observe()["stages"][stage]

    def test_none_when_zero_samples(self):
        stage = self._observe_stage("ddsim_a", [])
        assert stage["score_p50"] is None
        assert stage["score_p90"] is None

    def test_none_when_four_samples(self):
        stage = self._observe_stage("ddsim_a", [0.1, 0.2, 0.3, 0.4])
        assert stage["score_p50"] is None

    def test_non_none_at_exactly_five_samples(self):
        stage = self._observe_stage("ddsim_a", [0.1, 0.2, 0.3, 0.4, 0.5])
        assert stage["score_p50"] is not None

    def test_p50_is_median_of_five(self):
        # sorted: [0.1, 0.3, 0.5, 0.7, 0.9] — index 2 = 0.5
        stage = self._observe_stage("ddsim_a", [0.9, 0.1, 0.5, 0.3, 0.7])
        assert stage["score_p50"] == pytest.approx(0.5)

    def test_p50_correct_for_ten_samples(self):
        # sorted: [0.1..1.0] step 0.1 — index 5 = 0.6
        scores = [i / 10 for i in range(1, 11)]
        stage = self._observe_stage("analysis", scores)
        assert stage["score_p50"] == pytest.approx(0.6)

    def test_p90_correct_for_ten_samples(self):
        # sorted ten-sample list — index 9 = 1.0
        scores = [i / 10 for i in range(1, 11)]
        stage = self._observe_stage("analysis", scores)
        assert stage["score_p90"] == pytest.approx(1.0)

    def test_only_last_50_shard_events_used(self):
        """shard_events[-50:] limits the window; older events must be ignored."""
        # 60 events with score 0.1, then 50 events with score 0.9.
        # If all 110 are used, p50 would be 0.9 (half of events are 0.9).
        # If only last 50 are used, p50 must be exactly 0.9.
        old_scores = [0.1] * 60
        new_scores = [0.9] * 50
        all_events = (
            [_make_shard_event("ddsim_a", [s]) for s in old_scores]
            + [_make_shard_event("ddsim_a", [s]) for s in new_scores]
        )
        wfs = {"ddsim_a": _FakeWorkflow()}
        cm = _FakeCM(wfs, all_events)
        view = CampaignView(cm, terminal=None)
        stage = view.observe()["stages"]["ddsim_a"]
        assert stage["score_p50"] == pytest.approx(0.9)

    def test_scores_isolated_per_stage(self):
        """score_p50 for one stage must not be contaminated by another stage's events."""
        wfs = {"ddsim_a": _FakeWorkflow(), "ddsim_b": _FakeWorkflow()}
        events = (
            [_make_shard_event("ddsim_a", [0.9]) for _ in range(5)]
            + [_make_shard_event("ddsim_b", [0.1]) for _ in range(5)]
        )
        cm = _FakeCM(wfs, events)
        view = CampaignView(cm, terminal=None)
        stages = view.observe()["stages"]
        assert stages["ddsim_a"]["score_p50"] == pytest.approx(0.9)
        assert stages["ddsim_b"]["score_p50"] == pytest.approx(0.1)
