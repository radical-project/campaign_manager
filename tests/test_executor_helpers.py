"""Tests for executor module helpers (Pattern: _campaign_complete pure function)."""

from unittest.mock import MagicMock

from src.campaign.executor import _campaign_complete


def _group(replicas=1, running=0, finished=None):
    """Minimal _WorkflowInfo-like stub."""
    g = MagicMock()
    g.replicas = replicas
    g.running_count = running
    g.finished_replicas = finished if finished is not None else replicas
    return g


def _sharder(buffered=0):
    s = MagicMock()
    s.buffered = buffered
    return s


class TestCampaignComplete:
    def test_empty_groups_returns_false(self):
        assert _campaign_complete({}, {}) is False

    def test_all_replicas_zero_returns_false(self):
        """Groups exist but none have been activated (replicas == 0)."""
        groups = {"a": _group(replicas=0, running=0, finished=0)}
        assert _campaign_complete(groups, {}) is False

    def test_running_replica_blocks_completion(self):
        g = _group(replicas=3, running=1, finished=2)
        assert _campaign_complete({"a": g}, {}) is False

    def test_unfinished_replica_blocks_completion(self):
        g = _group(replicas=5, running=0, finished=3)
        assert _campaign_complete({"a": g}, {}) is False

    def test_all_done_no_sharders_returns_true(self):
        g = _group(replicas=4, running=0, finished=4)
        assert _campaign_complete({"a": g}, {}) is True

    def test_sharder_with_buffered_items_blocks(self):
        g = _group(replicas=2, running=0, finished=2)
        s = _sharder(buffered=3)
        assert _campaign_complete({"a": g}, {"s": s}) is False

    def test_sharder_empty_does_not_block(self):
        g = _group(replicas=2, running=0, finished=2)
        s = _sharder(buffered=0)
        assert _campaign_complete({"a": g}, {"s": s}) is True

    def test_unactivated_group_skipped(self):
        """Groups with replicas == 0 are skipped; only activated groups count."""
        active = _group(replicas=2, running=0, finished=2)
        pending = _group(replicas=0, running=0, finished=0)
        assert _campaign_complete({"active": active, "pending": pending}, {}) is True

    def test_multiple_groups_all_must_finish(self):
        a = _group(replicas=2, running=0, finished=2)
        b = _group(replicas=3, running=1, finished=2)  # still running
        assert _campaign_complete({"a": a, "b": b}, {}) is False

    def test_multiple_groups_all_done(self):
        a = _group(replicas=2, running=0, finished=2)
        b = _group(replicas=3, running=0, finished=3)
        assert _campaign_complete({"a": a, "b": b}, {}) is True
