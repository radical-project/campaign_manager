"""Tests for Pattern 2 — load_checkpoint_full version check + sidecar loading.

Requires radical.adr; the whole module is skipped when it is not installed.
"""

import json
import logging
from unittest.mock import MagicMock, patch

import pytest

radical_adr = pytest.importorskip("radical.adr")

from src.campaign.adr import CampaignOperator  # noqa: E402 — guarded above

# ---------------------------------------------------------------------------
# Minimal operator for tests (max_cycles=1 satisfies stopping condition)
# ---------------------------------------------------------------------------


class _MinOp(CampaignOperator):
    """Subclass with a trivial stopping condition so __init__ doesn't raise."""

    def __init__(self):
        fake_view = MagicMock()
        fake_view.observe.return_value = {
            "stages": {}, "terminal": [], "cycle": 0, "hits": 0
        }
        super().__init__(fake_view, engine=None, max_cycles=1)


# ---------------------------------------------------------------------------
# TestLoadCheckpointFull
# ---------------------------------------------------------------------------


class TestLoadCheckpointFull:
    def test_matching_version_no_warning(self, tmp_path, caplog):
        """Version == _CHECKPOINT_VERSION → no warning logged."""
        ckpt = tmp_path / "ckpt.json"
        ckpt.write_text(json.dumps({"version": 1}))
        op = _MinOp()
        with patch.object(op, "load_checkpoint"):
            with caplog.at_level(logging.WARNING, logger="src.campaign.adr.operator"):
                op.load_checkpoint_full(str(ckpt))
        assert "mismatch" not in caplog.text.lower()

    def test_version_mismatch_logs_warning(self, tmp_path, caplog):
        """Version != _CHECKPOINT_VERSION → warning contains 'mismatch'."""
        ckpt = tmp_path / "ckpt.json"
        ckpt.write_text(json.dumps({"version": 999}))
        op = _MinOp()
        with patch.object(op, "load_checkpoint"):
            with caplog.at_level(logging.WARNING, logger="src.campaign.adr.operator"):
                op.load_checkpoint_full(str(ckpt))
        assert "mismatch" in caplog.text.lower()

    def test_missing_version_treated_as_zero(self, tmp_path, caplog):
        """No 'version' key in checkpoint → defaults to 0 → triggers mismatch warning."""
        ckpt = tmp_path / "ckpt.json"
        ckpt.write_text(json.dumps({}))
        op = _MinOp()
        with patch.object(op, "load_checkpoint"):
            with caplog.at_level(logging.WARNING, logger="src.campaign.adr.operator"):
                op.load_checkpoint_full(str(ckpt))
        assert "mismatch" in caplog.text.lower()

    def test_load_checkpoint_called(self, tmp_path):
        """load_checkpoint() is always called regardless of version."""
        ckpt = tmp_path / "ckpt.json"
        ckpt.write_text(json.dumps({"version": 1}))
        op = _MinOp()
        with patch.object(op, "load_checkpoint") as mock_load:
            op.load_checkpoint_full(str(ckpt))
        mock_load.assert_called_once_with(str(ckpt))

    def test_sidecar_loads_extra_when_present(self, tmp_path):
        """.extra.json sidecar is loaded and passed to load_extra()."""
        ckpt = tmp_path / "ckpt.json"
        ckpt.write_text(json.dumps({"version": 1}))
        sidecar = tmp_path / "ckpt.extra.json"
        sidecar.write_text(json.dumps({"custom_state": 42}))
        op = _MinOp()
        received = {}

        def _load_extra(data):
            received.update(data)

        with patch.object(op, "load_checkpoint"):
            with patch.object(op, "load_extra", side_effect=_load_extra):
                op.load_checkpoint_full(str(ckpt))
        assert received == {"custom_state": 42}

    def test_no_sidecar_load_extra_not_called(self, tmp_path):
        """load_extra() is not called when no .extra.json sidecar exists."""
        ckpt = tmp_path / "ckpt.json"
        ckpt.write_text(json.dumps({"version": 1}))
        op = _MinOp()
        with patch.object(op, "load_checkpoint"):
            with patch.object(op, "load_extra") as mock_extra:
                op.load_checkpoint_full(str(ckpt))
        mock_extra.assert_not_called()

    def test_save_extra_returns_empty_dict_by_default(self):
        """Base save_extra() returns {} (subclasses override to add state)."""
        op = _MinOp()
        assert op.save_extra() == {}

    def test_non_json_checkpoint_warns_and_still_loads(self, tmp_path, caplog):
        """A binary / non-JSON checkpoint logs a warning but still calls load_checkpoint."""
        ckpt = tmp_path / "ckpt.bin"
        ckpt.write_bytes(b"\x80\x04\x95\x00\x00\x00\x00pickle\x00")  # random binary
        op = _MinOp()
        with patch.object(op, "load_checkpoint") as mock_load:
            with caplog.at_level(logging.WARNING, logger="src.campaign.adr.operator"):
                op.load_checkpoint_full(str(ckpt))
        assert "not json" in caplog.text.lower() or "version" in caplog.text.lower()
        mock_load.assert_called_once_with(str(ckpt))
