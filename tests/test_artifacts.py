"""Tests for ArtifactManifest (Pattern 7) and CampaignMetrics.record_manifest."""

import pytest

from src.campaign.artifacts import ArtifactManifest
from src.campaign.metrics import CampaignMetrics

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _valid_manifest(**overrides) -> ArtifactManifest:
    defaults = dict(
        artifact_id="artifact_search_0",
        created_by="search_0",
        endpoint_id="delta-gpu01",
        path="/lustre/projects/orbit/search_0/sim_output",
    )
    return ArtifactManifest(**{**defaults, **overrides})


# ---------------------------------------------------------------------------
# ArtifactManifest.validate()
# ---------------------------------------------------------------------------


class TestArtifactManifestValidate:
    def test_valid_passes(self):
        _valid_manifest().validate()

    @pytest.mark.parametrize("field", ["artifact_id", "created_by", "endpoint_id", "path"])
    def test_empty_required_field_raises(self, field):
        with pytest.raises(ValueError, match=field):
            _valid_manifest(**{field: ""}).validate()

    def test_sha256_valid_hex_passes(self):
        m = _valid_manifest(sha256={"sim_0.npz": "a" * 64})
        m.validate()

    def test_sha256_wrong_length_raises(self):
        m = _valid_manifest(sha256={"sim_0.npz": "abc"})
        with pytest.raises(ValueError, match="sha256"):
            m.validate()

    def test_sha256_none_passes(self):
        _valid_manifest(sha256=None).validate()


# ---------------------------------------------------------------------------
# ArtifactManifest.to_dict / from_dict round-trip
# ---------------------------------------------------------------------------


class TestArtifactManifestSerialization:
    def test_round_trip_preserves_all_fields(self):
        m = _valid_manifest(
            parent_ids=["artifact_root"],
            metadata={"score": 0.42, "num_sims": 20},
        )
        restored = ArtifactManifest.from_dict(m.to_dict())
        assert restored.artifact_id == m.artifact_id
        assert restored.created_by == m.created_by
        assert restored.endpoint_id == m.endpoint_id
        assert restored.path == m.path
        assert restored.parent_ids == ["artifact_root"]
        assert restored.metadata["score"] == pytest.approx(0.42)
        assert restored.version == 1

    def test_from_dict_drops_unknown_keys(self):
        d = _valid_manifest().to_dict()
        d["future_field"] = "value_from_newer_version"
        m = ArtifactManifest.from_dict(d)
        assert not hasattr(m, "future_field")

    def test_to_dict_sha256_none_serializes(self):
        d = _valid_manifest(sha256=None).to_dict()
        assert d["sha256"] is None

    def test_to_dict_parent_ids_is_copy(self):
        m = _valid_manifest(parent_ids=["x"])
        d = m.to_dict()
        d["parent_ids"].append("y")
        assert m.parent_ids == ["x"]

    def test_lineage_parent_ids_round_trip(self):
        parent = _valid_manifest()
        child = _valid_manifest(
            artifact_id="artifact_refine_0",
            created_by="refine_0",
            parent_ids=[parent.artifact_id],
        )
        restored = ArtifactManifest.from_dict(child.to_dict())
        assert restored.parent_ids == [parent.artifact_id]


# ---------------------------------------------------------------------------
# CampaignMetrics.record_manifest + to_dict persistence
# ---------------------------------------------------------------------------


class TestMetricsManifestRecording:
    def test_record_manifest_appends_to_manifest_events(self):
        metrics = CampaignMetrics()
        m = _valid_manifest()
        metrics.record_manifest(m)
        assert len(metrics.manifest_events) == 1
        assert metrics.manifest_events[0]["artifact_id"] == "artifact_search_0"

    def test_multiple_manifests_recorded_in_order(self):
        metrics = CampaignMetrics()
        for i in range(3):
            metrics.record_manifest(_valid_manifest(
                artifact_id=f"artifact_{i}",
                created_by=f"search_{i}",
            ))
        ids = [e["artifact_id"] for e in metrics.manifest_events]
        assert ids == ["artifact_0", "artifact_1", "artifact_2"]

    def test_manifest_events_in_to_dict(self):
        metrics = CampaignMetrics()
        metrics.record_manifest(_valid_manifest())
        d = metrics.to_dict()
        assert "manifest_events" in d
        assert len(d["manifest_events"]) == 1

    def test_recorded_at_field_present(self):
        metrics = CampaignMetrics()
        metrics.record_manifest(_valid_manifest())
        assert "_recorded_at" in metrics.manifest_events[0]
        assert metrics.manifest_events[0]["_recorded_at"] >= 0.0

    def test_empty_manifest_events_in_to_dict(self):
        metrics = CampaignMetrics()
        d = metrics.to_dict()
        assert d["manifest_events"] == []
