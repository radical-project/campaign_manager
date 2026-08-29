"""
ArtifactManifest — lightweight provenance record for campaign outputs.

Manifests travel by value (as JSON-serializable dicts) while large payloads
stay site-local on the compute endpoint.  Each manifest records where an
artifact lives, who created it, and its lineage.

Fields
------
artifact_id   Unique identifier (e.g. f"artifact_{replica_id}").
version       Schema version — increment when fields are added or removed.
created_by    Replica ID that produced this artifact.
parent_ids    Artifact IDs this artifact was derived from (empty for roots).
endpoint_id   Orbit endpoint identifier where the artifact files reside.
path          Absolute path to the artifact directory on *endpoint_id*.
sha256        Optional filename→hash mapping; None until runtime support lands.
metadata      Arbitrary key-value extras (score, num_sims, group, timestamp).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ArtifactManifest:
    """Provenance record for a campaign artifact stored on a remote endpoint.

    Instantiate in the workflow that produces the artifact; call validate()
    before passing to record_manifest() or storing in a ClassVar FIFO.
    Use to_dict() / from_dict() for JSON serialization.
    """

    artifact_id: str
    created_by: str
    endpoint_id: str
    path: str
    version: int = 1
    parent_ids: list[str] = field(default_factory=list)
    sha256: Optional[dict[str, str]] = None
    metadata: dict = field(default_factory=dict)

    def validate(self) -> None:
        """Raise ValueError if any required field is missing or empty."""
        for attr in ("artifact_id", "created_by", "endpoint_id", "path"):
            if not getattr(self, attr):
                raise ValueError(f"ArtifactManifest.{attr} must be non-empty")
        if self.sha256 is not None:
            for fname, digest in self.sha256.items():
                if len(digest) != 64:
                    raise ValueError(
                        f"ArtifactManifest.sha256[{fname!r}]: "
                        f"expected 64-char hex string, got {len(digest)} chars"
                    )

    def to_dict(self) -> dict:
        return {
            "artifact_id": self.artifact_id,
            "version": self.version,
            "created_by": self.created_by,
            "parent_ids": list(self.parent_ids),
            "endpoint_id": self.endpoint_id,
            "path": self.path,
            "sha256": self.sha256,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, d: dict) -> ArtifactManifest:
        """Construct from a dict, silently dropping unknown keys (forward-compat)."""
        known = {
            "artifact_id", "version", "created_by", "parent_ids",
            "endpoint_id", "path", "sha256", "metadata",
        }
        return cls(**{k: v for k, v in d.items() if k in known})
