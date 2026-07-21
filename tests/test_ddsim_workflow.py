"""Unit tests for DdSimWorkflow.on_replica_done() — candidate-aware trigger path.

Covers the fix in ddsim_workflow.py that passes candidate_id, score, and
source_stage to _trigger_dependent so the CM sharder receives real candidate
scores for score_p50 telemetry (used by Phase1Operator).
"""

import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

# Make the campaign module importable without running the full HPC stack.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(
    0,
    str(Path(__file__).resolve().parent.parent / "campaigns" / "ddsim_campaign"),
)

from ddsim_workflow import AnalysisWorkflow, DdSimWorkflow  # noqa: E402

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def reset_class_state():
    """Reset DdSimWorkflow class-level state between tests."""
    DdSimWorkflow.reset_state()
    AnalysisWorkflow.reset_state()
    yield
    DdSimWorkflow.reset_state()
    AnalysisWorkflow.reset_state()


def _make_workflow(group: str = "ddsim_a", score_mean: float = 0.5, score_noise: float = 0.0):
    """Return a DdSimWorkflow instance wired with a mock CM."""
    cm_mock = AsyncMock()
    wf = DdSimWorkflow(_cm=cm_mock, _group_name=group)
    wf.config = {
        "score_mean": score_mean,
        "score_noise": score_noise,
        "trigger_analysis": "analysis",
    }
    return wf, cm_mock


class TestOnReplicaDonePassesCandidateArgs:
    async def test_candidate_id_forwarded(self):
        """`candidate_id` must be the replica_id passed into on_replica_done."""
        wf, cm = _make_workflow()
        await wf.on_replica_done("ddsim_a_7", cm, "done")
        _, kwargs = cm.trigger_dependent.call_args
        assert kwargs["candidate_id"] == "ddsim_a_7"

    async def test_source_stage_is_group_name(self):
        """`source_stage` must be the workflow's _group_name, not hardcoded."""
        wf_a, cm_a = _make_workflow("ddsim_a")
        await wf_a.on_replica_done("ddsim_a_0", cm_a, "done")
        assert cm_a.trigger_dependent.call_args[1]["source_stage"] == "ddsim_a"

        wf_b, cm_b = _make_workflow("ddsim_b")
        await wf_b.on_replica_done("ddsim_b_0", cm_b, "done")
        assert cm_b.trigger_dependent.call_args[1]["source_stage"] == "ddsim_b"

    async def test_score_is_float_and_non_negative(self):
        """`score` passed to the CM must be a non-negative float."""
        wf, cm = _make_workflow(score_mean=0.7, score_noise=0.0)
        await wf.on_replica_done("ddsim_a_0", cm, "done")
        score = cm.trigger_dependent.call_args[1]["score"]
        assert isinstance(score, float)
        assert score >= 0.0

    async def test_score_matches_deterministic_gauss(self):
        """With noise=0, score == abs(gauss(mean, 0)) == mean."""
        wf, cm = _make_workflow(score_mean=0.65, score_noise=0.0)
        await wf.on_replica_done("ddsim_a_0", cm, "done")
        score = cm.trigger_dependent.call_args[1]["score"]
        assert abs(score - 0.65) < 1e-9

    async def test_trigger_stage_from_config(self):
        """The first positional arg to cm.trigger_dependent must be the trigger group."""
        wf, cm = _make_workflow()
        await wf.on_replica_done("ddsim_a_0", cm, "done")
        stage_arg = cm.trigger_dependent.call_args[0][0]
        assert stage_arg == "analysis"

    async def test_failed_state_is_noop(self):
        """on_replica_done must skip trigger entirely when final_state != 'done'."""
        wf, cm = _make_workflow()
        await wf.on_replica_done("ddsim_a_0", cm, "failed")
        cm.trigger_dependent.assert_not_called()

    async def test_score_stashed_in_class_scores(self):
        """Score must be appended to DdSimWorkflow._scores[group] for AnalysisWorkflow."""
        wf, cm = _make_workflow("ddsim_a", score_mean=0.8, score_noise=0.0)
        await wf.on_replica_done("ddsim_a_0", cm, "done")
        assert len(DdSimWorkflow._scores.get("ddsim_a", [])) == 1
        assert abs(DdSimWorkflow._scores["ddsim_a"][0] - 0.8) < 1e-9

    async def test_n_sim_incremented(self):
        """DdSimWorkflow._n_sim[group] must increment on each completed replica."""
        wf, cm = _make_workflow("ddsim_a")
        await wf.on_replica_done("ddsim_a_0", cm, "done")
        await wf.on_replica_done("ddsim_a_1", cm, "done")
        assert DdSimWorkflow._n_sim["ddsim_a"] == 2
