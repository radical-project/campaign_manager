"""Unit tests for the phased ADR operators (ddsim_operator_phased.py).

Covers without any HPC infrastructure:
  1. Phase1Operator.extract() — running-max best_score_p50
  2. Phase1Operator.criteria() — stops at n_ddsim_a threshold
  3. Phase2Operator.extract() — tracks ddsim_b_finished
  4. PhasedParentOperator.watch() — reads snapshot.objectives (not .observations)
  5. PhasedParentOperator.watch() — persists running max into state.artifacts
  6. launch_phase2() — cutoff = max(0.10, p50 * 0.85); set_score_cutoff called
  7. PhasedParentPolicy — all three lifecycle transitions + idle
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

pytest.importorskip("radical.adr")

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(
    0,
    str(Path(__file__).resolve().parent.parent / "campaigns" / "ddsim_campaign"),
)

from ddsim_operator_phased import (
    _STAGE1_ID as _PHASE1_ID,
)
from ddsim_operator_phased import (
    ParentOperator as PhasedParentOperator,
)
from ddsim_operator_phased import (
    ParentPolicy as PhasedParentPolicy,
)
from ddsim_operator_phased import (  # noqa: E402
    Stage1Operator as Phase1Operator,
)
from ddsim_operator_phased import (
    Stage2Operator as Phase2Operator,
)
from radical.adr.state import Snapshot

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def anyio_backend():
    return "asyncio"


# ── Helpers ──────────────────────────────────────────────────────────────────


def _snapshot(**kwargs) -> Snapshot:
    """Build a minimal Snapshot; any field can be overridden via kwargs."""
    defaults = dict(
        operator_id="test",
        cycle=0,
        timestamp=0.0,
        source="self",
        observations={},
        runtime={},
        objectives={},
        artifacts={},
        directives={},
    )
    defaults.update(kwargs)
    return Snapshot(**defaults)


class _FakeView:
    """Minimal CampaignViewProtocol implementation for operator construction.

    Pass keyword arguments to control what observe() returns per stage.
    This matters for extract() tests because CampaignOperator.extract()
    calls self.view.observe() — not snapshot.observations.
    """

    def __init__(
        self,
        ddsim_a_finished: int = 0,
        ddsim_b_finished: int = 0,
        analysis_score_p50: float | None = None,
    ):
        self._ddsim_a_finished = ddsim_a_finished
        self._ddsim_b_finished = ddsim_b_finished
        self._analysis_score_p50 = analysis_score_p50
        self.cutoff_calls: list = []
        self.trigger_calls: list = []

    def observe(self) -> dict:
        return {
            "cycle": 0,
            "terminal": [],
            "free_cpus": 0,
            "free_gpus": 0,
            "stages": {
                "ddsim_a":  _stage(finished=self._ddsim_a_finished),
                "ddsim_b":  _stage(finished=self._ddsim_b_finished),
                "analysis": _stage(finished=0, score_p50=self._analysis_score_p50),
            },
        }

    def set_priority(self, stage, priority): ...
    def set_batch_size(self, stage, size): ...

    def set_score_cutoff(self, stage: str, value: float) -> bool:
        self.cutoff_calls.append((stage, value))
        return True

    async def trigger(self, stage: str, replicas: int) -> int:
        self.trigger_calls.append((stage, replicas))
        return replicas

    async def reactivate(self) -> None:
        pass


def _stage(finished=0, score_p50=None):
    return {
        "status": "running",
        "priority": 0,
        "started": 0,
        "running": 0,
        "finished": finished,
        "cap": 4,
        "ready": True,
        "deps": [],
        "queue_depth": 0,
        "bp_state": "HOLD",
        "pending": 0,
        "starved": False,
        "is_source": True,
        "requires_gpu": False,
        "stalls": 0,
        "avg_duration_s": None,
        "score_p50": score_p50,
        "score_p90": None,
    }


def _parent_op(n_a=20, n_b=20):
    """Build a PhasedParentOperator with a fake view; engine=None."""
    return PhasedParentOperator(_FakeView(), engine=None, n_ddsim_a=n_a, n_ddsim_b=n_b)


# ── Phase1Operator ────────────────────────────────────────────────────────────


class TestPhase1OperatorExtract:
    """extract() reads stage data from view.observe(), not snapshot.observations."""

    def _snap(self):
        return _snapshot()

    def test_tracks_ddsim_a_finished(self):
        op = Phase1Operator(_FakeView(ddsim_a_finished=7), engine=None, n_ddsim_a=5)
        obs = op.extract(self._snap())
        assert obs["ddsim_a_finished"] == 7

    def test_best_score_p50_updates_when_higher(self):
        view = _FakeView(analysis_score_p50=0.6)
        op = Phase1Operator(view, engine=None, n_ddsim_a=5)
        op.extract(self._snap())
        assert op.best_score_p50 == pytest.approx(0.6)

        view._analysis_score_p50 = 0.8
        op.extract(self._snap())
        assert op.best_score_p50 == pytest.approx(0.8)

    def test_best_score_p50_does_not_decrease(self):
        view = _FakeView(analysis_score_p50=0.9)
        op = Phase1Operator(view, engine=None, n_ddsim_a=5)
        op.extract(self._snap())

        view._analysis_score_p50 = 0.3
        op.extract(self._snap())
        assert op.best_score_p50 == pytest.approx(0.9)

    def test_none_score_p50_is_ignored(self):
        op = Phase1Operator(_FakeView(analysis_score_p50=None), engine=None, n_ddsim_a=5)
        op.extract(self._snap())
        assert op.best_score_p50 == pytest.approx(0.0)

    def test_missing_analysis_stage_is_safe(self):
        """When analysis stage has no score_p50, extract must not raise."""
        op = Phase1Operator(_FakeView(ddsim_a_finished=3), engine=None, n_ddsim_a=5)
        obs = op.extract(self._snap())
        assert obs["ddsim_a_finished"] == 3
        assert op.best_score_p50 == pytest.approx(0.0)

    def test_criteria_threshold(self):
        op = Phase1Operator(_FakeView(), engine=None, n_ddsim_a=5)
        goals = op.criteria()
        assert goals.threshold == pytest.approx(4.5)  # n - 0.5


# ── Phase2Operator ────────────────────────────────────────────────────────────


class TestPhase2OperatorExtract:
    def _snap(self):
        return _snapshot()

    def test_tracks_ddsim_b_finished(self):
        op = Phase2Operator(_FakeView(ddsim_b_finished=3), engine=None, n_ddsim_b=10)
        obs = op.extract(self._snap())
        assert obs["ddsim_b_finished"] == 3

    def test_criteria_threshold(self):
        op = Phase2Operator(_FakeView(), engine=None, n_ddsim_b=20)
        goals = op.criteria()
        assert goals.threshold == pytest.approx(19.5)


# ── PhasedParentOperator.watch() ─────────────────────────────────────────────


class TestPhasedParentWatch:
    def test_reads_best_score_p50_from_objectives_not_observations(self):
        """watch() must update state.artifacts from snapshot.objectives, not .observations.

        Before the bug fix, watch() read from snapshot.observations — annotated
        objectives never land there, so best_score_p50 was never picked up.

        Note: obs["phase1_score"] is read from snapshot.artifacts (previous-cycle
        value), so the update to state.artifacts is only visible in the NEXT
        cycle's snapshot.  We therefore assert on state.artifacts directly.
        """
        op = _parent_op()
        key = f"child:{_PHASE1_ID}:obj:best_score_p50"

        # Value only in .observations — state.artifacts must NOT be updated.
        op.watch(_snapshot(observations={key: 0.9999}))
        assert op.state.artifacts.get("stage1_best_score", 0.0) == pytest.approx(0.0), (
            "watch() must NOT read from snapshot.observations"
        )

        # Value only in .objectives — state.artifacts MUST be updated.
        op.watch(_snapshot(objectives={key: 0.7341}))
        assert op.state.artifacts.get("stage1_best_score") == pytest.approx(0.7341), (
            "watch() must read from snapshot.objectives"
        )

    def test_running_max_persisted_to_artifacts(self):
        """watch() must persist the running max into state.artifacts, not just obs."""
        op = _parent_op()
        key = f"child:{_PHASE1_ID}:obj:best_score_p50"

        op.watch(_snapshot(objectives={key: 0.5}))
        assert op.state.artifacts.get("stage1_best_score") == pytest.approx(0.5)

        # Lower value — artifact must keep the max.
        op.watch(_snapshot(objectives={key: 0.3}, artifacts={"stage1_best_score": 0.5}))
        assert op.state.artifacts.get("stage1_best_score") == pytest.approx(0.5)

        # Higher value — artifact must update.
        op.watch(_snapshot(objectives={key: 0.8}, artifacts={"stage1_best_score": 0.5}))
        assert op.state.artifacts.get("stage1_best_score") == pytest.approx(0.8)

    def test_phase_status_from_runtime(self):
        """watch() must return phase statuses from snapshot.runtime via the stored uids."""
        op = _parent_op()
        snap = _snapshot(
            artifacts={"stage1_uid": "uid-p1", "stage2_uid": "uid-p2"},
            runtime={"uid-p1": "DONE", "uid-p2": "RUNNING"},
        )
        obs = op.watch(snap)
        assert obs["stage1_status"] == "DONE"
        assert obs["stage2_status"] == "RUNNING"

    def test_no_uid_returns_not_started(self):
        op = _parent_op()
        obs = op.watch(_snapshot())
        assert obs["stage1_status"] == "NOT_STARTED"
        assert obs["stage2_status"] == "NOT_STARTED"
        assert obs["stage1_uid"] is None
        assert obs["stage2_uid"] is None


# ── PhasedParentOperator.launch_phase2() ─────────────────────────────────────


class TestLaunchPhase2Cutoff:
    async def test_cutoff_is_p50_times_085(self):
        """Triage cutoff = max(0.10, p50 * 0.85), set on 'analysis'."""
        op = _parent_op(n_b=1)
        op.state.artifacts["stage1_best_score"] = 0.7341
        # Prevent _phase2.start() from hitting a real engine.
        op._stage2.start = MagicMock(return_value=MagicMock(id="fake-uid"))

        await op.launch_stage2()

        view = op.view
        assert len(view.cutoff_calls) == 1
        stage, cutoff = view.cutoff_calls[0]
        assert stage == "analysis"
        assert cutoff == pytest.approx(round(0.7341 * 0.85, 3))

    async def test_cutoff_clamped_to_010_when_p50_is_zero(self):
        """When Phase1 never reported a score, cutoff must floor at 0.10."""
        op = _parent_op(n_b=1)
        op.state.artifacts["stage1_best_score"] = 0.0
        op._stage2.start = MagicMock(return_value=MagicMock(id="fake-uid"))

        await op.launch_stage2()

        _, cutoff = op.view.cutoff_calls[0]
        assert cutoff == pytest.approx(0.10)

    async def test_cutoff_clamped_when_p50_very_low(self):
        """0.08 * 0.85 = 0.068 → clamped to 0.10."""
        op = _parent_op(n_b=1)
        op.state.artifacts["stage1_best_score"] = 0.08
        op._stage2.start = MagicMock(return_value=MagicMock(id="fake-uid"))

        await op.launch_stage2()

        _, cutoff = op.view.cutoff_calls[0]
        assert cutoff == pytest.approx(0.10)

    async def test_ddsim_b_replicas_triggered(self):
        """launch_phase2() must trigger n_ddsim_b replicas of 'ddsim_b'."""
        op = _parent_op(n_b=20)
        op.state.artifacts["stage1_best_score"] = 0.5
        op._stage2.start = MagicMock(return_value=MagicMock(id="fake-uid"))

        await op.launch_stage2()

        assert ("ddsim_b", 20) in op.view.trigger_calls


# ── PhasedParentPolicy ────────────────────────────────────────────────────────


def _obs(
    phase1_uid=None,
    phase2_uid=None,
    phase1_status="NOT_STARTED",
    phase2_status="NOT_STARTED",
    phase1_score=0.0,
    cycle=0,
):
    return {
        "stage1_uid":    phase1_uid,
        "stage2_uid":    phase2_uid,
        "stage1_status": phase1_status,
        "stage2_status": phase2_status,
        "stage1_score":  phase1_score,
        "cycle":         cycle,
    }


class TestPhasedParentPolicy:
    def _policy(self):
        op = _parent_op()
        return PhasedParentPolicy(op)

    def _action_names(self, decision):
        return [a.task_name for a in decision.actions]

    async def test_transition1_launch_phase1_when_no_uid(self):
        """cycle 0 with no phase1_uid → launch_phase1 action."""
        policy = self._policy()
        decision = await policy.run(_obs())
        assert "launch_stage1" in self._action_names(decision)
        assert not decision.stop

    async def test_transition1_fires_on_any_cycle_without_phase1_uid(self):
        """If phase1 was never launched (uid is None), always re-launch."""
        policy = self._policy()
        decision = await policy.run(_obs(cycle=5))
        assert "launch_stage1" in self._action_names(decision)

    async def test_transition2_launch_phase2_when_phase1_done(self):
        """phase1 DONE, no phase2_uid → launch_phase2 action."""
        policy = self._policy()
        decision = await policy.run(_obs(
            phase1_uid="uid-1",
            phase1_status="DONE",
            phase1_score=0.7341,
            cycle=7,
        ))
        assert "launch_stage2" in self._action_names(decision)
        assert not decision.stop

    async def test_transition2_does_not_fire_if_phase1_not_done(self):
        """phase1 still RUNNING — must not launch phase2."""
        policy = self._policy()
        decision = await policy.run(_obs(phase1_uid="uid-1", phase1_status="RUNNING"))
        assert "launch_stage2" not in self._action_names(decision)

    async def test_transition2_does_not_fire_if_phase2_already_launched(self):
        """phase1 DONE but phase2 already has a uid — must stay idle."""
        policy = self._policy()
        decision = await policy.run(_obs(
            phase1_uid="uid-1", phase1_status="DONE",
            phase2_uid="uid-2", phase2_status="RUNNING",
        ))
        assert "launch_stage2" not in self._action_names(decision)
        assert not decision.stop

    async def test_transition3_stop_when_phase2_done(self):
        """phase2 DONE → Decision(stop=True)."""
        policy = self._policy()
        decision = await policy.run(_obs(
            phase1_uid="uid-1", phase1_status="DONE",
            phase2_uid="uid-2", phase2_status="DONE",
            cycle=22,
        ))
        assert decision.stop is True

    async def test_idle_while_phase1_running(self):
        """phase1 launched but not done, no phase2 → idle (no actions, no stop)."""
        policy = self._policy()
        decision = await policy.run(_obs(
            phase1_uid="uid-1",
            phase1_status="RUNNING",
            cycle=3,
        ))
        assert self._action_names(decision) == []
        assert not decision.stop

    async def test_idle_while_phase2_running(self):
        """Both phases active, phase2 not yet done → idle."""
        policy = self._policy()
        decision = await policy.run(_obs(
            phase1_uid="uid-1", phase1_status="DONE",
            phase2_uid="uid-2", phase2_status="RUNNING",
            cycle=15,
        ))
        assert self._action_names(decision) == []
        assert not decision.stop


# ── Edge cases — cutoff boundary arithmetic ───────────────────────────────────


class TestLaunchPhase2CutoffEdgeCases:
    """Edge cases for the triage cutoff formula: cutoff = round(max(0.10, p50 * 0.85), 3).

    Boundary:  p50 * 0.85 < 0.10  →  clamp fires, cutoff = 0.10
               p50 * 0.85 >= 0.10 →  cutoff = round(p50 * 0.85, 3)
    Cross-over at p50 = 0.10/0.85 ≈ 0.1176.
    """

    async def _launch_with_p50(self, p50: float) -> float:
        op = _parent_op(n_b=1)
        op.state.artifacts["stage1_best_score"] = p50
        op._stage2.start = MagicMock(return_value=MagicMock(id="fake-uid"))
        await op.launch_stage2()
        _, cutoff = op.view.cutoff_calls[0]
        return cutoff

    async def test_clamp_fires_when_p50_is_0115(self):
        """0.115 * 0.85 = 0.09775 < 0.10 → clamp fires → cutoff = 0.10."""
        cutoff = await self._launch_with_p50(0.115)
        assert cutoff == pytest.approx(0.10)

    async def test_no_clamp_when_p50_is_012(self):
        """0.12 * 0.85 = 0.102 > 0.10 → no clamp → cutoff = 0.102."""
        cutoff = await self._launch_with_p50(0.12)
        assert cutoff == pytest.approx(0.102)

    async def test_cutoff_is_085_for_perfect_score(self):
        """p50 = 1.0 → max score, cutoff = 0.85 (85% of Phase 1 best)."""
        cutoff = await self._launch_with_p50(1.0)
        assert cutoff == pytest.approx(0.85)

    async def test_cutoff_is_rounded_to_three_decimals(self):
        """round() is applied — result has at most 3 decimal places."""
        # 0.7 * 0.85 = 0.595 → already 3 dp; pick a value that produces more.
        # 0.333 * 0.85 = 0.28305 → round to 0.283
        cutoff = await self._launch_with_p50(0.333)
        assert cutoff == pytest.approx(0.283)
        # Verify it's not 0.28305 (unrounded)
        assert abs(cutoff - 0.28305) > 1e-5


class TestPhase1Phase2CriteriaMinimum:
    """Goal thresholds with smallest meaningful replica counts."""

    def test_phase1_n1_threshold(self):
        """n_ddsim_a=1 → threshold = 0.5 (fires when ≥1 replica finishes)."""
        op = Phase1Operator(_FakeView(), engine=None, n_ddsim_a=1)
        assert op.criteria().threshold == pytest.approx(0.5)

    def test_phase2_n1_threshold(self):
        """n_ddsim_b=1 → threshold = 0.5 (fires when the single replica finishes)."""
        op = Phase2Operator(_FakeView(), engine=None, n_ddsim_b=1)
        assert op.criteria().threshold == pytest.approx(0.5)

    def test_phase1_n5_threshold(self):
        """n_ddsim_a=5 (minimum for score_p50) → threshold = 4.5."""
        op = Phase1Operator(_FakeView(), engine=None, n_ddsim_a=5)
        assert op.criteria().threshold == pytest.approx(4.5)

    def test_phase2_n5_threshold(self):
        """n_ddsim_b=5 → threshold = 4.5."""
        op = Phase2Operator(_FakeView(), engine=None, n_ddsim_b=5)
        assert op.criteria().threshold == pytest.approx(4.5)


# ── Negative paths — failure handling in policy ───────────────────────────────


class TestPhasedParentPolicyNegativePaths:
    """Policy must stop immediately on FAILED or CANCELED — not loop to max_cycles.

    Before the fix, FAILED/CANCELED statuses fell through to the idle branch
    and the parent ran until the 500-cycle safety net.
    """

    def _policy(self):
        return PhasedParentPolicy(_parent_op())

    async def test_stops_when_phase2_failed(self):
        """phase2_status == 'FAILED' → Decision(stop=True), not idle."""
        policy = self._policy()
        decision = await policy.run(_obs(
            phase1_uid="uid-1", phase1_status="DONE",
            phase2_uid="uid-2", phase2_status="FAILED",
            cycle=10,
        ))
        assert decision.stop is True

    async def test_stops_when_phase2_canceled(self):
        """phase2_status == 'CANCELED' → Decision(stop=True)."""
        policy = self._policy()
        decision = await policy.run(_obs(
            phase1_uid="uid-1", phase1_status="DONE",
            phase2_uid="uid-2", phase2_status="CANCELED",
            cycle=10,
        ))
        assert decision.stop is True

    async def test_stops_when_phase1_failed(self):
        """phase1 FAILED before phase2 is launched → Decision(stop=True)."""
        policy = self._policy()
        decision = await policy.run(_obs(
            phase1_uid="uid-1", phase1_status="FAILED",
            cycle=3,
        ))
        assert decision.stop is True

    async def test_stops_when_phase1_canceled(self):
        """phase1 CANCELED before phase2 is launched → Decision(stop=True)."""
        policy = self._policy()
        decision = await policy.run(_obs(
            phase1_uid="uid-1", phase1_status="CANCELED",
            cycle=3,
        ))
        assert decision.stop is True

    async def test_phase1_failed_after_phase2_launch_is_idle(self):
        """phase1 FAILED but phase2 already has a uid — falls to idle, not stop.

        This is an unusual ordering (phase1 DONE triggered phase2, then phase1
        somehow enters FAILED), but the guard condition 'phase2_uid is None'
        means we do NOT stop here — the parent waits for phase2 to resolve.
        """
        policy = self._policy()
        decision = await policy.run(_obs(
            phase1_uid="uid-1", phase1_status="FAILED",
            phase2_uid="uid-2", phase2_status="RUNNING",
            cycle=5,
        ))
        # Not stopped — phase2 is still running.
        assert not decision.stop
