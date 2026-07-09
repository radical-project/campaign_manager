"""Unit tests for src.campaign.replanning — the drain → replan → resume handshake."""

import pytest

from src.campaign import ReplanningController, ReplanningState
from src.campaign.monitor import DriftEvent, DriftKind

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _drift(kind=DriftKind.BUDGET_LOCKED, stage_id="s2"):
    return DriftEvent(kind=kind, stage_id=stage_id, observed=1.5, expected=1.0, deviation_pct=50.0)


async def test_log_only_policy_takes_no_action():
    c = ReplanningController(plan_id="p", plan_version=1)
    result = await c.on_drift(_drift(), policy="log_only")
    assert result is None
    assert c.state is ReplanningState.NORMAL


async def test_drain_timeout_emits_replan_request():
    seen = []

    async def sink(req):
        seen.append(req)

    c = ReplanningController(plan_id="p", plan_version=3, request_sink=sink, drain_timeout_s=0.05)
    result = await c.on_drift(_drift())

    # No response_source provided → controller parks in AWAITING_PLAN.
    assert result is None
    assert c.state is ReplanningState.AWAITING_PLAN
    assert c.is_paused() is True
    assert len(seen) == 1
    assert seen[0].triggering_kind == DriftKind.BUDGET_LOCKED.value
    assert seen[0].triggering_stage_id == "s2"
    assert seen[0].plan_id == "p"
    assert seen[0].plan_version == 3


async def test_drift_ignored_while_already_handshaking():
    c = ReplanningController(plan_id="p", plan_version=1, drain_timeout_s=0.05)
    await c.on_drift(_drift())  # → AWAITING_PLAN
    assert c.state is ReplanningState.AWAITING_PLAN
    # A second drift while not NORMAL must be ignored (no exception, no change).
    result = await c.on_drift(_drift(kind=DriftKind.BUDGET_BURN))
    assert result is None
    assert c.state is ReplanningState.AWAITING_PLAN
