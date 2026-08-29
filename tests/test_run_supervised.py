"""Tests for run_supervised / CampaignAbortedError (Pattern 3).

Isolated from test_adr_bridge.py because that file imports radical.adr at
module level, which is not installed in the test environment.  These tests
only depend on src.campaign.adr (our own code).
"""

import asyncio

import pytest

from src.campaign.adr.supervisor import CampaignAbortedError, run_supervised

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _StoppableFakeCM:
    """Minimal CM stub: stop() unblocks wait()."""

    def __init__(self):
        self._stop_event = asyncio.Event()
        self.stop_called = False

    async def stop(self):
        self.stop_called = True
        self._stop_event.set()

    async def wait(self):
        await self._stop_event.wait()

    def metrics(self):
        return None


class _ScriptedView:
    """View whose observe() returns pre-specified per-tick stage snapshots."""

    def __init__(self, tick_stages: list[dict]):
        self._ticks = tick_stages
        self._idx = 0

    def observe(self) -> dict:
        stages = self._ticks[min(self._idx, len(self._ticks) - 1)]
        self._idx += 1
        return {"stages": stages, "terminal": [], "cycle": self._idx, "hits": 0}

    def set_priority(self, *a, **k): return True
    def set_batch_size(self, *a, **k): return True
    def set_score_cutoff(self, *a, **k): return True
    async def trigger(self, *a, **k): return 0


class _MinimalOperator:
    """Yields N snapshots and stops; no radical.adr machinery needed."""

    def __init__(self, view, n_cycles: int):
        self.view = view
        self._n = n_cycles

    def _validate_stopping_condition(self):
        pass

    async def run(self):
        for i in range(self._n):
            yield type("Snap", (), {"cycle": i})()

    async def shutdown(self):
        pass


def _bad_ticks(n: int) -> list[dict]:
    """n cumulative-failure stage snapshots (1 failure added per tick)."""
    return [{"sim": {"finished": i + 1, "n_failed": i + 1}} for i in range(n)]


# ---------------------------------------------------------------------------
# TestCampaignAbortedError
# ---------------------------------------------------------------------------


class TestCampaignAbortedError:
    async def test_no_max_failed_ticks_never_aborts(self):
        """max_failed_ticks=None: all-failed ticks are silently tolerated."""
        view = _ScriptedView(_bad_ticks(5))
        cm = _StoppableFakeCM()
        op = _MinimalOperator(view, n_cycles=5)
        cm._stop_event.set()
        await run_supervised(cm, op, tick_s=0, max_failed_ticks=None)

    async def test_consecutive_bad_ticks_raises_campaign_aborted(self):
        """Exactly max_failed_ticks consecutive all-failed ticks triggers abort."""
        view = _ScriptedView(_bad_ticks(10))
        cm = _StoppableFakeCM()
        op = _MinimalOperator(view, n_cycles=10)
        with pytest.raises(CampaignAbortedError):
            await run_supervised(cm, op, tick_s=0, max_failed_ticks=3)

    async def test_cm_stop_called_before_raise(self):
        """cm.stop() must be called as part of the abort sequence."""
        view = _ScriptedView(_bad_ticks(10))
        cm = _StoppableFakeCM()
        op = _MinimalOperator(view, n_cycles=10)
        with pytest.raises(CampaignAbortedError):
            await run_supervised(cm, op, tick_s=0, max_failed_ticks=3)
        assert cm.stop_called

    async def test_success_tick_resets_counter(self):
        """A tick with at least one success resets the consecutive-failure counter."""
        stages = [
            {"sim": {"finished": 1, "n_failed": 1}},   # bad (count=1)
            {"sim": {"finished": 2, "n_failed": 2}},   # bad (count=2)
            {"sim": {"finished": 4, "n_failed": 2}},   # good: 2 succeeded → reset to 0
            {"sim": {"finished": 5, "n_failed": 3}},   # bad (count=1)
            {"sim": {"finished": 6, "n_failed": 4}},   # bad (count=2)
        ]
        view = _ScriptedView(stages)
        cm = _StoppableFakeCM()
        op = _MinimalOperator(view, n_cycles=5)
        cm._stop_event.set()
        await run_supervised(cm, op, tick_s=0, max_failed_ticks=3)  # must not raise

    async def test_zero_completion_tick_does_not_increment(self):
        """A tick where nothing finishes (delta_finished=0) is neutral."""
        stages = [
            {"sim": {"finished": 1, "n_failed": 1}},  # bad (count=1)
            {"sim": {"finished": 2, "n_failed": 2}},  # bad (count=2)
            {"sim": {"finished": 2, "n_failed": 2}},  # idle: delta=0 — neutral
            {"sim": {"finished": 3, "n_failed": 3}},  # bad (count=3, but only 3 cycles)
        ]
        view = _ScriptedView(stages)
        cm = _StoppableFakeCM()
        op = _MinimalOperator(view, n_cycles=3)  # 4th row never reached
        cm._stop_event.set()
        await run_supervised(cm, op, tick_s=0, max_failed_ticks=3)  # must not raise

    async def test_campaign_aborted_error_is_runtime_error(self):
        assert issubclass(CampaignAbortedError, RuntimeError)
