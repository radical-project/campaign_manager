"""CampaignOperator — an ADR Operator that supervises an AsyncCampaignManager.

This is the "agent layer" seam: instead of the SchedulingBandit/ShardBandit
deciding *inside* the CM, a CampaignOperator runs the ADR Run→Observe→Decide→Act
loop *alongside* a running CM and nudges its scheduling levers (priority, batch
size, dependent triggers).  The CM still owns scheduling, execution lifecycle,
and resources — the operator only observes and advises (the ADR sacred boundary).

The operator is decoupled from the CM via ``CampaignView`` (see view.py), so it
unit-tests against a fake view with no live CM, no engine, and no LLM key.

Typical use (supervised alongside a live CM)::

    from src.campaign.adr import CampaignView, CampaignOperator
    from src.campaign.adr import make_scheduling_policy

    view = CampaignView(cm, target=5)
    op   = CampaignOperator(view, engine=cm._asyncflow)
    op.policy = make_scheduling_policy(op)          # rule + optional LLM
    await cm.start()
    await run_supervised(cm, op)                     # see run_supervised below
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional

from radical.adr import Operator, act, observe, goals
from radical.adr.goals import Goal

from .view import CampaignViewProtocol


class CampaignOperator(Operator):
    """ADR Operator whose acts mutate an AsyncCampaignManager's scheduling state."""

    # ── State (proxied to state.objectives, persisted across cycles) ────────
    target:  int = 0           # goal threshold (terminal-stage completions)

    def __init__(
        self,
        view: CampaignViewProtocol,
        engine: Any = None,
        *,
        target: Optional[int] = None,
        policy=None,
        observer=None,
        max_cycles: Optional[int] = None,
    ) -> None:
        super().__init__(engine, policy=policy, observer=observer, max_cycles=max_cycles)
        # _view is a plain instance attr (not an annotated state key).
        object.__setattr__(self, "_view", view)
        # Seed the goal threshold: explicit arg wins, else the view's inference.
        if target is None:
            target = int(view.observe().get("target", 0) or 0)
        self.target = int(target)

    @property
    def view(self) -> CampaignViewProtocol:
        return object.__getattribute__(self, "_view")

    # ── Goals ───────────────────────────────────────────────────────────────

    @goals
    def criteria(self):
        # target <= 0 → no early-stop goal (let the CM finish naturally).
        if self.target <= 0:
            return []
        # Goal.satisfied uses strict '>'; subtract 0.5 so integer hit-counts
        # satisfy at exactly `target` (hits >= target).
        return Goal(name="target_reached", metric="hits",
                    threshold=self.target - 0.5, direction="maximize")

    # ── Observe ─────────────────────────────────────────────────────────────

    @observe
    def extract(self, snapshot) -> dict:
        obs = self.view.observe()
        obs["cycle"] = snapshot.cycle
        return obs

    # ── Act levers (delegate to the view) ───────────────────────────────────

    @act
    async def set_priority(self, stage: str, priority: int) -> dict:
        ok = self.view.set_priority(stage, priority)
        return {"lever": "set_priority", "stage": stage, "priority": priority, "ok": ok}

    @act
    async def set_batch_size(self, stage: str, size: int) -> dict:
        ok = self.view.set_batch_size(stage, size)
        return {"lever": "set_batch_size", "stage": stage, "size": size, "ok": ok}

    @act
    async def trigger(self, stage: str, replicas: int) -> dict:
        n = await self.view.trigger(stage, replicas)
        return {"lever": "trigger", "stage": stage, "replicas": n}


async def run_supervised(
    cm,
    operator: CampaignOperator,
    tick_s: float = 1.0,
) -> None:
    """Run a CampaignOperator's decision loop alongside a running CM.

    The CM is started by the caller.  This drives the operator one cycle per
    ``tick_s`` until the CM completes (``cm.wait()``) or the operator's goal
    fires.  Cancels the operator loop cleanly when the campaign ends.
    """
    async def _drive() -> None:
        async for _snapshot in operator.run():
            await asyncio.sleep(tick_s)

    drive_task = asyncio.ensure_future(_drive())
    try:
        await cm.wait()
    finally:
        await operator.shutdown()
        if not drive_task.done():
            drive_task.cancel()
            try:
                await drive_task
            except asyncio.CancelledError:
                pass
