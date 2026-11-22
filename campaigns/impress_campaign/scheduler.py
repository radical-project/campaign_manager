"""Scheduling policies for this AuCaM campaign (`--policy`).

The policy decides which queued launch runs next.  A policy's *arm* is one of
the campaign's workflow TYPES — the bandit's word for it, used throughout this
module.  The three types are the same in every case, so the differences here
are differences in steering:

| name | what it does |
|---|---|
| `bandit` | UCB1 over the arms, rewarded by IMPRESS `progress` (the default) |
| `throughput` | UCB1 rewarded by completion alone — AuCaM's built-in reward |
| `fifo` | Queue order, no learning |
| `even` | An equal share of launches per arm — the unsteered baseline the figures compare against |

`throughput` beside `bandit` is the one worth running twice: a refractory arm
that finishes without satisfying a single objective is a success to the first
and worth nothing to the second, which is why IMPRESS hands AuCaM a science
score (`impress_a.integrations.aucam.reward_fn`) rather than a return code.
"""

from __future__ import annotations

import logging
import math
from typing import Any

from src.aucam import (
    Allocation,
    BanditSchedulingPolicy,
    FIFOSchedulingPolicy,
    LaunchRequest,
    SchedulingContext,
    SchedulingPolicy,
    SizingContext,
    WorkflowResult,
)

logger = logging.getLogger(__name__)


class EvenSplitPolicy(SchedulingPolicy):
    """No information, no learning: give every arm the same number of launches.

    The baseline the figures compare AuCaM against, and the reason it exists rather
    than `fifo` being used for that: FIFO follows the controller's queue order, and
    the controller tops its arms up in a fixed order with the tractable arm first, so FIFO
    quietly behaves like "always prefer the arm listed first" — which on these specs
    is close to the right answer. Measuring against it flatters nothing and proves
    nothing.

    An even split is what someone with no way to tell the arms apart would actually
    do with an allocation, which is the thing a scheduler has to beat.
    """

    def __init__(self) -> None:
        self._launched: dict[str, int] = {}

    def select(self, candidates: list, ctx: SchedulingContext) -> list:
        slots = ctx.max_concurrency - sum(ctx.running_by_type.values())
        if slots <= 0:
            return []
        running = dict(ctx.running_by_type)
        selected: list = []
        # Fewest launches first, queue order breaking ties, so the split stays even
        # however the controller happens to have ordered the queue.
        for req in sorted(candidates, key=lambda r: self._launched.get(r.workflow_type, 0)):
            if len(selected) >= slots:
                break
            wf_type = req.workflow_type
            if running.get(wf_type, 0) < ctx.cap(wf_type):
                selected.append(req)
                running[wf_type] = running.get(wf_type, 0) + 1
                self._launched[wf_type] = self._launched.get(wf_type, 0) + 1
        return selected


class RecordingPolicy(SchedulingPolicy):
    """Wraps any policy and records which type each launch went to, and when.

    Steering happens in the scheduler and none of it reaches the controller:
    `select()` is handed the queue and answers with the launches, so the launch
    order exists nowhere else — AuCaM's own telemetry is per task, and the
    controller sees only per-cycle counts.  Figure 2 is drawn from this record,
    and the baseline run needs the same one to be comparable, so this wraps
    whatever `--policy` built rather than belonging to the bandit.

    Per launch:  {"event": "select", "t", "wf_type", "c_eff"}

    `c_eff` is the exploration weight behind that decision
    (`exploration * sqrt(budget_remaining)`), recomputed here because the
    scheduler keeps it local to the call; None for a policy that does not
    explore.  Events go through the controller's `record()`, so the whole
    telemetry file shares one clock.
    """

    def __init__(self, inner: SchedulingPolicy, record: Any) -> None:
        self._inner, self._record = inner, record
        explore = getattr(inner, "_exploration", None)
        self._explore = float(explore) if explore is not None else None

    def select(self, candidates: list, ctx: SchedulingContext) -> list:
        chosen = self._inner.select(candidates, ctx)
        rem = float(getattr(ctx, "budget_remaining_fraction", 1.0))
        c_eff = None if self._explore is None else self._explore * math.sqrt(max(0.0, rem))
        for req in chosen:
            self._record(
                {
                    "event": "select",
                    "wf_type": getattr(req, "workflow_type", None) or getattr(req, "name", "?"),
                    "c_eff": c_eff,
                }
            )
        return chosen

    # Everything else is the wrapped policy's.  Delegated explicitly rather than with
    # __getattr__, so a new hook on SchedulingPolicy fails loudly here instead of
    # silently bypassing the recording.
    def on_outcome(self, request: Any, result: WorkflowResult) -> None:
        self._inner.on_outcome(request, result)

    def size(self, request: LaunchRequest, ctx: SizingContext) -> Allocation | None:
        return self._inner.size(request, ctx)


def make_policy(name: str, *, reward_fn: Any, cost_aware: bool = False) -> SchedulingPolicy:
    """A fresh policy for `name`.  Each campaign needs its own instance.

    `cost_aware` makes the bandit weigh each reward by what the run cost, so it
    ranks types by reward per gpu_hour rather than by reward alone.
    """
    if name == "throughput":
        return BanditSchedulingPolicy()  # AuCaM's own reward: completed or not
    if name == "fifo":
        return FIFOSchedulingPolicy()
    if name == "even":
        return EvenSplitPolicy()
    return BanditSchedulingPolicy(reward_fn=reward_fn, cost_aware=cost_aware)  # "bandit"
