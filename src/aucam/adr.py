"""Bridges to radical.adr (S1): an ADR policy as the campaign's controller, and
as its scheduling policy.

An ADR ``Policy`` has one async step, ``decide(obs) -> Decision``: actions
(spawn / cancel tasks), goals, stop, and free-form ``directives``.

- ``ADRController`` makes the campaign's decisions with it: spawn → launch
  request, cancel → CANCEL, goals, stop → terminate.
- ``ADRSchedulingPolicy`` ranks queued workflow types from
  ``directives["rank"]`` (and ``directives["value"]`` for sizing).  By
  default it reads the directives of the campaign's ``ADRController`` — one
  ADR call per policy cycle for both; without one, it calls its own ADR policy.

radical.adr is optional: these classes only read the Decision's fields, so
any object with the same fields works (e.g. in tests).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from .controller import (
    CampaignController,
    CancelRule,
    CostCancelRule,
    Goal,
    LaunchRule,
    PolicyResult,
)
from .scheduler import (
    RankingSchedulingPolicy,
    SchedulingContext,
    SchedulingPolicy,
    parse_ranking,
)
from .types import CampaignDecision, LaunchPriority, LaunchRequest

logger = logging.getLogger(__name__)


def _kind(action: Any) -> str:
    kind = getattr(action, "kind", None)
    return str(getattr(kind, "value", kind))


class ADRController(CampaignController):
    """A campaign controller that decides with a radical.adr ``Policy`` (S1).

    Every policy cycle AuCaM awaits ``policy.decide(obs)`` (the controller's
    observation plus ``obs["cycle"]``) and applies the Decision:

    ==========================  ===============================================
    ADR Decision                AuCaM
    ==========================  ===============================================
    SPAWN_TASK / SPAWN_BLOCK    launch request: ``task_name`` is the workflow
                                type, ``task_kwargs`` its context
                                (``priority`` in it sets the LaunchPriority)
    CANCEL_TASK                 CANCEL for the workflow id ``target_uid``
    UPDATE_PARAM                stored in ``self.params``
    goals / remove_goals        campaign goals
    stop                        end the campaign
    directives                  kept in ``self.directives`` (read by
                                ADRSchedulingPolicy)
    ==========================  ===============================================

    ``launch``, ``goal_type``, ``goal_n`` and ``rules`` work as for
    CampaignController, but the rules only make the decisions in cycles where
    the ADR policy fails or times out (``decide_timeout_s``).  ``goal_type`` /
    ``goal_n`` also end the campaign when reached.
    """

    def __init__(
        self,
        policy: Any,
        *,
        launch: list[str] | None = None,
        goal_type: str | None = None,
        goal_n: int | None = None,
        rules: list[CancelRule | CostCancelRule | LaunchRule] | None = None,
    ) -> None:
        super().__init__(launch=launch, goal_type=goal_type, goal_n=goal_n, rules=rules)
        self.policy = policy
        self.directives: dict[str, Any] = {}
        self.directives_seq = 0  # increases with every Decision that has directives
        self.params: dict[str, Any] = {}
        self._cycle = 0
        self._warned: set[str] = set()

    async def decide_async(self, obs: dict) -> PolicyResult:
        self._cycle += 1
        decision = await self.policy.decide({**obs, "cycle": self._cycle})
        return self.translate(decision)

    def translate(self, decision: Any) -> PolicyResult:
        """An ADR Decision as a PolicyResult."""
        res = PolicyResult()
        for action in getattr(decision, "actions", None) or []:
            kind = _kind(action)
            if kind in ("spawn_task", "spawn_block"):
                name = getattr(action, "task_name", None)
                if not name:
                    self._warn_once("spawn", "[ADR] spawn action without task_name — ignored")
                    continue
                context = dict(getattr(action, "task_kwargs", None) or {})
                priority = context.pop("priority", None)
                res.launch_requests.append(
                    LaunchRequest.make(
                        name,
                        LaunchPriority(str(priority).upper())
                        if priority
                        else LaunchPriority.NORMAL,
                        context=context or None,
                    )
                )
            elif kind == "cancel_task":
                uid = getattr(action, "target_uid", None)
                if uid:
                    res.decisions[uid] = CampaignDecision.CANCEL
            elif kind == "update_param":
                self.params[getattr(action, "param_key", None)] = getattr(
                    action, "param_value", None
                )
            else:
                self._warn_once(kind, f"[ADR] action {kind!r} is not supported — ignored")
        res.goals = [
            Goal(g.name, g.metric, float(g.threshold), g.direction)
            for g in getattr(decision, "goals", None) or []
        ]
        res.remove_goals = list(getattr(decision, "remove_goals", None) or [])
        reached = self._goal_type is not None and self.is_goal_reached()
        res.terminate = bool(getattr(decision, "stop", False)) or reached
        directives = getattr(decision, "directives", None) or {}
        if directives:
            self.directives = dict(directives)
            self.directives_seq += 1
        return res

    def _warn_once(self, key: str, msg: str) -> None:
        if key not in self._warned:
            self._warned.add(key)
            logger.warning(msg)

    def state(self) -> Any:
        st = super().state()
        st["adr"] = {"cycle": self._cycle, "params": self.params, "directives": self.directives}
        return st

    def restore(self, state: Any) -> None:
        super().restore(state)
        adr = state.get("adr") if isinstance(state, dict) else None
        if isinstance(adr, dict):
            self._cycle = int(adr.get("cycle", 0))
            self.params = dict(adr.get("params") or {})
            self.directives = dict(adr.get("directives") or {})


class ADRSchedulingPolicy(RankingSchedulingPolicy):
    """Ranks queued workflow types from ADR ``directives`` (S1).

    The directives are ``{"rank": [type, ...], "value": {type: factor}}``,
    taken from ``controller`` (an ADRController: its last Decision, so no extra
    ADR call) or, without one, from this policy's own ADR ``policy``, called in
    refresh() with the controller's observation plus ``cycle`` and
    ``queued`` (the queued workflow types).  See RankingSchedulingPolicy for
    how the ranking is applied and when ``fallback`` decides.

    ``rule_fn(candidates, ctx)`` can override select(): return a list to
    launch, or None to use the ranking.
    """

    def __init__(
        self,
        *,
        controller: ADRController | None = None,
        policy: Any = None,
        rule_fn: Callable[[list[LaunchRequest], SchedulingContext], list[LaunchRequest] | None]
        | None = None,
        fallback: SchedulingPolicy | None = None,
        max_age_cycles: int = 3,
        refresh_timeout_s: float = 60.0,
    ) -> None:
        super().__init__(
            fallback=fallback, max_age_cycles=max_age_cycles, refresh_timeout_s=refresh_timeout_s
        )
        self.controller = controller
        self.policy = policy
        self._rule_fn = rule_fn
        self._seen_seq = 0
        if controller is None and policy is None and rule_fn is None:
            logger.warning(
                "[ADR] ADRSchedulingPolicy without controller= or policy= — using the fallback"
            )

    async def refresh(self, ctx: SchedulingContext, queued: list[LaunchRequest], obs: dict) -> None:
        self._cycle = max(self._cycle, ctx.policy_cycles)
        if self.policy is not None:
            decision = await self.policy.decide(
                {**obs, "cycle": ctx.policy_cycles, "queued": [r.workflow_type for r in queued]}
            )
            directives = getattr(decision, "directives", None) or {}
        elif self.controller is not None and self.controller.directives_seq != self._seen_seq:
            self._seen_seq = self.controller.directives_seq
            directives = self.controller.directives
        else:
            return  # nothing new: the ranking ages and the fallback takes over
        if "rank" in directives:
            rank, factors = parse_ranking(directives)
            self.set_ranking(rank, factors, ctx.policy_cycles)
            logger.info("[ADR] rank=%s value=%s", rank, factors)

    def select(
        self, candidates: list[LaunchRequest], ctx: SchedulingContext
    ) -> list[LaunchRequest]:
        if self._rule_fn is not None:
            override = self._rule_fn(candidates, ctx)
            if override is not None:
                return override
        return super().select(candidates, ctx)
