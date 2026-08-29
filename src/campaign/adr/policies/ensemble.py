"""Ensemble policies: BlendPolicy and ConsensusPolicy."""

from __future__ import annotations

import asyncio
import collections
import logging

from radical.adr import Decision, Policy, decide
from radical.adr.decision import Action, ActionKind

log = logging.getLogger(__name__)


class BlendPolicy(Policy):
    """Blend two policies' numeric lever actions by averaging.

    When both policies emit the same (task_name, stage) pair for a blendable
    lever, a single averaged action is emitted instead of two conflicting writes.

    Blendable levers: set_priority→priority, set_batch_size→size,
    set_score_cutoff→value. Non-blendable actions pass through deduplicated.
    stop uses AND semantics; goals/remove_goals/directives are unioned.
    """

    _BLENDABLE: dict[str, str] = {
        "set_priority":     "priority",
        "set_batch_size":   "size",
        "set_score_cutoff": "value",
    }

    def __init__(self, policies: list[Policy]) -> None:
        if len(policies) != 2:
            raise ValueError(f"BlendPolicy requires exactly 2 policies, got {len(policies)}")
        super().__init__()
        self._policies = policies

    async def decide(self, obs: dict) -> Decision:
        d0 = await self._policies[0].decide(obs)
        d1 = await self._policies[1].decide(obs)

        def _index(actions: list) -> tuple[dict, list]:
            indexed: dict[tuple, Action] = {}
            passthrough: list[Action] = []
            for a in actions:
                if (a.kind == ActionKind.SPAWN_TASK
                        and a.task_name in BlendPolicy._BLENDABLE
                        and "stage" in a.task_kwargs):
                    indexed[(a.task_name, a.task_kwargs["stage"])] = a
                else:
                    passthrough.append(a)
            return indexed, passthrough

        idx0, pass0 = _index(d0.actions)
        idx1, pass1 = _index(d1.actions)
        blended: list[Action] = []

        for key in set(idx0) | set(idx1):
            task_name, stage = key
            val_key = BlendPolicy._BLENDABLE[task_name]
            if key in idx0 and key in idx1:
                v0 = idx0[key].task_kwargs[val_key]
                v1 = idx1[key].task_kwargs[val_key]
                avg = (v0 + v1) / 2
                merged_val = int(round(avg)) if val_key in ("priority", "size") else avg
                merged_kwargs = {**idx0[key].task_kwargs, val_key: merged_val}
                blended.append(Action(kind=ActionKind.SPAWN_TASK, task_name=task_name,
                                      task_kwargs=merged_kwargs))
                log.debug("BlendPolicy: %s(stage=%s)  p0=%g  p1=%g  → blended=%g",
                          task_name, stage, v0, v1, merged_val)
            else:
                blended.append(idx0.get(key) or idx1[key])

        seen: set = set()
        for a in pass0 + pass1:
            dedup_key = (a.kind, a.task_name, a.param_key)
            if dedup_key not in seen:
                seen.add(dedup_key)
                blended.append(a)

        return Decision(
            actions=blended,
            stop=d0.stop and d1.stop,
            goals=d0.goals + d1.goals,
            remove_goals=list(set(d0.remove_goals) | set(d1.remove_goals)),
            directives={**d0.directives, **d1.directives},
        )


class ConsensusPolicy(Policy):
    """Majority-vote ensemble over N ≥ 2 inner policies.

    Each cycle all member policies run in parallel via ``asyncio.gather()``.
    Their ``set_priority`` actions are compared by stage ranking (highest
    priority first).  Behaviour depends on ``mode``:

    ``mode="performance"`` *(default)*
        **Majority path** (> N/2 policies share the same full ranking):
        emit the *maximum* priority each stage receives from any agreeing
        policy — safe because agreeing policies share the same ranking tuple,
        so max-per-stage cannot reorder stages.

        **No-majority path**: pick the inner policy whose top non-source stage
        is most *starved* (``pending > 0``, ``running < cap``, not a source).
        The starved-pending count is the primary key; the policy's own priority
        for that stage breaks ties.  Falls back to averaging when no stage is
        starved (e.g. very first cycle).

        **Stop**: OR — one policy requesting stop is enough.

    ``mode="robust"``
        **Majority path**: *average* priorities from the agreeing subset only
        (minority is discarded from the computation).  Stages are averaged only
        over policies that actually emitted a priority for that stage.

        **No-majority path**: average all active policies per stage, again
        averaging only over those that covered each stage.

        **Stop**: majority required — more than N/2 policies must agree.

    Non-priority actions (batch sizes, triggers) pass through from all
    policies, deduplicated by ``(kind, task_name, param_key)``.

    After each cycle ``policy.summary`` returns a dict describing which path
    ran and who led (useful for ``PolicyRecorder`` JSONL output).
    ``policy._regime`` is also set for ``LoggingPolicy`` compatibility.
    """

    _VALID_MODES: frozenset = frozenset({"performance", "robust"})

    def __init__(self, policies: list[Policy], *, mode: str = "performance") -> None:
        if len(policies) < 2:
            raise ValueError("ConsensusPolicy requires at least 2 policies")
        if mode not in self._VALID_MODES:
            raise ValueError(
                f"ConsensusPolicy: unknown mode {mode!r}; "
                f"expected one of {sorted(self._VALID_MODES)}"
            )
        super().__init__()
        self._policies = policies
        self.mode = mode
        self._last_summary: dict = {}
        self._regime: str = ""

    @property
    def summary(self) -> dict:
        return dict(self._last_summary)

    @staticmethod
    def _extract_priorities(d: Decision) -> dict[str, int]:
        out: dict[str, int] = {}
        for a in d.actions:
            if getattr(a, "task_name", None) == "set_priority":
                kw = getattr(a, "task_kwargs", {})
                if "stage" in kw and "priority" in kw:
                    out[kw["stage"]] = int(kw["priority"])
        return out

    @staticmethod
    def _ranking(priorities: dict[str, int]) -> tuple[str, ...]:
        return tuple(s for s, _ in sorted(priorities.items(), key=lambda x: -x[1]))

    @decide
    async def run(self, obs: dict) -> Decision:
        decisions = await asyncio.gather(*(p.decide(obs) for p in self._policies))

        # Stop semantics differ by mode.
        if self.mode == "robust":
            stop = sum(d.stop for d in decisions) * 2 > len(decisions)
        else:
            stop = any(d.stop for d in decisions)

        # Pair each policy with its extracted priorities; drop empty emitters.
        pairs = [
            (pol, self._extract_priorities(d))
            for pol, d in zip(self._policies, decisions, strict=False)
        ]
        active = [(pol, p) for pol, p in pairs if p]
        if not active:
            return Decision(stop=stop)

        active_policies = [pol for pol, _ in active]
        active_pris = [p for _, p in active]

        rankings = [self._ranking(p) for p in active_pris]
        counts = collections.Counter(rankings)
        top_ranking, top_count = counts.most_common(1)[0]

        has_majority = top_count * 2 > len(active_pris)

        if self.mode == "performance":
            # ── Performance mode: obs-driven first ───────────────────────────
            # avg_duration_s / starved state is ground truth; majority voting is
            # a fallback only when no obs signal is available.  This prevents a
            # rule+bandit wrong majority (both rank a>b) from overriding LLM's
            # correct b-first signal.
            stages_obs: dict = obs.get("stages", {}) if obs else {}

            def _obs_score(pris: dict) -> tuple:
                """Compound score for picking the best-informed policy.

                Tier 2 — non-source stage is genuinely starved: score by
                          (pending, stage_priority, −best_source_avg_dur).
                          The third element breaks ties using the Tier 1 signal
                          so that a policy recommending a fast source stage wins
                          even when all policies tie on the starved stage.
                Tier 1 — source stages compete by throughput: score by
                          (−avg_duration_s, stage_priority).  Faster wins; only
                          fires once avg_duration_s data is available.
                Tier 0 — no usable signal; falls back to majority or averaging.
                """
                # Tier 2: starved non-source stage
                for stage in self._ranking(pris):
                    info = stages_obs.get(stage, {})
                    if not info.get("is_source", False) and info.get("starved", False):
                        pending = info.get("pending", 0)
                        # Tiebreaker: best Tier 1 signal for this policy.
                        t1_dur = 0.0
                        for s2 in self._ranking(pris):
                            i2 = stages_obs.get(s2, {})
                            if i2.get("is_source", False):
                                d2 = i2.get("avg_duration_s")
                                if d2 is not None and d2 > 0:
                                    t1_dur = -d2
                                    break
                        return (2, pending, pris.get(stage, 0), t1_dur)
                # Tier 1: fastest source stage.
                for stage in self._ranking(pris):
                    info = stages_obs.get(stage, {})
                    if info.get("is_source", False):
                        avg_dur = info.get("avg_duration_s")
                        if avg_dur is not None and avg_dur > 0:
                            return (1, 0, -avg_dur, pris.get(stage, 0))
                return (0, 0, 0, 0.0)

            scores = [_obs_score(p) for p in active_pris]
            best_idx = max(range(len(active_pris)), key=lambda i: scores[i])
            best_tier = scores[best_idx][0]

            if best_tier >= 1:
                merged = active_pris[best_idx]
                winner = type(active_policies[best_idx]).__name__
                path_label = "obs-starved" if best_tier == 2 else "obs-throughput"
                detail = scores[best_idx][1] if best_tier == 2 else f"{-scores[best_idx][2]:.3f}s"
                self._last_summary = {"path": path_label, "winner": winner, "detail": detail}
                self._regime = f"{path_label} {winner}→{detail}"
                log.debug(
                    "ConsensusPolicy [performance]: %s  winner=%s  detail=%s  merged=%s",
                    path_label, winner, detail, merged,
                )
            elif has_majority:
                # No obs signal → fall back to majority-max.
                agreeing = [
                    (pol, p) for pol, p, r in zip(active_policies, active_pris, rankings, strict=False)
                    if r == top_ranking
                ]
                ag_pols = [pol for pol, _ in agreeing]
                ag_pris = [p for _, p in agreeing]
                agreers = [type(pol).__name__ for pol in ag_pols]
                merged = {stage: max(p.get(stage, 0) for p in ag_pris) for stage in top_ranking}
                path = "majority-max"
                self._last_summary = {
                    "path": path,
                    "count": f"{top_count}/{len(active_pris)}",
                    "agreers": agreers,
                }
                self._regime = f"{path} {top_count}/{len(active_pris)}"
                log.debug(
                    "ConsensusPolicy [performance]: %s (no obs)  ranking=%s  merged=%s  agreers=%s",
                    path, top_ranking, merged, agreers,
                )
            else:
                # No obs signal, no majority → average.
                all_stages: set = set().union(*active_pris)
                merged = {
                    stage: round(
                        sum(p[stage] for p in active_pris if stage in p)
                        / sum(1 for p in active_pris if stage in p)
                    )
                    for stage in all_stages
                }
                self._last_summary = {"path": "averaging"}
                self._regime = "averaging"
                log.debug(
                    "ConsensusPolicy [performance]: no obs, no majority → averaging %s",
                    dict(counts),
                )

        else:
            # ── Robust mode: traditional majority → average ───────────────────
            if has_majority:
                agreeing = [
                    (pol, p) for pol, p, r in zip(active_policies, active_pris, rankings, strict=False)
                    if r == top_ranking
                ]
                ag_pols = [pol for pol, _ in agreeing]
                ag_pris = [p for _, p in agreeing]
                agreers = [type(pol).__name__ for pol in ag_pols]
                merged = {
                    stage: round(
                        sum(p[stage] for p in ag_pris if stage in p)
                        / sum(1 for p in ag_pris if stage in p)
                    )
                    for stage in top_ranking
                }
                path = "majority-avg"
                self._last_summary = {
                    "path": path,
                    "count": f"{top_count}/{len(active_pris)}",
                    "agreers": agreers,
                }
                self._regime = f"{path} {top_count}/{len(active_pris)}"
                log.debug(
                    "ConsensusPolicy [robust]: %s  ranking=%s  merged=%s  agreers=%s",
                    path, top_ranking, merged, agreers,
                )
            else:
                all_stages = set().union(*active_pris)
                merged = {
                    stage: round(
                        sum(p[stage] for p in active_pris if stage in p)
                        / sum(1 for p in active_pris if stage in p)
                    )
                    for stage in all_stages
                }
                self._last_summary = {"path": "averaging"}
                self._regime = "averaging"
                log.debug(
                    "ConsensusPolicy [robust]: no majority %s → averaging  %s",
                    dict(counts), merged,
                )

        priority_actions = [
            Action(
                kind=ActionKind.SPAWN_TASK,
                task_name="set_priority",
                task_kwargs={"stage": s, "priority": v},
            )
            for s, v in merged.items()
        ]

        # Pass through all non-priority actions from all policies, deduplicated.
        seen: set = set()
        other_actions: list[Action] = []
        for d in decisions:
            for a in d.actions:
                if getattr(a, "task_name", None) == "set_priority":
                    continue
                dedup_key = (a.kind, a.task_name, getattr(a, "param_key", None))
                if dedup_key not in seen:
                    seen.add(dedup_key)
                    other_actions.append(a)

        return Decision(actions=priority_actions + other_actions, stop=stop)
