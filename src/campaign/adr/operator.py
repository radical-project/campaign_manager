"""CampaignOperator — generic ADR Operator base for AsyncCampaignManager campaigns.

Campaign-specific logic (goals, observation extensions, default policy) lives in
``campaigns/<name>/operator.py`` subclasses.  This module only owns the generic
scheduling levers and the observation mechanics that are common to all campaigns.

Typical usage::

    # In campaigns/my_campaign/operator.py:
    from src.campaign.adr import CampaignOperator
    from radical.adr import goals, observe
    from radical.adr.goals import Goal

    class MyCampaignOperator(CampaignOperator):
        n_target: int = 10

        def __init__(self, view, engine=None, *, n_target=10, **kwargs):
            super().__init__(view, engine=engine, **kwargs)
            self.n_target = int(n_target)
            self._validate_stopping_condition()

        @goals
        def criteria(self):
            if self.n_target <= 0:
                return []
            return Goal(name="done", metric="n_hits",
                        threshold=self.n_target - 0.5, direction="maximize")

    # In run_campaign.py:
    view = CampaignView(cm)
    op   = MyCampaignOperator(view, engine=asyncflow, n_target=10)
    op.policy = make_scheduling_policy(op, kind="rule")
    await cm.start()
    await run_supervised(cm, op)
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Optional

from radical.adr import Operator, act, goals, observe

from .supervisor import CampaignAbortedError, run_supervised  # noqa: F401
from .view import CampaignViewProtocol

log = logging.getLogger(__name__)

# Version written by radical.adr.Operator.save_checkpoint that this CM release
# has been validated against.  A mismatch triggers a warning but not an abort.
_CHECKPOINT_VERSION = 1


class CampaignOperator(Operator):
    """Generic ADR Operator base that supervises an AsyncCampaignManager.

    Subclass this in ``campaigns/<name>/operator.py`` and override:
      - ``@goals def criteria(self)`` — declare campaign-specific stopping goals
      - ``@observe def extract(self, snapshot)`` — extend obs with campaign metrics
        (call ``super().extract(snapshot)`` to get the base fields first)
      - ``default_policy(self)`` — return the campaign's preferred policy when the
        config doesn't specify one (returns None by default → no ADR supervision)

    The base ``@observe`` already computes:
      obs["n_hits"]  — total finished replicas across all terminal stages
      obs["cycle"]   — current ADR cycle number

    The base ``@goals`` returns [] (no stopping condition).  Campaigns MUST
    override ``@goals`` or pass ``max_cycles`` to avoid running forever.
    """

    # ── ADR registry inheritance ────────────────────────────────────────────────
    # Operator.__init_subclass__ resets _adl_act_registry / _adl_observe_fn /
    # _adl_goals_fn to empty/None for every new subclass, scanning only that
    # class's own vars().  We restore proper inheritance here so campaign
    # subclasses automatically get the base @act / @observe / @goals methods
    # without having to redeclare them.

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Merge act registry from parent classes (deepest MRO wins first).
        for base in cls.__mro__[1:]:
            parent_reg = getattr(base, "_adl_act_registry", {})
            for name, fn in parent_reg.items():
                if name not in cls._adl_act_registry:
                    cls._adl_act_registry[name] = fn
        # Inherit @observe if not overridden in this class.
        if cls._adl_observe_fn is None:
            for base in cls.__mro__[1:]:
                fn = getattr(base, "_adl_observe_fn", None)
                if fn is not None:
                    cls._adl_observe_fn = fn
                    break
        # Inherit @goals if not overridden in this class.
        if cls._adl_goals_fn is None:
            for base in cls.__mro__[1:]:
                fn = getattr(base, "_adl_goals_fn", None)
                if fn is not None:
                    cls._adl_goals_fn = fn
                    break

    # ── Constructor ─────────────────────────────────────────────────────────────

    def __init__(
        self,
        view: CampaignViewProtocol,
        engine: Any = None,
        *,
        policy=None,
        observer=None,
        max_cycles: Optional[int] = None,
    ) -> None:
        super().__init__(engine, policy=policy, observer=observer, max_cycles=max_cycles)
        object.__setattr__(self, "_view", view)

    @property
    def view(self) -> CampaignViewProtocol:
        return object.__getattribute__(self, "_view")

    # ── Stopping condition validation ───────────────────────────────────────────

    def _validate_stopping_condition(self) -> None:
        """Raise ValueError if this operator has no stopping condition at all.

        Call this at the END of a subclass __init__ (after setting all state)
        so that @goals can see the fully-initialised operator state.
        """
        goals_fn = type(self)._adl_goals_fn
        has_goals = False
        if goals_fn is not None:
            raw = goals_fn(self)
            static = raw if isinstance(raw, list) else [raw]
            has_goals = bool(static)
        has_max = object.__getattribute__(self, "_max_cycles") is not None
        if not has_goals and not has_max:
            raise ValueError(
                f"{type(self).__name__} has no stopping condition: "
                "override @goals to declare campaign goals or pass max_cycles."
            )

    # ── Checkpoint helpers ──────────────────────────────────────────────────────

    def save_extra(self) -> dict:
        """Return campaign-specific state to persist alongside the ADR checkpoint.

        Override in subclasses to save state that lives outside radical.adr's
        artifacts/runtime dicts (e.g. operator instance variables).  The returned
        dict is written to ``<checkpoint>.extra.json`` by save_checkpoint_full().
        Return {} (the default) to skip the sidecar file entirely.
        """
        return {}

    def load_extra(self, data: dict) -> None:
        """Restore campaign-specific state saved by save_extra().

        Called by load_checkpoint_full() when a ``.extra.json`` sidecar exists.
        Override alongside save_extra() in subclasses that persist custom state.
        """

    def save_checkpoint_full(self, path: str) -> None:
        """Save ADR checkpoint + campaign-specific extra state.

        Calls the radical.adr base save_checkpoint(), then writes save_extra()
        to a sidecar file next to the checkpoint if the subclass returns any data.
        """
        self.save_checkpoint(path)
        extra = self.save_extra()
        if extra:
            sidecar = Path(path).with_suffix(".extra.json")
            with open(sidecar, "w") as f:
                json.dump(extra, f, indent=2)
            log.info("extra checkpoint state saved to %s", sidecar)

    def load_checkpoint_full(self, path: str) -> None:
        """Version-checked checkpoint load + campaign-specific extra state.

        Reads the checkpoint version and warns if it differs from the version
        this CM release was validated against.  Always attempts the load so that
        additive schema changes (new fields with safe defaults) still work.
        Calls load_extra() with the sidecar data if a ``.extra.json`` file exists.
        """
        try:
            with open(path) as f:
                data = json.load(f)
            version = data.get("version", 0)
            if version != _CHECKPOINT_VERSION:
                log.warning(
                    "Checkpoint version mismatch: file has version=%d, "
                    "CM expects version=%d.  State may not load correctly.",
                    version, _CHECKPOINT_VERSION,
                )
        except (json.JSONDecodeError, UnicodeDecodeError):
            log.warning(
                "Could not read version from checkpoint %s (not JSON) "
                "— loading without version check.",
                path,
            )
        self.load_checkpoint(path)
        sidecar = Path(path).with_suffix(".extra.json")
        if sidecar.exists():
            with open(sidecar) as f:
                extra = json.load(f)
            self.load_extra(extra)
            log.info("extra checkpoint state loaded from %s", sidecar)

    # ── Default policy (campaigns override) ────────────────────────────────────

    def default_policy(self):
        """Return this campaign's preferred policy when the config omits 'policy'.

        Return None to disable ADR supervision by default.  Subclasses override
        this to provide a sensible default without forcing every user to configure
        a policy explicitly.
        """
        return None

    def rule_policy(self):
        """Return this campaign's rule-based policy for ``policy: rule`` in config.

        Return None to fall back to the generic DownstreamFirstPolicy.
        Subclasses override this to supply a campaign-specific deterministic rule
        instead of the shared depth-first heuristic.

        Example::

            def rule_policy(self):
                return MyCampaignRulePolicy(self)
        """
        return None

    # ── Goals (empty — campaigns declare their own) ─────────────────────────────

    @goals
    def criteria(self):
        return []

    # ── Observe ─────────────────────────────────────────────────────────────────

    @observe
    def extract(self, snapshot) -> dict:
        obs = self.view.observe()
        obs["cycle"] = snapshot.cycle
        stages = obs.get("stages", {})
        # n_hits: terminal-stage finished count — the primary campaign progress signal.
        obs["n_hits"] = sum(
            stages.get(t, {}).get("finished", 0)
            for t in obs["terminal"]
        )
        # n_sims: total finished replicas across ALL stages.  Useful as a compute
        # budget proxy (Goal metric="n_sims") when campaign cost scales with total work.
        obs["n_sims"] = sum(s.get("finished", 0) for s in stages.values())
        return obs

    # ── Act levers (delegate to the view) ───────────────────────────────────────

    @act
    async def set_priority(self, stage: str, priority: int) -> dict:
        ok = self.view.set_priority(stage, priority)
        return {"lever": "set_priority", "stage": stage, "priority": priority, "ok": ok}

    @act
    async def set_batch_size(self, stage: str, size: int) -> dict:
        ok = self.view.set_batch_size(stage, size)
        return {"lever": "set_batch_size", "stage": stage, "size": size, "ok": ok}

    @act
    async def set_score_cutoff(self, stage: str, value: float) -> dict:
        ok = self.view.set_score_cutoff(stage, value)
        return {"lever": "set_score_cutoff", "stage": stage, "value": value, "ok": ok}

    @act
    async def trigger(self, stage: str, replicas: int) -> dict:
        n = await self.view.trigger(stage, replicas)
        return {"lever": "trigger", "stage": stage, "replicas": n}


