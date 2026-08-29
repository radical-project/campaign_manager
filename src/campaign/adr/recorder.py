"""PolicyRecorder — an ADR Observer that logs each decision cycle to JSONL.

Wire it into a CampaignOperator to capture, per cycle, the priorities the policy
emitted, the live stage state it saw, and (for the bandit policy) its posterior
means.  The resulting JSONL is what ``plot_policy_comparison.py`` reads to plot
rule vs. bandit vs. LLM behaviour side by side.

One JSON object per line::

    {"cycle": 0, "t": 1.20, "policy": "bandit",
     "priorities": {"s1_ligand_filter": 101, ...},
     "summary":    {"s1_ligand_filter": 0.50, ...},     # bandit posterior means
     "hits": 0,
     "stages": {"s1_ligand_filter": {"finished": 3, "queue_depth": 5,
                                     "running": 2, "bp_state": "WIDEN"}, ...}}
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from .view import CampaignViewProtocol

if TYPE_CHECKING:
    from ..metrics import CampaignMetrics


class PolicyRecorder:
    """ADR ObserverBase implementation that appends one JSONL row per cycle."""

    def __init__(self, path, policy_kind: str = "?") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.policy_kind = policy_kind
        self._view: Optional[CampaignViewProtocol] = None
        self._policy = None
        self._metrics: Optional[CampaignMetrics] = None
        self._fh = None
        self._t0 = 0.0

    def bind(
        self,
        view: CampaignViewProtocol,
        policy,
        metrics: Optional[CampaignMetrics] = None,
    ) -> None:
        """Attach live view, policy, and optional metrics recorder."""
        self._view = view
        self._policy = policy
        self._metrics = metrics

    # ── ObserverBase hooks ──────────────────────────────────────────────────

    def on_start(self, operator_id: str, metadata: dict) -> None:
        self._t0 = time.monotonic()
        self._fh = open(self.path, "w")

    def on_cycle(self, snapshot, obs_or_decision=None, decision=None) -> None:
        # Operator API was updated: on_cycle(snapshot, obs, decision).
        # Accept both old two-arg and new three-arg call signatures.
        if decision is None:
            decision = obs_or_decision
        if self._fh is None:
            return
        priorities = {
            a.task_kwargs["stage"]: a.task_kwargs["priority"]
            for a in decision.actions
            if a.task_name == "set_priority" and "stage" in a.task_kwargs
        }
        summary = self._safe_summary()
        obs = self._view.observe() if self._view is not None else {}
        stages = {
            s: {
                "finished": info.get("finished"),
                "queue_depth": info.get("queue_depth"),
                "running": info.get("running"),
                "pending": info.get("pending"),
                "starved": info.get("starved"),
                "bp_state": info.get("bp_state"),
                "priority": info.get("priority"),
            }
            for s, info in obs.get("stages", {}).items()
        }
        t = round(time.monotonic() - self._t0, 3)
        row = {
            "cycle": snapshot.cycle,
            "t": t,
            "policy": self.policy_kind,
            "priorities": priorities,
            "summary": summary,
            "hits": obs.get("hits"),
            "stages": stages,
        }
        self._fh.write(json.dumps(row) + "\n")
        self._fh.flush()
        if self._metrics is not None:
            actions = [
                {"name": a.task_name, **a.task_kwargs}
                for a in decision.actions
            ]
            self._metrics.record_decision(
                cycle=snapshot.cycle,
                t=t,
                policy=self.policy_kind,
                actions=actions,
            )

    def on_stop(self, final, reason: str) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    # ── helpers ─────────────────────────────────────────────────────────────

    def _safe_summary(self) -> dict:
        """Bandit posterior means if the active policy exposes .summary, else {}."""
        pol = self._policy
        summ = getattr(pol, "summary", None)
        if isinstance(summ, dict):
            return summ
        return {}
