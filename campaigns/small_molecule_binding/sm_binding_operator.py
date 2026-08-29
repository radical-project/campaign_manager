"""SmMolBindingOperator — ADR operator for the small molecule binding campaign.

Campaign lifecycle
------------------
The campaign runs max_cycles ADR cycles.  Each cycle launches:
  - n  "explore" pipelines (broad backbone search, same input for now)
  - m  "exploit" pipelines (refinement on same input for now)

The operator tracks how many cycles have been launched and waits for all
replicas in a cycle to finish before triggering the next cycle.

Stopping condition: all max_cycles × (n + m) pipelines have finished
(Goal metric="n_hits", threshold=total-0.5, direction=maximize).

Dummy policy
------------
SmMolBindingPolicy always passes the same input JSON to every pipeline.
Future versions will inspect result.json files written by each completed
pipeline and use score differences to generate varied inputs for exploit.

Config keys consumed by the operator (from cm.adr.goals in config.yaml):
  max_cycles  int   number of explore/exploit cycles to run   (default: 1)
  explore_n   int   number of explore pipelines per cycle     (default: 2)
  exploit_m   int   number of exploit pipelines per cycle     (default: 1)
"""

from __future__ import annotations

import logging

from radical.adr import goals, observe
from radical.adr import Decision
from radical.adr.goals import Goal
from radical.adr.policy.base import Policy, decide

from src.campaign.adr import CampaignOperator
from src.campaign.adr.operator import CampaignOperator as _BaseCampaignOperator

log = logging.getLogger(__name__)


class SmMolBindingPolicy(Policy):
    """Dummy cycle-based policy for the small molecule binding campaign.

    Each ADR tick it either:
      - Triggers n explore + m exploit pipelines (cycle start), or
      - Waits for the current cycle's pipelines to finish.

    The policy advances to the next cycle only when all replicas from the
    previous cycle are reflected in obs["n_hits"].
    """

    def __init__(self, op: "SmMolBindingOperator") -> None:
        super().__init__()
        self._act          = op.get_actions()
        self._explore_n    = op.explore_n
        self._exploit_m    = op.exploit_m
        self._max_cycles   = op.max_cycles
        self._per_cycle    = op.explore_n + op.exploit_m

        self._campaign_cycle   = 0   # current cycle index (0-based)
        self._cycle_launched   = False
        self._cumulative_total = 0   # n_hits expected after current cycle completes

    @decide
    async def run(self, obs: dict) -> Decision:
        n_hits = int(obs.get("n_hits", 0))

        # Advance cycle when all pipelines for this cycle have finished.
        if self._cycle_launched and n_hits >= self._cumulative_total:
            log.info(
                "ADR: cycle %d complete (%d/%d total pipelines done)",
                self._campaign_cycle, n_hits, self._cumulative_total,
            )
            self._campaign_cycle += 1
            self._cycle_launched = False

        # Launch pipelines for the next cycle if still within budget.
        if not self._cycle_launched and self._campaign_cycle < self._max_cycles:
            self._cycle_launched = True
            self._cumulative_total += self._per_cycle

            actions = []
            if self._explore_n > 0:
                actions.append(self._act.trigger("explore", self._explore_n))
            if self._exploit_m > 0:
                actions.append(self._act.trigger("exploit", self._exploit_m))

            log.info(
                "ADR: launching cycle %d — %d explore + %d exploit"
                "  (cumulative target: %d)",
                self._campaign_cycle,
                self._explore_n,
                self._exploit_m,
                self._cumulative_total,
            )
            return Decision(actions=actions)

        # Nothing to do this tick — just wait.
        return Decision()


class SmMolBindingOperator(CampaignOperator):
    """ADR operator for the explore/exploit small molecule binding campaign."""

    def __init__(
        self,
        view,
        engine=None,
        *,
        max_cycles: int = 1,
        explore_n:  int = 2,
        exploit_m:  int = 1,
        **kwargs,
    ):
        super().__init__(view, engine=engine, **kwargs)
        self.max_cycles = int(max_cycles)
        self.explore_n  = int(explore_n)
        self.exploit_m  = int(exploit_m)
        self._validate_stopping_condition()

    def rule_policy(self) -> SmMolBindingPolicy:
        return SmMolBindingPolicy(self)

    def default_policy(self) -> SmMolBindingPolicy:
        return SmMolBindingPolicy(self)

    # ── Goals ──────────────────────────────────────────────────────────────────

    @goals
    def criteria(self):
        """Stop when all max_cycles × (explore_n + exploit_m) pipelines finish."""
        total = self.max_cycles * (self.explore_n + self.exploit_m)
        if total <= 0:
            return []
        return Goal(
            name="all_cycles_done",
            metric="n_hits",
            threshold=total - 0.5,
            direction="maximize",
        )

    # ── Observe ────────────────────────────────────────────────────────────────

    @observe
    def extract(self, snapshot) -> dict:
        obs = _BaseCampaignOperator.extract(self, snapshot)
        stages = obs.get("stages", {})
        obs["explore_finished"] = stages.get("explore", {}).get("finished", 0)
        obs["exploit_finished"] = stages.get("exploit", {}).get("finished", 0)
        return obs
