"""DreamerCampaignOperator — ADR operator for the SPHERICAL Dreamer campaign.

The Dreamer campaign is a multi-stage drug-discovery funnel (s1 → s5).
This operator stops when ``n_target`` terminal-stage (s5_fep_ranking) replicas
have finished, as declared in ``cm.adr.goals.n_target`` in the config.
"""

from __future__ import annotations

from radical.adr import goals
from radical.adr.goals import Goal

from src.campaign.adr import CampaignOperator
from src.campaign.adr.policies import BanditSchedulingPolicy


class DreamerCampaignOperator(CampaignOperator):
    """ADR operator for the SPHERICAL Dreamer drug-discovery funnel campaign."""

    n_target: int = 5

    def __init__(self, view, engine=None, *, n_target: int = 5, **kwargs):
        super().__init__(view, engine=engine, **kwargs)
        self.n_target = int(n_target)
        self._validate_stopping_condition()

    def default_policy(self):
        """Default to bandit scheduling when no policy is specified in config."""
        return BanditSchedulingPolicy(self, warmstart=True)

    @goals
    def criteria(self):
        if self.n_target <= 0:
            return []
        return Goal(
            name="leads_found",
            metric="n_hits",
            threshold=self.n_target - 0.5,
            direction="maximize",
        )
