"""DDSim-specific CampaignOperator with domain-appropriate goals.

Goals
-----
Scientific (number of runs):
    md_pipeline_complete  — miniapps_finished ≥ n_md_runs (4).
    All MD trajectories have been analyzed by ML.  The ADR policies compete
    on *how fast* this is achieved (time-to-first-miniapps, all-complete).
    Expressed as a count of runs so it is meaningful regardless of timing.

Budget / efficiency:
    gpus_fully_utilized  — free_gpus < 1 (minimize).
    Under policy=none, after md finishes, miniapps waits while inference
    holds the pass-2 GPU slot — one GPU idles and compute budget is wasted.
    A policy that satisfies this goal is provably more resource-efficient.

Operational health:
    low_task_failures  — task_fail_rate < max_fail_rate (5 %).
    Failing tasks waste GPU hours.  If this fires during a run it signals
    a hardware or config problem, not a scheduling decision.

Config knobs (all under cm.adr in the campaign YAML):
    n_md_runs:      int    expected miniapps completions (= md.replicas, default 4)
    max_fail_rate:  float  failure rate ceiling         (default 0.05)

Usage
-----
    operator = DDSimCampaignOperator(view, engine=asyncflow,
                                     n_md_runs=4, max_fail_rate=0.05)
    operator.policy = make_scheduling_policy(operator, kind="rule")
    await run_supervised(cm, operator)
"""

from __future__ import annotations

from typing import Any, Optional

from radical.adr import goals, observe
from radical.adr.goals import Goal

from src.campaign.adr import CampaignOperator
from src.campaign.adr.view import CampaignViewProtocol


class DDSimCampaignOperator(CampaignOperator):
    """CampaignOperator subclass with DDSim/ESM2 campaign-specific goals.

    Adds per-stage finished counts to the observation so they can be
    referenced by Goal metrics (e.g. ``miniapps_finished``).
    """

    def __init__(
        self,
        view: CampaignViewProtocol,
        engine: Any = None,
        *,
        n_md_runs: int = 4,
        max_fail_rate: float = 0.05,
        target: Optional[int] = None,
        policy=None,
        observer=None,
        max_cycles: Optional[int] = None,
    ) -> None:
        super().__init__(
            view,
            engine,
            target=target,
            policy=policy,
            observer=observer,
            max_cycles=max_cycles,
        )
        self._n_md_runs = int(n_md_runs)
        self._max_fail_rate = float(max_fail_rate)

    # ── Observation ────────────────────────────────────────────────────────

    # Logical DAG topology for ADR depth calculations.
    # Config dependencies were removed in favour of _on_completion DAG routing,
    # but the ADR policies (DownstreamFirstPolicy depth ordering, BanditSchedulingPolicy
    # warmstart priors) need topology to work correctly.  Injecting it here fixes the
    # ADR observation without touching the CM config or scheduling at all.
    _LOGICAL_DEPS: dict[str, list[str]] = {
        "miniapps": ["md"],
        "dummy": ["inference", "miniapps"],
    }

    @observe
    def extract(self, snapshot) -> dict:
        obs = self.view.observe()
        obs["cycle"] = snapshot.cycle

        stages = obs.get("stages", {})

        # Restore logical topology so _stage_depth() computes correct depths:
        #   inference=0, md=0, miniapps=1, dummy=2
        # CampaignView.observe() computes `starved` before we inject these deps
        # (it checks w.dependencies, which is [] for all groups when _on_completion
        # routing is used).  We recompute `starved` here after topology is known.
        for name, info in stages.items():
            logical = self._LOGICAL_DEPS.get(name, [])
            if logical and not info.get("deps"):
                info["deps"] = logical
                info["is_source"] = False

        # Recompute `starved` using corrected is_source.
        # starved = has waiting replicas AND below concurrency cap AND not a source.
        # Source stages (inference, md) are never "starved" in the pipeline sense —
        # their pending count is the pre-loaded work library, not a dependency stall.
        for _name, info in stages.items():
            if info.get("is_source", True):
                info["starved"] = False
            else:
                pending = info.get("pending", 0)
                running = info.get("running", 0)
                cap = info.get("cap", 0)
                info["starved"] = bool(pending > 0 and (cap == 0 or running < cap))

        # Flatten per-stage finished counts so Goal.metric can reference them
        # directly (e.g. metric="miniapps_finished").
        for name, info in stages.items():
            obs[f"{name}_finished"] = info.get("finished", 0)

        return obs

    # ── Goals ──────────────────────────────────────────────────────────────

    @goals
    def criteria(self):
        return [
            # ── Scientific: number-of-runs completion ─────────────────────
            # All MD trajectories analyzed by ML.  n_md_runs completed
            # miniapps replicas is the core pipeline-B scientific output.
            # The ADR policies compete on how fast this is reached
            # (time-to-first-miniapps) and whether they get all n here.
            Goal(
                name="md_pipeline_complete",
                metric="miniapps_finished",
                # subtract 0.5 so integer counts satisfy at exactly n_md_runs
                threshold=self._n_md_runs - 0.5,
                direction="maximize",
            ),
            # ── Budget / efficiency: no idle GPUs ─────────────────────────
            # Under policy=none, after md finishes, miniapps waits while
            # inference holds the contested pass-2 GPU slot.  One GPU idles
            # and the compute budget is wasted.  A policy that satisfies this
            # goal is provably more resource-efficient.
            #
            # free_gpus ≤ 0 → all 4 GPUs in use.
            # threshold=0.5 → Goal satisfied when free_gpus < 1.
            Goal(
                name="gpus_fully_utilized",
                metric="free_gpus",
                threshold=0.5,
                direction="minimize",
            ),
            # ── Operational health: low task failure rate ─────────────────
            # Failing tasks waste GPU hours without contributing scientific
            # output.  If task_fail_rate exceeds max_fail_rate, it signals
            # a hardware or config problem — not a scheduling decision.
            Goal(
                name="low_task_failures",
                metric="task_fail_rate",
                threshold=self._max_fail_rate,
                direction="minimize",
            ),
        ]
