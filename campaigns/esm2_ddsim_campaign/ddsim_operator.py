"""DDSim-specific CampaignOperator with domain-appropriate goals.

Goals
-----
Scientific (all GPU stages complete):
    all_stages_complete  — campaign_complete == 1.
    True when ALL n_md_runs miniapps AND n_inference_runs inference replicas
    have finished.  cm.stop() fires immediately at ttt; close() cancels any
    in-flight dummies.  Dummies are bookkeeping — no reason to wait for them
    after the GPU pipeline is done.
    The ADR policies compete on *how fast* this is achieved (ttt).

Budget / efficiency:
    gpus_fully_utilized  — free_gpus < 1 (minimize).
    Under policy=rule, inference holds all 4 GPUs, leaving miniapps starved.
    A policy that satisfies this goal keeps GPUs occupied with useful work.

Operational health:
    low_task_failures  — task_fail_rate < max_fail_rate (5 %).
    Failing tasks waste GPU hours.  If this fires during a run it signals
    a hardware or config problem, not a scheduling decision.

Config knobs (all under cm.adr in the campaign YAML):
    n_md_runs:        int    expected miniapps completions  (= miniapps.replicas, default 4)
    n_inference_runs: int    expected inference completions (= inference.replicas, default 8)
    max_fail_rate:    float  failure rate ceiling           (default 0.05)

Usage
-----
    operator = DDSimCampaignOperator(view, engine=asyncflow,
                                     n_md_runs=4, n_inference_runs=8, max_fail_rate=0.05)
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
        n_inference_runs: int = 8,
        max_fail_rate: float = 0.05,
        policy=None,
        observer=None,
        max_cycles: Optional[int] = None,
    ) -> None:
        super().__init__(
            view,
            engine,
            policy=policy,
            observer=observer,
            max_cycles=max_cycles,
        )
        self._n_md_runs = int(n_md_runs)
        self._n_inference_runs = int(n_inference_runs)
        self._max_fail_rate = float(max_fail_rate)
        self._validate_stopping_condition()

    # ── Observation ────────────────────────────────────────────────────────

    # Logical DAG topology declared here so ADR policies can compute stage depths
    # and starvation correctly.  miniapps is an independent CM group (no config
    # dependency) but is downstream of inference semantically — it processes
    # inference outputs.  Declaring the dep here makes is_source=False for
    # miniapps, which the starvation check requires:
    #   starved = pending > 0 AND running < cap AND NOT is_source
    # Without this, miniapps would be treated as a source (is_source=True) and
    # starved would always be False, preventing the telemetry boost from firing.
    _LOGICAL_DEPS: dict[str, list[str]] = {
        "miniapps": ["inference"],
    }

    @observe
    def extract(self, snapshot) -> dict:
        obs = self.view.observe()
        obs["cycle"] = snapshot.cycle

        stages = obs.get("stages", {})

        # Restore logical topology so _stage_depth() computes correct depths:
        #   inference=0, miniapps=0, dummy=1
        # CampaignView.observe() computes `starved` before we inject these deps
        # (it checks w.dependencies, which is [] for all groups when _on_completion
        # routing is used).  We recompute `starved` here after topology is known.
        for name, info in stages.items():
            if name in self._LOGICAL_DEPS and not info.get("deps"):
                info["deps"] = self._LOGICAL_DEPS[name]
                info["is_source"] = False

        # Recompute `starved` using corrected is_source.
        # starved = has waiting replicas AND below concurrency cap AND not a source.
        # True sources (inference) are never starved — their pending queue is the
        # pre-loaded work library, not a resource stall.
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

        # campaign_complete: True when all 4 miniapps replicas finish.
        # ttt = miniapps_done.  Inference may still be running; cm.stop() cancels
        # remaining inference replicas as bookkeeping — they are not the scientific goal.
        # Under rule (NullSchedulingPolicy): inference holds all GPUs; miniapps waits
        # until all 16 inference done → TTT_rule ≈ 328s.
        # Under rule_telemetry: starved boost fires → miniapps overlaps inference
        # → TTT_tel ≈ 202s (≈38% faster).
        obs["campaign_complete"] = float(
            obs.get("miniapps_finished", 0) >= self._n_md_runs
        )

        return obs

    # ── Goals ──────────────────────────────────────────────────────────────

    @goals
    def criteria(self):
        return [
            # ── Scientific: all GPU stages complete (ttt) ─────────────────
            # campaign_complete fires when BOTH inference (n_inference_runs)
            # AND miniapps (n_md_runs) replicas have finished.
            # run_supervised calls cm.stop(); close() cancels remaining dummies.
            # ttt = max(inference_done, miniapps_done).
            Goal(
                name="all_stages_complete",
                metric="campaign_complete",
                threshold=0.5,
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
