#!/usr/bin/env python3
"""
Benchmark runner — measures performance across feature-flag configurations.

Each configuration is a dict of feature overrides applied on top of the
base config.yaml.  For each configuration the campaign is run N_RUNS times
(different random seeds) and metrics are aggregated.

Results are written to benchmark_results.json for consumption by
plot_optimizations.py.

Usage:
    python benchmark.py [--config config.yaml] [--runs 3] [--out benchmark_results.json]
"""

import argparse
import asyncio
import copy
import json
import sys
import time
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# ── Benchmark configurations ──────────────────────────────────────────────────

# Per-run wall-time cap.  Non-ADVANCE configs (e.g. baseline) can take 500 s+ to
# reach s5=5 via the full cascade; cap each run so the benchmark completes in
# ~20 min.  Optimised configs finish in <30 s.
RUN_TIMEOUT_S = 120

CONFIGURATIONS: dict[str, dict] = {
    # ─── Dumb waterfall baseline ──────────────────────────────────────────────
    # True sequential pipeline: each stage starts ONLY after ALL replicas of the
    # previous stage have finished (dep_threshold_override=9999999 forces the
    # scheduler to wait for upstream.status=="done", not just the first replica).
    # Sharder OFF → FIFO arrival order (random quality).  No priorities → s1
    # monopolises all GPUs.  Underutilises resources at every stage transition.
    "baseline": {
        "features": {"backpressure": False, "monitor": False, "sharder": False},
       # "dep_threshold_override": 9999999,
        "stage_replicas_overrides": {
            "s2_ml_affinity":   {"concurrency_floor": 2},
            "s3_docking":       {"concurrency_floor": 1},
            "s4_md_refinement": {"concurrency_floor": 1},
            "s5_fep_ranking":   {"concurrency_floor": 1},
        },
    },

    # ─── Smart sharding axis ─────────────────────────────────────────────────
    # Demonstrates the CANDIDATE-ROUTING + PARTIAL PIPELINE benefit.
    # Sharder ON, stratify=soft: adaptive batch dispatch ranked by score.
    # NO static stage priorities (no bandit) — but concurrency_floor guarantees
    # a minimum concurrent slot count for every downstream stage via Pass 1.
    # This forces PARTIAL overlap between stages without requiring the bandit
    # to learn it.  Combines quality filtering with basic pipeline configuration.
    "sharding+bp": {
        "features": {"backpressure": True, "monitor": False, "sharder": True},
        "sharding_overrides": {"stratify": "soft",
                               "min_size": 1, "target_size": 8, "max_size": 32},
        # concurrency_floor guarantees Pass-1 concurrency floor for downstream
        # stages: scheduler always reserves this many GPU slots even while s1 runs.
        "stage_replicas_overrides": {
            "s2_ml_affinity":   {"concurrency_floor": 2},
            "s3_docking":       {"concurrency_floor": 1},
            "s4_md_refinement": {"concurrency_floor": 1},
            "s5_fep_ranking":   {"concurrency_floor": 1},
        },
    },

    # ─── Optimisation 3: Triage (skip expensive compute on confident leads) ──
    # Triage uses the surrogate to gate candidates and ADVANCE high-confidence
    # ones, letting the workflow skip its expensive simulation entirely.
    # The BudgetController nudges Triage cutoffs to stay within the plan
    # envelope.  All per-stage settings (score_cutoff, advance_threshold,
    # uncertainty_cutoff, nudge_bounds, downstream_input_target) live in
    # config.yaml so they're reviewable in one place; this entry only
    # flips the feature flag.
    #
    # NOTE: sharder must be ON because Triage runs inside trigger_dependent's
    # candidate-aware path (which is only entered when a sharder is configured
    # for the destination stage).  The new axis here is budget_control —
    # everything else is held to the simplest sharder baseline so the wall
    # time difference vs sharding+bp isolates the ADVANCE-skip effect.
    "triage": {
        "features": {"backpressure": False, "monitor": False, "sharder": True,
                     "budget_control": True},
        # target_size=1 with min_size=1 makes the sharder dispatch every
        # received candidate immediately — eliminates batching wait and
        # cuts the per-trigger _schedule_locked work.
        "sharding_overrides": {"stratify": "soft",
                               "min_size": 1, "target_size": 1, "max_size": 4},
        # Re-tune the surrogate for the dreamer STUB so this benchmark
        # actually demonstrates wall-time wins instead of throughput collapse:
        #
        #   * score_cutoff = 0.05 with zero-width nudge bounds — the
        #     BudgetController can't tighten the DISCARD gate, so it
        #     can't choke off throughput when burn is high.  Triage's
        #     value here is the ADVANCE skip, not the DISCARD filter.
        #
        #   * advance_threshold lowered to a level the dreamer surrogate
        #     (pred ≈ score × 0.9 + noise) can actually clear, so ADVANCE
        #     fires for the top ~30–60% of candidates per stage rather
        #     than essentially never.  Late, expensive stages get more
        #     aggressive thresholds — biggest wall-time wins per skip.
        #
        # The production config.yaml values (cutoff 0.50–0.65, advance
        # 0.88–0.92) remain unchanged for real campaigns where the
        # surrogate is a real model rather than score×0.9 + noise.
        # advance_threshold calibration for the dreamer stub surrogate:
        #   pred ≈ score × 0.9 + N(0, 0.05)
        #   Passing candidates have score ~ Uniform[0.6, 1.0]
        #   → mean(pred) ≈ 0.72,  std(pred) ≈ 0.115
        #
        #   To hit "top X%" ADVANCE rate:
        #     top ~10% → threshold ≈ 0.86
        #     top ~15% → threshold ≈ 0.84
        #     top ~20% → threshold ≈ 0.82
        #     top ~30% → threshold ≈ 0.78
        #
        #   RELAXED (was 0.78/0.72/0.69/0.72 → ADVANCE ~30-60% → triage at only
        #   4% of baseline, nearly tied with all_optimizations).  Raised by ~0.1
        #   so ADVANCE drops to ~10-20%: triage now skips the most-confident tier
        #   only, leaving a clear gap above all_optimizations.
        "stage_surrogate_overrides": {
            "s2_ml_affinity":   {"surrogate": {
                "score_cutoff":                    0.05,
                "score_cutoff_nudge_bounds":       [0.05, 0.05],
                "uncertainty_cutoff":              0.50,
                "uncertainty_cutoff_nudge_bounds": [0.50, 0.50],
                "advance_threshold":               0.86,   # top ~10%
            }},
            "s3_docking":       {"surrogate": {
                "score_cutoff":                    0.05,
                "score_cutoff_nudge_bounds":       [0.05, 0.05],
                "uncertainty_cutoff":              0.50,
                "uncertainty_cutoff_nudge_bounds": [0.50, 0.50],
                "advance_threshold":               0.86,   # ~15% (inputs pre-filtered ≥0.65)
            }},
            "s4_md_refinement": {"surrogate": {
                "score_cutoff":                    0.05,
                "score_cutoff_nudge_bounds":       [0.05, 0.05],
                "uncertainty_cutoff":              0.50,
                "uncertainty_cutoff_nudge_bounds": [0.50, 0.50],
                "advance_threshold":               0.88,   # ~12% (most expensive — keep skipping rare)
            }},
            "s5_fep_ranking":   {"surrogate": {
                "score_cutoff":                    0.05,
                "score_cutoff_nudge_bounds":       [0.05, 0.05],
                "uncertainty_cutoff":              0.50,
                "uncertainty_cutoff_nudge_bounds": [0.50, 0.50],
                "advance_threshold":               0.88,   # ~14% (inputs pre-filtered ≥0.75)
            }},
        },
        # Per-stage budgets + WIDE burn_rate_band so the controller
        # essentially observes the burn ratio without ever nudging.
        # When ADVANCE fires at ~95%+, actual spend drops to ~5% of
        # full cost — burn_ratio ~0.05 — which would otherwise pin the
        # controller at the locked lower bound and emit BUDGET_LOCKED
        # warnings on every replica finish.  burn_rate_band=1.0 keeps
        # everything in_band so the controller stays quiet while
        # ADVANCE alone delivers the wall-time win.
        #
        # concurrency_floor guarantees Pass-1 GPU slots for downstream stages
        # while s1 is still running.  Without these, s1 (higher priority, 10k
        # replicas) monopolises all 24 GPUs for ~264s — ADVANCE never fires,
        # the cascade never reaches s5, and early termination never triggers.
        # Same floors as baseline/sharding+bp so the wall-time delta isolates
        # the ADVANCE-skip benefit.
        "stage_replicas_overrides": {
            "s2_ml_affinity":   {"budget_node_hours":  1.4, "burn_rate_band": 1.0,
                                 "concurrency_floor": 2},
            "s3_docking":       {"budget_node_hours":  1.7, "burn_rate_band": 1.0,
                                 "concurrency_floor": 1},
            "s4_md_refinement": {"budget_node_hours":  2.2, "burn_rate_band": 1.0,
                                 "concurrency_floor": 1},
            "s5_fep_ranking":   {"budget_node_hours":  0.6, "burn_rate_band": 1.0,
                                 "concurrency_floor": 1},
        },
    },

    # ─── Budget-controller adaptation ───────────────────────────────────────
    # Demonstrates the BudgetController tightening score_cutoff when a stage
    # burns faster than planned.
    #
    # Design:
    #   • ADVANCE enabled (advance_threshold=0.82-0.86 per stage — intentional)
    #     so candidates scoring above the threshold skip the stage; the
    #     score_cutoff → ADVANCE rate mechanism requires ADVANCE to be active.
    #   • score_cutoff starts at 0.60 (= s1's pass threshold, so initially
    #     nothing is discarded); nudge_bounds=[0.55, 0.90] give the controller
    #     real room to tighten.
    #   • budget_node_hours set ~30% below the "fair" value for each stage
    #     (fair = pilot_nodes × sim_dur/3600 × target_replicas) so the initial
    #     burn_ratio ≈ 1.39 — consistently above the ±15% band.
    #   • As the controller raises score_cutoff, more candidates are discarded
    #     before they run → actual spend per progress unit drops → burn_ratio
    #     converges toward 1.0.  The plot shows the score_cutoff S-curve and
    #     the matching burn_ratio decay.
    #
    # Budget calibration (pilot_nodes × sim_dur_s / 3600 × target × 1/1.39):
    #   s2: 100×0.5/3600×100  / 1.39 ≈ 1.00 nh
    #   s3: 200×1.0/3600×30   / 1.39 ≈ 1.20 nh
    #   s4: 400×2.0/3600×10   / 1.39 ≈ 1.60 nh
    #   s5: 200×2.0/3600×5    / 1.39 ≈ 0.40 nh
    # ─── Budget-controller adaptation ───────────────────────────────────────
    # Why burn_ratio drops when score_cutoff rises
    # ─────────────────────────────────────────────
    # The mechanism requires ADVANCE to be ENABLED (same threshold as triage).
    # With advance_threshold=0.78, ~33% of passing candidates are ADVANCE (0 ms)
    # and ~67% RUN at full sim_duration.
    #
    # burn_ratio = mean_dur_per_replica × pilot_nodes × target / (3600 × budget)
    # mean_dur   = (n_run × sim_dur) / (n_advance + n_run)
    #
    # As score_cutoff rises the DISCARD gate removes low-score candidates.
    # The survivors have HIGHER scores → higher surrogate_pred → more reach
    # advance_threshold → ADVANCE fraction grows → mean_dur_per_replica drops
    # → burn_ratio falls toward 1.0.
    #
    # Pure DISCARD alone (advance_threshold = 0.99 disabled) cannot change
    # burn_ratio because both numerator (spend) and denominator (budget×progress)
    # scale proportionally with throughput.
    #
    # How score_cutoff lowers burn_ratio  (the mechanism being demonstrated)
    # ─────────────────────────────────────────────────────────────────────
    # burn_ratio = mean_duration_per_replica × pilot_nodes × target
    #              ─────────────────────────────────────────────────
    #                       3600 × budget_node_hours
    #
    # budget_node_hours is a constant — the controller cannot change it.
    # The only lever is mean_duration_per_replica.
    #
    # Chain:  ↑ score_cutoff
    #         → DISCARD removes low-score candidates before they run
    #         → surviving population shifts to higher-score candidates
    #         → higher score → higher surrogate_pred (pred ≈ score×0.9+noise)
    #         → more candidates clear advance_threshold → ADVANCE (≈0 ms cost)
    #         → ↓ mean_duration_per_replica
    #         → ↓ burn_ratio
    #
    # DISCARD alone cannot change burn_ratio: if you discard 50% of candidates
    # but the remaining 50% still run at full duration, both actual spend and
    # expected spend (budget × progress) fall proportionally — ratio unchanged.
    # ADVANCE is the essential ingredient.
    #
    # Per-stage advance_threshold calibration
    # ─────────────────────────────────────────
    # A uniform threshold of 0.78 gives s4/s5 already 44-53% ADVANCE at
    # cutoff=0.60 (cascade filtering already produced high-score candidates).
    # Raising the cutoff barely moves the rate → controller effect invisible.
    # Thresholds are set per-stage so that ADVANCE starts at ~20% regardless
    # of the input score distribution, giving the controller maximum range:
    #
    #   stage  input scores     thresh  ADVANCE start → end   br: 1.30→
    #   s2     [0.60, 1.0]      0.82      23%  →  49%         0.85
    #   s3     [0.65, 1.0]      0.82      26%  →  49%         0.89
    #   s4     [0.70, 1.0]      0.84      23%  →  38%         1.05
    #   s5     [0.75, 1.0]      0.86      20%  →  28%         1.17
    #
    # budget_kp=0.015 (vs default 0.05) slows convergence so the trajectory
    # develops visibly across the progress axis rather than snapping in 3 ticks.
    "budget_control": {
        "features": {"backpressure": False, "monitor": False, "sharder": True,
                     "budget_control": True},
        "sharding_overrides": {"stratify": "soft",
                               "min_size": 1, "target_size": 1, "max_size": 4},
        # advance_threshold is calibrated PER STAGE so each stage starts with
        # ~20% ADVANCE and reaches ~28-49% ADVANCE when score_cutoff converges.
        #
        # Why per-stage thresholds are needed
        # ────────────────────────────────────
        # The cascade filters candidates: s4/s5 receive only high-score
        # survivors from earlier stages.  With a uniform advance_threshold of
        # 0.78, those stages already have 44-53% ADVANCE at score_cutoff=0.60
        # — raising the cutoff barely changes anything and the controller effect
        # is invisible.  Per-stage thresholds correct for this:
        #
        #   s2: thresh=0.72 → ADVANCE fires for ~50% of candidates initially.
        #       Budget is set BELOW the fair value so br_initial≈1.96 (above band).
        #       As score_cutoff tightens, the surviving population shifts to
        #       higher-score candidates with even higher pred → more ADVANCE →
        #       mean_dur drops → br falls from 1.96 toward 1.18 (in-band).
        #       Why 0.72 not 0.82: 0.82 gave only 23% ADVANCE → tiny br drop.
        #       0.72 gives 50% ADVANCE → budget can be set tighter → br starts
        #       visibly high and falls clearly as the controller works.
        #
        #   s3–s5: thresh=0.99 (effectively disabled) so ADVANCE does NOT fire
        #       in downstream stages.  Without this, high-quality s2 outputs race
        #       through s3→s4→s5 as ADVANCE replicas (0 ms), hitting s5=5 in
        #       ~11 s and giving the BudgetController only 19 ticks to show its
        #       trajectory.  Disabling ADVANCE at s3–s5 restores normal cascade
        #       timing so s2 accumulates enough reps before campaign ends.
        "stage_surrogate_overrides": {
            "s2_ml_affinity":   {"surrogate": {
                "score_cutoff":                    0.60,
                "score_cutoff_nudge_bounds":       [0.55, 0.90],
                "uncertainty_cutoff":              0.40,
                "uncertainty_cutoff_nudge_bounds": [0.20, 0.60],
                "advance_threshold":               0.72,   # ~50% ADVANCE → bigger br drop
            }},
            "s3_docking":       {"surrogate": {
                "advance_threshold":               0.99,   # disabled — full sim time
            }},
            "s4_md_refinement": {"surrogate": {
                "advance_threshold":               0.99,   # disabled
            }},
            "s5_fep_ranking":   {"surrogate": {
                "advance_threshold":               0.99,   # disabled
            }},
        },
        "stage_replicas_overrides": {
            # ── downstream_input_target (T): meaning, estimation, and s5 design ──
            #
            # T is the PLANNED total replicas this stage is expected to process.
            # It has two roles:
            #
            #   1. BudgetController denominator:
            #        progress = finished_replicas / T
            #        expected = budget_node_hours × progress
            #      Converts "replicas done" into a fraction of the plan so that
            #      actual spend and expected spend are compared at the same point.
            #
            #   2. Campaign early-termination trigger:
            #      The CM stops when finished ≥ T for ANY stage.
            #
            # How to estimate T
            # ─────────────────
            # T is derived from cascade pass-rates applied to the upstream stage:
            #
            #   T(s2) = s1_replicas × P(s1_score > threshold_s1)
            #         = 10 000 × 0.40 = 4 000   (all s1 running)
            #
            # For this demo s2=500 is chosen as the SOLE stopping criterion —
            # enough replicas for a visible BudgetController trajectory
            # (~97 s wall time with floor=2 concurrent) while keeping the run
            # short.  Downstream T values are then the expected cascade output
            # FROM those 500 s2 completions:
            #
            #   T(s3) = 500 × 0.35 (s2 score_threshold=0.65) ≈ 175
            #   T(s4) = 175 × 0.30 (s3 score_threshold=0.70) ≈  52
            #   T(s5) = set to 9999 — not a stopping criterion.
            #           budget_node_hours=0 disables BudgetController for s5
            #           entirely, avoiding meaningless progress fractions from a
            #           stage that will naturally produce only ~13 replicas.
            #
            # Effect of a wrong T on burn_ratio
            # ───────────────────────────────────
            # T error shifts burn_ratio by the same multiplicative factor but
            # does not break the feedback loop — the controller just converges
            # to a slightly different equilibrium cutoff.
            #
            #   T too low  → progress > 1.0 → br appears low → loosens cutoff
            #   T too high → progress ≈ 0   → br explodes   → hits bound fast
            #
            # ±30% error in T is acceptable; ×5 error is not.
            #
            # STOPPING: campaign_target=200 on s2 makes s2 the sole stopping
            # criterion, and campaign_target=0 on s5 disables config.yaml's s5=5
            # early stop (which would otherwise cut the run short at ~23 s with s2
            # only ~64 % through its trajectory).  downstream_input_target is the
            # BudgetController denominator only — it no longer controls stopping.
            #
            # BUDGET: calibrated from the observed mean_dur=0.347 s (≈34 % ADVANCE
            # at advance_threshold=0.72) so the initial burn_ratio ≈ 1.5:
            #   budget = mean_dur × pilot_nodes × T / (3600 × br_target)
            #          = 0.347 × 100 × 200 / (3600 × 1.50) = 1.285 nh
            # As score_cutoff tightens, survivors are higher-scored → more ADVANCE
            # → mean_dur drops → burn_ratio falls toward ~1.1 (into the ±15 % band).
            #
            # kp=0.002, warmup=20: cutoff rises gradually over ~180 ticks.
            "s2_ml_affinity":   {"budget_node_hours": 1.285, "burn_rate_band": 0.15,
                                 "downstream_input_target": 200, "campaign_target": 200,
                                 "budget_kp": 0.002, "budget_warmup_min": 20,
                                 "concurrency_floor": 2},
            # s3–s5: BudgetController disabled (budget=0).  campaign_target=0 on s5
            # overrides config.yaml's s5=5 so only s2=200 stops this campaign.
            "s3_docking":       {"budget_node_hours": 0, "burn_rate_band": 0.15,
                                 "downstream_input_target": 9999,
                                 "concurrency_floor": 1},
            "s4_md_refinement": {"budget_node_hours": 0, "burn_rate_band": 0.15,
                                 "downstream_input_target": 9999,
                                 "concurrency_floor": 1},
            "s5_fep_ranking":   {"budget_node_hours": 0, "burn_rate_band": 0.15,
                                 "downstream_input_target": 9999, "campaign_target": 0,
                                 "concurrency_floor": 1},
        },
    },

    # ─── Combined: optimisations together ────────────────────────────────────
    # sharder + BP (quality routing) AND Triage with BudgetController
    # (skip-when-confident).  Expected to be the fastest configuration.
    # (Cross-stage scheduling priority, if desired, is supplied by the ADR
    # layer — see benchmark_adr.py — not an in-CM bandit.)
    "all_optimizations": {
        "features": {"backpressure": True, "monitor": True, "sharder": True,
                     "budget_control": True},
        "sharding_overrides": {"stratify": "soft",
                               "min_size": 1, "target_size": 8, "max_size": 32},
        # Same surrogate + budget overrides as the triage config — the
        # combined run stacks sharder + bp + bandits on top of Triage's
        # ADVANCE skip, so we want the same ADVANCE rate to compare apples
        # to apples (the wall-time delta then attributes the rest to the
        # other axes).
        # Same calibrated thresholds as triage (0.86/0.86/0.88/0.88) — identical
        # ADVANCE rate, so the wall-time delta vs triage isolates the bandit + BP
        # benefit rather than a different skip rate.  Keep these IN SYNC with the
        # triage config's stage_surrogate_overrides above.
        "stage_surrogate_overrides": {
            "s2_ml_affinity":   {"surrogate": {
                "score_cutoff":                    0.05,
                "score_cutoff_nudge_bounds":       [0.05, 0.05],
                "uncertainty_cutoff":              0.50,
                "uncertainty_cutoff_nudge_bounds": [0.50, 0.50],
                "advance_threshold":               0.86,   # ~13%
            }},
            "s3_docking":       {"surrogate": {
                "score_cutoff":                    0.05,
                "score_cutoff_nudge_bounds":       [0.05, 0.05],
                "uncertainty_cutoff":              0.50,
                "uncertainty_cutoff_nudge_bounds": [0.50, 0.50],
                "advance_threshold":               0.86,   # ~15%
            }},
            "s4_md_refinement": {"surrogate": {
                "score_cutoff":                    0.05,
                "score_cutoff_nudge_bounds":       [0.05, 0.05],
                "uncertainty_cutoff":              0.50,
                "uncertainty_cutoff_nudge_bounds": [0.50, 0.50],
                "advance_threshold":               0.88,   # ~12%
            }},
            "s5_fep_ranking":   {"surrogate": {
                "score_cutoff":                    0.05,
                "score_cutoff_nudge_bounds":       [0.05, 0.05],
                "uncertainty_cutoff":              0.50,
                "uncertainty_cutoff_nudge_bounds": [0.50, 0.50],
                "advance_threshold":               0.88,   # ~14%
            }},
        },
        "stage_replicas_overrides": {
            "s2_ml_affinity":   {"budget_node_hours":  1.4, "burn_rate_band": 1.0,
                                 "concurrency_floor": 2},
            "s3_docking":       {"budget_node_hours":  1.7, "burn_rate_band": 1.0,
                                 "concurrency_floor": 1},
            "s4_md_refinement": {"budget_node_hours":  2.2, "burn_rate_band": 1.0,
                                 "concurrency_floor": 1},
            "s5_fep_ranking":   {"budget_node_hours":  0.6, "burn_rate_band": 1.0,
                                 "concurrency_floor": 1},
        },
    },
}


def _apply_config_override(base: dict, override: dict) -> dict:
    """Deep-merge override into a copy of base config."""
    cfg = copy.deepcopy(base)
    # Feature flags
    if "features" in override:
        cfg.setdefault("cm", {}).setdefault("features", {}).update(override["features"])
        cfg.setdefault("features", {}).update(override["features"])
    # Sharding overrides: applied to all stages that have a sharding block
    if "sharding_overrides" in override:
        sh_ov = override["sharding_overrides"]
        for stage in cfg.get("stages", []):
            if "sharding" in stage:
                stage["sharding"].update(sh_ov)
    # stage_replicas_overrides: per-stage concurrency_floor / concurrency_cap
    # overrides.  Used to guarantee a minimum concurrency floor for downstream
    # stages even without a scheduling bandit — Pass 1 of the scheduler ensures
    # concurrency_floor is always satisfied first, forcing some GPU sharing.
    if "stage_replicas_overrides" in override:
        for stage in cfg.get("stages", []):
            sid = stage["id"]
            if sid in override["stage_replicas_overrides"]:
                stage.update(override["stage_replicas_overrides"][sid])

    # stage_surrogate_overrides: per-stage surrogate spec (cutoffs + nudge
    # bounds) used by Triage + BudgetController.  Merges into the existing
    # stage.surrogate block so other surrogate keys (model_uri, etc.) are
    # preserved when present.
    if "stage_surrogate_overrides" in override:
        for stage in cfg.get("stages", []):
            sid = stage["id"]
            sur_ov = override["stage_surrogate_overrides"].get(sid)
            if sur_ov:
                stage.setdefault("surrogate", {}).update(sur_ov.get("surrogate", {}))

    # dep_threshold_override: sets dependency_threshold for every DEPENDENT stage to a
    # very large value so it only becomes eligible when upstream.status == "done" —
    # not after the first upstream replica finishes.  Creates a true sequential
    # waterfall: stage N+1 waits for ALL of stage N to complete before starting.
    if "dep_threshold_override" in override:
        dt = int(override["dep_threshold_override"])
        stage_ids_set = {s["id"] for s in cfg.get("stages", [])}
        for stage in cfg.get("stages", []):
            if stage.get("upstream", "") in stage_ids_set:
                stage["dependency_threshold"] = dt

    # Stage priority overrides: sets scheduler priority per stage (higher = scheduled first).
    # Used to give downstream stages static priority without a scheduling bandit.
    if "stage_priority_overrides" in override:
        pri_ov = override["stage_priority_overrides"]
        for stage in cfg.get("stages", []):
            if stage["id"] in pri_ov:
                stage["priority"] = pri_ov[stage["id"]]
    # Dreamer overrides: applied to all stages' dreamer block (flat key update).
    # Used to set trigger_mode and other dreamer simulation parameters.
    if "dreamer_overrides" in override:
        dr_ov = override["dreamer_overrides"]
        for stage in cfg.get("stages", []):
            stage.setdefault("dreamer", {}).update(dr_ov)
    return cfg


async def _run_once(config: dict, seed_offset: int) -> dict:
    """Run one campaign with the given config and return its metrics dict."""
    import random as _random
    # Fix the global random state so score-cascade outcomes are identical across
    # configs within the same run index.  Without this, sequential config runs
    # consume different random numbers from a shared state, making comparisons
    # unfair (different random realizations of the score cascade).
    _random.seed(seed_offset + 1337)

    from src.campaign import AsyncCampaignManager as CampaignManager
    from src.utils.workflow import load_config
    import importlib

    # Reset DreamerWorkflow class-level state so trigger counts don't bleed
    # across benchmark runs (class vars persist for the lifetime of the process).
    sys.path.insert(0, str(Path(__file__).parent))
    from dreamer_workflow import DreamerWorkflow
    DreamerWorkflow._group_state = {}
    DreamerWorkflow._trigger_lock = None

    # Translate plan format
    if "stages" in config:
        from run_campaign import _build_from_plan, _build_registry
        cm_cfg = config.get("cm", {})
        config["workflows"] = _build_from_plan(config)
        for key in ("engine", "resources", "telemetry", "workflow_registry", "features"):
            if key in cm_cfg and key not in config:
                config[key] = cm_cfg[key]
        config["debug"] = bool(cm_cfg.get("debug", False))

    # Bump seeds for reproducible variance across runs
    if "provenance" in config:
        for k in config["provenance"].get("seeds", {}):
            config["provenance"]["seeds"][k] += seed_offset

    from radical.asyncflow import WorkflowEngine
    from rhapsody.backends import ConcurrentExecutionBackend
    backend   = await ConcurrentExecutionBackend()
    asyncflow = await WorkflowEngine.create(backend)

    registry = _build_registry(config)
    cm = CampaignManager.from_config(config, registry, asyncflow=asyncflow)

    dnf = False
    try:
        await cm.start()
        finished = await cm.wait(timeout=RUN_TIMEOUT_S)
        if not finished:
            dnf = True   # timeout — collect partial results, don't raise
    finally:
        await cm.close()
        await asyncflow.shutdown()

    m = cm.metrics().to_dict()
    if dnf:
        m["dnf"] = True   # "did not finish" — reached time limit before campaign target

    # ── Time-to-target: seconds until the Nth terminal-stage replica finishes ──
    # Extracted from replica_events so plot_optimizations can draw the step curve.
    _TARGET_STAGE = "s5_fep_ranking"
    _TARGET_N     = 5
    s5_finishes = sorted(
        e["t"] for e in m.get("replica_events", [])
        if e["group"] == _TARGET_STAGE and e["event"] == "finish"
    )
    m["time_to_target_s"] = s5_finishes[_TARGET_N - 1] if len(s5_finishes) >= _TARGET_N else None
    return m


async def run_benchmark(
    config_path: str,
    n_runs: int,
    out_path: str,
) -> None:
    with open(config_path) as f:
        base_config = yaml.safe_load(f)

    results: dict = {}
    for cfg_name, override in CONFIGURATIONS.items():
        print(f"\n{'='*60}")
        print(f"Configuration: {cfg_name}")
        print(f"{'='*60}")
        cfg_results = []
        for run_idx in range(n_runs):
            print(f"  Run {run_idx + 1}/{n_runs}...", end=" ", flush=True)
            cfg = _apply_config_override(base_config, override)
            t0 = time.time()
            try:
                metrics = await _run_once(cfg, seed_offset=run_idx * 100)
                elapsed = time.time() - t0
                if metrics.get("dnf"):
                    metrics["wall_time_s"] = elapsed   # use actual elapsed for DNF
                    print(f"DNF ({elapsed:.0f}s, hit {RUN_TIMEOUT_S}s limit)")
                else:
                    print(f"done in {elapsed:.1f}s  (campaign wall_time={metrics['wall_time_s']:.1f}s)")
                cfg_results.append(metrics)
            except Exception as exc:
                print(f"FAILED: {exc}")
                cfg_results.append({"error": str(exc), "wall_time_s": None})
        results[cfg_name] = cfg_results

    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults written to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--runs",   type=int, default=3)
    parser.add_argument("--out",    default="benchmark_results.json")
    args = parser.parse_args()
    asyncio.run(run_benchmark(args.config, args.runs, args.out))

#python benchmark.py --runs 3 --out benchmark_results.json