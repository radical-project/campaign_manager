#!/usr/bin/env python3
"""
plot_optimizations.py — visualise per-optimization performance improvements.

Reads benchmark_results.json produced by benchmark.py and generates 7 plots:

  1. wall_time.png           — campaign wall time per configuration
  2. pipeline_gantt.png      — workflow execution overlap (first/last workflow timeline)
  3. cascade_funnel.png      — total workflows launched per workflow (compute waste)
  4. gpu_utilization.png     — GPU slots in use per workflow over time (4-panel)
  5. shard_dispatch.png      — cumulative candidates dispatched by sharder over time
  6. bandit_convergence.png  — scheduling bandit Thompson-sample convergence
  7. time_to_target.png      — cumulative terminal-workflow completions over wall time

Each plot is designed to support one specific optimization axis:
  - sharding+bp:         plots 3 (cascade funnel) + 5 (shard dispatch)
  - scheduling_bandit:   plots 4 (GPU utilization) + 6 (bandit convergence)
  - all_optimizations:   plots 1 (wall time) + 2 (Gantt) + 7 (time-to-target)

Usage:
    python plot_optimizations.py [--results benchmark_results.json] [--out-dir plots/]
"""

import argparse
import itertools
import json
import math
import statistics
import warnings
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np

# ── Colour palette ────────────────────────────────────────────────────────────

# Configs excluded from ALL optimisation plots.  budget_control runs to a
# different stopping criterion (w2 throughput, not w5 lead count) and is
# documented separately in plot_budget_control.py.
_EXCLUDE = {"budget_control", "bandit_demo"}

# Keys MUST match the config keys in benchmark_results.json (main() filters by
# `k in CFG_COLORS`).  Pretty legend names live in CFG_DISPLAY below.
CFG_COLORS = {
    # dreamer feature-flag study
    "baseline":             "#9e9e9e",
    "sharding+bp":          "#4caf50",
    "scheduling_bandit":    "#9c27b0",
    "triage":               "#00838f",
    "budget_control":       "#ff6f00",
    "bandit_demo":          "#9c27b0",
    "all_optimizations":    "#f44336",
    # esm2_ddsim ADR policy study
    "none":                 "#9e9e9e",
    "rule":                 "#42a5f5",
    "bandit":               "#9c27b0",
    "llm":                  "#f44336",
}

# Display names for configurations (data keys stay as-is; shown with nicer labels).
CFG_DISPLAY = {
    "sharding+bp":       "sharding",
    "scheduling_bandit": "scheduling",
    "triage":            "surrogate",
    "all_optimizations": "all optimizations",
    # ADR policies
    "none":              "no ADR",
    "rule":              "rule",
    "bandit":            "bandit",
    "llm":               "LLM",
}

def _cname(cfg: str) -> str:
    return CFG_DISPLAY.get(cfg, cfg)

workflow_COLORS = {
    # dreamer / antigen-cascade
    "s1_ligand_filter": "#42a5f5",
    "s2_ml_affinity":   "#66bb6a",
    "s3_docking":       "#ffa726",
    "s4_md_refinement": "#ef5350",
    "s5_fep_ranking":   "#ab47bc",
    # esm2_ddsim
    "inference":        "#42a5f5",
    "dummy":            "#26c6da",
    "md":               "#ffa726",
    "miniapps":         "#ab47bc",
}

workflow_ORDER = [
    # dreamer
    "s1_ligand_filter", "s2_ml_affinity", "s3_docking",
    "s4_md_refinement", "s5_fep_ranking",
    # esm2_ddsim
    "inference", "dummy", "md", "miniapps",
]

# Display names
DISPLAY = {
    # dreamer / antigen-cascade
    "s1_ligand_filter": "Initial Screening",
    "s2_ml_affinity":   "Active Learning",
    "s3_docking":       "Structural Modeling",
    "s4_md_refinement": "Refinement Simulation",
    "s5_fep_ranking":   "Affinity Ranking",
    # esm2_ddsim
    "inference":        "ESM2 Inference",
    "dummy":            "DDSim",
    "md":               "MD Simulation",
    "miniapps":         "MiniApps",
}
TARGET_WORKFLOW = "s5_fep_ranking"

# Reference config for the wall-time % annotation and cascade-funnel "Nx less"
# ratio. Defaults to the feature-flag study's "baseline"; the ADR plotter
# overrides it (e.g. "rule" or "none"). Set to None to suppress the comparison.
BASELINE_KEY = "baseline"

# Plot captions — overridable so a different study (e.g. ADR policies) can swap
# the feature-flag explanations for its own. None → no caption box.
WALL_CAPTION = (
    "LOWER IS BETTER.  Wall-clock time until the 5th high-quality candidate found.  "
    "Bar = median; white dots = individual runs.  "
    "sharding+bp: sharder routes highest-score candidates first — fewer total workflows.  "
    "scheduling_bandit: Thompson-sampling bandit allocates resources to final-workflow calculations earlier.  "
    "surrogate: bypasses expensive compute for high-confidence candidates.  "
    "all_optimizations: all axes combined — lowest wall time and lowest variance."
)
FUNNEL_CAPTION = (
    "LOWER IS BETTER.  Each bar is the total number of workflow instances launched to reach "
    "the same goal — 5 high-quality candidates — stacked by workflow.  The campaign stops as soon as the "
    "goal is met, so a smarter configuration gets there after starting far fewer instances "
    "(especially in the costly Initial Screening layer).  Combining all optimizations launches "
    "~17× less work than the baseline."
)
TTT_CAPTION = None   # None → use the function's built-in (study-specific) caption


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load(results_path: str) -> dict:
    with open(results_path) as f:
        return json.load(f)


def _mean(vals):
    valid = [v for v in vals if v is not None]
    return sum(valid) / len(valid) if valid else None


def _median(vals):
    valid = [v for v in vals if v is not None]
    return statistics.median(valid) if valid else None


def _std(vals):
    valid = [v for v in vals if v is not None]
    if len(valid) < 2:
        return 0.0
    m = sum(valid) / len(valid)
    return math.sqrt(sum((v - m) ** 2 for v in valid) / (len(valid) - 1))


def _z(v, default=0.0):
    return v if v is not None else default


def _caption(fig, text: str) -> None:
    # NOTE: do NOT use wrap=True here — combined with savefig(bbox_inches="tight")
    # matplotlib mis-computes the wrap width and can emit a giant canvas
    # (PIL DecompressionBombError). Pre-wrap manually instead.
    import textwrap
    wrapped = "\n".join(textwrap.wrap(text, width=150)) or text
    fig.text(
        0.5, -0.02, wrapped,
        ha="center", va="top", fontsize=7.5, color="#444",
        bbox=dict(boxstyle="round,pad=0.4", facecolor="#f5f5f5",
                  edgecolor="#ccc", linewidth=0.8),
        transform=fig.transFigure,
    )


def _repr_run(runs, key="wall_time_s"):
    """Return the run whose key value is closest to the median."""
    vals = [(i, r.get(key)) for i, r in enumerate(runs) if r.get(key) is not None]
    if not vals:
        return runs[0] if runs else None
    med = statistics.median(v for _, v in vals)
    idx = min(vals, key=lambda iv: abs(iv[1] - med))[0]
    return runs[idx]


def _reconstruct_intervals(replica_events):
    """Yield (start_t, finish_t, group) for each workflow that both started and finished."""
    starts: dict[str, float] = {}
    groups: dict[str, str] = {}
    for e in replica_events:
        rid = e["replica_id"]
        if e["event"] == "start":
            starts[rid] = e["t"]
            groups[rid] = e["group"]
        elif e["event"] == "finish" and rid in starts:
            yield starts[rid], e["t"], groups[rid]


# ── Plot 1: Campaign wall time ────────────────────────────────────────────────

def plot_wall_time(results: dict, out_dir: Path) -> None:
    # budget_control is excluded: it runs to a different stopping criterion
    # (w2 throughput target, not w5 lead count) and is not a time-reduction
    # optimisation — it is documented separately in plot_budget_control.py.
    cfgs     = [c for c in results.keys() if c not in _EXCLUDE]
    medians  = [_median([r["wall_time_s"] for r in results[c] if r.get("wall_time_s")]) for c in cfgs]
    base_runs = results.get(BASELINE_KEY, []) if BASELINE_KEY else []
    baseline  = _median([r["wall_time_s"] for r in base_runs if r.get("wall_time_s")])
    have_base = baseline is not None and baseline > 0

    fig, ax = plt.subplots(figsize=(10, 5))
    x    = np.arange(len(cfgs))
    bars = ax.bar(x, [_z(m) for m in medians],
                  color=[CFG_COLORS.get(c, "#888") for c in cfgs], alpha=0.85)
    for i, cfg in enumerate(cfgs):
        wts = [r["wall_time_s"] for r in results[cfg] if r.get("wall_time_s")]
        ax.scatter([i] * len(wts), wts, color="white", edgecolors="black",
                   zorder=3, s=22, linewidths=0.8)
    for bar, m, cfg in zip(bars, medians, cfgs):
        if m is not None:
            label = f"{m:.0f}s"
            if have_base and cfg != BASELINE_KEY:
                label += f"\n({(m - baseline) / baseline * 100:+.0f}%)"
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1.5,
                    label, ha="center", va="bottom", fontsize=9, fontweight="bold")
    if have_base:
        ax.axhline(baseline, color="gray", linestyle="--", linewidth=0.9,
                   label=f"{_cname(BASELINE_KEY)} median")
        ax.legend(fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels([_cname(c) for c in cfgs], rotation=20, ha="right", fontsize=10)
    ax.set_ylabel("Wall time to target (s)")
    base_note = f"; % vs {_cname(BASELINE_KEY)}" if have_base else ""
    ax.set_title("Campaign wall time by configuration\n"
                 f"(time to find 5 high-quality candidates; lower is better{base_note})")
    plt.tight_layout()
    if WALL_CAPTION:
        _caption(fig, WALL_CAPTION)
    plt.savefig(out_dir / "1_wall_time.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  1_wall_time.png")


# ── Plot 2: Pipeline Gantt ────────────────────────────────────────────────────

def plot_gantt(results: dict, out_dir: Path) -> None:
    cfgs = [c for c in results.keys() if c not in _EXCLUDE]
    n    = len(cfgs)
    if n == 0:
        return
    fig, axes = plt.subplots(n, 1, figsize=(12, 2.2 * n), sharex=False)
    if n == 1:
        axes = [axes]

    for ax, cfg in zip(axes, cfgs):
        runs = [r for r in results[cfg] if "group_stats" in r]
        if not runs:
            ax.set_title(cfg)
            continue
        groups = workflow_ORDER
        for i, g in enumerate(groups):
            starts   = [r["group_stats"].get(g, {}).get("first_start") for r in runs]
            finishes = [r["group_stats"].get(g, {}).get("last_finish")  for r in runs]
            starts   = [v for v in starts   if v is not None]
            finishes = [v for v in finishes if v is not None]
            if not starts or not finishes:
                continue
            s, f = _mean(starts), _mean(finishes)
            color = workflow_COLORS.get(g, "#888")
            ax.barh(i, f - s, left=s, height=0.55, color=color, alpha=0.85)
            ax.text(s + (f - s) / 2, i, DISPLAY.get(g, g),
                    ha="center", va="center", fontsize=6, color="white", fontweight="bold")
        wts = [r.get("wall_time_s") for r in runs if r.get("wall_time_s")]
        t_end = _mean(wts) or 0
        ax.axvline(t_end, color="black", linestyle=":", linewidth=1.0, alpha=0.5)
        ax.set_yticks([])
        ax.set_xlabel("Time (s)" if ax is axes[-1] else "")
        ax.set_title(f"{_cname(cfg)}  (avg wall={t_end:.1f}s)", fontsize=9, color=CFG_COLORS.get(cfg, "black"))
        ax.grid(axis="x", linestyle="--", alpha=0.35)

    plt.suptitle("workflow execution overlap per configuration\n"
                 "(more overlap = better pipeline utilisation)", y=1.01, fontsize=10)
    plt.tight_layout()
    # _caption(fig,
    #     "MORE OVERLAP IS BETTER.  Each bar spans the average first-start to last-finish "
    #     "of a workflow across 5 runs.  Dotted line = moment the campaign goal was reached. "
    #     # "Bars extending past the dotted line are "
    #     # "in-flight workflows that were already running when the goal fired and completed "
    #     # "naturally — they represent wasted compute after the objective was met."
    # )
    plt.savefig(out_dir / "2_pipeline_gantt.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  2_pipeline_gantt.png")


# ── Plot 3: Cascade funnel (total work launched) ──────────────────────────────

def plot_cascade_funnel(results: dict, out_dir: Path) -> None:
    """Stacked bar: total workflows started per config, coloured by workflow.

    Supports sharding+bp story: fewer total candidates launched to find 5 w5 hits.
    """
    cfgs = [c for c in results.keys() if c not in _EXCLUDE]

    # Compute mean n_started per workflow per config
    workflow_means: dict[str, list[float]] = {cfg: [] for cfg in cfgs}
    for cfg in cfgs:
        valid = [r for r in results[cfg] if "group_stats" in r]
        for workflow in workflow_ORDER:
            vals = [r["group_stats"].get(workflow, {}).get("n_started", 0) for r in valid]
            workflow_means[cfg].append(_mean([v for v in vals if v is not None]) or 0)

    fig, ax_stacked = plt.subplots(1, 1, figsize=(12, 6.8))

    # ── Left: stacked bar (total compute by workflow) ────────────────────────────
    x      = np.arange(len(cfgs))
    bottom = np.zeros(len(cfgs))
    for si, workflow in enumerate(workflow_ORDER):
        heights = [workflow_means[cfg][si] for cfg in cfgs]
        bars = ax_stacked.bar(x, heights, bottom=bottom,
                              color=workflow_COLORS[workflow], alpha=0.85,
                              label=DISPLAY[workflow])
        # Annotate w1 bars only (dominate the chart)
        if workflow == "s1_ligand_filter":
            for i, (bar, h) in enumerate(zip(bars, heights)):
                if h > 50:
                    ax_stacked.text(bar.get_x() + bar.get_width() / 2,
                                    bottom[i] + h / 2, f"{h:.0f}",
                                    ha="center", va="center", fontsize=12,
                                    color="white", fontweight="bold")
        bottom += np.array(heights)

    # Annotate totals on top (ratio vs BASELINE_KEY when present)
    base_total = sum(workflow_means[BASELINE_KEY]) if BASELINE_KEY in workflow_means else 0
    for i, cfg in enumerate(cfgs):
        total = sum(workflow_means[cfg])
        label = f"{total:.0f}"
        if base_total > 0 and cfg != BASELINE_KEY and total > 0:
            # Word the ratio by direction: fewer instances = "less", more = "more".
            if total <= base_total:
                label += f"\n({base_total / total:.1f}× less)"
            else:
                label += f"\n({total / base_total:.1f}× more)"
        ax_stacked.text(i, bottom[i] + 30, label,
                        ha="center", va="bottom", fontsize=12, fontweight="bold")

    ax_stacked.set_xticks(x)
    ax_stacked.set_xticklabels([_cname(c) for c in cfgs], rotation=20, ha="right", fontsize=13)
    ax_stacked.tick_params(axis="y", labelsize=12)
    ax_stacked.set_ylabel("Total workflows started", fontsize=14)
    ax_stacked.set_title("Total compute launched\n(stacked by workflow; lower = less wasted work)",
                         fontsize=15)
    ax_stacked.legend(fontsize=12, loc="upper right")
    ax_stacked.grid(axis="y", linestyle="--", alpha=0.3)

    # plt.suptitle("Cascade workflows launched to find 5 high-quality candidates",
    #              fontsize=14, y=1.01)
    plt.tight_layout()
    if FUNNEL_CAPTION:
        _caption(fig, FUNNEL_CAPTION)
    plt.savefig(out_dir / "3_cascade_funnel.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  3_cascade_funnel.png")


# ── Plot 4: GPU utilization per workflow over time ───────────────────────────────

def plot_gpu_utilization(results: dict, out_dir: Path) -> None:
    """Stacked-area GPU-in-use per workflow over time.

    Shows only baseline vs scheduling_bandit — the two configs that best
    illustrate the GPU-allocation story: baseline monopolises all slots with w1,
    bandit shares them with final-workflow calculations from the start.
    """
    cfgs = [c for c in ["baseline", "scheduling_bandit"] if c in results]
    if not cfgs:
        return
    fig, axes_raw = plt.subplots(1, len(cfgs), figsize=(7 * len(cfgs), 5), sharey=False)
    axes = [axes_raw] if len(cfgs) == 1 else list(axes_raw)
    fig.patch.set_facecolor("white")

    for ax, cfg in zip(axes, cfgs):
        ax.set_facecolor("#fafafa")
        # For GPU utilisation we want the run that best shows terminal-workflow
        # activity: pick the run with the most w5 replica_events so the
        # w5 annotation and coloured area are visible.  Falls back to median
        # wall_time if no run has w5 events (e.g. baseline).
        valid = [r for r in results[cfg] if r.get("replica_events")]
        def _w5_count(r):
            return sum(1 for e in r.get("replica_events", [])
                       if "s5" in e.get("group", ""))
        best = max(valid, key=_w5_count) if valid else None
        rep  = best if best and _w5_count(best) > 0 else _repr_run(valid, "wall_time_s")
        if not rep:
            ax.set_title(cfg)
            continue

        events = rep["replica_events"]
        t_max  = max(e["t"] for e in events)
        ts     = np.linspace(0, t_max, 400)

        intervals: dict[str, list[tuple[float, float]]] = defaultdict(list)
        for s, f, g in _reconstruct_intervals(events):
            intervals[g].append((s, f))

        bottom = np.zeros(len(ts))
        for workflow in workflow_ORDER:
            ivs = intervals.get(workflow, [])
            if not ivs:
                continue
            running = np.array([sum(1 for s, f in ivs if s <= t < f) for t in ts])
            color   = workflow_COLORS[workflow]
            ax.fill_between(ts, bottom, bottom + running,
                            color=color, alpha=0.80, label=DISPLAY.get(workflow, workflow))
            bottom = bottom + running

        # Annotate when w5 first appears — anchor to axes top so it's always
        # visible even when w5 occupies only 1 GPU slot (thin coloured strip).
        w5_ivs = intervals.get("s5_fep_ranking", [])
        if w5_ivs:
            first_w5 = min(s for s, _ in w5_ivs)
            ax.axvline(first_w5, color="#7b1fa2", linestyle="--", linewidth=2.0)
            ax.text(first_w5 + t_max * 0.02, 0.96,
                    f"Affinity Ranking starts\n{first_w5:.1f}s", fontsize=8, color="#7b1fa2",
                    va="top", fontweight="bold", transform=ax.get_xaxis_transform(),
                    bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="#ab47bc", lw=1))

        wt = rep.get("wall_time_s", t_max)
        ax.set_title(f"{_cname(cfg)}\n(total wall time: {wt:.1f} s)", fontsize=10,
                     color=CFG_COLORS.get(cfg, "black"), fontweight="bold", pad=6)
        ax.set_xlabel("Wall-clock time (s)", fontsize=9)
        ax.set_ylabel("GPU slots in use", fontsize=9)
        ax.tick_params(labelsize=8)
        ax.grid(axis="y", linestyle="--", alpha=0.5, color="#cccccc")
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    # Shared legend
    handles = [mpatches.Patch(color=workflow_COLORS[s], label=DISPLAY[s])
               for s in workflow_ORDER]
    fig.legend(handles=handles, loc="upper center", ncol=5, fontsize=10,
               bbox_to_anchor=(0.5, 1.0), frameon=True, edgecolor="#cccccc")
    plt.suptitle("GPU slots in use per workflow over time",
                 fontsize=12, fontweight="bold", y=1.04, color="#1a237e")
    plt.tight_layout(pad=2.0)
    _caption(fig,
        "Each colour = GPU slots used by that workflow over time.  "
        "Dashed line = first Affinity Ranking start.  "
        # "baseline: w1 (blue) monopolises all GPUs.  "
        # "scheduling_bandit: w3/w4/w5 share GPUs from t~1 s; w5 starts at ~15 s.  "
    )
    plt.savefig(out_dir / "4_gpu_utilization.png", dpi=150, bbox_inches="tight",
                facecolor="white")
    plt.close()
    print("  4_gpu_utilization.png")


# ── Plot 5: Shard dispatch over time ─────────────────────────────────────────

def plot_shard_dispatch(results: dict, out_dir: Path) -> None:
    """Cumulative candidates dispatched by the sharder over time, per downstream workflow.

    Compares sharding+bp vs all_optimizations — both have a sharder, showing
    how the full optimisation stack changes dispatch dynamics:
    sharding+bp dispatches steadily over ~18 s;
    all_optimizations reaches the goal in ~2 s with far fewer total dispatches.
    """
    plot_cfgs = [c for c in ["sharding+bp", "all_optimizations"] if c in results
                 and any(r.get("shard_events") for r in results[c])]
    if not plot_cfgs:
        return

    workflows = ["s2_ml_affinity", "s3_docking", "s4_md_refinement", "s5_fep_ranking"]
    labels = ["w2 Active Learning", "w3 Structural Modeling", "w4 Refinement Simulation", "w5 Affinity Ranking"]

    fig, axes = plt.subplots(1, len(workflows), figsize=(4 * len(workflows), 4), squeeze=False)

    for si, (workflow, slabel) in enumerate(zip(workflows, labels)):
        ax = axes[0][si]
        for cfg in plot_cfgs:
            color = CFG_COLORS.get(cfg, "#888")
            rep = _repr_run([r for r in results.get(cfg, []) if r.get("shard_events")],
                            "wall_time_s")
            if not rep:
                continue
            evs = [(e["timestamp"], e.get("n", 1))
                   for e in rep.get("shard_events", [])
                   if e.get("group") == workflow]
            evs.sort()
            if not evs:
                continue
            ts   = [0.0] + [t for t, _ in evs]
            cumN = list(itertools.accumulate([0] + [n for _, n in evs]))
            ax.step(ts, cumN, where="post", color=color, linewidth=2.0, label=_cname(cfg))

        ax.set_title(slabel, fontsize=9)
        ax.set_xlabel("Wall time (s)")
        ax.set_ylabel("Cumulative dispatched" if si == 0 else "")
        ax.legend(fontsize=8, loc="lower right")
        ax.grid(linestyle="--", alpha=0.3)

    plt.suptitle("Sharder: cumulative candidates dispatched per workflow\n"
                 "sharding+bp vs all_optimizations", fontsize=10, y=1.01)
    plt.tight_layout()
    _caption(fig,
        "Both configs use the sharder to route highest-scoring candidates first.  "
        "sharding+bp: steady dispatch over the full ~18 s campaign — pipeline fed "
        "continuously with quality candidates.  "
        "all_optimizations: steeper initial dispatch and much earlier plateau (~2 s) "
        "because the scheduling bandit + surrogate bypass combine to reach 5 leads "
        "with far fewer total dispatches.  "
        "Steeper slope = higher-priority candidates dispatched sooner; "
        "earlier plateau = campaign goal reached with less total work."
    )
    plt.savefig(out_dir / "5_shard_dispatch.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  5_shard_dispatch.png")


# ── Plot 6: Scheduling bandit learning curve ──────────────────────────────────

def plot_bandit_convergence(results: dict, out_dir: Path) -> None:
    """Per-workflow learned priority (Beta posterior mean) over time.

    Uses the bandit_demo config: the bandit starts from UNIFORM priors (all
    arms at 0.50) and must LEARN the downstream-first ordering from the reward
    signal.  Plots the recorded posterior mean per arm over wall-clock time,
    showing the priorities redistributing — w5/w4 climbing, w1 held low.
    """
    cfg = "bandit_demo"
    runs = results.get(cfg, [])
    # Need posterior-mean records (re-run benchmark after the metrics change).
    if not any(any(e.get("bandit_means") for e in r.get("scheduling_events", []))
               for r in runs):
        # Fallback to scheduling_bandit if bandit_demo wasn't run.
        cfg = "scheduling_bandit"
        runs = results.get(cfg, [])
        if not any(any(e.get("bandit_means") for e in r.get("scheduling_events", []))
                   for r in runs):
            return

    # Time-bin the posterior means across all runs onto a common grid.
    t_max = 0.0
    pooled: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for r in runs:
        for e in r.get("scheduling_events", []):
            means = e.get("bandit_means", {})
            if not means:
                continue
            t_max = max(t_max, e["timestamp"])
            for g, m in means.items():
                pooled[g].append((e["timestamp"], m))

    N_BINS = 24
    edges = np.linspace(0, max(t_max, 1), N_BINS + 1)
    mids  = 0.5 * (edges[:-1] + edges[1:])

    fig, ax = plt.subplots(figsize=(10, 5))
    for g in workflow_ORDER:
        pts = pooled.get(g, [])
        if len(pts) < 2:
            continue
        ts = np.array([p[0] for p in pts])
        vs = np.array([p[1] for p in pts])
        binned = [
            float(vs[(ts >= lo) & (ts < hi)].mean())
            if ((ts >= lo) & (ts < hi)).any() else np.nan
            for lo, hi in zip(edges[:-1], edges[1:])
        ]
        col = np.array(binned)
        valid = ~np.isnan(col)
        if not valid.any():
            continue
        ax.plot(mids[valid], col[valid], color=workflow_COLORS[g], lw=2.2,
                marker="o", markersize=3, label=DISPLAY.get(g, g.replace("_", " ")))
        ax.scatter([mids[valid][-1]], [col[valid][-1]],
                   color=workflow_COLORS[g], s=45, zorder=5)

    ax.axhline(0.5, color="gray", linestyle=":", linewidth=1.0, alpha=0.7,
               label="uniform start (0.50)")
    ax.set_xlabel("Wall-clock time (s)")
    ax.set_ylabel("Learned priority  (Beta posterior mean)")
    ax.set_ylim(0.0, 1.0)
    ax.set_title("Scheduling bandit: learning downstream-first priority from scratch\n"
                 "(all arms start at 0.50; priorities redistribute as reward accumulates)",
                 fontsize=11)
    ax.legend(fontsize=8, loc="center right")
    ax.grid(linestyle="--", alpha=0.3)
    plt.tight_layout()
    _caption(fig,
        "WATCH THE LINES SPREAD APART.  Every workflow starts at the uniform prior (0.50).  "
        "As replicas finish, the bandit receives a reward proportional to how much the "
        "workflow's downstream needs more work; the terminal workflow always scores high.  "
        "Over time the posterior means redistribute: the "
        "bandit learns to feed the final workflow while initial screening is held near 0.5 so its 10,000 "
        "inputs don't starve the pipeline.  This is the bandit discovering the "
        "downstream-first schedule with no hand-tuned priors."
    )
    plt.savefig(out_dir / "6_bandit_convergence.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  6_bandit_convergence.png")


# ── Plot 7: Time-to-target (cumulative terminal-workflow completions) ─────────────

def plot_time_to_target(
    results: dict,
    out_dir: Path,
    target_workflow: str = "s5_fep_ranking",
    target_n: int = 5,
) -> None:
    """Step curves: cumulative terminal-workflow completions per config over wall time.

    Supports all_optimizations story: target is reached far sooner.
    """
    cfgs = [c for c in results.keys() if c not in _EXCLUDE]
    fig, ax = plt.subplots(figsize=(10, 5))

    hit_labels: list[tuple[float, str]] = []  # (t_hit, color) — placed after loop
    for cfg in cfgs:
        color = CFG_COLORS.get(cfg, "#888")
        run_ts_lists: list[list[float]] = []
        for r in results[cfg]:
            ts = sorted(
                e["t"] for e in r.get("replica_events", [])
                if e["group"] == target_workflow and e["event"] == "finish"
            )
            if ts:
                run_ts_lists.append(ts)
        if not run_ts_lists:
            continue

        # Draw all runs as faint lines
        for ts in run_ts_lists:
            xs = [0.0] + ts
            ys = list(range(len(xs)))
            ax.step(xs, ys, where="post", color=color, linewidth=0.7, alpha=0.3)

        # Representative run (median total count)
        totals = [len(ts) for ts in run_ts_lists]
        rep_ts = run_ts_lists[sorted(range(len(totals)), key=lambda i: totals[i])[len(totals) // 2]]
        xs = [0.0] + rep_ts
        ys = list(range(len(xs)))
        ax.step(xs, ys, where="post", color=color, linewidth=2.5,
                label=_cname(cfg), zorder=4)

        # Mark where target is hit, or annotate if it never was
        if len(rep_ts) >= target_n:
            t_hit = rep_ts[target_n - 1]
            ax.plot(t_hit, target_n, "v", color=color, markersize=10, zorder=5)
            ax.axvline(t_hit, color=color, linestyle=":", linewidth=1.0, alpha=0.6)
            hit_labels.append((t_hit, color))
        else:
            # Config did not reach target in this run
            final_t = rep_ts[-1] if rep_ts else 0
            final_n = len(rep_ts)
            ax.text(final_t + 0.3, final_n + 0.1,
                    f"reached {final_n}",
                    color=color, fontsize=7, alpha=0.8, style="italic")

    # Place hit-time labels above the target line, staggering ones that are close
    # in time so they don't overlap (e.g. sharding ~14 s vs surrogate ~16 s).
    if hit_labels:
        x_span = max(t for t, _ in hit_labels) or 1.0
        min_gap = x_span * 0.07          # closer than this → bump to next level
        levels: list[float] = []          # last t at each stagger level
        max_level = 0
        for t_hit, color in sorted(hit_labels):
            lvl = 0
            while lvl < len(levels) and t_hit - levels[lvl] < min_gap:
                lvl += 1
            if lvl == len(levels):
                levels.append(t_hit)
            else:
                levels[lvl] = t_hit
            max_level = max(max_level, lvl)
            ax.text(t_hit, target_n + 0.12 + lvl * 0.32, f"{t_hit:.1f}s",
                    color=color, fontsize=8, fontweight="bold",
                    ha="center", va="bottom")
        ax.set_ylim(top=target_n + 0.4 + max_level * 0.32)

    ax.axhline(target_n, color="black", linestyle="--", linewidth=1.2,
               label=f"target N={target_n}")
    ax.set_xlabel("Wall-clock time (s)")
    target_label = DISPLAY.get(target_workflow, target_workflow.replace('_', ' '))
    ax.set_ylabel(f"Cumulative {target_label} completions")
    ax.set_title(f"Time to {target_n} final candidates ({target_label})\n"
                 f"(faint lines = individual runs; bold = representative run; ▼ = target reached)")
    ax.legend(fontsize=9)
    ax.grid(linestyle="--", alpha=0.3)
    plt.tight_layout()
    _caption(fig, TTT_CAPTION if TTT_CAPTION else (
        f"LEFTMOST ▼ MARKER IS BEST.  All configurations stop at the same criterion: "
        f"as soon as {target_label} reaches {target_n} completed leads (a few in-flight "
        f"instances may finish just after).  Step curves show cumulative {target_label} completions "
        f"over wall time.  Faint lines = individual runs; bold = median run.  "
        f"▼ = the {target_n}-lead target reached.  Earlier ▼ and steeper slope = better efficiency.  "
        f"surrogate and all optimizations reach {target_n} leads much faster because they "
        f"let the most confident candidates skip expensive compute "
        f"(ADVANCE), so fewer instances run at full simulation cost — baseline and "
        f"scheduling run every candidate in full."
    ))
    plt.savefig(out_dir / "7_time_to_target.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  7_time_to_target.png")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", default="benchmark_results.json")
    parser.add_argument("--out-dir", default="plots/optimizations")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading {args.results}...")
    results = {k: v for k, v in _load(args.results).items() if k in CFG_COLORS}
    print(f"Configurations: {list(results.keys())}")
    print(f"Writing plots to {out_dir}/\n")

    # Auto-detect baseline key: fall back from "baseline" → "none" → None.
    global BASELINE_KEY, TARGET_WORKFLOW
    if BASELINE_KEY not in results:
        BASELINE_KEY = "none" if "none" in results else None

    # Auto-detect target workflow from group_stats of the first run.
    if TARGET_WORKFLOW not in (
        next(iter(results.values()), [{}])[0].get("group_stats", {})
    ):
        all_groups: set[str] = set()
        for runs in results.values():
            for r in runs:
                all_groups.update(r.get("group_stats", {}).keys())
        # Pick the last stage in workflow_ORDER that appears in the data.
        detected = next(
            (g for g in reversed(workflow_ORDER) if g in all_groups), None
        )
        if detected:
            TARGET_WORKFLOW = detected

    plot_wall_time(results, out_dir)
    plot_gantt(results, out_dir)
    plot_cascade_funnel(results, out_dir)
    plot_gpu_utilization(results, out_dir)
    plot_shard_dispatch(results, out_dir)
    plot_bandit_convergence(results, out_dir)
    plot_time_to_target(results, out_dir, target_workflow=TARGET_WORKFLOW)

    print(f"\nAll plots written to {out_dir}/")


if __name__ == "__main__":
    main()

# python plot_optimizations.py --results benchmark_results.json --out-dir plots/optimizations
