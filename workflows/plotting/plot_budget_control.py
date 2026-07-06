"""Narrative plot for the two Triage / BudgetController mechanisms.

Three-panel story (wall-time is in plot_optimizations.py / 1_wall_time.png):

  Left   — ADVANCE skips per workflow (skip-workflow mechanism).
            Counts sub-50ms workflow durations in triage and all_optimizations.
            Shows where the surrogate is confident enough to bypass computation.

  Centre — Controller cutoff adaptation (threshold-adjustment mechanism).
            Solid lines  = budget_control config: score_cutoff RISES as the
            BudgetController reacts to over-budget burn.
            Dashed lines = triage config: score_cutoff stays flat (zero-width
            nudge bounds + wide burn_rate_band mean ADVANCE alone delivers savings).
            Contrasting the two shows both mechanisms at a glance.

  Right  — Burn-ratio convergence (did the controller work?).
            budget_control only.  Lines converging toward 1.0 confirm the
            controller successfully slowed spend to match the plan envelope.

Usage
-----
    python plot_budget_control.py
        --results benchmark_results.json
        --out     plots/diagrams/budget_control_illustration.png
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from statistics import mean, median
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


# ── Visual constants ─────────────────────────────────────────────────────────

_BG   = "#F4F6FB"
_NAVY = "#1A237E"
_GRAY = "#666666"
_BAND = "#9aa5b1"

_CONFIG_COLOR = {
    "triage":            "#00838F",
    "budget_control":    "#FF6F00",
    "all_optimizations": "#F44336",
}

_workflow_COLOR = {
    "s2_ml_affinity":   "#66bb6a",
    "s3_docking":       "#ffa726",
    "s4_md_refinement": "#ef5350",
    "s5_fep_ranking":   "#ab47bc",
}
_workflowS = list(_workflow_COLOR)
_workflow_LABELS = {"s2_ml_affinity": "s2", "s3_docking": "s3",
                 "s4_md_refinement": "s4", "s5_fep_ranking": "s5"}


# ── Data helpers ──────────────────────────────────────────────────────────────

def _advance_counts(runs: list[dict], skip_dur_s: float = 0.05) -> dict[str, int]:
    """Count workflows whose duration < skip_dur_s (= ADVANCE short-circuits)."""
    by_workflow: dict[str, int] = defaultdict(int)
    for r in runs:
        for ev in r.get("replica_events", []):
            if ev["event"] != "finish":
                continue
            if (ev.get("dur") or 0.0) < skip_dur_s:
                by_workflow[ev["group"]] += 1
    return dict(by_workflow)


def _budget_trajectories(
    runs: list[dict],
    field: str,
    progress_grid: np.ndarray,
) -> dict[str, np.ndarray]:
    """Median trajectory per workflow interpolated onto a common progress grid."""
    per_workflow_runs: dict[str, list[list[tuple[float, float]]]] = defaultdict(list)
    for r in runs:
        per_workflow: dict[str, list[tuple[float, float]]] = defaultdict(list)
        for ev in r.get("budget_events", []):
            per_workflow[ev["stage_id"]].append((ev["progress"], ev[field]))
        for sid, pts in per_workflow.items():
            pts.sort()
            per_workflow_runs[sid].append(pts)

    out: dict[str, np.ndarray] = {}
    for sid, all_runs in per_workflow_runs.items():
        interp = []
        for pts in all_runs:
            if len(pts) < 2:
                continue
            xs = np.array([p[0] for p in pts])
            ys = np.array([p[1] for p in pts])
            interp.append(np.interp(progress_grid, xs, ys))
        if interp:
            out[sid] = np.median(np.vstack(interp), axis=0)
    return out


# ── Panel builders ────────────────────────────────────────────────────────────

def _panel_advance(ax, results: dict) -> None:
    """Grouped bar: ADVANCE skip counts for triage and all_optimizations."""
    cfg_keys = [("triage", "Triage — ADVANCE"),
                ("all_optimizations", "All optimisations")]
    width = 0.36
    xs = np.arange(len(_workflowS))

    totals: dict[tuple[str, str], int] = defaultdict(int)
    for cfg_name, _ in cfg_keys:
        for r in results.get(cfg_name, []):
            for ev in r.get("replica_events", []):
                if ev["event"] == "finish":
                    totals[(cfg_name, ev["group"])] += 1

    for i, (cfg_name, label) in enumerate(cfg_keys):
        counts = _advance_counts(results.get(cfg_name, []))
        ys = [counts.get(s, 0) for s in _workflowS]
        x_off = xs + (i - 0.5) * width
        ax.bar(x_off, ys, width=width,
               color=_CONFIG_COLOR.get(cfg_name, "#888"), label=label,
               edgecolor=_NAVY, linewidth=0.8)
        ymax = max(ys + [1])
        for x, y, s in zip(x_off, ys, _workflowS):
            if y > 0:
                tot = totals.get((cfg_name, s), 0)
                rate = f"\n({100*y//max(tot,1)}%)" if tot else ""
                ax.text(x, y + 0.04 * ymax, f"{y}{rate}",
                        ha="center", fontsize=7, color=_NAVY)

    ax.set_xticks(xs)
    ax.set_xticklabels([_workflow_LABELS[s] for s in _workflowS], fontsize=10)
    ax.set_ylabel("ADVANCE skips  (total across all runs)", fontsize=10)
    ax.set_title("Mechanism 1 — surrogate skips expensive compute\n"
                 "when it is confident enough about the candidate",
                 fontsize=11, fontweight="bold", color=_NAVY, pad=4)
    ax.legend(loc="upper left", fontsize=9, framealpha=0.92)
    ax.grid(True, alpha=0.25, axis="y", linewidth=0.5)
    ax.tick_params(labelsize=9)


def _panel_cutoff(ax, results: dict) -> None:
    """score_cutoff trajectories for budget_control config only.

    Triage lines are omitted: triage uses score_cutoff_nudge_bounds=[0.05,0.05]
    so the cutoff is frozen at 0.05 throughout, producing a flat invisible line
    that clutters the legend without adding information.
    """
    progress = np.linspace(0.0, 1.0, 100)
    bc_traj  = _budget_trajectories(results.get("budget_control", []),
                                    "score_cutoff", progress)

    if not bc_traj:
        ax.text(0.5, 0.5,
                "No budget_events found.\n"
                "Run with features.budget_control: true.",
                ha="center", va="center", fontsize=10, color=_GRAY,
                transform=ax.transAxes)
        ax.set_axis_off()
        return

    for sid in _workflowS:
        if sid not in bc_traj:
            continue
        color = _workflow_COLOR[sid]
        lbl   = _workflow_LABELS[sid]
        ys    = bc_traj[sid]
        ax.plot(progress, ys, color=color, lw=2.4, label=lbl)
        ax.scatter([progress[0]],  [ys[0]],  color=color, s=50, marker="o", zorder=5)
        ax.scatter([progress[-1]], [ys[-1]], color=color, s=50, marker="s", zorder=5)

    ax.set_xlabel("Progress  (finished / target)", fontsize=10)
    ax.set_ylabel("score_cutoff", fontsize=10)
    ax.set_title("BudgetController raises score_cutoff when over-budget\n"
                 "↑ cutoff → fewer, higher-quality candidates run → lower cost per workflow",
                 fontsize=11, fontweight="bold", color=_NAVY, pad=4)
    ax.set_xlim(0, 1.0)
    ax.set_ylim(0.55, 1.0)
    ax.legend(loc="lower right", fontsize=9, framealpha=0.92)
    ax.grid(True, alpha=0.25, linewidth=0.5)
    ax.tick_params(labelsize=9)
    ax.text(0.01, 0.01, "● start   ■ end   (s2 only — downstream workflows have pre-filtered inputs\n"
            "  with near-constant surrogate predictions; mechanism has no range there)",
            transform=ax.transAxes, ha="left", va="bottom",
            fontsize=7, color=_GRAY, style="italic")


def _panel_burn(ax, results: dict) -> None:
    """burn_ratio convergence for budget_control; shows plan envelope."""
    progress = np.linspace(0.0, 1.0, 100)
    bc_traj  = _budget_trajectories(results.get("budget_control", []),
                                    "burn_ratio", progress)

    band = 0.15
    ax.axhline(1.0, color=_GRAY, linestyle="--", lw=1.2, label="plan  (br = 1.0)")
    ax.fill_between(progress, 1 - band, 1 + band, alpha=0.18, color=_BAND,
                    label=f"±{int(band*100)}% acceptable band")

    if bc_traj:
        for sid in _workflowS:
            if sid not in bc_traj:
                continue
            color = _workflow_COLOR[sid]
            ax.plot(progress, bc_traj[sid], color=color, lw=2.4,
                    label=_workflow_LABELS[sid])
    else:
        ax.text(0.5, 0.5,
                "No budget_events for budget_control config.\n"
                "Run: python benchmark.py --runs 3",
                ha="center", va="center", fontsize=10, color=_GRAY,
                transform=ax.transAxes)

    ymax = (max([2.5] + [float(np.nanmax(v)) for v in bc_traj.values()])
            if bc_traj else 2.5)
    ax.set_xlim(0, 1.0)
    ax.set_ylim(0, min(3.5, ymax * 1.1))
    ax.set_xlabel("Progress", fontsize=10)
    ax.set_ylabel("burn_ratio  (actual / plan)", fontsize=10)
    ax.set_title("Spend converges toward plan as cutoff tightens\n"
                 "only high-quality candidates run → shorter avg compute → burn_ratio ↓",
                 fontsize=11, fontweight="bold", color=_NAVY, pad=4)
    ax.legend(loc="upper right", fontsize=9, framealpha=0.92)
    ax.grid(True, alpha=0.25, linewidth=0.5)
    ax.tick_params(labelsize=9)


# ── Top-level ─────────────────────────────────────────────────────────────────

def make_plot(results_path: Path, out_path: Path) -> None:
    with open(results_path) as f:
        results = json.load(f)

    # Stacked vertically: both panels share the same progress x-axis, and a
    # taller aspect ratio fits the presentation's left-column image box without
    # being stretched (the old side-by-side layout was 2.7:1 and blurred when
    # forced into a ~1.4:1 slide box).
    fig, axes = plt.subplots(2, 1, figsize=(9, 8.5), sharex=True)
    fig.patch.set_facecolor(_BG)
    for ax in axes:
        ax.set_facecolor("#FFFFFF")

    _panel_cutoff(axes[0], results)
    _panel_burn  (axes[1], results)

    fig.suptitle(
        "BudgetController — score_cutoff adapts to bring spend back to plan",
        fontsize=13, fontweight="bold", color=_NAVY, y=1.0,
    )
    # Boxed caption at the bottom — same style as the other optimisation plots
    # (plot_optimizations._caption): small grey text in a rounded light box.
    fig.text(
        0.5, -0.02,
        "Top: BudgetController raises score_cutoff when a workflow burns over-budget — "
        "the tighter gate admits only higher-quality candidates.   "
        "Bottom: fewer, higher-quality candidates run faster on average, so actual "
        "spend converges toward the plan envelope (burn_ratio → 1.0).",
        ha="center", va="top", fontsize=7.5, color="#444", wrap=True,
        bbox=dict(boxstyle="round,pad=0.4", facecolor="#f5f5f5",
                  edgecolor="#ccc", linewidth=0.8),
        transform=fig.transFigure,
    )
    plt.tight_layout(rect=(0, 0, 1, 0.97))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=150, bbox_inches="tight", facecolor=_BG)
    plt.close()
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", type=Path,
                        default=Path(__file__).parent / "benchmark_results.json")
    parser.add_argument("--out", type=Path,
                        default=Path(__file__).parent / "plots" / "diagrams"
                                / "budget_control_illustration.png")
    args = parser.parse_args()
    if not args.results.exists():
        print(f"Results not found: {args.results}")
        print("Run:  python benchmark.py --runs 3 --out benchmark_results.json")
        raise SystemExit(1)
    make_plot(args.results, args.out)
