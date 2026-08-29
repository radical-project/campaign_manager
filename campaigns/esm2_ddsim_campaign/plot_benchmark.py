#!/usr/bin/env python3
"""
plot_benchmark.py — benchmark outcome figures for the ESM2/DDSim campaign.

Reads benchmark JSON produced by benchmark.py and generates 4 PNG files
comparing none / rule / bandit / llm scheduling policies:

  1_wall_time.png          total campaign wall time, per policy
  2_time_to_miniapps.png   time to first miniapps completion (primary metric)
  3_pipeline_gantt.png     pipeline execution windows per group per policy
  4_throughput.png         group completion counts per policy

Usage:
    python plot_benchmark.py [--results benchmark_adr_gpu.json] [--out-dir plots]
"""

from __future__ import annotations

import argparse
import json
import statistics
import textwrap
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np

# ── ADR policy palette ────────────────────────────────────────────────────────

CFG_COLORS = {
    "none":           "#9e9e9e",
    "rule":           "#4caf50",
    "rule_telemetry": "#2196f3",
    "bandit":         "#9c27b0",
    "llm":            "#00838f",
}
CFG_DISPLAY = {
    "none":           "no ADR (static)",
    "rule":           "rule",
    "rule_telemetry": "rule + telemetry",
    "bandit":         "bandit",
    "llm":            "llm",
}
_EXCLUDE: set[str] = set()

BASELINE_KEY = "none"

# ── Campaign-specific constants ───────────────────────────────────────────────

# Two parallel pipelines:
#   Pipeline A (blue): inference → dummy
#   Pipeline B (green): md → miniapps  ← primary ADR target
GROUP_LABELS = {
    "inference": "ESM2 Inference",
    "dummy": "DDSim",
    "md": "MD Simulation",
    "miniapps": "MiniApps",
}
GROUP_COLORS = {
    "inference": "#1565c0",  # Pipeline A — dark blue
    "dummy": "#64b5f6",  # Pipeline A — light blue
    "md": "#2e7d32",  # Pipeline B — dark green
    "miniapps": "#81c784",  # Pipeline B — light green
}
# Display order within each policy cluster in the Gantt
GROUP_ORDER = ["md", "miniapps", "inference", "dummy"]


# ── Helpers ───────────────────────────────────────────────────────────────────


def _cname(cfg: str) -> str:
    return CFG_DISPLAY.get(cfg, cfg)


def _valid(vals):
    return [v for v in vals if v is not None]


def _mean(vals):
    v = _valid(vals)
    return sum(v) / len(v) if v else None


def _median(vals):
    v = _valid(vals)
    return statistics.median(v) if v else None


def _z(v, default=0.0):
    return v if v is not None else default


def _caption(fig, text: str) -> None:
    wrapped = "\n".join(textwrap.wrap(text, width=150)) or text
    fig.text(
        0.5,
        -0.02,
        wrapped,
        ha="center",
        va="top",
        fontsize=7.5,
        color="#444",
        bbox=dict(boxstyle="round,pad=0.4", facecolor="#f5f5f5", edgecolor="#ccc", linewidth=0.8),
        transform=fig.transFigure,
    )


# ── Plot 1: Wall time ─────────────────────────────────────────────────────────


def plot_wall_time(results: dict, out_dir: Path) -> None:
    cfgs = [c for c in results if c not in _EXCLUDE]
    medians = [
        _median([r["wall_time_s"] for r in results[c] if r.get("wall_time_s")]) for c in cfgs
    ]
    base_runs = results.get(BASELINE_KEY, [])
    baseline = _median([r["wall_time_s"] for r in base_runs if r.get("wall_time_s")])
    have_base = baseline is not None and baseline > 0

    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(len(cfgs))
    bars = ax.bar(
        x, [_z(m) for m in medians], color=[CFG_COLORS.get(c, "#888") for c in cfgs], alpha=0.85
    )
    for i, cfg in enumerate(cfgs):
        wts = [r["wall_time_s"] for r in results[cfg] if r.get("wall_time_s")]
        ax.scatter(
            [i] * len(wts), wts, color="white", edgecolors="black", zorder=3, s=22, linewidths=0.8
        )
    for bar, m, cfg in zip(bars, medians, cfgs):
        if m is not None:
            label = f"{m:.0f}s"
            if have_base and cfg != BASELINE_KEY:
                label += f"\n({(m - baseline) / baseline * 100:+.0f}%)"
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 1.5,
                label,
                ha="center",
                va="bottom",
                fontsize=9,
                fontweight="bold",
            )
    if have_base:
        ax.axhline(
            baseline,
            color="gray",
            linestyle="--",
            linewidth=0.9,
            label=f"{_cname(BASELINE_KEY)} median",
        )
        ax.legend(fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels([_cname(c) for c in cfgs], rotation=20, ha="right", fontsize=10)
    ax.set_ylabel("Wall time (s)")
    base_note = f"; % vs {_cname(BASELINE_KEY)}" if have_base else ""
    ax.set_title(f"Campaign wall time by ADR policy  (lower is better{base_note})")
    plt.tight_layout()
    _caption(
        fig,
        "LOWER IS BETTER.  Total wall-clock time for the full campaign (all four "
        "workflows) to complete.  Bar = median; white dots = individual runs.",
    )
    plt.savefig(out_dir / "1_wall_time.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  1_wall_time.png")


# ── Plot 2: Time to first miniapps ────────────────────────────────────────────


def plot_time_to_miniapps(results: dict, out_dir: Path) -> None:
    """Primary metric: lower = ADR helped Pipeline B get GPU access earlier."""
    cfgs = [c for c in results if c not in _EXCLUDE]
    medians = [
        _median(
            [
                r.get("time_to_first_miniapps_s")
                for r in results[c]
                if r.get("time_to_first_miniapps_s") is not None
            ]
        )
        for c in cfgs
    ]
    base_runs = results.get(BASELINE_KEY, [])
    baseline = _median(
        [
            r.get("time_to_first_miniapps_s")
            for r in base_runs
            if r.get("time_to_first_miniapps_s") is not None
        ]
    )
    have_base = baseline is not None and baseline > 0

    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(len(cfgs))
    bars = ax.bar(
        x, [_z(m) for m in medians], color=[GROUP_COLORS["miniapps"]] * len(cfgs), alpha=0.85
    )
    # policy-colored outline
    for bar, cfg in zip(bars, cfgs):
        bar.set_edgecolor(CFG_COLORS.get(cfg, "#888"))
        bar.set_linewidth(2.5)
    for i, cfg in enumerate(cfgs):
        vals = [
            r.get("time_to_first_miniapps_s")
            for r in results[cfg]
            if r.get("time_to_first_miniapps_s") is not None
        ]
        ax.scatter(
            [i] * len(vals), vals, color="white", edgecolors="black", zorder=3, s=22, linewidths=0.8
        )
    for bar, m, cfg in zip(bars, medians, cfgs):
        if m is not None:
            label = f"{m:.0f}s"
            if have_base and cfg != BASELINE_KEY:
                label += f"\n({(m - baseline) / baseline * 100:+.0f}%)"
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 1.5,
                label,
                ha="center",
                va="bottom",
                fontsize=9,
                fontweight="bold",
            )
    if have_base:
        ax.axhline(
            baseline,
            color="gray",
            linestyle="--",
            linewidth=0.9,
            label=f"{_cname(BASELINE_KEY)} median",
        )
        ax.legend(fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels([_cname(c) for c in cfgs], rotation=20, ha="right", fontsize=10)
    ax.set_ylabel("Time to first MiniApps completion (s)")
    ax.set_title("Time to first MiniApps completion  (lower is better)")
    plt.tight_layout()
    _caption(
        fig,
        "PRIMARY METRIC — LOWER IS BETTER.  Wall-clock seconds from campaign start "
        "to the first MiniApps (Pipeline B) replica finishing.  Under no-ADR, "
        "inference monopolises all pass-2 GPU slots so MiniApps cannot start until "
        "inference exhausts its replicas.  ADR policies detect this and boost "
        "MiniApps priority — reducing time-to-first-result.",
    )
    plt.savefig(out_dir / "2_time_to_miniapps.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  2_time_to_miniapps.png")


# ── Plot 3: Pipeline Gantt ────────────────────────────────────────────────────


def plot_pipeline_gantt(results: dict, out_dir: Path) -> None:
    """Horizontal Gantt showing [first_start, last_finish] per group per policy."""
    cfgs = [c for c in results if c not in _EXCLUDE]
    n_groups = len(GROUP_ORDER)
    gap = 0.6  # vertical gap between policy clusters
    h = 0.7  # bar height

    fig, ax = plt.subplots(figsize=(13, max(4, len(cfgs) * (n_groups * h + gap) + 1)))

    ytick_pos, ytick_lbl = [], []
    for pi, cfg in enumerate(reversed(cfgs)):
        base_y = pi * (n_groups * h + gap)
        runs = [r for r in results[cfg] if "group_stats" in r]
        for gi, group in enumerate(GROUP_ORDER):
            y = base_y + gi * h
            if not runs:
                continue
            t0s = [r["group_stats"].get(group, {}).get("first_start") for r in runs]
            t1s = [r["group_stats"].get(group, {}).get("last_finish") for r in runs]
            t0s = _valid(t0s)
            t1s = _valid(t1s)
            if not t0s or not t1s:
                continue
            t0 = statistics.mean(t0s)
            t1 = statistics.mean(t1s)
            ax.barh(y, t1 - t0, left=t0, height=h * 0.85, color=GROUP_COLORS[group], alpha=0.88)
            mid = t0 + (t1 - t0) / 2
            ax.text(
                mid,
                y,
                GROUP_LABELS[group],
                ha="center",
                va="center",
                fontsize=7.5,
                color="white",
                fontweight="bold",
            )
        cluster_mid = base_y + (n_groups * h) / 2 - h / 2
        ytick_pos.append(cluster_mid)
        ytick_lbl.append(_cname(cfg))

    ax.set_yticks(ytick_pos)
    ax.set_yticklabels(ytick_lbl, fontsize=11)
    ax.set_xlabel("Wall-clock time (s)", fontsize=10)
    ax.set_title(
        "Pipeline Execution Windows per Policy\n"
        "Pipeline A: ESM2 Inference → DDSim  ·  Pipeline B: MD → MiniApps",
        fontweight="bold",
        fontsize=11,
    )
    legend_patches = [
        mpatches.Patch(color=GROUP_COLORS[g], label=GROUP_LABELS[g]) for g in GROUP_ORDER
    ]
    ax.legend(handles=legend_patches, loc="lower right", fontsize=9)
    ax.grid(axis="x", linestyle="--", alpha=0.35)
    plt.tight_layout()
    _caption(
        fig,
        "Each bar spans [first_start, last_finish] for that group, averaged across runs.  "
        "Pipeline B (MD → MiniApps, green) should start earlier under ADR policies "
        "because the ADR agent detects GPU contention and boosts MiniApps priority "
        "over the inference pass-2 slots.",
    )
    plt.savefig(out_dir / "3_pipeline_gantt.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  3_pipeline_gantt.png")


# ── Plot 4: Throughput ────────────────────────────────────────────────────────


def plot_throughput(results: dict, out_dir: Path) -> None:
    """Grouped bar: avg finished replicas per group per policy."""
    cfgs = [c for c in results if c not in _EXCLUDE]
    groups = GROUP_ORDER
    x = np.arange(len(groups))
    width = 0.8 / max(len(cfgs), 1)

    fig, ax = plt.subplots(figsize=(10, 5))
    for i, cfg in enumerate(cfgs):
        runs = [r for r in results[cfg] if "group_stats" in r]
        vals = [
            _mean([r["group_stats"].get(g, {}).get("n_finished") for r in runs]) for g in groups
        ]
        offset = (i - len(cfgs) / 2 + 0.5) * width
        bars = ax.bar(
            x + offset,
            [_z(v) for v in vals],
            width * 0.9,
            label=_cname(cfg),
            color=CFG_COLORS.get(cfg, "#888"),
            alpha=0.85,
        )
        for bar, v in zip(bars, vals):
            if v:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 0.2,
                    f"{v:.0f}",
                    ha="center",
                    va="bottom",
                    fontsize=8,
                )

    ax.set_xticks(x)
    ax.set_xticklabels(
        [GROUP_LABELS.get(g, g) for g in groups], rotation=15, ha="right", fontsize=10
    )
    ax.set_ylabel("Avg replicas completed", fontsize=10)
    ax.set_title("Group throughput per scheduling policy", fontsize=12)
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_dir / "4_throughput.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  4_throughput.png")


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results", default="benchmark_adr_gpu.json")
    ap.add_argument("--out-dir", default="plots")
    args = ap.parse_args()

    with open(args.results) as f:
        results = json.load(f)
    results = {k: v for k, v in results.items() if k in CFG_COLORS}
    if not results:
        raise SystemExit(
            f"No ADR policy keys {list(CFG_COLORS)} found in {args.results}. "
            "Did you run benchmark.py?"
        )

    global BASELINE_KEY
    BASELINE_KEY = "none" if "none" in results else next(iter(results))

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Policies: {list(results.keys())}")
    plot_wall_time(results, out_dir)
    plot_time_to_miniapps(results, out_dir)
    plot_pipeline_gantt(results, out_dir)
    plot_throughput(results, out_dir)
    print(f"\nPlots written to {out_dir}/")


if __name__ == "__main__":
    main()
