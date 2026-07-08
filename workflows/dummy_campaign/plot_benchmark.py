#!/usr/bin/env python3
"""
plot_benchmark.py — benchmark outcome figures for the dummy minimization campaign.

Reads benchmark_results.json produced by benchmark.py and generates 4 PNG
files comparing none / rule / bandit scheduling policies:

  1_wall_time.png     total campaign wall time, per policy
  2_best_score.png    distribution of best scores found, per policy
  3_throughput.png    refine completions and total evaluations, per policy
  4_time_vs_score.png wall time vs best score scatter, per policy

Usage:
    python plot_benchmark.py [--results benchmark_results.json] [--out-dir plots]
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import textwrap
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# ── ADR policy palette ────────────────────────────────────────────────────────

CFG_COLORS = {
    "none":   "#9e9e9e",
    "rule":   "#4caf50",
    "bandit": "#9c27b0",
    "llm":    "#00838f",
}
CFG_DISPLAY = {
    "none":   "no ADR (static)",
    "rule":   "rule",
    "bandit": "bandit",
    "llm":    "llm",
}
_EXCLUDE: set[str] = set()

BASELINE_KEY = "none"   # reference policy for % annotations; falls back to "rule"

WALL_CAPTION = (
    "LOWER IS BETTER.  Total wall-clock time for the campaign to complete, per ADR scheduling "
    "policy.  Bar = median; white dots = individual runs.  % is vs the no-ADR baseline."
)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _cname(cfg: str) -> str:
    return CFG_DISPLAY.get(cfg, cfg)


def _mean(vals):
    valid = [v for v in vals if v is not None]
    return sum(valid) / len(valid) if valid else None


def _median(vals):
    valid = [v for v in vals if v is not None]
    return statistics.median(valid) if valid else None


def _z(v, default=0.0):
    return v if v is not None else default


def _caption(fig, text: str) -> None:
    wrapped = "\n".join(textwrap.wrap(text, width=150)) or text
    fig.text(
        0.5, -0.02, wrapped,
        ha="center", va="top", fontsize=7.5, color="#444",
        bbox=dict(boxstyle="round,pad=0.4", facecolor="#f5f5f5",
                  edgecolor="#ccc", linewidth=0.8),
        transform=fig.transFigure,
    )


# ── Plots ─────────────────────────────────────────────────────────────────────

def plot_wall_time(results: dict, out_dir: Path) -> None:
    cfgs     = [c for c in results.keys() if c not in _EXCLUDE]
    medians  = [_median([r["wall_time_s"] for r in results[c] if r.get("wall_time_s")]) for c in cfgs]
    base_key = BASELINE_KEY if BASELINE_KEY in results else None
    base_runs = results.get(base_key, []) if base_key else []
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
            if have_base and cfg != base_key:
                label += f"\n({(m - baseline) / baseline * 100:+.0f}%)"
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
                    label, ha="center", va="bottom", fontsize=9, fontweight="bold")
    if have_base:
        ax.axhline(baseline, color="gray", linestyle="--", linewidth=0.9,
                   label=f"{_cname(base_key)} median")
        ax.legend(fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels([_cname(c) for c in cfgs], rotation=20, ha="right", fontsize=10)
    ax.set_ylabel("Wall time (s)")
    base_note = f"; % vs {_cname(base_key)}" if have_base else ""
    ax.set_title(f"Campaign wall time by ADR policy  (lower is better{base_note})")
    plt.tight_layout()
    if WALL_CAPTION:
        _caption(fig, WALL_CAPTION)
    plt.savefig(out_dir / "1_wall_time.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  1_wall_time.png")


def plot_best_score(results: dict, out_dir: Path) -> None:
    """Box plot of best_score per policy (lower is better)."""
    cfgs = [c for c in results.keys() if c not in _EXCLUDE]
    data = [[r["best_score"] for r in results[c] if "best_score" in r] for c in cfgs]
    if not any(data):
        return

    fig, ax = plt.subplots(figsize=(8, 5))
    bp = ax.boxplot(data, patch_artist=True, notch=False,
                    medianprops=dict(color="white", linewidth=2))
    for patch, cfg in zip(bp["boxes"], cfgs):
        patch.set_facecolor(CFG_COLORS.get(cfg, "#888"))
        patch.set_alpha(0.85)
    ax.set_xticks(range(1, len(cfgs) + 1))
    ax.set_xticklabels([_cname(c) for c in cfgs], fontsize=12)
    ax.set_ylabel("Best score achieved", fontsize=12)
    ax.set_title("Best score distribution per scheduling policy", fontsize=13)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    _caption(fig,
             "LOWER IS BETTER.  Each box shows the distribution of the lowest score found "
             "across runs for that policy.  The campaign runs a search → refine pipeline: "
             "search generates random candidates; refine improves the most promising ones.  "
             "A lower best score means the policy found a better candidate within the same "
             "number of instances.")
    plt.tight_layout()
    plt.savefig(out_dir / "2_best_score.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  2_best_score.png")


def plot_refine_done(results: dict, out_dir: Path) -> None:
    """Bar chart of avg refine completions and n_evaluated per policy."""
    cfgs = [c for c in results.keys() if c not in _EXCLUDE]
    refine = [_mean([r.get("refine_done", 0) for r in results[c]]) or 0 for c in cfgs]
    n_eval = [_mean([r.get("n_evaluated",  0) for r in results[c]]) or 0 for c in cfgs]

    x     = np.arange(len(cfgs))
    width = 0.35
    fig, ax = plt.subplots(figsize=(8, 5))
    bars1 = ax.bar(x - width / 2, refine, width, label="refine completed",
                   color=[CFG_COLORS.get(c, "#888") for c in cfgs], alpha=0.85)
    bars2 = ax.bar(x + width / 2, n_eval, width, label="total evaluated",
                   color=[CFG_COLORS.get(c, "#888") for c in cfgs], alpha=0.4)
    for bar, v in zip(bars1, refine):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.2,
                f"{v:.1f}", ha="center", va="bottom", fontsize=10)
    for bar, v in zip(bars2, n_eval):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.2,
                f"{v:.1f}", ha="center", va="bottom", fontsize=10)
    ax.set_xticks(x)
    ax.set_xticklabels([_cname(c) for c in cfgs], fontsize=12)
    ax.set_ylabel("Count (avg over runs)", fontsize=12)
    ax.set_title("Throughput per scheduling policy", fontsize=13)
    ax.legend(fontsize=10)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_dir / "3_throughput.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  3_throughput.png")


def plot_score_vs_time(results: dict, out_dir: Path) -> None:
    """Scatter of wall_time_s vs best_score per policy run."""
    cfgs = [c for c in results.keys() if c not in _EXCLUDE]
    fig, ax = plt.subplots(figsize=(8, 5))
    for cfg in cfgs:
        runs   = [r for r in results[cfg] if "best_score" in r and "wall_time_s" in r]
        times  = [r["wall_time_s"] for r in runs]
        scores = [r["best_score"] for r in runs]
        color  = CFG_COLORS.get(cfg, "#888")
        ax.scatter(times, scores, color=color, alpha=0.75, s=60,
                   label=_cname(cfg), zorder=3)
        if times and scores:
            ax.scatter([_median(times)], [_median(scores)],
                       color=color, s=140, marker="D", edgecolors="white",
                       linewidths=1.2, zorder=4)
    ax.set_xlabel("Wall-clock time (s)", fontsize=12)
    ax.set_ylabel("Best score (lower is better)", fontsize=12)
    ax.set_title("Wall time vs best score per policy\n"
                 "(◆ = median; lower-left corner is best)", fontsize=12)
    ax.legend(fontsize=10)
    ax.grid(linestyle="--", alpha=0.3)
    _caption(fig,
             "LOWER-LEFT IS BEST.  Each point is one benchmark run: x = wall-clock seconds "
             "to campaign completion, y = best score found.  A good policy reaches a lower "
             "score in less time.  ◆ marks the median run per policy.")
    plt.tight_layout()
    plt.savefig(out_dir / "4_time_vs_score.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  4_time_vs_score.png")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results", default="benchmark_results.json")
    ap.add_argument("--out-dir", default="plots")
    args = ap.parse_args()

    with open(args.results) as f:
        results = json.load(f)
    results = {k: v for k, v in results.items() if k in CFG_COLORS}
    if not results:
        raise SystemExit(
            f"No ADR policy keys {list(CFG_COLORS)} found in {args.results}. "
            "Did you run benchmark.py?")

    global BASELINE_KEY
    BASELINE_KEY = "none" if "none" in results else "rule"

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Policies: {list(results.keys())}")
    plot_wall_time(results, out_dir)
    plot_best_score(results, out_dir)
    plot_refine_done(results, out_dir)
    plot_score_vs_time(results, out_dir)
    print(f"\nPlots written to {out_dir}/")


if __name__ == "__main__":
    main()
