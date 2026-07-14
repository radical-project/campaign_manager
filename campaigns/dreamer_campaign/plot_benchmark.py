#!/usr/bin/env python3
"""
plot_benchmark.py — benchmark outcome figures for the Dreamer campaign.

Reads benchmark_results.json produced by benchmark.py and generates 4 PNG
files comparing rule / bandit / llm / none scheduling policies:

  1_wall_time.png        wall time to target, per policy
  2_pipeline_gantt.png   stage execution overlap, per policy
  3_cascade_funnel.png   total instances launched per stage, per policy
  4_time_to_target.png   cumulative terminal-stage completions over wall time

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
    "none": "#9e9e9e",
    "rule": "#4caf50",
    "bandit": "#9c27b0",
    "llm": "#00838f",
}
CFG_DISPLAY = {
    "none": "no ADR (static)",
    "rule": "rule",
    "bandit": "bandit",
    "llm": "llm",
}
_EXCLUDE: set[str] = set()

BASELINE_KEY = "none"  # reference policy for % annotations; falls back to "rule"

WALL_CAPTION = (
    "LOWER IS BETTER.  Wall-clock time until the 5th lead, per ADR scheduling policy.  "
    "Bar = median; white dots = individual runs.  % is vs the no-ADR baseline.  "
    "rule = deterministic downstream-first; bandit = Thompson-sampling as an ADR agent; "
    "llm = LLM-driven (falls back to rule on slow/failed calls)."
)
FUNNEL_CAPTION = (
    "LOWER IS BETTER.  Total workflow instances launched to reach 5 leads, stacked by stage, "
    "per ADR policy.  Fewer = the policy steered the cascade more efficiently."
)
TTT_CAPTION = (
    "LEFTMOST ▼ IS BEST.  Cumulative terminal-stage completions over wall time, per ADR policy.  "
    "Faint = individual runs; bold = median; ▼ = 5-lead target."
)

# ── Stage colours / order ─────────────────────────────────────────────────────

WORKFLOW_COLORS = {
    "s1_ligand_filter": "#42a5f5",
    "s2_ml_affinity": "#66bb6a",
    "s3_docking": "#ffa726",
    "s4_md_refinement": "#ef5350",
    "s5_fep_ranking": "#ab47bc",
    "inference": "#42a5f5",
    "dummy": "#26c6da",
    "md": "#ffa726",
    "miniapps": "#ab47bc",
}
WORKFLOW_ORDER = [
    "s1_ligand_filter",
    "s2_ml_affinity",
    "s3_docking",
    "s4_md_refinement",
    "s5_fep_ranking",
    "inference",
    "dummy",
    "md",
    "miniapps",
]
DISPLAY = {
    "s1_ligand_filter": "Initial Screening",
    "s2_ml_affinity": "Active Learning",
    "s3_docking": "Structural Modeling",
    "s4_md_refinement": "Refinement Simulation",
    "s5_fep_ranking": "Affinity Ranking",
    "inference": "ESM2 Inference",
    "dummy": "DDSim",
    "md": "MD Simulation",
    "miniapps": "MiniApps",
}
TARGET_WORKFLOW = "s5_fep_ranking"


# ── Helpers ───────────────────────────────────────────────────────────────────


def _cname(cfg: str) -> str:
    return CFG_DISPLAY.get(cfg, cfg)


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


def _repr_run(runs, key="wall_time_s"):
    vals = [(i, r.get(key)) for i, r in enumerate(runs) if r.get(key) is not None]
    if not vals:
        return runs[0] if runs else None
    med = statistics.median(v for _, v in vals)
    idx = min(vals, key=lambda iv: abs(iv[1] - med))[0]
    return runs[idx]


# ── Plots ─────────────────────────────────────────────────────────────────────


def plot_wall_time(results: dict, out_dir: Path) -> None:
    cfgs = [c for c in results.keys() if c not in _EXCLUDE]
    medians = [
        _median([r["wall_time_s"] for r in results[c] if r.get("wall_time_s")]) for c in cfgs
    ]
    base_key = BASELINE_KEY if BASELINE_KEY in results else None
    base_runs = results.get(base_key, []) if base_key else []
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
    for bar, m, cfg in zip(bars, medians, cfgs, strict=False):
        if m is not None:
            label = f"{m:.0f}s"
            if have_base and cfg != base_key:
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
            label=f"{_cname(base_key)} median",
        )
        ax.legend(fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels([_cname(c) for c in cfgs], rotation=20, ha="right", fontsize=10)
    ax.set_ylabel("Wall time to target (s)")
    base_note = f"; % vs {_cname(base_key)}" if have_base else ""
    ax.set_title(
        "Campaign wall time by ADR policy\n"
        f"(time to reach terminal stage target; lower is better{base_note})"
    )
    plt.tight_layout()
    if WALL_CAPTION:
        _caption(fig, WALL_CAPTION)
    plt.savefig(out_dir / "1_wall_time.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  1_wall_time.png")


def plot_gantt(results: dict, out_dir: Path) -> None:
    cfgs = [c for c in results.keys() if c not in _EXCLUDE]
    n = len(cfgs)
    if n == 0:
        return
    fig, axes = plt.subplots(n, 1, figsize=(12, 2.2 * n), sharex=False)
    if n == 1:
        axes = [axes]

    for ax, cfg in zip(axes, cfgs, strict=False):
        runs = [r for r in results[cfg] if "group_stats" in r]
        if not runs:
            ax.set_title(cfg)
            continue
        for i, g in enumerate(WORKFLOW_ORDER):
            starts = [r["group_stats"].get(g, {}).get("first_start") for r in runs]
            finishes = [r["group_stats"].get(g, {}).get("last_finish") for r in runs]
            starts = [v for v in starts if v is not None]
            finishes = [v for v in finishes if v is not None]
            if not starts or not finishes:
                continue
            s, f = _mean(starts), _mean(finishes)
            color = WORKFLOW_COLORS.get(g, "#888")
            ax.barh(i, f - s, left=s, height=0.55, color=color, alpha=0.85)
            ax.text(
                s + (f - s) / 2,
                i,
                DISPLAY.get(g, g),
                ha="center",
                va="center",
                fontsize=6,
                color="white",
                fontweight="bold",
            )
        wts = [r.get("wall_time_s") for r in runs if r.get("wall_time_s")]
        t_end = _mean(wts) or 0
        ax.axvline(t_end, color="black", linestyle=":", linewidth=1.0, alpha=0.5)
        ax.set_yticks([])
        ax.set_xlabel("Time (s)" if ax is axes[-1] else "")
        ax.set_title(
            f"{_cname(cfg)}  (avg wall={t_end:.1f}s)",
            fontsize=9,
            color=CFG_COLORS.get(cfg, "black"),
        )
        ax.grid(axis="x", linestyle="--", alpha=0.35)

    plt.suptitle(
        "Stage execution overlap per ADR policy\n(more overlap = better pipeline utilisation)",
        y=1.01,
        fontsize=10,
    )
    plt.tight_layout()
    plt.savefig(out_dir / "2_pipeline_gantt.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  2_pipeline_gantt.png")


def plot_cascade_funnel(results: dict, out_dir: Path) -> None:
    cfgs = [c for c in results.keys() if c not in _EXCLUDE]
    workflow_means: dict[str, list[float]] = {cfg: [] for cfg in cfgs}
    for cfg in cfgs:
        valid = [r for r in results[cfg] if "group_stats" in r]
        for workflow in WORKFLOW_ORDER:
            vals = [r["group_stats"].get(workflow, {}).get("n_started", 0) for r in valid]
            workflow_means[cfg].append(_mean([v for v in vals if v is not None]) or 0)

    fig, ax = plt.subplots(figsize=(12, 6.8))
    x = np.arange(len(cfgs))
    bottom = np.zeros(len(cfgs))
    base_key = BASELINE_KEY if BASELINE_KEY in workflow_means else None
    for si, workflow in enumerate(WORKFLOW_ORDER):
        heights = [workflow_means[cfg][si] for cfg in cfgs]
        bars = ax.bar(
            x,
            heights,
            bottom=bottom,
            color=WORKFLOW_COLORS[workflow],
            alpha=0.85,
            label=DISPLAY[workflow],
        )
        if workflow == "s1_ligand_filter":
            for i, (bar, h) in enumerate(zip(bars, heights, strict=False)):
                if h > 50:
                    ax.text(
                        bar.get_x() + bar.get_width() / 2,
                        bottom[i] + h / 2,
                        f"{h:.0f}",
                        ha="center",
                        va="center",
                        fontsize=12,
                        color="white",
                        fontweight="bold",
                    )
        bottom += np.array(heights)

    base_total = sum(workflow_means[base_key]) if base_key else 0
    for i, cfg in enumerate(cfgs):
        total = sum(workflow_means[cfg])
        label = f"{total:.0f}"
        if base_total > 0 and cfg != base_key and total > 0:
            if total <= base_total:
                label += f"\n({base_total / total:.1f}× less)"
            else:
                label += f"\n({total / base_total:.1f}× more)"
        ax.text(i, bottom[i] + 30, label, ha="center", va="bottom", fontsize=12, fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels([_cname(c) for c in cfgs], rotation=20, ha="right", fontsize=13)
    ax.tick_params(axis="y", labelsize=12)
    ax.set_ylabel("Total workflows started", fontsize=14)
    ax.set_title(
        "Total compute launched per ADR policy\n(stacked by stage; lower = less wasted work)",
        fontsize=15,
    )
    ax.legend(fontsize=12, loc="upper right")
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    plt.tight_layout()
    if FUNNEL_CAPTION:
        _caption(fig, FUNNEL_CAPTION)
    plt.savefig(out_dir / "3_cascade_funnel.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  3_cascade_funnel.png")


def plot_time_to_target(
    results: dict,
    out_dir: Path,
    target_workflow: str = "s5_fep_ranking",
    target_n: int = 5,
) -> None:
    cfgs = [c for c in results.keys() if c not in _EXCLUDE]
    fig, ax = plt.subplots(figsize=(10, 5))

    hit_labels: list[tuple[float, str]] = []
    for cfg in cfgs:
        color = CFG_COLORS.get(cfg, "#888")
        run_ts_lists: list[list[float]] = []
        for r in results[cfg]:
            ts = sorted(
                e["t"]
                for e in r.get("replica_events", [])
                if e["group"] == target_workflow and e["event"] == "finish"
            )
            if ts:
                run_ts_lists.append(ts)
        if not run_ts_lists:
            continue

        for ts in run_ts_lists:
            xs = [0.0] + ts
            ys = list(range(len(xs)))
            ax.step(xs, ys, where="post", color=color, linewidth=0.7, alpha=0.3)

        totals = [len(ts) for ts in run_ts_lists]
        rep_ts = run_ts_lists[sorted(range(len(totals)), key=lambda i: totals[i])[len(totals) // 2]]
        xs = [0.0] + rep_ts
        ys = list(range(len(xs)))
        ax.step(xs, ys, where="post", color=color, linewidth=2.5, label=_cname(cfg), zorder=4)

        if len(rep_ts) >= target_n:
            t_hit = rep_ts[target_n - 1]
            ax.plot(t_hit, target_n, "v", color=color, markersize=10, zorder=5)
            ax.axvline(t_hit, color=color, linestyle=":", linewidth=1.0, alpha=0.6)
            hit_labels.append((t_hit, color))
        else:
            final_t = rep_ts[-1] if rep_ts else 0
            final_n = len(rep_ts)
            ax.text(
                final_t + 0.3,
                final_n + 0.1,
                f"reached {final_n}",
                color=color,
                fontsize=7,
                alpha=0.8,
                style="italic",
            )

    if hit_labels:
        x_span = max(t for t, _ in hit_labels) or 1.0
        min_gap = x_span * 0.07
        levels: list[float] = []
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
            ax.text(
                t_hit,
                target_n + 0.12 + lvl * 0.32,
                f"{t_hit:.1f}s",
                color=color,
                fontsize=8,
                fontweight="bold",
                ha="center",
                va="bottom",
            )
        ax.set_ylim(top=target_n + 0.4 + max_level * 0.32)

    ax.axhline(target_n, color="black", linestyle="--", linewidth=1.2, label=f"target N={target_n}")
    ax.set_xlabel("Wall-clock time (s)")
    target_label = DISPLAY.get(target_workflow, target_workflow.replace("_", " "))
    ax.set_ylabel(f"Cumulative {target_label} completions")
    ax.set_title(
        f"Time to {target_n} final candidates ({target_label})\n"
        "(faint = individual runs; bold = median; ▼ = target reached)"
    )
    ax.legend(fontsize=9)
    ax.grid(linestyle="--", alpha=0.3)
    plt.tight_layout()
    _caption(fig, TTT_CAPTION)
    plt.savefig(out_dir / "4_time_to_target.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  4_time_to_target.png")


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
            "Did you run benchmark.py?"
        )

    global BASELINE_KEY
    BASELINE_KEY = "none" if "none" in results else "rule"

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Policies: {list(results.keys())}")
    plot_wall_time(results, out_dir)
    plot_gantt(results, out_dir)
    plot_cascade_funnel(results, out_dir)
    plot_time_to_target(results, out_dir)
    print(f"\nPlots written to {out_dir}/")


if __name__ == "__main__":
    main()
