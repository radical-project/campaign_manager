#!/usr/bin/env python3
"""
Plot individual replica start/end times for each ADR policy.

Each horizontal bar = one replica (start → end wall-clock seconds).
Replicas are grouped by workflow category and ordered by start time
within each group.  One panel per policy.

Reads:  benchmark_adr_gpu.json  (default, --input to override)
Writes: plots/replica_timeline.png  (default, --out to override)

Usage:
    python plot_replica_timeline.py
    python plot_replica_timeline.py --input benchmark_adr_gpu.json --out plots/rt.png
    python plot_replica_timeline.py --policies none rule
"""

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.transforms as mtransforms
import numpy as np

# ── Appearance ────────────────────────────────────────────────────────────────

GROUP_COLORS = {
    "inference": "#4C72B0",   # blue
    "md":        "#C44E52",   # red
    "miniapps":  "#55A868",   # green
    "dummy":     "#DD8452",   # orange
}
GROUP_ORDER  = ["inference", "md", "miniapps", "dummy"]
GROUP_LABELS = {
    "inference": "Inference (ESM2)",
    "md":        "MD Simulation",
    "miniapps":  "MiniApps (ML)",
    "dummy":     "Dummy (Scoring)",
}
DEFAULT_COLOR = "#9E9E9E"
BAR_HEIGHT    = 0.75
GROUP_GAP     = 2.0   # blank Y-rows separating groups


# ── Data helpers ──────────────────────────────────────────────────────────────

def parse_replicas(run: dict) -> dict:
    """Build {replica_id: {start, end, group, ok, dur}} from replica_events."""
    starts, ends, groups = {}, {}, {}
    for ev in run.get("replica_events", []):
        rid = ev["replica_id"]
        groups[rid] = ev["group"]
        if ev["event"] == "start":
            starts[rid] = float(ev["t"])
        elif ev["event"] == "finish":
            ends[rid] = float(ev["t"])
    return {
        rid: {
            "start": starts[rid],
            "end":   ends.get(rid, starts[rid]),
            "group": groups.get(rid, "unknown"),
            "ok":    rid in ends,
            "dur":   ends[rid] - starts[rid] if rid in ends else 0.0,
        }
        for rid in starts
    }


def assign_y(replicas: dict) -> tuple[dict, dict]:
    """
    Assign a Y coordinate to each replica.

    Returns
    -------
    y_map        : {replica_id: y_float}
    group_spans  : {group_name: (y_top, y_bottom)}  (top < bottom; y inverted)
    """
    # Collect replicas per group, sorted by numeric index in replica_id
    by_group: dict[str, list[tuple[int, str]]] = {}
    for rid, info in replicas.items():
        grp = info["group"]
        try:
            idx = int(rid.rsplit("_", 1)[-1])
        except (ValueError, IndexError):
            idx = len(by_group.get(grp, []))
        by_group.setdefault(grp, []).append((idx, rid))

    for grp in by_group:
        by_group[grp].sort(key=lambda x: x[0])

    # Canonical order first, then any extras alphabetically
    present   = [g for g in GROUP_ORDER if g in by_group]
    remaining = sorted(set(by_group) - set(GROUP_ORDER))

    y_map: dict[str, float] = {}
    group_spans: dict[str, tuple[float, float]] = {}
    y = 0.0

    for grp in present + remaining:
        items = by_group[grp]
        y_start = y
        for _, rid in items:
            y_map[rid] = y
            y += 1.0
        group_spans[grp] = (y_start, y - 1.0)
        y += GROUP_GAP

    return y_map, group_spans


# ── Panel drawing ─────────────────────────────────────────────────────────────

def draw_panel(
    ax,
    replicas: dict,
    title: str,
    wall_time: float | None = None,
    group_stats: dict | None = None,
) -> None:
    """Draw one policy panel onto *ax*."""
    y_map, group_spans = assign_y(replicas)
    gs = group_stats or {}

    # ── Replica bars ──────────────────────────────────────────────────────────
    for rid, info in replicas.items():
        if rid not in y_map:
            continue
        start = info["start"]
        dur   = max(info["end"] - start, 0.5)   # minimum width for zero-dur
        color = GROUP_COLORS.get(info["group"], DEFAULT_COLOR)
        hatch = "///" if not info["ok"] else None

        ax.barh(
            y_map[rid], dur, left=start,
            height=BAR_HEIGHT,
            color=color, alpha=0.82,
            edgecolor="white", linewidth=0.25,
            hatch=hatch,
        )

    # ── Group background bands + Y-axis labels ────────────────────────────────
    ytick_pos, ytick_lbl = [], []
    for grp, (y_top, y_bot) in group_spans.items():
        color = GROUP_COLORS.get(grp, DEFAULT_COLOR)

        # Shaded band behind the group
        ax.axhspan(y_top - 0.5, y_bot + 0.5, color=color, alpha=0.06, lw=0)

        # Horizontal separator line above the group (except the first)
        if y_top > 0:
            ax.axhline(y_top - (GROUP_GAP / 2 + 0.5),
                       color="#cccccc", lw=0.8, ls="-", zorder=0)

        # Y tick at group midpoint
        mid = (y_top + y_bot) / 2
        ytick_pos.append(mid)
        n_rep = int(y_bot - y_top + 1)
        lbl = GROUP_LABELS.get(grp, grp)

        # Annotate with group stats when available
        gst = gs.get(grp, {})
        if gst:
            mean_dur = gst.get("mean_dur_s", 0)
            ytick_lbl.append(f"{lbl}\nn={n_rep}  μ={mean_dur:.0f}s")
        else:
            ytick_lbl.append(f"{lbl}\nn={n_rep}")

    ax.set_yticks(ytick_pos)
    ax.set_yticklabels(ytick_lbl, fontsize=7.5, fontweight="bold", va="center")
    ax.tick_params(axis="y", length=0)   # hide tick marks; labels carry the info

    # ── Mean-duration annotation on each bar group ────────────────────────────
    # Draw a thin vertical dashed line at the group mean end time
    for grp, (y_top, y_bot) in group_spans.items():
        gst = gs.get(grp, {})
        if not gst:
            continue
        mean_dur = gst.get("mean_dur_s", 0)
        # Compute mean start time from the replica data for this group
        grp_starts = [
            info["start"]
            for info in replicas.values()
            if info["group"] == grp and info["ok"]
        ]
        if grp_starts:
            # Show a bracket spanning the group's first-start to last-end
            first_start = min(grp_starts)
            grp_ends = [
                info["end"]
                for info in replicas.values()
                if info["group"] == grp and info["ok"]
            ]
            last_end = max(grp_ends) if grp_ends else first_start
            # Annotate span at top of group
            ax.annotate(
                f"{last_end - first_start:.0f}s span",
                xy=(last_end, y_top - 0.4),
                fontsize=6, color=GROUP_COLORS.get(grp, DEFAULT_COLOR),
                ha="right", va="top",
            )

    # ── Wall-time marker ──────────────────────────────────────────────────────
    if wall_time is not None:
        ax.axvline(
            wall_time, color="#333333", lw=1.2, ls=":",
            alpha=0.6, label=f"wall={wall_time:.0f}s",
            zorder=5,
        )
        ymax = max(v for v in y_map.values()) + 1 if y_map else 1
        ax.text(
            wall_time + 0.5, ymax * 0.02,
            f"wall\n{wall_time:.0f}s",
            fontsize=6.5, color="#333333", va="top",
        )

    ax.invert_yaxis()
    ax.set_xlabel("Elapsed time (s)", fontsize=8)
    ax.set_title(title, fontweight="bold", fontsize=10, pad=5)
    ax.grid(axis="x", ls="--", alpha=0.30)
    ax.set_xlim(left=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Plot individual replica runtimes per ADR policy"
    )
    ap.add_argument(
        "--input", "-i",
        default="benchmark_adr_gpu.json",
        help="Benchmark JSON file (default: benchmark_adr_gpu.json)",
    )
    ap.add_argument(
        "--out", "-o",
        default="plots/replica_timeline.png",
        help="Output PNG path (default: plots/replica_timeline.png)",
    )
    ap.add_argument(
        "--policies", nargs="+", default=None,
        help="Subset of policies to plot (default: all in file)",
    )
    ap.add_argument(
        "--dpi", type=int, default=150,
        help="Output DPI (default: 150)",
    )
    args = ap.parse_args()

    with open(args.input) as f:
        data = json.load(f)

    policies = args.policies or list(data.keys())
    policies = [p for p in policies if p in data]
    if not policies:
        raise SystemExit(f"No matching policies found in {args.input}")

    # ── Layout ────────────────────────────────────────────────────────────────
    n = len(policies)
    ncols = min(n, 2)
    nrows = (n + ncols - 1) // ncols

    # Estimate figure height from replica count
    sample_run  = data[policies[0]][0]
    n_replicas  = len(parse_replicas(sample_run))
    n_groups    = len({
        ev["group"] for ev in sample_run.get("replica_events", [])
        if ev["event"] == "start"
    })
    panel_h = max(6.0, n_replicas * 0.14 + n_groups * GROUP_GAP * 0.14 + 1.5)
    fig_w   = 13 * ncols
    fig_h   = panel_h * nrows + 1.0   # +1 for suptitle

    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(fig_w, fig_h),
        squeeze=False,
    )
    fig.suptitle(
        "ADR Policy Comparison — Individual Replica Runtimes",
        fontsize=13, fontweight="bold",
    )

    for idx, policy in enumerate(policies):
        ax  = axes[idx // ncols][idx % ncols]
        run = data[policy][0]

        replicas    = parse_replicas(run)
        wall_time   = run.get("wall_time_s")
        group_stats = run.get("group_stats", {})

        title = (
            f"policy = {policy.upper()}"
            + (f"    wall = {wall_time:.0f}s" if wall_time else "")
        )
        draw_panel(ax, replicas, title, wall_time, group_stats)

    # Hide unused axes (when n is odd and ncols=2)
    for idx in range(len(policies), nrows * ncols):
        axes[idx // ncols][idx % ncols].set_visible(False)

    # ── Legend ────────────────────────────────────────────────────────────────
    handles = [
        mpatches.Patch(
            color=GROUP_COLORS.get(g, DEFAULT_COLOR),
            label=GROUP_LABELS.get(g, g),
        )
        for g in GROUP_ORDER
        if g in {ev["group"] for p in policies for ev in data[p][0].get("replica_events", [])}
    ]
    handles.append(
        mpatches.Patch(facecolor="#cccccc", hatch="///", label="failed replica")
    )
    fig.legend(
        handles=handles,
        loc="lower center",
        bbox_to_anchor=(0.5, 0.0),
        ncol=len(handles),
        fontsize=9,
        framealpha=0.9,
    )

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout(rect=[0, 0.03, 1, 0.97])
    plt.savefig(args.out, dpi=args.dpi, bbox_inches="tight")
    print(f"Saved → {args.out}")


if __name__ == "__main__":
    main()
