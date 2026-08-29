#!/usr/bin/env python3
"""
plot_telemetry_gantt.py — side-by-side Gantt comparison for telemetry benchmark.

Reads telemetry_benchmark_results.json and produces a single figure with
one Gantt row per replica, columns = rule | rule_telemetry, so reviewers
can directly compare when each group got scheduled.

Usage:
    python plot_telemetry_gantt.py [--results telemetry_benchmark_results.json]
                                   [--run 0] [--out plots/telemetry/gantt_compare.png]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# ── Config ────────────────────────────────────────────────────────────────────
GROUP_COLORS = {
    "inference": "#4878D0",
    "dummy":     "#EE854A",
    "miniapps":  "#6ACC65",
    "md":        "#D65F5F",
}
GROUP_ORDER = ["miniapps", "md", "inference", "dummy"]   # deepest pipeline first

POLICY_LABELS = {
    "rule":            "rule (no telemetry)",
    "rule_telemetry":  "rule_telemetry (low cpu_util → GPU priority boost)",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_events(events: list[dict]) -> dict[str, dict]:
    """Return {replica_id: {group, start, end}} from a flat event list."""
    replicas: dict[str, dict] = {}
    for e in events:
        rid = e.get("replica_id") or e.get("group", "?")
        if rid not in replicas:
            replicas[rid] = {"group": e.get("group", ""), "start": None, "end": None}
        ev = e.get("event", "")
        if ev in ("start", "started"):
            replicas[rid]["start"] = float(e["t"])
        elif ev in ("finish", "finished", "failed"):
            replicas[rid]["end"] = float(e["t"])
    return replicas


def _sort_key(rid: str) -> tuple:
    """Sort replicas by group pipeline depth then replica index."""
    for i, g in enumerate(GROUP_ORDER):
        if rid.startswith(g):
            idx = int(rid.split("_")[-1]) if rid.split("_")[-1].isdigit() else 0
            return (i, idx)
    return (len(GROUP_ORDER), rid)


# ── Plot ──────────────────────────────────────────────────────────────────────

def plot_gantt_compare(
    data: dict,
    run_idx: int = 0,
    out_path: str | Path = "plots/telemetry/gantt_compare.png",
) -> None:
    policies = list(data.keys())
    n_cols = len(policies)

    # Parse replica spans for each policy.
    all_spans: dict[str, dict] = {}
    for p in policies:
        runs = [r for r in data[p] if "replica_events" in r]
        if run_idx >= len(runs):
            print(f"[warn] policy={p}: run_idx={run_idx} not available (only {len(runs)} runs)")
            all_spans[p] = {}
        else:
            all_spans[p] = _parse_events(runs[run_idx]["replica_events"])

    # Unified replica ordering across both policies.
    all_rids = sorted(
        set().union(*[s.keys() for s in all_spans.values()]),
        key=_sort_key,
    )
    n_rows = len(all_rids)
    if n_rows == 0:
        print("No replica events found.")
        return

    # Determine shared x-axis limit.
    t_max = max(
        (info["end"] for spans in all_spans.values() for info in spans.values()
         if info.get("end") is not None),
        default=1.0,
    )
    t_max *= 1.05

    # ── Figure layout ─────────────────────────────────────────────────────────
    fig, axes = plt.subplots(
        1, n_cols,
        figsize=(7 * n_cols, max(4, min(7.5, 0.25 * n_rows + 1.5))),
        sharey=True,
    )
    if n_cols == 1:
        axes = [axes]

    bar_h = 0.45

    for col, (p, ax) in enumerate(zip(policies, axes)):
        spans = all_spans[p]
        ttt = None
        runs = [r for r in data[p] if "replica_events" in r]
        if run_idx < len(runs):
            ttt = runs[run_idx].get("time_to_target_s")
            sc  = runs[run_idx].get("starved_cycles", 0)
            mb  = runs[run_idx].get("mean_boost", 0.0)

        for row, rid in enumerate(all_rids):
            info = spans.get(rid)
            group = rid.rsplit("_", 1)[0] if "_" in rid else rid
            color = GROUP_COLORS.get(group, "#AAAAAA")

            if info and info.get("start") is not None and info.get("end") is not None:
                t0, t1 = info["start"], info["end"]
                ax.barh(
                    row, t1 - t0, left=t0,
                    height=bar_h, color=color, alpha=0.85,
                    linewidth=0.4, edgecolor="white",
                )
                # Label inside bar if wide enough, otherwise to the right.
                label_x = t0 + (t1 - t0) / 2
                label_in = (t1 - t0) > t_max * 0.06
                ax.text(
                    label_x if label_in else t1 + t_max * 0.01,
                    row,
                    rid,
                    ha="center" if label_in else "left",
                    va="center",
                    fontsize=7.5,
                    color="white" if label_in else "#333333",
                    fontweight="bold" if label_in else "normal",
                    clip_on=True,
                )
            elif info and info.get("start") is not None:
                # Started but never finished (DNF / still running).
                ax.barh(
                    row, t_max - info["start"], left=info["start"],
                    height=bar_h, color=color, alpha=0.3,
                    linewidth=0.4, edgecolor=color, linestyle="--",
                )
            else:
                # Never scheduled — grey placeholder.
                ax.barh(row, t_max, left=0, height=bar_h * 0.3,
                        color="#DDDDDD", alpha=0.5)

        # ttt reference line.
        if ttt is not None:
            ax.axvline(ttt, color="crimson", linewidth=1.5, linestyle="--", alpha=0.85)
            ax.text(
                ttt + t_max * 0.01, n_rows - 0.5,
                f"ttt={ttt:.0f}s",
                color="crimson", fontsize=8, va="top",
            )

        # Subtitle: ttt for both panels; boost metrics only for rule_telemetry
        # (rule has no boost mechanism — showing 0s there implies no starvation
        # occurred, which is false and misleading).
        subtitle_parts = []
        if ttt is not None:
            subtitle_parts.append(f"ttt={ttt:.1f}s")
        elif runs and run_idx < len(runs):
            subtitle_parts.append("ttt=N/A")
        if p == "rule_telemetry" and runs and run_idx < len(runs):
            subtitle_parts.append(f"starved_cycles={sc}  mean_boost={mb:.2f}")
        subtitle = "  |  ".join(subtitle_parts)

        ax.set_title(
            f"{POLICY_LABELS.get(p, p)}\n{subtitle}",
            fontsize=10, pad=6,
        )
        ax.set_xlabel("Time (s)", fontsize=9)
        ax.set_xlim(0, t_max)
        ax.xaxis.set_major_locator(ticker.MaxNLocator(8))
        ax.grid(axis="x", linestyle=":", alpha=0.4)
        ax.set_axisbelow(True)

        if col == 0:
            ax.set_yticks(range(n_rows))
            ax.set_yticklabels(all_rids, fontsize=8)
        ax.set_ylim(-0.5, n_rows - 0.5)
        ax.invert_yaxis()

    # Shared legend.
    legend_groups = sorted({rid.rsplit("_", 1)[0] for rid in all_rids if "_" in rid})
    patches = [
        mpatches.Patch(color=GROUP_COLORS.get(g, "#AAAAAA"), label=g)
        for g in legend_groups
    ]
    patches.append(mpatches.Patch(color="crimson", alpha=0.85, label="time-to-target"))
    fig.legend(
        handles=patches,
        loc="lower center",
        ncol=len(patches),
        fontsize=9,
        framealpha=0.9,
        bbox_to_anchor=(0.5, 0.0),
    )

    fig.suptitle(
        f"Telemetry Benchmark — Gantt comparison (run {run_idx})",
        fontsize=12, y=1.01,
    )
    fig.tight_layout(rect=[0, 0.06, 1, 1])

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved → {out_path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--results", default="telemetry_benchmark_results.json",
        help="JSON output from benchmark_telemetry.py",
    )
    parser.add_argument(
        "--run", type=int, default=0,
        help="Which run index to plot (default: 0)",
    )
    parser.add_argument(
        "--out", default="gantt_compare.png",
        help="Output PNG path",
    )
    args = parser.parse_args()

    with open(args.results) as f:
        data = json.load(f)

    plot_gantt_compare(data, run_idx=args.run, out_path=args.out)
