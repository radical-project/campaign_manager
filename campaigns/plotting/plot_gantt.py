#!/usr/bin/env python3
"""
Gantt chart of replica execution from a campaign metrics JSON.

Reads replica_events from a campaign metrics JSON and plots each replica as a
horizontal bar in a swimlane per stage.  Overlapping replicas within a stage
are packed into rows so concurrent work is visible without overlap.

Useful for diagnosing THROTTLE vs starvation:
  - Dense stage lane with no gaps → stage is fully utilized (healthy or bottleneck)
  - Sparse lane with gaps → starvation (waiting on upstream or resource limits)
  - Wide bars with few lanes → long replicas throttling downstream stages

Supported input formats:
  1. Direct metrics JSON:  metrics.to_dict() output — replica_events at top level
  2. Benchmark results JSON:  {policy: [{run with replica_events}, ...]}
     Use --policy / --run to select which run to plot (default: first policy, run 0)

Usage:
    python plot_gantt.py campaign.json
    python plot_gantt.py bench_results.json --policy flat_rule_hier --run 2
    python plot_gantt.py bench_results.json -o gantt.png --title "Benchmark 3"
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt


# ── Data loading ─────────────────────────────────────────────────────────────


def _load_json(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def load_events(
    path: Path, policy: str | None, run: int
) -> tuple[list[dict], float | None]:
    """Return (replica_events, wall_time_s) from the JSON file.

    Handles both direct metrics.to_dict() output and benchmark results format.
    """
    data = _load_json(path)

    # Detect benchmark format: top-level values are lists of run dicts
    # that each contain a "replica_events" key.
    first_val = next(iter(data.values())) if isinstance(data, dict) else None
    is_benchmark = (
        isinstance(first_val, list)
        and first_val
        and isinstance(first_val[0], dict)
        and "replica_events" in first_val[0]
    )

    if is_benchmark:
        available = list(data.keys())
        if policy is None:
            policy = available[0]
        elif policy not in data:
            sys.exit(
                f"Policy {policy!r} not found in {path.name}.\n"
                f"Available: {available}"
            )
        runs = data[policy]
        if run >= len(runs):
            sys.exit(
                f"Run index {run} out of range for policy {policy!r} "
                f"(max {len(runs) - 1})."
            )
        run_dict = runs[run]
    elif isinstance(data, dict) and "replica_events" in data:
        run_dict = data
    else:
        sys.exit(
            f"Cannot find replica_events in {path.name}.\n"
            "Expected either a direct metrics.to_dict() JSON or a benchmark "
            "results file with structure {{policy: [run, ...]}}."
        )

    events = run_dict.get("replica_events", [])
    wall_time_s = run_dict.get("wall_time_s")
    return events, wall_time_s


# ── Span construction ─────────────────────────────────────────────────────────


def build_spans(events: list[dict], wall_time_s: float | None) -> list[dict]:
    """Pair start/finish events into (group, t_start, t_end, status) spans."""
    starts: dict[str, dict] = {}
    spans: list[dict] = []

    for ev in events:
        rid = ev["replica_id"]
        kind = ev["event"]
        if kind == "start":
            starts[rid] = ev
        elif kind in ("finish", "finished", "failed"):
            start_ev = starts.pop(rid, None)
            if start_ev is None:
                continue
            t_start = start_ev["t"]
            t_end = ev["t"]
            spans.append(
                {
                    "group": ev["group"],
                    "replica_id": rid,
                    "t_start": t_start,
                    "t_end": t_end,
                    "duration_s": ev.get("dur") or (t_end - t_start),
                    "status": "failed" if kind == "failed" else "done",
                    "attempt": start_ev.get("attempt", 0),
                }
            )

    # Replicas that started but never finished (still running at snapshot)
    for rid, start_ev in starts.items():
        t_end = wall_time_s if wall_time_s is not None else None
        spans.append(
            {
                "group": start_ev["group"],
                "replica_id": rid,
                "t_start": start_ev["t"],
                "t_end": t_end,
                "duration_s": None,
                "status": "running",
                "attempt": start_ev.get("attempt", 0),
            }
        )

    return spans


# ── Lane packing ──────────────────────────────────────────────────────────────


def pack_into_rows(spans: list[dict]) -> dict[str, list[tuple]]:
    """Assign each span a row within its stage lane using greedy interval packing.

    Returns {group: [(span, row_index), ...]} sorted by start time.
    Concurrent replicas get different rows; finished replicas can reuse rows.
    """
    by_stage: dict[str, list[dict]] = {}
    for sp in spans:
        by_stage.setdefault(sp["group"], []).append(sp)

    result: dict[str, list[tuple]] = {}
    for g, g_spans in by_stage.items():
        sorted_spans = sorted(g_spans, key=lambda s: s["t_start"])
        slot_ends: list[float] = []  # end time per row
        assignments: list[tuple] = []
        for sp in sorted_spans:
            t_s = sp["t_start"]
            t_e = sp["t_end"] if sp["t_end"] is not None else float("inf")
            row = next(
                (i for i, end in enumerate(slot_ends) if end <= t_s), None
            )
            if row is None:
                row = len(slot_ends)
                slot_ends.append(t_e)
            else:
                slot_ends[row] = t_e
            assignments.append((sp, row))
        result[g] = assignments
    return result


# ── Plotting ──────────────────────────────────────────────────────────────────

# Separation between stage swimlanes (in data units where row_h=1)
_LANE_GAP = 0.5
_BAR_H = 0.75  # bar height within a row (fraction of row height)


def plot_gantt(
    spans: list[dict],
    wall_time_s: float | None,
    title: str,
    out_path: Path,
) -> None:
    if not spans:
        sys.exit("No complete replica spans found — nothing to plot.")

    # Stage order by first start time
    first_start: dict[str, float] = {}
    for sp in spans:
        g = sp["group"]
        if g not in first_start or sp["t_start"] < first_start[g]:
            first_start[g] = sp["t_start"]
    stages = sorted(first_start, key=lambda g: first_start[g])

    # Greedy row packing per stage
    stage_rows = pack_into_rows(spans)

    # Compute y-offset per stage (variable lane heights)
    row_h = 1.0
    y_offsets: dict[str, float] = {}
    lane_n_rows: dict[str, int] = {}
    y = 0.0
    for g in stages:
        assignments = stage_rows[g]
        n_rows = (max(row for _, row in assignments) + 1) if assignments else 1
        lane_n_rows[g] = n_rows
        y_offsets[g] = y
        y += n_rows * row_h + _LANE_GAP
    total_height = y

    # Color palette: one hue per stage
    n_stages = len(stages)
    cmap = plt.get_cmap("tab10" if n_stages <= 10 else "tab20")
    stage_colors = {g: cmap(i % cmap.N) for i, g in enumerate(stages)}

    # Figure height: proportional to total rows + lanes
    fig_h = max(3.0, 1.0 + total_height * 0.55)
    fig, ax = plt.subplots(figsize=(14, fig_h))

    x_max = 0.0
    any_failed = False
    any_running = False

    for g in stages:
        for sp, row in stage_rows[g]:
            y_center = y_offsets[g] + (row + 0.5) * row_h
            t_s = sp["t_start"]
            t_e = sp["t_end"]

            if t_e is not None:
                width = t_e - t_s
                x_max = max(x_max, t_e)
            else:
                # Running replica: extend bar to wall_time_s or current x_max
                fallback = wall_time_s or (x_max if x_max > 0 else t_s + 1.0)
                width = max(0.001, fallback - t_s)
                x_max = max(x_max, t_s + width)
                any_running = True

            status = sp["status"]
            if status == "failed":
                any_failed = True
                hatch = "///"
                alpha = 0.85
                edgecolor = "#cc0000"
            elif status == "running":
                hatch = ".."
                alpha = 0.55
                edgecolor = "white"
            else:
                hatch = None
                alpha = 0.88
                edgecolor = "white"

            ax.barh(
                y_center,
                width,
                left=t_s,
                height=_BAR_H,
                color=stage_colors[g],
                edgecolor=edgecolor,
                linewidth=0.35,
                hatch=hatch,
                alpha=alpha,
            )

    # Y-axis: stage labels centered in each lane
    ytick_pos = [
        y_offsets[g] + (lane_n_rows[g] * row_h) / 2 for g in stages
    ]
    ax.set_yticks(ytick_pos)
    ax.set_yticklabels(stages, fontsize=9)
    ax.set_ylim(-_LANE_GAP / 2, total_height - _LANE_GAP / 2)

    # Horizontal separators between stages
    for g in stages[:-1]:
        sep_y = y_offsets[g] + lane_n_rows[g] * row_h + _LANE_GAP / 2
        ax.axhline(sep_y, color="gray", linewidth=0.6, linestyle=":", alpha=0.5)

    # X-axis
    ax.set_xlabel("Time from campaign start (s)", fontsize=10)
    ax.set_xlim(0.0, x_max * 1.03)
    ax.grid(axis="x", linestyle="--", linewidth=0.4, alpha=0.35)
    ax.set_axisbelow(True)

    # Legend
    legend_patches = [
        mpatches.Patch(color=stage_colors[g], label=g) for g in stages
    ]
    if any_failed:
        legend_patches.append(
            mpatches.Patch(
                facecolor="gray", edgecolor="#cc0000", hatch="///", label="failed"
            )
        )
    if any_running:
        legend_patches.append(
            mpatches.Patch(facecolor="gray", hatch="..", alpha=0.55, label="running")
        )
    ax.legend(handles=legend_patches, loc="lower right", fontsize=8, framealpha=0.9)

    # Title
    wall_str = f"  |  wall {wall_time_s:.1f} s" if wall_time_s else ""
    n_replicas = len(spans)
    ax.set_title(
        f"{title}{wall_str}  |  {n_replicas} replicas  |  {n_stages} stages",
        fontsize=11,
        pad=8,
    )

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out_path}")


# ── CLI ───────────────────────────────────────────────────────────────────────


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Gantt chart of replica execution from a campaign metrics JSON."
    )
    ap.add_argument(
        "json_path",
        help="Path to a campaign metrics JSON or benchmark results JSON.",
    )
    ap.add_argument(
        "--policy",
        default=None,
        help="Policy key (benchmark results JSON only; default: first policy).",
    )
    ap.add_argument(
        "--run",
        type=int,
        default=0,
        help="Run index within the policy (benchmark results JSON only; default: 0).",
    )
    ap.add_argument(
        "--output",
        "-o",
        default=None,
        help="Output PNG path (default: <json_path stem>.gantt.png next to input).",
    )
    ap.add_argument(
        "--title",
        default=None,
        help="Plot title (default: derived from input filename).",
    )
    args = ap.parse_args()

    path = Path(args.json_path)
    if not path.exists():
        sys.exit(f"File not found: {path}")

    events, wall_time_s = load_events(path, args.policy, args.run)
    spans = build_spans(events, wall_time_s)

    if args.output:
        out_path = Path(args.output)
    else:
        stem = path.stem
        if args.policy:
            stem = f"{stem}_{args.policy}_run{args.run}"
        out_path = path.with_name(stem + ".gantt.png")

    title = args.title or path.stem
    if args.policy:
        title = f"{title} [{args.policy} run {args.run}]"

    plot_gantt(spans, wall_time_s, title, out_path)


if __name__ == "__main__":
    main()
