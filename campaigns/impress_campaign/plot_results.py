#!/usr/bin/env python3
"""
Plot multi-arm AuCaM campaign results from the telemetry JSON written by
MultiArmController.

Usage
-----
    python plot_results.py --telemetry results.jsonl --out plots/

Produces three narrative figures:
    01_execution_timeline.png  — swim-lane Gantt of every workflow instance
    02_completion_race.png     — cumulative completions vs wall time
    03_roi.png                 — completions-per-gpu_hour by arm (efficiency)

Requires: matplotlib, numpy  (pip install matplotlib numpy)
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# ── Palette / style ───────────────────────────────────────────────────────────

ARM_COLOR = {
    "binding_easy":   "#27ae60",
    "binding_medium": "#e67e22",
    "binding_hard":   "#c0392b",
}
ARM_SHORT = {
    "binding_easy":   "Easy",
    "binding_medium": "Medium",
    "binding_hard":   "Hard",
}
ARMS = list(ARM_COLOR)

plt.rcParams.update({
    "font.family":        "DejaVu Sans",
    "font.size":          10,
    "axes.spines.top":    False,
    "axes.spines.right":  False,
    "axes.grid":          True,
    "grid.alpha":         0.25,
    "grid.linestyle":     "--",
})


# ── Load ──────────────────────────────────────────────────────────────────────

def load(path: str) -> tuple[list[dict], list[dict]]:
    cycles, completions = [], []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            ev = json.loads(line)
            if ev["event"] == "cycle":
                cycles.append(ev)
            elif ev["event"] == "complete":
                completions.append(ev)
    return cycles, completions


def _reconstruct_gantt(completions: list[dict], max_lanes: int = 2) -> list[dict]:
    """
    Reconstruct (start, end, arm, lane, rc, progress) for each workflow instance.

    Strategy:
    - Sort completions by end time per arm.
    - Assign to lanes greedily: each completion starts immediately after the
      previous completion in that lane (sequential within a lane).
    - First completion in each arm starts at t=0 (campaign start).
    """
    t0 = 0.0  # campaign start (all arms launch at t=0)
    rows = []
    for arm in ARMS:
        evs = sorted([e for e in completions if e.get("arm") == arm], key=lambda e: e["t"])
        # Distribute evs across up to max_lanes lanes
        lane_end: list[float] = [t0] * max_lanes
        for ev in evs:
            # pick the lane whose end time is earliest
            lane_idx = int(np.argmin(lane_end))
            start = lane_end[lane_idx]
            end   = ev["t"]
            rows.append({
                "arm":      arm,
                "lane":     lane_idx,
                "start":    start,
                "end":      end,
                "rc":       ev.get("rc", 0),
                "progress": ev.get("progress", 0.0),
            })
            lane_end[lane_idx] = end
    return rows


# ── Plot 1: Swim-lane execution Gantt ─────────────────────────────────────────

def plot_execution_timeline(completions: list[dict], out: Path, max_lanes: int = 2) -> None:
    rows = _reconstruct_gantt(completions, max_lanes)
    t_goal = max(e["t"] for e in completions)

    # Build y-axis: one row per (arm, lane), arms ordered easy → medium → hard
    lane_order: list[tuple[str, int]] = []
    for arm in reversed(ARMS):                   # easy at bottom, hard at top for readability
        lanes_used = sorted(set(r["lane"] for r in rows if r["arm"] == arm))
        for lane in reversed(lanes_used):
            lane_order.append((arm, lane))

    y_map = {key: i for i, key in enumerate(lane_order)}
    total_rows = len(lane_order)

    fig, ax = plt.subplots(figsize=(13, max(4.0, 0.42 * total_rows + 2.0)))
    ax.set_axisbelow(True)

    for r in rows:
        y    = y_map[(r["arm"], r["lane"])]
        col  = ARM_COLOR[r["arm"]]
        dur  = r["end"] - r["start"]
        cancelled = r["rc"] != 0
        no_result = r["progress"] == 0.0 and not cancelled

        ax.barh(y, dur, left=r["start"], height=0.60,
                color=col, alpha=0.80, linewidth=0.4, edgecolor="white")

        if cancelled:
            ax.barh(y, dur, left=r["start"], height=0.60,
                    color="none", hatch="////", linewidth=0.3, edgecolor=col, alpha=0.45)
            ax.plot(r["end"], y, "x", color="#7f8c8d", markersize=7, markeredgewidth=2.0)
        elif no_result:
            # stagnated without results — hatched differently
            ax.barh(y, dur, left=r["start"], height=0.60,
                    color="none", hatch="....", linewidth=0.3, edgecolor="#7f8c8d", alpha=0.5)
            ax.plot(r["end"], y, "o", color="#7f8c8d", markersize=5,
                    markerfacecolor="none", markeredgewidth=1.5)
        else:
            ax.plot(r["end"], y, "|", color="white", markersize=7, markeredgewidth=1.8)

        # Duration text
        if dur > 0.6:
            txt = f"{dur:.1f}s"
            ax.text(r["start"] + dur / 2, y, txt,
                    va="center", ha="center", fontsize=7, color="white", fontweight="bold")

    # Goal-reached line
    ax.axvline(t_goal, color="#8e44ad", linestyle="--", linewidth=1.5, zorder=5, label=f"Goal reached  t={t_goal:.0f}s")

    # Y-axis labels
    ytick_labels = []
    for arm, lane in lane_order:
        n_ok = sum(1 for r in rows if r["arm"] == arm and r["lane"] == lane and r["rc"] == 0 and r["progress"] > 0)
        n_tot = sum(1 for r in rows if r["arm"] == arm and r["lane"] == lane)
        ytick_labels.append(f"{ARM_SHORT[arm]} lane {lane+1}  ({n_ok}/{n_tot} yielded results)")

    ax.set_yticks(range(total_rows))
    ax.set_yticklabels(ytick_labels, fontsize=8)
    ax.set_xlabel("Wall-clock time (s)")
    ax.set_title("Execution timeline — AuCaM routes compute to the winning arm", fontweight="bold", fontsize=11)
    ax.set_xlim(left=-0.2)

    legend_handles = [
        mpatches.Patch(color=ARM_COLOR[a], label=ARM_SHORT[a]) for a in ARMS
    ] + [
        mpatches.Patch(facecolor="lightgrey", hatch="////", edgecolor="grey", label="Cancelled (goal met)"),
        mpatches.Patch(facecolor="lightgrey", hatch="....", edgecolor="grey", label="Stagnated — 0 results"),
        mpatches.Patch(color="#8e44ad", label=f"Goal reached  t={t_goal:.0f}s"),
    ]
    ax.legend(handles=legend_handles, loc="lower right", fontsize=8, framealpha=0.9)

    fig.tight_layout()
    fig.savefig(out / "01_execution_timeline.png", dpi=150)
    plt.close(fig)
    print("  01_execution_timeline.png")


# ── Plot 2: Completion race ───────────────────────────────────────────────────

def plot_completion_race(completions: list[dict], out: Path) -> None:
    """
    Step functions over wall time.
    Solid line = completions with real results (progress > 0).
    Dashed line = all completions including stagnated ones (progress = 0).
    """
    by_arm: dict[str, list[dict]] = {a: [] for a in ARMS}
    for ev in completions:
        arm = ev.get("arm")
        if arm in by_arm:
            by_arm[arm].append(ev)

    t_goal = max(e["t"] for e in completions)
    t_max  = t_goal + 1.0
    goal_n = sum(1 for e in completions if e.get("rc", 0) == 0 and e.get("progress", 0) > 0)

    fig, ax = plt.subplots(figsize=(11, 5))

    for arm in ARMS:
        evs = sorted(by_arm[arm], key=lambda e: e["t"])
        col = ARM_COLOR[arm]

        # All-completions (rc=0, any progress)
        ok_evs  = [e for e in evs if e.get("rc", 0) == 0]
        # Result-bearing completions
        good_evs = [e for e in ok_evs if e.get("progress", 0) > 0]

        if good_evs:
            ts = [0.0] + [e["t"] for e in good_evs] + [t_max]
            ys = [0]   + list(range(1, len(good_evs) + 1)) + [len(good_evs)]
            ax.step(ts, ys, where="post", color=col, linewidth=2.5, label=f"{ARM_SHORT[arm]}  ({len(good_evs)} results)", zorder=4)
            ax.fill_between(ts, ys, step="post", color=col, alpha=0.08)
        else:
            ax.step([0.0, t_max], [0, 0], where="post", color=col, linewidth=2.5,
                    label=f"{ARM_SHORT[arm]}  (0 results — stagnated)", linestyle="--", zorder=4)

        # Mark natural stagnations with open circles
        for ev in ok_evs:
            if ev.get("progress", 0) == 0:
                ax.plot(ev["t"], 0.2, "o", color=col, markersize=7,
                        markerfacecolor="none", markeredgewidth=1.5, zorder=5)

        # Mark cancellations with ×
        for ev in evs:
            if ev.get("rc", 0) != 0:
                n_good_at_t = sum(1 for e in good_evs if e["t"] <= ev["t"])
                ax.plot(ev["t"], n_good_at_t, "x", color=col, markersize=9,
                        markeredgewidth=2.2, zorder=6)

    # Annotations for stagnation markers
    ax.annotate("○ = stagnated (0 results)",
                xy=(0, 0), xytext=(0.4, 1.2),
                fontsize=8, color="#7f8c8d",
                xycoords="data", textcoords="data")

    # Goal line
    ax.axhline(goal_n, color="#8e44ad", linestyle=":", linewidth=1.5, alpha=0.7)
    ax.text(0.3, goal_n + 0.4, f"Goal = {goal_n} results", color="#8e44ad", fontsize=9)

    ax.axvline(t_goal, color="#8e44ad", linestyle="--", linewidth=1.5, zorder=4)
    ax.text(t_goal + 0.2, goal_n * 0.45,
            f"Goal satisfied\nt = {t_goal:.0f}s",
            color="#8e44ad", fontsize=9, va="center")

    ax.set_xlabel("Wall-clock time (s)")
    ax.set_ylabel("Cumulative workflow completions with results")
    ax.set_title("Completion race — bandit concentrates budget on easy (the only productive arm)",
                 fontweight="bold", fontsize=11)
    ax.set_xlim(-0.3, t_max)
    ax.set_ylim(-0.8, goal_n + 3)
    ax.legend(loc="upper left", fontsize=9)

    fig.tight_layout()
    fig.savefig(out / "02_completion_race.png", dpi=150)
    plt.close(fig)
    print("  02_completion_race.png")


# ── Plot 3: ROI — completions per gpu_hour ────────────────────────────────────

def plot_roi(completions: list[dict], out: Path) -> None:
    """
    Left:  stacked bar — gpu_hours split by productive vs wasted per arm
    Right: completions delivered per gpu_hour (ROI)
    """
    gpu_good:  dict[str, float] = {a: 0.0 for a in ARMS}
    gpu_waste: dict[str, float] = {a: 0.0 for a in ARMS}
    n_good:    dict[str, int]   = {a: 0   for a in ARMS}

    for ev in completions:
        arm = ev.get("arm")
        if arm not in ARMS:
            continue
        spent = ev.get("spent") or {}
        g = spent.get("gpu_hours", 0.0)
        if ev.get("rc", 0) == 0 and ev.get("progress", 0) > 0:
            gpu_good[arm]  += g
            n_good[arm]    += 1
        else:
            gpu_waste[arm] += g

    fig, (ax_budget, ax_roi) = plt.subplots(1, 2, figsize=(11, 5))
    fig.suptitle("Resource efficiency — medium & hard consume GPU without producing results",
                 fontweight="bold", fontsize=11)

    arms_data = [a for a in ARMS if (gpu_good[a] + gpu_waste[a]) > 0]
    labels    = [ARM_SHORT[a] for a in arms_data]
    colors    = [ARM_COLOR[a] for a in arms_data]

    # Left: stacked budget bar (productive + wasted)
    good_vals  = [gpu_good[a]  for a in arms_data]
    waste_vals = [gpu_waste[a] for a in arms_data]

    bars_g = ax_budget.bar(labels, good_vals, color=colors, alpha=0.85, width=0.5,
                           label="Productive (results returned)")
    bars_w = ax_budget.bar(labels, waste_vals, bottom=good_vals, color=colors, alpha=0.35,
                           width=0.5, hatch="////", edgecolor="grey",
                           label="Wasted (stagnated or cancelled)")

    ax_budget.set_ylabel("gpu_hours")
    ax_budget.set_title("GPU budget per arm")
    ax_budget.legend(fontsize=8)

    total_wasted = sum(gpu_waste.values())
    total_gpu    = sum(gpu_good.values()) + sum(gpu_waste.values())
    ax_budget.text(0.98, 0.97,
                   f"Total wasted: {total_wasted:.3f} gpu_h\n({100*total_wasted/total_gpu:.0f}% of budget)",
                   transform=ax_budget.transAxes, ha="right", va="top",
                   fontsize=8, color="#c0392b",
                   bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="#c0392b", alpha=0.8))

    for bar, g, w in zip(bars_g, good_vals, waste_vals):
        total = g + w
        if total > 0:
            ax_budget.text(bar.get_x() + bar.get_width() / 2, total + 0.002,
                           f"{total:.3f}", ha="center", va="bottom", fontsize=8)

    # Right: ROI
    roi_vals = [n_good[a] / (gpu_good[a] + gpu_waste[a])
                if (gpu_good[a] + gpu_waste[a]) > 0 else 0
                for a in arms_data]

    bars_roi = ax_roi.bar(labels, roi_vals, color=colors, alpha=0.85, width=0.5)
    ax_roi.set_ylabel("Completions with results per gpu_hour")
    ax_roi.set_title("ROI — results per gpu_hour spent")

    for bar, val, arm in zip(bars_roi, roi_vals, arms_data):
        txt = f"{val:.0f}" if val >= 1 else ("" if val == 0 else f"{val:.1f}")
        label = txt if val > 0 else "0\n(no results)"
        col_txt = "#c0392b" if val == 0 else "#1a252f"
        ax_roi.text(bar.get_x() + bar.get_width() / 2,
                    max(val, max(roi_vals) * 0.03) + max(roi_vals) * 0.02,
                    label, ha="center", va="bottom", fontsize=9, color=col_txt, fontweight="bold")

    # Efficiency ratio annotation
    if roi_vals[0] > 0 and any(v == 0 for v in roi_vals[1:]):
        ax_roi.text(0.98, 0.97,
                    f"Easy is {roi_vals[0]:.0f}× more efficient\nthan medium & hard",
                    transform=ax_roi.transAxes, ha="right", va="top",
                    fontsize=8, color="#27ae60",
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="white",
                              edgecolor="#27ae60", alpha=0.8))

    fig.tight_layout()
    fig.savefig(out / "03_roi.png", dpi=150)
    plt.close(fig)
    print("  03_roi.png")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(description="Plot multi-arm AuCaM campaign results")
    p.add_argument("--telemetry", required=True, help="JSONL file from MultiArmController")
    p.add_argument("--out", default="plots", help="output directory for PNG figures")
    p.add_argument("--lanes", type=int, default=2, help="max concurrent lanes per arm (for Gantt)")
    args = p.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    cycles, completions = load(args.telemetry)
    print(f"Loaded {len(cycles)} cycle events, {len(completions)} completion events")
    print(f"Writing to {out}/")

    plot_execution_timeline(completions, out, max_lanes=args.lanes)
    plot_completion_race(completions, out)
    plot_roi(completions, out)

    print("Done.")


if __name__ == "__main__":
    main()
