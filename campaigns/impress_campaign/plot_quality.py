#!/usr/bin/env python3
"""Figures for experiment 2: N good designs, for the fewest gpu_hours.

    q1_run_trajectories.png  — what the controller decided for each run
    q2_bandit_learning.png   — which type the scheduler learned to launch
    q3_cost_of_n_designs.png — what the two together saved

From four runs of the same campaign, differing only in the scheduling policy and
in whether the controller stops runs; `run_quality_experiment.sh` runs all four
and then this script:

    python plot_quality.py --runs runs/ --out figures/quality/
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from plot_results import DIM, TYPE_COLOR, TYPE_SHORT, TYPES, completions, load, spent

RUNS = {  # file stem -> (scheduler, controller stops)
    "q_even_nostops": ("even split", False),
    "q_bandit_nostops": ("bandit", False),
    "q_even": ("even split", True),
    "q_bandit": ("bandit", True),
}
NO_DESIGN_Y = -0.08  # where a run with no design yet is drawn


def trajectories(events: list[dict]) -> dict[str, dict]:
    """Per run: its type, its (gpu_hours, quality) points, and how it ended."""
    runs: dict[str, dict] = {}
    for e in events:
        if e.get("event") == "progress":
            r = runs.setdefault(e["wf_id"], {"type": e["wf_type"], "pts": []})
            r["pts"].append((e[DIM], e["quality"]))
        elif e.get("event") == "complete" and "wf_id" in e:
            r = runs.setdefault(e["wf_id"], {"type": e["wf_type"], "pts": []})
            r["end"] = e
    return runs


def _end_marker(end: dict | None) -> tuple[str, str]:
    if end is None or end.get("after_goal"):
        return "o", "none"  # still running when the campaign finished
    if end.get("good"):
        return "*", "full"
    return "X", "full"


def plot_trajectories(events_off: list[dict], events_on: list[dict], bar: float, out: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(13, 5.2), sharey=True)
    fig.suptitle(
        "Every run's best design as it spends: without and with the controller deciding",
        fontweight="bold",
        fontsize=11,
    )
    xmax = 0.0
    for ax, events, title in (
        (axes[0], events_off, "No controller stops: each run goes until IMPRESS ends it"),
        (axes[1], events_on, "Controller stops a run at the bar, or when it has no design"),
    ):
        # Refractory last, so its flat no-design lines are not hidden under the others.
        for run in sorted(trajectories(events).values(), key=lambda r: r["type"] == TYPES[-1]):
            if run["type"] not in TYPE_COLOR or not run["pts"]:
                continue
            xs = [x for x, _ in run["pts"]]
            ys = [NO_DESIGN_Y if q is None else q for _, q in run["pts"]]
            end = run.get("end")
            if end:
                xs.append(spent(end))
                ys.append(ys[-1] if end.get("quality") is None else end["quality"])
            c = TYPE_COLOR[run["type"]]
            ax.step(xs, ys, where="post", color=c, alpha=0.55, lw=1.3)
            marker, fill = _end_marker(end)
            ax.plot(
                xs[-1],
                ys[-1],
                marker=marker,
                ms=9 if marker == "*" else 6,
                color=c,
                fillstyle=fill,
                mec=c,
                zorder=3,
            )
            xmax = max(xmax, xs[-1])
        ax.axhline(bar, color="#2c3e50", ls="--", lw=1.2)
        ax.text(0.002, bar + 0.015, f"quality bar {bar}", fontsize=8.5, color="#2c3e50")
        ax.set_title(title, loc="left", fontsize=10)
        ax.set_xlabel(f"{DIM} spent by the run")
        ax.grid(alpha=0.3, ls=":")
    for ax in axes:
        ax.set_xlim(0, xmax * 1.03)
    axes[0].set_ylabel("quality of its best design")
    axes[0].set_yticks([NO_DESIGN_Y, 0, 0.2, 0.4, 0.6, 0.8])
    axes[0].set_yticklabels(["no design", "0", "0.2", "0.4", "0.6", "0.8"])
    handles = [Patch(color=TYPE_COLOR[t], label=TYPE_SHORT[t]) for t in TYPES] + [
        Line2D([], [], marker="*", ls="", color="#555", ms=9, label="good design"),
        Line2D([], [], marker="X", ls="", color="#555", label="ended without one"),
        Line2D([], [], marker="o", ls="", color="#555", fillstyle="none", label="campaign done"),
    ]
    fig.legend(handles=handles, loc="lower center", ncol=6, fontsize=8.5, frameon=False)
    fig.tight_layout(rect=(0, 0.06, 1, 1))
    fig.savefig(out / "q1_run_trajectories.png", dpi=150)
    plt.close(fig)


def plot_learning(events: list[dict], out: Path) -> None:
    """Which type each launch went to, and what each type had cost per good design by then."""
    picks = [e for e in events if e.get("event") == "select" and e.get("wf_type") in TYPE_COLOR]
    fig, (top, bottom) = plt.subplots(
        2, 1, figsize=(12, 6), sharex=True, gridspec_kw={"height_ratios": [1, 1.2]}
    )
    fig.suptitle(
        "The scheduler learning which type gives good designs cheapest",
        fontweight="bold",
        fontsize=11,
    )
    for e in picks:
        t = e["wf_type"]
        top.scatter(e["t"], TYPES.index(t), marker="s", s=60, color=TYPE_COLOR[t])
    top.set_yticks(range(len(TYPES)))
    top.set_yticklabels(
        [f"{TYPE_SHORT[t]}\n({sum(e['wf_type'] == t for e in picks)} launched)" for t in TYPES],
        fontsize=9,
    )
    top.set_ylim(-0.6, len(TYPES) - 0.4)
    top.set_title("When each launch went out, and to which type", loc="left", fontsize=10.5)
    top.grid(axis="x", alpha=0.25, ls=":")

    totals = {t: [0.0, 0] for t in TYPES}  # gpu_hours spent, good designs
    curves: dict[str, tuple[list, list]] = {t: ([], []) for t in TYPES}
    for e in sorted(completions(events), key=lambda e: e["t"]):
        t = e.get("wf_type")
        if t not in totals or e.get("after_goal"):
            continue
        totals[t][0] += spent(e)
        totals[t][1] += bool(e.get("good"))
        curves[t][0].append(e["t"])
        curves[t][1].append(totals[t][0] / totals[t][1] if totals[t][1] else None)

    # A type with no good design yet has no finite cost per design: it is drawn on
    # a "no good design" row above the rest, as figure 1 draws runs with no design.
    finite = [y for t in TYPES for y in curves[t][1] if y is not None]
    top_y = 1.25 * max(finite, default=0.05)
    for t in TYPES:
        xs, ys = curves[t]
        if not xs:
            continue
        ys = [top_y if y is None else y for y in ys]
        n_good = totals[t][1]
        label = (
            f"{TYPE_SHORT[t]}  ({ys[-1]:.3f} per good design)"
            if n_good
            else f"{TYPE_SHORT[t]}  (no good design for {totals[t][0]:.3f})"
        )
        bottom.step(xs, ys, where="post", color=TYPE_COLOR[t], lw=2, label=label)
        bottom.plot(xs, ys, "o", color=TYPE_COLOR[t], ms=3.5)
    ticks = [k * 0.01 for k in range(int(top_y * 0.9 / 0.01) + 1)]
    bottom.set_yticks([*ticks, top_y])
    bottom.set_yticklabels([f"{y:.2f}" for y in ticks] + ["no good\ndesign"])
    bottom.set_ylim(0, top_y * 1.08)
    bottom.set_ylabel(f"{DIM} per good design\n(lower is better)")
    bottom.set_xlabel("elapsed seconds")
    bottom.set_title(
        "...and what each type had cost per good design by then", loc="left", fontsize=10.5
    )
    bottom.legend(fontsize=8.5, loc="lower right", framealpha=0.9)
    bottom.axhline(top_y * 0.96, color="#999", lw=0.6, ls=":")
    bottom.grid(alpha=0.3, ls=":")
    fig.tight_layout()
    fig.savefig(out / "q2_bandit_learning.png", dpi=150)
    plt.close(fig)


def spend_breakdown(events: list[dict], bar: float) -> tuple[float, float, float]:
    """A campaign's gpu_hours, split three ways.

    (reaching the bar on runs that left a good design,
     spent by those runs after they crossed it,
     spent on runs that never left a good design)
    """
    crossed: dict[str, float] = {}  # wf_id -> gpu_hours when its design first reached the bar
    for e in events:
        if e.get("event") == "progress" and (e.get("quality") or -1) >= bar:
            crossed.setdefault(e["wf_id"], e[DIM])
    needed = after = wasted = 0.0
    for e in completions(events):
        total = spent(e)
        if e.get("good"):
            at = min(crossed.get(e.get("wf_id"), total), total)
            needed += at
            after += total - at
        else:
            wasted += total
    return needed, after, wasted


def plot_cost(all_events: dict[str, list[dict]], bar: float, out: Path) -> None:
    labels, parts = [], []
    for stem, (policy, stops) in RUNS.items():
        if stem in all_events:
            labels.append(f"{policy}\n{'controller stops' if stops else 'no stops'}")
            parts.append(spend_breakdown(all_events[stem], bar))
    n_good = max(sum(1 for e in completions(ev) if e.get("good")) for ev in all_events.values())
    totals = [sum(p) for p in parts]

    fig, ax = plt.subplots(figsize=(9, 5.5))
    x = list(range(len(labels)))
    bottom = [0.0] * len(labels)
    for k, (label, style) in enumerate(
        (
            (f"reaching quality {bar}", {"color": "#2d6a9f"}),
            ("extra cycles after reaching it", {"color": "#e8a33d", "hatch": ".."}),
            ("runs that never reached it", {"color": "#bbbbbb", "hatch": "//"}),
        )
    ):
        values = [p[k] for p in parts]
        ax.bar(x, values, bottom=bottom, label=label, edgecolor="white", **style)
        bottom = [b + v for b, v in zip(bottom, values, strict=True)]
    for i, total in enumerate(totals):
        saving = totals[0] / total if total else 0
        note = "baseline" if i == 0 else f"{saving:.1f}× less"
        ax.text(
            i,
            total,
            f"{total:.2f}\n{note}",
            ha="center",
            va="bottom",
            fontsize=10,
            fontweight="bold",
        )
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_ylim(0, max(totals) * 1.18)
    ax.set_ylabel(f"{DIM} to reach {n_good} good designs")
    ax.set_title(
        f"What it cost to reach {n_good} good designs, and where it went",
        fontweight="bold",
        fontsize=11,
    )
    ax.legend(fontsize=9, frameon=False, loc="upper right")
    ax.grid(axis="y", alpha=0.3, ls=":")
    fig.tight_layout()
    fig.savefig(out / "q3_cost_of_n_designs.png", dpi=150)
    plt.close(fig)


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--runs", default="runs", help="directory holding the q_*.jsonl telemetry")
    p.add_argument("--quality-bar", type=float, default=0.7)
    p.add_argument("--out", default="figures/quality")
    args = p.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    found = {s: load(str(f)) for s in RUNS if (f := Path(args.runs) / f"{s}.jsonl").is_file()}
    if "q_bandit" not in found:
        raise SystemExit(f"no q_bandit.jsonl in {args.runs}/")

    if "q_even_nostops" in found:
        plot_trajectories(found["q_even_nostops"], found["q_bandit"], args.quality_bar, out)
    plot_learning(found["q_bandit"], out)
    plot_cost(found, args.quality_bar, out)
    print(f"Done — {out}/")


if __name__ == "__main__":
    main()
