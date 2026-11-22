#!/usr/bin/env python3
"""
Figures for the IMPRESS AuCaM campaign: what AuCaM contributed.

Each figure answers a question a reader would ask about the campaign manager
itself, and answers it from one run's telemetry rather than from an assertion:

    01_steering_vs_baseline.png  — did steering cost less than not steering?
    02_bandit_learning.png       — did it learn, and does it stop exploring?
    03_cancellation_savings.png  — what did cancelling in flight save?

Figure 1 needs TWO runs, because "AuCaM helped" is a claim about a baseline:
the same campaign, same goal, steered and unsteered.

    python run_campaign.py --multi-arm --goal-n 60 --max-concurrent 2 \\
                           --cost-cancel 0.008 --telemetry-out runs/bandit.jsonl
    python run_campaign.py --multi-arm --goal-n 60 --max-concurrent 2 \\
                           --policy even --telemetry-out runs/even.jsonl
    python plot_results.py --telemetry runs/bandit.jsonl \\
                           --baseline runs/even.jsonl --out figures/

`run_results_experiment.sh` runs both, and this script.

Without `--baseline` the other two figures are still written, and figure 1 is
skipped with a line saying why — a one-run comparison against a number this
script made up would be worse than no figure.

Requires: matplotlib, numpy  (`pip install -e ".[plotting]"`).
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MaxNLocator

# ── Palette / style ───────────────────────────────────────────────────────────

TYPE_COLOR = {  # Okabe-Ito: distinguishable with colour-blindness too
    "binding_tractable": "#009E73",
    "binding_borderline": "#56B4E9",
    "binding_refractory": "#D55E00",
}
TYPE_SHORT = {
    "binding_tractable": "Tractable",
    "binding_borderline": "Borderline",
    "binding_refractory": "Refractory",
}
TYPES = list(TYPE_COLOR)
STEERED_C, BASELINE_C = "#2d6a9f", "#95a5a6"
SAVED_C = "#d0d7de"

#: What AuCaM charges these types in. The campaign's budget is GPU-hours: CPU time
#: is the agent process rather than the science, and the adapter does not bill it.
DIM = "gpu_hours"


def load(path: str) -> list[dict]:
    """Every telemetry event, in the order the campaign wrote it."""
    with open(path) as fh:
        return [json.loads(line) for line in fh if line.strip()]


# ── Derived quantities ────────────────────────────────────────────────────────


def wf_type(event: dict) -> str:
    """Which workflow type an event belongs to.

    `arm` is the key telemetry used before the campaign settled on calling these
    workflow TYPES (the scheduler's word for one is an arm), and the saved
    snapshots are worth keeping readable, so both spellings are accepted.
    """
    return event.get("wf_type") or event.get("arm") or "?"


def completions(events: list[dict]) -> list[dict]:
    return [e for e in events if e.get("event") == "complete"]


def spent(event: dict) -> float:
    return float((event.get("spent") or {}).get(DIM, 0.0))


def was_cancelled(event: dict) -> bool:
    """A run AuCaM stopped, rather than one that ran itself out.

    The controller writes `rc` from `WorkflowResult.returncode`, which is negative
    for a cancellation and 0 for a run that ended on its own terms.
    """
    return int(event.get("rc", 0)) < 0


def produced_result(event: dict) -> bool:
    """A result: a run that found a design satisfying every objective (`progress` > 0)."""
    return float(event.get("progress") or 0.0) > 0.0


def summarise(events: list[dict]) -> dict:
    """The handful of numbers figure 1 compares between two runs."""
    done = completions(events)
    results = [e for e in done if produced_result(e)]
    total = sum(spent(e) for e in done)
    wasted = sum(spent(e) for e in done if not produced_result(e))
    return {
        "runs": len(done),
        "results": len(results),
        "spent": total,
        "wasted": wasted,
        "per_result": total / len(results) if results else math.inf,
        "t_last_result": max((e["t"] for e in results), default=0.0),
        "by_arm": {a: sum(spent(e) for e in done if wf_type(e) == a) for a in TYPES},
    }


def full_cost_estimate(events: list[dict], wtype: str) -> float | None:
    """What a run of `wtype` costs when nothing stops it.

    The median of the finished (not cancelled) runs of that type in `events`, so
    figure 3 compares against observed behaviour rather than against the declared
    spending limit, a ceiling nobody reaches. Pass the baseline's events too: a
    steered campaign finishes few runs of the type it cancels, and the median keeps
    one odd run from standing in for all of them. None when there is none.
    """
    finished = sorted(
        spent(e) for e in completions(events) if wf_type(e) == wtype and not was_cancelled(e)
    )
    return finished[len(finished) // 2] if finished else None


# ── 1. Steering vs no steering ────────────────────────────────────────────────


def plot_steering(steered: list[dict], baseline: list[dict], out: Path) -> None:
    """The headline: the same campaign, steered and not.

    Two panels, because "cheaper" and "sooner" are different claims and a campaign
    can buy one with the other: left is what the goal cost, right is how the spend
    accumulated on the way there.

    STEERING IS TWO MECHANISMS, and the labels say so because the gap belongs to
    both.  The scheduler's `BanditSchedulingPolicy` steers by CHOOSING — when the
    queue is contended it stops picking the arm that does not pay — and the
    controller's `CancelRule` / `CostCancelRule` steer by STOPPING, killing a run
    that is already going nowhere.  They are substitutes on this campaign: each
    alone recovers most of the waste (measured 1.8x and 1.5x against neither),
    and together only a little more, because both are aimed at the same
    refractory runs.  Naming only the bandit would credit it with the rules' half.
    """
    a, b = summarise(steered), summarise(baseline)

    fig, (ax_cost, ax_curve) = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle(
        "What steering bought — the same campaign and goal, steered and not",
        fontweight="bold",
        fontsize=11,
    )

    # Left: GPU-hours per result, and how much went to runs without a result.
    labels = ["AuCaM steering\n(bandit + cancel rules)", "Unsteered\n(even split, no rules)"]
    produced = [a["spent"] - a["wasted"], b["spent"] - b["wasted"]]
    wasted = [a["wasted"], b["wasted"]]
    x = np.arange(2)
    ax_cost.bar(
        x, produced, 0.5, color=[STEERED_C, BASELINE_C], label="spent on runs with a result"
    )
    ax_cost.bar(
        x,
        wasted,
        0.5,
        bottom=produced,
        color=SAVED_C,
        hatch="//",
        edgecolor="#7f8c8d",
        label="spent on runs without a result",
    )
    for i, s in enumerate((a, b)):
        ax_cost.text(
            i,
            s["spent"],
            f"  {s['spent']:.3f} {DIM}\n  {s['results']} results",
            ha="center",
            va="bottom",
            fontsize=9,
        )
    ax_cost.set_xticks(x, labels)
    ax_cost.set_ylabel(DIM)
    ax_cost.set_title("Budget spent to reach the goal", loc="left", fontsize=10.5)
    # Below the bars: the ratio badge sits above them, and "upper left" put the two
    # boxes on top of each other.
    ax_cost.legend(fontsize=8, loc="lower center", bbox_to_anchor=(0.5, -0.26), ncol=2)
    ax_cost.set_ylim(0, max(a["spent"], b["spent"]) * 1.45 or 1)

    if a["per_result"] < b["per_result"] and math.isfinite(b["per_result"]):
        ax_cost.text(
            0.5,
            0.93,
            f"{b['per_result'] / a['per_result']:.1f}× less {DIM} per result",
            transform=ax_cost.transAxes,
            ha="center",
            fontsize=9,
            color=STEERED_C,
            bbox={"boxstyle": "round", "fc": "white", "ec": STEERED_C},
        )

    # Right: results against spend. A steeper curve is a campaign buying more
    # science with the same allocation, which is the whole claim.
    for events, label, colour in (
        (steered, "AuCaM steering (bandit + cancel rules)", STEERED_C),
        (baseline, "Unsteered (even split, no rules)", BASELINE_C),
    ):
        run, got = 0.0, 0
        xs, ys = [0.0], [0]
        for e in sorted(completions(events), key=lambda e: e["t"]):
            run += spent(e)
            got += 1 if produced_result(e) else 0
            xs.append(run)
            ys.append(got)
        ax_curve.step(xs, ys, where="post", color=colour, lw=2, label=label)

    ax_curve.set_xlabel(f"{DIM} consumed")
    ax_curve.set_ylabel("Results")
    ax_curve.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax_curve.set_title("Results per unit of allocation", loc="left", fontsize=10.5)
    ax_curve.legend(fontsize=9, loc="upper left")
    ax_curve.grid(alpha=0.3, ls=":")

    fig.tight_layout()
    fig.savefig(out / "01_steering_vs_baseline.png", dpi=150)
    plt.close(fig)


# ── 2. The bandit learning ────────────────────────────────────────────────────


def plot_learning(events: list[dict], out: Path) -> None:
    """What the scheduler learned, and what it did about it.

    BOTH PANELS ARE ON ELAPSED SECONDS, and that is the whole point of the
    figure.  The top is the decision record (which arm each launch went to); the
    bottom is the evidence behind it (each arm's running mean, the number UCB1
    ranks by).  Indexing them by launch number and completion number instead —
    two different sequences that drift apart, because a refractory run takes
    eight cycles and a tractable one takes one — put the cause and its effect at
    unrelated positions and made the figure unreadable.

    On one clock the claim is checkable: an arm's mean falls, and after a delay
    its squares stop appearing.  The delay is real and worth seeing — a choice
    can only use evidence from runs that had already finished, which is why a
    bandit keeps sampling a bad arm for a while before giving up.

    The exploration weight (dashed) is what sets the length of that patience: it
    scales with the square root of the budget left, so a campaign explores while
    it can afford to and exploits once it cannot.
    """
    picks = [e for e in events if e.get("event") == "select" and wf_type(e) in TYPE_COLOR]
    if not picks:
        print("  02: no launch decisions recorded — skipped")
        return

    fig, (ax_strip, ax_explore) = plt.subplots(
        2, 1, figsize=(12, 6), gridspec_kw={"height_ratios": [1.1, 1]}, sharex=True
    )
    fig.suptitle(
        "The scheduler learning which arm is worth the allocation",
        fontweight="bold",
        fontsize=11,
    )

    counts: dict[str, int] = {}
    for e in picks:
        wtype = wf_type(e)
        counts[wtype] = counts.get(wtype, 0) + 1
        ax_strip.scatter(
            e.get("t", 0.0), TYPES.index(wtype), marker="s", s=70, color=TYPE_COLOR[wtype], zorder=3
        )

    ax_strip.set_yticks(range(len(TYPES)))
    ax_strip.set_yticklabels(
        [f"{TYPE_SHORT[a]}\n({counts.get(a, 0)} launched)" for a in TYPES], fontsize=9
    )
    ax_strip.set_ylim(-0.6, len(TYPES) - 0.4)
    ax_strip.set_title("When each launch went out, and to which arm", loc="left", fontsize=10.5)
    ax_strip.grid(axis="x", alpha=0.25, ls=":")

    # Each arm's running mean, against the time its evidence actually arrived.
    done = sorted(completions(events), key=lambda e: e.get("t", 0.0))
    seen: dict[str, list[float]] = {a: [] for a in TYPES}
    curves: dict[str, tuple[list[float], list[float]]] = {a: ([], []) for a in TYPES}
    for e in done:
        wtype = wf_type(e)
        score = e.get("progress")
        if wtype not in seen or score is None:
            continue
        seen[wtype].append(float(score))
        curves[wtype][0].append(float(e.get("t", 0.0)))
        curves[wtype][1].append(sum(seen[wtype]) / len(seen[wtype]))

    for wtype in TYPES:
        step_x, step_y = curves[wtype]
        if not step_x:
            continue
        ax_explore.step(
            step_x,
            step_y,
            where="post",
            color=TYPE_COLOR[wtype],
            lw=2,
            label=f"{TYPE_SHORT[wtype]}  (mean {step_y[-1]:.2f}, {len(step_x)} completed)",
        )
    ax_explore.set_ylabel("running mean score")
    ax_explore.set_ylim(-0.05, 1.3)
    ax_explore.set_xlabel("elapsed seconds")
    ax_explore.legend(fontsize=8, loc="upper center", ncol=3, framealpha=0.9)

    c_eff = [(e.get("t", 0.0), e.get("c_eff")) for e in picks if e.get("c_eff") is not None]
    if c_eff:
        ax_c = ax_explore.twinx()
        ax_c.plot([t for t, _ in c_eff], [c for _, c in c_eff], color="#7f8c8d", ls="--", lw=1.4)
        ax_c.set_ylabel("exploration weight", color="#7f8c8d", fontsize=9)
        ax_c.tick_params(axis="y", labelcolor="#7f8c8d", labelsize=8)
        # Its own range: it drifts by a few hundredths, and a 0-anchored axis would
        # draw that as a flat line and say nothing.
        lo, hi = min(c for _, c in c_eff), max(c for _, c in c_eff)
        pad = max(0.02, (hi - lo) * 0.6)
        ax_c.set_ylim(lo - pad, hi + pad)

    ax_explore.set_title(
        "…and what it had learned by then (dashed grey: exploration weight)",
        loc="left",
        fontsize=10.5,
    )
    ax_explore.grid(alpha=0.3, ls=":")

    fig.tight_layout()
    fig.savefig(out / "02_bandit_learning.png", dpi=150)
    plt.close(fig)


# ── 3. Cancellation ───────────────────────────────────────────────────────────


def plot_cancellation(events: list[dict], out: Path, baseline: list[dict] | None = None) -> None:
    """What the controller's cancellations saved, per cancelled run.

    Each bar is one cancelled run: what AuCaM was charged for it, against what a
    run of that type costs when it is left alone. The gap is allocation that went
    back to the campaign instead of into a run already known to be going nowhere.
    """
    # Only cancellations the campaign CHOSE. A run still in flight when the goal
    # is reached is cancelled too, and counting it would credit AuCaM with
    # avoiding work it merely ran out of reasons to do — usually as a near-zero
    # bar beside the real ones, since it had barely started.
    cancelled = [e for e in completions(events) if was_cancelled(e) and not e.get("after_goal")]
    shutdown = sum(1 for e in completions(events) if was_cancelled(e) and e.get("after_goal"))
    if not cancelled:
        print("  03: nothing was cancelled mid-campaign — skipped")
        return

    charged, avoided, labels, colours = [], [], [], []
    for i, e in enumerate(cancelled, start=1):
        wtype = wf_type(e)
        full = full_cost_estimate(events + (baseline or []), wtype)
        used = spent(e)
        charged.append(used)
        avoided.append(max(0.0, (full - used)) if full else 0.0)
        # Numbered, because several cancellations usually land on the same type and
        # three bars all reading "Refractory" look like one bar drawn three times.
        # A number and not a timestamp: the bars are independent runs, and a time
        # on each would invite reading them as one accumulating series.
        labels.append(f"run {i}\n{TYPE_SHORT.get(wtype, wtype)}")
        colours.append(TYPE_COLOR.get(wtype, "#7f8c8d"))

    fig, ax = plt.subplots(figsize=(max(7.5, 1.6 * len(cancelled) + 4.5), 5))
    x = np.arange(len(cancelled))
    ax.bar(x, charged, 0.55, color=colours, label=f"{DIM} used (charged by AuCaM)")
    ax.bar(
        x,
        avoided,
        0.55,
        bottom=charged,
        color=SAVED_C,
        hatch="//",
        edgecolor="#7f8c8d",
        label=f"{DIM} avoided by cancelling",
    )

    total_saved = sum(avoided)
    ax.set_xticks(x, labels, fontsize=9)
    xlabel = "each bar is one cancelled run, independent of the others"
    if shutdown:
        xlabel += f"  ({shutdown} more cancelled when the campaign ended: not shown)"
    ax.set_xlabel(xlabel)
    ax.set_ylabel(DIM)
    ax.set_title(
        f"Cancelling in flight returned {total_saved:.3f} {DIM} to the campaign",
        fontweight="bold",
        fontsize=11,
    )
    # What AuCaM charged is a sliver next to what it avoided, so it is written out:
    # an invisible bar reads as "this run cost nothing", which is not the claim.
    for xi, (used, saved) in enumerate(zip(charged, avoided, strict=False)):
        ax.text(
            xi,
            used + saved,
            f"charged {used:.4f}\navoided {saved:.4f}",
            ha="center",
            va="bottom",
            fontsize=8.5,
        )
    ax.set_ylim(0, max(c + v for c, v in zip(charged, avoided, strict=False)) * 1.28 or 1)
    ax.legend(fontsize=8)
    ax.grid(axis="y", alpha=0.3, ls=":")

    fig.tight_layout()
    fig.savefig(out / "03_cancellation_savings.png", dpi=150)
    plt.close(fig)


# ── CLI ───────────────────────────────────────────────────────────────────────


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--telemetry", required=True, help="JSONL from the steered run")
    p.add_argument("--baseline", help="JSONL from an unsteered run (--policy even), for figure 1")
    p.add_argument("--out", default="figures", help="output directory for PNG figures")
    args = p.parse_args()

    started = time.time()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    events = load(args.telemetry)

    if args.baseline:
        plot_steering(events, load(args.baseline), out)
    else:
        print("  01: skipped — pass --baseline (a --policy even run) to compare against")
    plot_learning(events, out)
    plot_cancellation(events, out, load(args.baseline) if args.baseline else None)

    # Only what is actually on disk from this invocation: the plot_* helpers skip
    # (and say why) when the telemetry cannot answer their question, and naming a
    # figure that was skipped sends a reader looking for a file that is not there.
    written = sorted(f.name for f in out.glob("*.png") if f.stat().st_mtime >= started)
    for name in written:
        print(f"  {name}")
    print(f"Done — {out}/" if written else "Nothing to draw from this telemetry.")


if __name__ == "__main__":
    main()
