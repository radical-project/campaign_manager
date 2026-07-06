#!/usr/bin/env python3
"""
plot_deadline_yield.py — leads-by-deadline comparison for the ADR policy benchmark
run in ``--mode deadline-yield``.

benchmark_adr.py's deadline-yield mode records, per run, how many terminal-stage
leads each policy produced within a fixed wall-clock window (``leads_by_deadline``).
Higher = better.  This script renders one bar per policy (median leads) with the
full per-run spread overlaid (individual-run dots + min..max whisker), so the
LLM's high run-to-run variance is visible rather than hidden by the median.

The honest headline this figure carries: for tight-loop pipeline scheduling, the
deterministic downstream-first rule is near-optimal and stable; the bandit lands
in the middle; the LLM is high-variance and does not reliably beat the rule; and
every adaptive policy beats 'none' (static priorities).

Usage:
    python plot_deadline_yield.py [--results benchmark_deadline.json] [--out plots/deadline_yield.png]
"""

from __future__ import annotations

import argparse
import json
import statistics as st
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ── ADR policy palette (matches plot_adr_optimizations.py) ────────────────────
CFG_COLORS = {
    "none":   "#9e9e9e",   # static priorities (no ADR)
    "rule":   "#4caf50",   # deterministic downstream-first
    "bandit": "#9c27b0",   # Thompson-sampling as an ADR agent
    "llm":    "#00838f",   # LLM-driven
}
CFG_DISPLAY = {
    "none":   "none\n(static)",
    "rule":   "rule\n(downstream-first)",
    "bandit": "bandit",
    "llm":    "llm\n(GPT-4o-mini)",
}
ORDER = ["none", "rule", "bandit", "llm"]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results", default="benchmark_deadline.json")
    ap.add_argument("--out", default="plots/deadline_yield.png")
    ap.add_argument("--deadline", type=float, default=None,
                    help="window length in seconds (for the title; else read from data)")
    args = ap.parse_args()

    with open(args.results) as f:
        results = json.load(f)

    # Collect per-policy lead counts, preserving the canonical order.
    policies = [p for p in ORDER if p in results] + \
               [p for p in results if p not in ORDER]
    leads: dict[str, list[int]] = {}
    deadline = args.deadline
    for pol in policies:
        runs = results.get(pol, [])
        L = [r.get("leads_by_deadline") for r in runs
             if r.get("leads_by_deadline") is not None]
        if L:
            leads[pol] = L
            if deadline is None:
                ds = [r.get("deadline_s") for r in runs if r.get("deadline_s")]
                if ds:
                    deadline = ds[0]
    if not leads:
        raise SystemExit(
            f"No 'leads_by_deadline' in {args.results}. "
            "Run: benchmark_adr.py --mode deadline-yield ...")

    policies = [p for p in policies if p in leads]
    medians = [st.median(leads[p]) for p in policies]
    colors  = [CFG_COLORS.get(p, "#777") for p in policies]
    x = list(range(len(policies)))

    fig, ax = plt.subplots(figsize=(1.7 * len(policies) + 2.5, 5.2))

    bars = ax.bar(x, medians, color=colors, width=0.62, zorder=2,
                  edgecolor="white", linewidth=1.0)

    # Per-run spread: min..max whisker + individual run dots (jittered).
    for i, p in enumerate(policies):
        vals = leads[p]
        lo, hi = min(vals), max(vals)
        ax.plot([i, i], [lo, hi], color="#333", lw=1.4, zorder=3, alpha=0.7)
        ax.plot([i - 0.06, i + 0.06], [lo, lo], color="#333", lw=1.4, zorder=3, alpha=0.7)
        ax.plot([i - 0.06, i + 0.06], [hi, hi], color="#333", lw=1.4, zorder=3, alpha=0.7)
        # deterministic jitter from index so dots don't overlap the whisker
        for j, v in enumerate(vals):
            dx = ((j % 5) - 2) * 0.035
            ax.plot(i + dx, v, "o", ms=5, color="white",
                    markeredgecolor="#333", markeredgewidth=0.8, zorder=4)
        # median label above the bar
        ax.text(i, hi + max(medians) * 0.03, f"med {st.median(vals):.0f}",
                ha="center", va="bottom", fontsize=10, fontweight="bold",
                color=CFG_COLORS.get(p, "#333"))

    ax.set_xticks(x)
    ax.set_xticklabels([CFG_DISPLAY.get(p, p) for p in policies], fontsize=10)
    ax.set_ylabel("Terminal leads produced in window", fontsize=11)
    ax.set_ylim(0, max(max(v) for v in leads.values()) * 1.18)
    ax.grid(axis="y", alpha=0.25, zorder=0)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)

    win = f"{deadline:.0f}s" if deadline else "fixed"
    ax.set_title(f"Deadline-yield: leads produced in a {win} window  (HIGHER IS BETTER)",
                 fontsize=12, fontweight="bold", pad=14)

    n_runs = max(len(v) for v in leads.values())
    caption = (
        f"Bar = median over {n_runs} runs; dots = individual runs; whisker = min..max.  "
        "Downstream-first (rule) is near-optimal and stable; the bandit trails; the "
        "LLM is high-variance and does not reliably beat the rule; all adaptive policies "
        "beat static 'none'.  Tight-loop priority scheduling rewards a stable heuristic "
        "over per-cycle LLM reasoning."
    )
    fig.text(0.5, -0.02, caption, ha="center", va="top", fontsize=8.5,
             color="#555", wrap=True)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"wrote {out}")
    print("\nLeads-by-deadline summary:")
    for p in policies:
        v = leads[p]
        print(f"  {p:8} median={st.median(v):>4.0f}  mean={sum(v)/len(v):>5.1f}  "
              f"[{min(v)}..{max(v)}]  (n={len(v)})")


if __name__ == "__main__":
    main()
