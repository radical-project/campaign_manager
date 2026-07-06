#!/usr/bin/env python3
"""
plot_policy_comparison.py — compare ADR scheduling policies side by side.

Reads one or more JSONL decision logs produced by the PolicyRecorder
(``run_campaign.py --policy <kind> --record``) and plots how each policy steers
the campaign over time.

Produces a single-row figure: priority assigned to each workflow over cycles,
one panel per policy (shows *how* each policy ranks stages — fixed vs. learned
vs. reasoned).

Usage:
    python plot_policy_comparison.py adr-decisions-rule.jsonl \
        adr-decisions-bandit.jsonl adr-decisions-llm.jsonl \
        [--out plots/policy_comparison.png]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Display names + colours — covers both dreamer (s1…s5) and esm2_ddsim campaigns.
DISPLAY = {
    # dreamer / antigen-cascade
    "s1_ligand_filter": "Initial Screening",
    "s2_ml_affinity":   "Active Learning",
    "s3_docking":       "Structural Modeling",
    "s4_md_refinement": "Refinement Simulation",
    "s5_fep_ranking":   "Affinity Ranking",
    # esm2_ddsim
    "inference":        "ESM2 Inference",
    "dummy":            "DDSim",
    "md":               "MD Simulation",
    "miniapps":         "MiniApps",
}
COLORS = {
    # dreamer / antigen-cascade
    "s1_ligand_filter": "#42a5f5",
    "s2_ml_affinity":   "#66bb6a",
    "s3_docking":       "#ffa726",
    "s4_md_refinement": "#ef5350",
    "s5_fep_ranking":   "#ab47bc",
    # esm2_ddsim — pipeline A (blue/teal) and pipeline B (orange/purple)
    "inference":        "#42a5f5",
    "dummy":            "#26c6da",
    "md":               "#ffa726",
    "miniapps":         "#ab47bc",
}

# Matplotlib color cycle used as fallback for any stage not in COLORS.
_FALLBACK_COLORS = plt.rcParams["axes.prop_cycle"].by_key()["color"]


def _load(path: Path) -> list[dict]:
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _series(rows: list[dict], field: str) -> tuple[list[int], dict[str, list]]:
    """Return (cycles, {stage: [values]}) for a per-stage dict field."""
    cycles = [r["cycle"] for r in rows]
    stages: dict[str, list] = {}
    for r in rows:
        d = r.get(field, {}) or {}
        for s, v in d.items():
            stages.setdefault(s, [None] * len(cycles))
    for i, r in enumerate(rows):
        d = r.get(field, {}) or {}
        for s in stages:
            stages[s][i] = d.get(s)
    return cycles, stages


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("logs", nargs="+", help="PolicyRecorder JSONL file(s)")
    ap.add_argument("--out", default="plots/policy_comparison.png")
    args = ap.parse_args()

    runs = []
    for p in args.logs:
        rows = _load(Path(p))
        if rows:
            runs.append((rows[0].get("policy", Path(p).stem), rows))
    if not runs:
        print("No non-empty logs given.")
        return

    fig, axes = plt.subplots(1, len(runs), squeeze=False,
                             figsize=(5.2 * len(runs), 4.2))

    # ── Priority per stage over cycles, one panel per policy ─────────────────
    for j, (policy, rows) in enumerate(runs):
        ax = axes[0][j]
        cycles, stages = _series(rows, "priorities")
        for i, s in enumerate(sorted(stages)):
            ys = stages[s]
            xs = [c for c, y in zip(cycles, ys) if y is not None]
            yv = [y for y in ys if y is not None]
            if xs:
                color = COLORS.get(s) or _FALLBACK_COLORS[i % len(_FALLBACK_COLORS)]
                ax.plot(xs, yv, marker="o", markersize=2, lw=1.6,
                        color=color, label=DISPLAY.get(s, s))
        ax.set_title(f"policy = {policy}", fontsize=12, fontweight="bold")
        ax.set_xlabel("decision cycle")
        if j == 0:
            ax.set_ylabel("assigned priority")
        ax.grid(linestyle="--", alpha=0.3)
        if j == len(runs) - 1:
            ax.legend(fontsize=8, loc="upper right")

    plt.suptitle("ADR scheduling policy comparison — assigned priority over time",
                 fontsize=13, y=1.01)
    plt.tight_layout()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()
