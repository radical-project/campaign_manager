#!/usr/bin/env python3
"""Figure for the AuCaM capability demo (run_impact.py).

Usage:
    python gen_impact_fig.py --json-in /tmp/impact_run.json [--out PATH]

Panels:
  1. Campaign "explore" timeline — every workflow run in launch order; the
     bandit tries slow_poor less and less often; cancelled runs stop early.
  2. Cancellation — CPU used by each cancelled slow_poor run vs. a full run.
"""

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402

_HERE = Path(__file__).parent
ap = argparse.ArgumentParser()
ap.add_argument("--json-in", default=str(_HERE / "impact_run.json"))
ap.add_argument("--out", default=str(_HERE / "../../../docs/figures/aucam_impact.png"))
args = ap.parse_args()

d = json.loads(Path(args.json_in).read_text())
meta = d["meta"]
explore = d["campaign"]

C_FAST, C_SLOW, C_SAVED, C_ANN = "#1a7f37", "#cf222e", "#d0d7de", "#57606a"
plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False})
fig, axes = plt.subplots(1, 2, figsize=(13, 4.8), gridspec_kw={"width_ratios": [2.2, 1]})

# ── Panel 1: timeline of campaign "explore" ──────────────────────────────────
ax = axes[0]
runs = explore["runs"]
for r in runs:
    start = r["end_s"] - r["wall_s"]
    y = 1 if r["type"] == "slow_poor" else 0
    color = C_SLOW if r["type"] == "slow_poor" else C_FAST
    ax.barh(y, r["wall_s"], left=start, height=0.5, color=color, edgecolor="white")
    if r["rc"] == -1:  # cancelled: show the part of a full run that was avoided
        ax.barh(
            y,
            meta["slow_wall_s"] - r["wall_s"],
            left=r["end_s"],
            height=0.5,
            color="none",
            edgecolor=C_SLOW,
            hatch="///",
            linewidth=0.8,
        )
slow_idx = [i + 1 for i, r in enumerate(runs) if r["type"] == "slow_poor"]
gaps = [b - a - 1 for a, b in zip(slow_idx, slow_idx[1:], strict=False)]
ax.set_yticks([0, 1], ["fast_good", "slow_poor"])
ax.set_xlabel("time since campaign start (s)")
ax.set_title("1. One campaign: the bandit learns, the controller cancels", loc="left")
ax.text(
    0.01,
    0.86,
    f"slow_poor tried at launches {', '.join(map(str, slow_idx))} "
    f"({', '.join(map(str, gaps))} fast_good runs in between)",
    transform=ax.transAxes,
    va="top",
    fontsize=9,
    color=C_ANN,
)
ax.legend(
    handles=[
        Patch(color=C_FAST, label="fast_good (completes)"),
        Patch(color=C_SLOW, label="slow_poor (cancelled)"),
        Patch(
            facecolor="none", edgecolor=C_SLOW, hatch="///", label="run time avoided by cancelling"
        ),
    ],
    loc="upper right",
    ncol=3,
    fontsize=8,
    frameon=False,
)
ax.set_ylim(-0.6, 2.2)

# ── Panel 2: CPU per cancelled slow_poor run ─────────────────────────────────
ax = axes[1]
cancelled = [r for r in runs if r["type"] == "slow_poor" and r["rc"] == -1]
full = meta["slow_wall_s"] * meta["slow_cpu"]
xs = range(1, len(cancelled) + 1)
used = [r["cpu_s"] for r in cancelled]
ax.bar(xs, used, color=C_SLOW, label="CPU·s used (charged by AuCaM)")
ax.bar(xs, [full - u for u in used], bottom=used, color=C_SAVED, label="CPU·s avoided")
ax.axhline(full, ls="--", color=C_ANN, lw=1)
ax.text(0.5, full + 0.4, f"full workflow = {full:.0f} CPU·s", fontsize=8, color=C_ANN)
ax.set_xticks(list(xs))
ax.set_xlabel("cancelled slow_poor workflow")
ax.set_ylabel("CPU·s")
saved = sum(full - u for u in used)
ax.set_title(f"2. Cancellation saved {saved:.0f} CPU·s", loc="left")
ax.legend(loc="upper right", fontsize=8, frameon=False)
ax.set_xlim(0.4, len(cancelled) + 0.6)
ax.set_ylim(0, full * 1.45)

fig.suptitle(
    f"AuCaM capability demo — goal: {meta['goal_n']} fast_good completions, one workflow slot",
    fontsize=11,
    x=0.01,
    ha="left",
)
fig.tight_layout()
out = Path(args.out)
out.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(out, dpi=150)
print(f"figure → {out}")
