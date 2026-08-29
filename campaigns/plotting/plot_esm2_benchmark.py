#!/usr/bin/env python3
"""Generate plots for slide 15 (ESM2 + DDSim campaign) from telemetry_benchmark_results.json."""
import json, sys, argparse
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
from pathlib import Path

# ── Light theme (matches presentation) ───────────────────────────────────────
BG_FIG  = "#F4F7FD"
BG_AX   = "#FFFFFF"
C_INF   = "#3B82F6"   # blue  — inference
C_DUMMY = "#F59E0B"   # amber — dummy
C_RULE  = "#64748B"   # slate — rule policy
C_TELE  = "#3B82F6"   # blue  — rule_telemetry
GRID    = "#E2E8F0"
TEXT    = "#1E293B"
TEXT2   = "#64748B"

POLICY_LABELS = {
    "rule":           "Rule (baseline)",
    "rule_telemetry": "Rule + Telemetry",
}
POLICY_COLORS = {
    "rule":           C_RULE,
    "rule_telemetry": C_TELE,
}


def load(path: Path):
    with open(path) as f:
        return json.load(f)


def _style_ax(ax):
    ax.set_facecolor(BG_AX)
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color(GRID)
    ax.tick_params(colors=TEXT2, labelsize=8)
    ax.xaxis.label.set_color(TEXT2)
    ax.yaxis.label.set_color(TEXT2)
    ax.grid(axis="x", color=GRID, linewidth=0.6, zorder=0)


def make_pipeline_plot(data: dict, out: Path):
    """
    Two-panel figure:
      Top:    Gantt — inference & dummy spans + shard trigger markers (single representative run)
      Bottom: ESM2 score distribution across all runs, both policies
    """
    fig = plt.figure(figsize=(7.2, 6.2), facecolor=BG_FIG)
    gs  = fig.add_gridspec(2, 1, height_ratios=[3, 1.6], hspace=0.42,
                           left=0.10, right=0.97, top=0.93, bottom=0.08)
    ax_gantt = fig.add_subplot(gs[0])
    ax_score = fig.add_subplot(gs[1])

    # ── TOP: Gantt ────────────────────────────────────────────────────────────
    _style_ax(ax_gantt)
    ax_gantt.grid(axis="x", color=GRID, linewidth=0.6, zorder=0)
    ax_gantt.grid(axis="y", visible=False)

    policies     = ["rule", "rule_telemetry"]
    bar_h        = 0.32
    group_gap    = 0.18   # gap between inference and dummy row within a policy
    policy_gap   = 0.55   # gap between the two policies
    yticks, ylabels = [], []

    base_y = 0.0
    for pi, policy in enumerate(policies):
        run  = data[policy][0]           # representative run (run 0)
        gs_d = run["group_stats"]
        se   = run["shard_events"]

        inf_start  = gs_d["inference"]["first_start"]
        inf_end    = gs_d["inference"]["last_finish"]
        dum_start  = gs_d["dummy"]["first_start"]
        dum_end    = gs_d["dummy"]["last_finish"]
        wall       = run["wall_time_s"]

        # Inference bar
        y_inf = base_y + bar_h + group_gap
        ax_gantt.barh(y_inf, inf_end - inf_start, left=inf_start,
                      height=bar_h, color=C_INF, alpha=0.85, zorder=3)
        yticks.append(y_inf);  ylabels.append("inference")

        # Dummy bar
        y_dum = base_y
        ax_gantt.barh(y_dum, dum_end - dum_start, left=dum_start,
                      height=bar_h, color=C_DUMMY, alpha=0.85, zorder=3)
        yticks.append(y_dum);  ylabels.append("dummy")

        # Shard trigger markers (colored by score)
        scores = [e["mean_score"] for e in se]
        times  = [e["timestamp"] for e in se]
        norm   = plt.Normalize(0.5, 1.0)
        cmap   = plt.cm.RdYlGn
        for t, sc in zip(times, scores):
            ax_gantt.vlines(t, y_dum, y_inf + bar_h, color=cmap(norm(sc)),
                            linewidth=1.4, alpha=0.75, zorder=4)

        # Wall-time end marker
        ax_gantt.axvline(wall, ymin=(base_y - 0.04) / (base_y + 2 * bar_h + group_gap + 0.12),
                         color=POLICY_COLORS[policy], linewidth=1.2,
                         linestyle="--", alpha=0.5, zorder=2)

        # Policy label on left
        mid_y = (y_dum + y_inf + bar_h) / 2
        ax_gantt.text(-18, mid_y, POLICY_LABELS[policy],
                      ha="right", va="center", fontsize=8.5, fontweight="bold",
                      color=POLICY_COLORS[policy])

        base_y += 2 * bar_h + group_gap + policy_gap

    ax_gantt.set_yticks(yticks)
    ax_gantt.set_yticklabels(ylabels, fontsize=8)
    ax_gantt.set_xlabel("Wall time (s)", fontsize=9)
    ax_gantt.set_title("Pipeline streaming — inference triggers dummy incrementally",
                       fontsize=9.5, color=TEXT, pad=6, loc="left")

    # Legend
    inf_patch  = mpatches.Patch(color=C_INF,   alpha=0.85, label="inference (ESM2)")
    dum_patch  = mpatches.Patch(color=C_DUMMY,  alpha=0.85, label="dummy (DDSim)")
    trig_line  = Line2D([0], [0], color="forestgreen", linewidth=1.5,
                         label="trigger (high score)")
    ax_gantt.legend(handles=[inf_patch, dum_patch, trig_line],
                    fontsize=7.5, framealpha=0.85, loc="lower right",
                    handlelength=1.4, borderpad=0.5)

    # ── BOTTOM: ESM2 score distribution ──────────────────────────────────────
    _style_ax(ax_score)
    ax_score.grid(axis="y", color=GRID, linewidth=0.6, zorder=0)
    ax_score.grid(axis="x", visible=False)

    all_scores = []   # (x, score, policy_idx) for strip plot
    bp_data    = []   # one list per policy for box plot
    for pi, policy in enumerate(policies):
        scores = [e["mean_score"]
                  for run in data[policy]
                  for e in run["shard_events"]]
        bp_data.append(scores)
        for s in scores:
            all_scores.append((pi, s, pi))

    bp = ax_score.boxplot(bp_data, positions=[0, 1], widths=0.45,
                          patch_artist=True, zorder=3,
                          medianprops=dict(color="white", linewidth=2),
                          whiskerprops=dict(color=TEXT2),
                          capprops=dict(color=TEXT2),
                          flierprops=dict(marker="o", markersize=3,
                                          markerfacecolor=TEXT2, alpha=0.4))
    for patch, policy in zip(bp["boxes"], policies):
        patch.set_facecolor(POLICY_COLORS[policy])
        patch.set_alpha(0.7)

    # Jitter dots
    rng = np.random.default_rng(42)
    for pi, policy in enumerate(policies):
        scores = bp_data[pi]
        jitter = rng.uniform(-0.12, 0.12, len(scores))
        ax_score.scatter(np.array([pi] * len(scores)) + jitter, scores,
                         color=POLICY_COLORS[policy], s=22, alpha=0.75, zorder=5)

    # Mean annotation
    for pi, policy in enumerate(policies):
        m = np.mean(bp_data[pi])
        ax_score.text(pi, 1.04, f"μ={m:.2f}", ha="center", va="bottom",
                      fontsize=8, color=POLICY_COLORS[policy], fontweight="bold")

    ax_score.set_xticks([0, 1])
    ax_score.set_xticklabels([POLICY_LABELS[p] for p in policies], fontsize=8.5)
    ax_score.set_ylabel("ESM2 embedding score", fontsize=9)
    ax_score.set_ylim(0.45, 1.08)
    ax_score.set_title("ESM2 embedding quality — 5 runs × 8 triggers each (40 scores per policy)",
                       fontsize=9.5, color=TEXT, pad=6, loc="left")

    fig.suptitle("ESM2 + DDSim Campaign · Telemetry Benchmark",
                 fontsize=11, color=TEXT, fontweight="bold", y=0.985)

    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor=BG_FIG)
    plt.close(fig)
    print(f"Saved: {out}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", default="telemetry_benchmark_results.json")
    ap.add_argument("--out",  default="esm2_pipeline.png")
    args = ap.parse_args()

    data = load(Path(args.json))
    make_pipeline_plot(data, Path(args.out))


if __name__ == "__main__":
    main()
