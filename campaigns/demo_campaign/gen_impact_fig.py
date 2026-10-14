#!/usr/bin/env python3 -I
"""Generate AuCaM impact demo figure — 3 panels showing each feature's contribution.

Usage:
    python gen_impact_fig.py [--json-in PATH] [--out PATH]

Defaults:
    --json-in  impact_run.json  (same directory as this script)
    --out      ../../docs/figures/aucam_impact.png
"""
import argparse, json, math
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
from matplotlib.ticker import MaxNLocator

_HERE = Path(__file__).parent
ap = argparse.ArgumentParser()
ap.add_argument('--json-in', default=str(_HERE / 'impact_run.json'))
ap.add_argument('--out', default=str(_HERE / '../../docs/figures/aucam_impact.png'))
_args = ap.parse_args()

DATA   = Path(_args.json_in)
OUTPNG = Path(_args.out)

d = json.loads(DATA.read_text())
campaigns = d['campaigns']
meta      = d['meta']
N = len(campaigns)
xs = np.arange(1, N + 1)

# ── Colors ──────────────────────────────────────────────────────────────────
C_FAST  = '#1a7f37'   # green
C_SLOW  = '#cf222e'   # red
C_BP    = '#9a6700'   # amber
C_TREND = '#0969da'   # blue
C_GRID  = '#eaeef2'
C_ANN   = '#57606a'
C_BG    = '#f6f8fa'

# ── Derived series ───────────────────────────────────────────────────────────
elapsed    = np.array([c['elapsed_s']                       for c in campaigns])
bp_s       = np.array([c['bp_s']                            for c in campaigns])
slow_n     = np.array([c['launches'].get('slow_poor', 0)    for c in campaigns], int)
fast_n     = np.array([c['launches'].get('fast_good', 0)    for c in campaigns], int)
cpu_saved  = np.array([c['cpu_saved']                       for c in campaigns])

# CPU wasted WITHOUT cancellation (slow_poor would run full SLOW_WALL_S)
slow_count  = np.array([c['cancelled'].get('slow_poor', 0)  for c in campaigns], int)
cpu_wasted_full = slow_count * meta['slow_wall_s'] * meta['slow_cpu']
cpu_wasted_act  = cpu_saved * 0 + slow_count * (
    meta['slow_wall_s'] * meta['slow_cpu'] - cpu_saved / np.maximum(slow_count, 1)
)
# Per-cancelled run: expected cost and actual cost
for_panel1 = []
for c in campaigns:
    evs = c['cancel_events']
    for ev in evs:
        for_panel1.append({
            'campaign': c['name'],
            'expected': ev['expected_s'] * meta['slow_cpu'],
            'actual':   ev['actual_s']   * meta['slow_cpu'],
            'saved':    ev['saved_cpu_s'],
        })

# UCB at end of each campaign
C_ucb = 2.0
def ucb(n, mean, total):
    if n == 0 or total == 0:
        return float('inf')
    return mean + C_ucb * math.sqrt(math.log(total) / n)

ucb_fast = []
ucb_slow = []
for c in campaigns:
    arm = c['arm_stats']
    n_f = int(arm.get('fast_good', {}).get('n', 0))
    n_s = int(arm.get('slow_poor', {}).get('n', 0))
    m_f = float(arm.get('fast_good', {}).get('mean', 0))
    m_s = float(arm.get('slow_poor', {}).get('mean', 0))
    total = n_f + n_s
    ucb_fast.append(ucb(n_f, m_f, total))
    ucb_slow.append(ucb(n_s, m_s, total))

ucb_fast = np.array(ucb_fast)
ucb_slow = np.array(ucb_slow)

# ── Figure ───────────────────────────────────────────────────────────────────
fig, axes = plt.subplots(1, 3, figsize=(16, 5.2))
fig.patch.set_facecolor('white')
fig.suptitle(
    f'AuCaM Impact Demo — {N} campaigns · goal = {meta["goal_n"]} fast_good completions '
    f'· pool = {meta["cpu_pool"]:.0f} CPU units',
    fontsize=10.5, color=C_ANN, y=1.01
)

# ────────────────────────────────────────────────────────────────────────────
# Panel 1 — Cancellation: per-run stacked bars (actual + saved per cancel)
# ────────────────────────────────────────────────────────────────────────────
ax = axes[0]
ax.set_facecolor('white')
ax.set_title('Cancellation Saves CPU', fontsize=13, fontweight='bold', pad=8)

full_cost = meta['slow_wall_s'] * meta['slow_cpu']  # CPU·s if run to completion

if for_panel1:
    run_ids  = np.arange(1, len(for_panel1) + 1)
    actual_  = np.array([e['actual']   for e in for_panel1])
    saved_   = np.array([e['saved']    for e in for_panel1])
    labels_  = []
    run_counter: dict[str, int] = {}
    for e in for_panel1:
        letter = e['campaign'].split('-')[1].upper()
        run_counter[letter] = run_counter.get(letter, 0) + 1
        labels_.append(f"{letter}-{run_counter[letter]}")

    # Stacked: actual consumed (red) + saved (green) on top
    ax.bar(run_ids, actual_, color=C_SLOW, alpha=0.75, zorder=2,
           label=f'CPU consumed (cancelled)')
    ax.bar(run_ids, saved_,  bottom=actual_, color=C_FAST, alpha=0.75, zorder=2,
           label=f'CPU saved by cancellation')

    # Reference line: full-run cost
    ax.axhline(full_cost, color='#888', ls='--', lw=1.1, alpha=0.7,
               label=f'full run = {full_cost:.0f} CPU·s')

    # Inline savings labels inside each green segment
    for xi, a, s in zip(run_ids, actual_, saved_):
        mid = a + s / 2
        ax.text(xi, mid, f'−{s:.0f}', ha='center', va='center',
                fontsize=8.5, color='white', fontweight='bold')

    ax.set_xticks(run_ids)
    ax.set_xticklabels(labels_, fontsize=9)
    ax.set_xlabel('Cancelled slow_poor run', fontsize=10)
    ax.set_ylabel('CPU·seconds', fontsize=10)
    ax.set_ylim(0, full_cost * 1.28)
    ax.grid(axis='y', color=C_GRID, linewidth=0.8, zorder=0)
    ax.tick_params(labelsize=9)
    ax.legend(fontsize=8.5, loc='upper right', framealpha=0.95)

    # Callout inside chart
    ax.annotate(
        f'cancelled at ~3 s\nnot full {meta["slow_wall_s"]:.0f} s',
        xy=(run_ids[0], actual_[0] / 2),
        xytext=(run_ids[0] + 1.8, full_cost * 0.42),
        arrowprops=dict(arrowstyle='->', color=C_ANN, lw=1.1),
        fontsize=8.5, color=C_ANN,
        bbox=dict(boxstyle='round,pad=0.3', fc='white', ec='#d0d7de', alpha=0.95)
    )

# ────────────────────────────────────────────────────────────────────────────
# Panel 2 — Bandit learning: arm selection + UCB scores per campaign
# ────────────────────────────────────────────────────────────────────────────
ax = axes[1]
ax_ucb = ax.twinx()
ax.set_facecolor('white')
ax.set_title('Bandit Arm Selection', fontsize=13, fontweight='bold', pad=8)

w = 0.35
ax.bar(xs - w/2, fast_n, width=w, color=C_FAST, label='fast_good launches', zorder=2)
ax.bar(xs + w/2, slow_n, width=w, color=C_SLOW, label='slow_poor launches\n(→ backpressure + wasted CPU)', zorder=2)

# UCB lines on twin axis
ax_ucb.plot(xs, ucb_fast, 'o-', color=C_FAST, lw=1.5, ms=5,
            label='UCB fast_good', alpha=0.7)
ax_ucb.plot(xs, ucb_slow, 's--', color=C_SLOW, lw=1.5, ms=5,
            label='UCB slow_poor', alpha=0.7)
ax_ucb.set_ylabel('UCB score', fontsize=9, color=C_ANN)
ax_ucb.tick_params(labelsize=8, colors=C_ANN)
ax_ucb.set_ylim(0.5, max(max(ucb_fast), max(ucb_slow)) * 1.1)

# Shade exploit campaigns (slow_n == 0)
for i, (xi, s) in enumerate(zip(xs, slow_n)):
    if s == 0:
        ax.axvspan(xi - 0.5, xi + 0.5, color='#e8fde8', alpha=0.7, zorder=0)

# Legend
leg1 = ax.legend(fontsize=8, loc='upper left', framealpha=0.95)
leg2 = ax_ucb.legend(fontsize=8, loc='upper right', framealpha=0.95)
ax.add_artist(leg1)

ax.set_xlabel('Campaign', fontsize=10)
ax.set_ylabel('Workflow launches', fontsize=10)
ax.set_xticks(xs)
ax.set_xticklabels([c['name'].replace('impact-', '').upper() for c in campaigns])
ax.yaxis.set_major_locator(MaxNLocator(integer=True))
ax.grid(axis='y', color=C_GRID, linewidth=0.8, zorder=0)
ax.tick_params(labelsize=9)

ax.annotate('green bg = exploit campaign\n(bandit skips slow_poor)',
            xy=(0.02, 0.02), xycoords='axes fraction',
            ha='left', fontsize=8, color=C_FAST,
            bbox=dict(boxstyle='round,pad=0.3', fc='white', ec='#c8e6c9', alpha=0.9))

# ────────────────────────────────────────────────────────────────────────────
# Panel 3 — Throughput: time to goal per campaign
# ────────────────────────────────────────────────────────────────────────────
ax = axes[2]
ax.set_facecolor('white')
ax.set_title('Goal Time Improvement', fontsize=13, fontweight='bold', pad=8)

# Color bars by explore (slow launched) vs exploit (no slow)
bar_colors = [C_SLOW if s > 0 else C_FAST for s in slow_n]
bars = ax.bar(xs, elapsed, color=bar_colors, alpha=0.75, zorder=2)

# Trend line (rolling 3-campaign average)
window = 3
trend = np.convolve(elapsed, np.ones(window)/window, mode='valid')
trend_xs = xs[window-1:]
ax.plot(trend_xs, trend, 'o-', color=C_TREND, lw=2, ms=5,
        label=f'{window}-campaign rolling avg', zorder=3)

# Ideal line: 0 slow_poor (all fast_good only)
min_time = meta['goal_n'] * meta['fast_wall_s']
ax.axhline(min_time, color=C_FAST, ls='--', lw=1.2, alpha=0.8,
           label=f'ideal (no slow_poor): {min_time:.1f}s', zorder=1)

# Annotations
exploit_idx = [i for i, s in enumerate(slow_n) if s == 0]
explore_idx  = [i for i, s in enumerate(slow_n) if s > 0]

if explore_idx:
    avg_explore = np.mean(elapsed[explore_idx])
    ax.annotate(f'explore avg: {avg_explore:.1f}s',
                xy=(xs[explore_idx[0]], avg_explore + 0.3),
                fontsize=8.5, color=C_SLOW, ha='left',
                bbox=dict(boxstyle='round,pad=0.3', fc='white', ec='#ffd8d8', alpha=0.9))

if exploit_idx:
    avg_exploit = np.mean(elapsed[exploit_idx])
    ax.annotate(f'exploit avg: {avg_exploit:.1f}s',
                xy=(xs[exploit_idx[-1]], avg_exploit - 0.8),
                fontsize=8.5, color=C_FAST, ha='right',
                bbox=dict(boxstyle='round,pad=0.3', fc='white', ec='#c8e6c9', alpha=0.9))

ax.set_xlabel('Campaign', fontsize=10)
ax.set_ylabel('Time to goal (s)', fontsize=10)
ax.set_xticks(xs)
ax.set_xticklabels([c['name'].replace('impact-', '').upper() for c in campaigns])
ax.set_ylim(0, max(elapsed) * 1.2)
ax.grid(axis='y', color=C_GRID, linewidth=0.8, zorder=0)
ax.tick_params(labelsize=9)

leg_els = [
    mpatches.Patch(color=C_SLOW, alpha=0.75, label='explore (slow_poor launched)'),
    mpatches.Patch(color=C_FAST, alpha=0.75, label='exploit (fast_good only)'),
    Line2D([0],[0], color=C_TREND, lw=2, marker='o', ms=5,
           label=f'{window}-campaign rolling avg'),
    Line2D([0],[0], color=C_FAST, ls='--', lw=1.2, label=f'ideal: {min_time:.1f}s'),
]
ax.legend(handles=leg_els, fontsize=8, loc='upper right', framealpha=0.95)

# ── Save ─────────────────────────────────────────────────────────────────────
plt.tight_layout(rect=[0, 0, 1, 0.97])
fig.savefig(OUTPNG, dpi=150, bbox_inches='tight', facecolor='white')
print(f'saved → {OUTPNG}  ({OUTPNG.stat().st_size // 1024} KB)')
