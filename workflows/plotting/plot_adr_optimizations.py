#!/usr/bin/env python3
"""
plot_adr_optimizations.py — plot_optimizations-style figures for the ADR policy
benchmark (benchmark_adr.py output).

benchmark_adr.py emits the same per-run metrics shape as benchmark.py
(wall_time_s, replica_events, group_stats, time_to_target_s), keyed by ADR
policy name (none / rule / bandit / llm) instead of feature-flag config.  This
script reuses plot_optimizations.py's config-agnostic plot functions with an
ADR-policy colour palette, so you get the rich outcome plots — not just the
per-cycle decision trace that plot_policy_comparison.py shows.

Produces (under --out-dir):
  1_wall_time.png       wall time to target, per policy
  2_pipeline_gantt.png  stage execution overlap, per policy
  3_cascade_funnel.png  total instances launched per stage, per policy (compute)
  7_time_to_target.png  cumulative terminal-stage completions over wall time

(GPU-utilization / shard-dispatch / bandit-convergence are skipped — they
hardcode the bandit-study config names and the in-CM bandit was removed; the
ADR bandit's posteriors live in the decision logs → plot_policy_comparison.py.)

Usage:
    python plot_adr_optimizations.py [--results benchmark_adr_results.json] [--out-dir plots/adr]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import plot_optimizations as po   # reuse its plotting functions + workflow palette

# ── ADR policy palette (overrides the feature-flag config palette) ────────────
ADR_CFG_COLORS = {
    "none":   "#9e9e9e",   # static priorities (no ADR)
    "rule":   "#4caf50",   # deterministic downstream-first
    "bandit": "#9c27b0",   # Thompson-sampling, as an ADR agent
    "llm":    "#00838f",   # LLM-driven
}
ADR_CFG_DISPLAY = {
    "none":   "no ADR (static)",
    "rule":   "rule",
    "bandit": "bandit",
    "llm":    "llm",
}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results", default="benchmark_adr_results.json")
    ap.add_argument("--out-dir", default="plots/adr")
    args = ap.parse_args()

    with open(args.results) as f:
        results = json.load(f)
    # Keep only known ADR policy keys (mirrors plot_optimizations.main's
    # `if k in CFG_COLORS` filter, but for the ADR palette).
    results = {k: v for k, v in results.items() if k in ADR_CFG_COLORS}
    if not results:
        raise SystemExit(
            f"No ADR policy keys {list(ADR_CFG_COLORS)} found in {args.results}. "
            "Did you run benchmark_adr.py?")

    # Monkeypatch the reused module's config palette + study-specific cosmetics
    # so its plot functions label/colour by ADR policy and reference the right
    # baseline. The functions look these names up at call time.
    po.CFG_COLORS = ADR_CFG_COLORS
    po.CFG_DISPLAY = ADR_CFG_DISPLAY
    po._EXCLUDE = set()                       # don't drop any policy
    # Reference policy for the wall-time % and funnel ratio: prefer 'none'
    # (true no-ADR baseline) if present, else the deterministic 'rule'.
    po.BASELINE_KEY = "none" if "none" in results else "rule"
    base = po.CFG_DISPLAY.get(po.BASELINE_KEY, po.BASELINE_KEY)
    po.WALL_CAPTION = (
        "LOWER IS BETTER.  Wall-clock time until the 5th lead, per ADR scheduling "
        f"policy.  Bar = median; white dots = individual runs.  % is vs '{base}'.  "
        "rule = deterministic downstream-first; bandit = Thompson-sampling as an "
        "ADR agent; llm = LLM-driven (falls back to rule on slow/failed calls)."
    )
    po.FUNNEL_CAPTION = (
        "LOWER IS BETTER.  Total workflow instances launched to reach 5 leads, "
        "stacked by stage, per ADR policy.  Fewer = the policy steered the cascade "
        f"more efficiently.  Ratio is vs '{base}'."
    )
    po.TTT_CAPTION = (
        "LEFTMOST ▼ IS BEST.  Cumulative terminal-stage completions over wall time, "
        "per ADR policy.  Faint = individual runs; bold = median; ▼ = 5-lead target."
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Policies: {list(results.keys())}")
    po.plot_wall_time(results, out_dir)
    po.plot_gantt(results, out_dir)
    po.plot_cascade_funnel(results, out_dir)
    po.plot_time_to_target(results, out_dir)
    print(f"\nADR outcome plots written to {out_dir}/")


if __name__ == "__main__":
    main()
