#!/usr/bin/env python3
"""Offline A/B policy comparison on a recorded observation trace.

Every phased campaign run automatically saves an observation trace to
adr-logs/trace_<job_id>.jsonl via RecordingObserver.  This script loads
that trace and replays alternative policies against it — no engine, no
tasks, no HPC — printing a cycle-by-cycle decision comparison table.

Three policies compared
───────────────────────
  ORIGINAL      ParentPolicy as deployed.  Stage 2 launches as soon
                as Stage 1 is DONE, regardless of Stage 1 score quality.

  CONSERVATIVE  Quality gate: Stage 1 must be DONE *and*
                stage1_score ≥ SCORE_GATE (default 0.70).
                Protects against committing Stage 2 resources when Stage 1
                produced low-quality candidates.

  AGGRESSIVE    Pre-emptive: launches Stage 2 as soon as
                stage1_score ≥ PREEMPT_THRESHOLD (default 0.50),
                even while Stage 1 is still running.

Usage
─────
    cd campaigns/ddsim_campaign
    python replay_policies.py adr-logs/trace_20274766.jsonl
    python replay_policies.py adr-logs/trace_20274766.jsonl --score-gate 0.65 --preempt 0.45
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from radical.adr import Decision, replay_trace
from radical.adr.policy.base import Policy, decide


# ── Replay-only policies ──────────────────────────────────────────────────────
# These are open-loop — they only see the frozen obs dict, never touch the engine.
# Each returns Decision(directives={"label": "..."}) for easy table rendering.

class ConservativePolicy(Policy):
    """Stage 1 must be DONE *and* stage1_score ≥ score_gate before Stage 2."""

    def __init__(self, score_gate: float) -> None:
        super().__init__()
        self._gate = score_gate

    @decide
    async def run(self, obs: dict) -> Decision:
        p1_uid, p2_uid     = obs["stage1_uid"], obs["stage2_uid"]
        p1_status, p2_status = obs["stage1_status"], obs["stage2_status"]
        score              = obs["stage1_score"]

        if p1_uid is None:
            return Decision(directives={"label": "launch_stage1"})
        if p1_status in ("FAILED", "CANCELED") and p2_uid is None:
            return Decision(stop=True)
        if p2_status in ("FAILED", "CANCELED"):
            return Decision(stop=True)
        if p1_status == "DONE" and p2_uid is None:
            if score >= self._gate:
                return Decision(directives={"label": "launch_stage2"})
            return Decision(directives={"label": f"hold  score={score:.3f}<{self._gate}"})
        if p2_status == "DONE":
            return Decision(stop=True)
        return Decision(directives={"label": "idle"})


class AggressivePolicy(Policy):
    """Launch Stage 2 as soon as stage1_score ≥ preempt_threshold."""

    def __init__(self, preempt_threshold: float) -> None:
        super().__init__()
        self._threshold = preempt_threshold

    @decide
    async def run(self, obs: dict) -> Decision:
        p1_uid, p2_uid     = obs["stage1_uid"], obs["stage2_uid"]
        p1_status, p2_status = obs["stage1_status"], obs["stage2_status"]
        score              = obs["stage1_score"]

        if p1_uid is None:
            return Decision(directives={"label": "launch_stage1"})
        if p1_status in ("FAILED", "CANCELED") and p2_uid is None:
            return Decision(stop=True)
        if p2_status in ("FAILED", "CANCELED"):
            return Decision(stop=True)
        if p2_uid is None and score >= self._threshold:
            return Decision(directives={"label": f"launch_stage2 ⚡ score={score:.3f}"})
        if p1_status == "DONE" and p2_uid is None:
            return Decision(directives={"label": "launch_stage2"})
        if p2_status == "DONE":
            return Decision(stop=True)
        return Decision(directives={"label": "idle"})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _recorded_label(decision_dict: dict) -> str:
    """Convert a serialised Decision (from JSONL) to a short label."""
    if decision_dict.get("stop"):
        return "STOP"
    names = [a.get("task_name", "?") for a in decision_dict.get("actions", [])]
    return ", ".join(names) if names else "idle"


def _replay_label(decision: Decision) -> str:
    return decision.directives.get("label", "STOP" if decision.stop else "idle")


def _p2_cycle(labels: list[str], records: list[dict]) -> str:
    for i, lbl in enumerate(labels):
        if "launch_stage2" in lbl:
            return str(records[i]["obs"]["cycle"])
    return "never"


# ── Main ──────────────────────────────────────────────────────────────────────

async def run(trace_path: Path, score_gate: float, preempt_threshold: float) -> None:
    records: list[dict] = []
    with open(trace_path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    if not records:
        print(f"[error] trace is empty: {trace_path}")
        sys.exit(1)

    cons_decisions = await replay_trace(
        str(trace_path), ConservativePolicy(score_gate)
    )
    agg_decisions = await replay_trace(
        str(trace_path), AggressivePolicy(preempt_threshold)
    )

    COL = 32
    header = (
        f"{'c':>3}  {'p1_status':<12}  {'score':>6}  {'p2?':<4}  "
        f"{'ORIGINAL':<{COL}}  {'CONSERVATIVE':<{COL}}  {'AGGRESSIVE':<{COL}}"
    )
    sep = "─" * len(header)

    print()
    print("── Phased campaign — A/B policy replay ────────────────────────────")
    print(f"   trace              : {trace_path}")
    print(f"   cycles             : {len(records)}")
    print(f"   Conservative gate  : stage1_score ≥ {score_gate}")
    print(f"   Aggressive preempt : stage1_score ≥ {preempt_threshold}")
    print()
    print(header)
    print(sep)

    orig_labels = []
    cons_labels = []
    agg_labels  = []

    for i, rec in enumerate(records):
        obs      = rec["obs"]
        orig_lbl = _recorded_label(rec["decision"])
        cons_lbl = _replay_label(cons_decisions[i])
        agg_lbl  = _replay_label(agg_decisions[i])

        orig_labels.append(orig_lbl)
        cons_labels.append(cons_lbl)
        agg_labels.append(agg_lbl)

        diverge = orig_lbl != cons_lbl or orig_lbl != agg_lbl
        marker  = " ◄" if diverge else ""

        print(
            f"{obs['cycle']:>3}  {obs['stage1_status']:<12}  "
            f"{obs['stage1_score']:>6.3f}  "
            f"{'yes' if obs['stage2_uid'] else 'no':<4}  "
            f"{orig_lbl:<{COL}}  {cons_lbl:<{COL}}  {agg_lbl:<{COL}}{marker}"
        )

    print(sep)
    print()
    print("   Stage 2 launch cycle:")
    print(f"     Original     : cycle {_p2_cycle(orig_labels, records)}")
    print(f"     Conservative : cycle {_p2_cycle(cons_labels, records)}")
    print(f"     Aggressive   : cycle {_p2_cycle(agg_labels, records)}")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A/B policy comparison on a recorded phased campaign trace"
    )
    parser.add_argument("trace", help="path to trace JSONL (adr-logs/trace_<job>.jsonl)")
    parser.add_argument(
        "--score-gate", type=float, default=0.70, metavar="G",
        help="Conservative policy: minimum stage1_score to launch Stage 2 (default 0.70)",
    )
    parser.add_argument(
        "--preempt", type=float, default=0.50, metavar="P",
        help="Aggressive policy: stage1_score threshold for pre-emptive Stage 2 (default 0.50)",
    )
    args = parser.parse_args()
    asyncio.run(run(Path(args.trace), args.score_gate, args.preempt))
