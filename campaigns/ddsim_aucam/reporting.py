"""Output helpers: JSONL results writer and post-run summary tables."""

from __future__ import annotations

import json
from pathlib import Path

from src.aucam import CampaignOutcome, SchedulingPolicy, WorkflowResult

from config import CampaignConfig


class _ResultsWriter:
    """Appends one JSON line per completed WorkflowResult to a JSONL file."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._f = open(self._path, "a")

    def write(self, result: WorkflowResult) -> None:
        record = {
            "workflow_id": result.workflow_id,
            "workflow_type": result.workflow_type,
            "returncode": result.returncode,  # 0=ok  -1=cancelled  N=error
            "wall_time_s": result.wall_time_s,
            "spent": result.spent,
            "resource_profile": result.resource_profile,
            "final_metrics": result.final_metrics,
        }
        self._f.write(json.dumps(record) + "\n")
        self._f.flush()

    def close(self) -> None:
        self._f.close()


def _log_comparison(triples: list[tuple[str, CampaignOutcome, CampaignConfig]]) -> None:
    """Print a per-arm MSE and CPU-hours table."""
    best_name = None
    best_score = float("inf")

    w = 97
    print("\n" + "─" * w)
    print(
        f"{'Campaign':<26}  {'goal':>5}  "
        f"{'fast n':>6}  {'fast best':>9}  {'fast avg':>9}  "
        f"{'slow n':>6}  {'slow best':>9}  {'slow avg':>9}  "
        f"{'cpu_h':>8}"
    )
    print("─" * w)

    for name, outcome, _ccfg in triples:
        s = outcome.summary or {}
        goal = "YES" if outcome.goal_reached else "no"
        cpu = outcome.spent.get("cpu_hours", 0.0)

        def _fmt(arm: str, field: str, s: dict = s) -> str:
            v = (s.get(arm) or {}).get(field)
            return f"{v:.4f}" if v is not None else "      –"

        fn = (s.get("miniapps_fast") or {}).get("n", 0)
        sn = (s.get("miniapps_slow") or {}).get("n", 0)

        print(
            f"{name:<26}  {goal:>5}  "
            f"{fn:>6}  {_fmt('miniapps_fast', 'best'):>9}  {_fmt('miniapps_fast', 'avg'):>9}  "
            f"{sn:>6}  {_fmt('miniapps_slow', 'best'):>9}  {_fmt('miniapps_slow', 'avg'):>9}  "
            f"{cpu:>8.5f}"
        )

        overall_best = (s.get("overall") or {}).get("best")
        if overall_best is not None and overall_best < best_score:
            best_score = overall_best
            best_name = name

    print("─" * w)
    if best_name:
        print(f"Best campaign: {best_name}  (overall best MSE={best_score:.4f})")
    print()


def _print_policy_summary(policy: SchedulingPolicy | None) -> None:
    """Print bandit arm pull counts, mean reward (μ), and UCB score."""
    if policy is None:
        return
    if hasattr(policy, "arm_stats"):
        stats = policy.arm_stats()
        if stats:
            print("\n[POLICY] bandit arm statistics:")
            for arm, s in sorted(stats.items()):
                ucb = s.get("ucb", 0)
                ucb_str = "  ∞  " if ucb == float("inf") else f"{ucb:.3f}"
                print(
                    f"   {arm:20s}  n={s.get('n', 0):4d}  μ={s.get('mean', 0):.3f}  ucb={ucb_str}"
                )
