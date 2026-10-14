#!/usr/bin/env python3
"""
compare_paths.py — Path 1 (TaskProfiler) vs Path 2 (JSONL correlation) proof.

Two modes:

  Live (default) — runs a fresh local campaign and compares in-process:
    cd campaign_manager
    python campaigns/dummy_campaign/compare_paths.py

  Post-hoc — reads results saved by a prior sbatch/Dragon run:
    python campaigns/dummy_campaign/compare_paths.py \\
        --post --telemetry-dir /path/to/aucam-tel \\
        [--output /tmp/comparison.json]

  The sbatch run auto-saves results.jsonl into --telemetry-dir; this mode
  reads that file alongside the telemetry JSONL and compares both paths.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.aucam import (
    CampaignBudget,
    CampaignManager,
    CampaignSpec,
    ScoreDirection,
    WorkflowConfig,
    WorkflowDependency,
    WorkflowResult,
)
from src.aucam.campaign import _CampaignInstance

from controller import ScoreController
import workflows as _wf


# ── JSONL correlation helper (mirrors what _CampaignInstance uses) ────────────

def _make_correlator(telemetry_dir: str, session_id: str):
    """Return a minimal object that can call _correlate_from_jsonl."""
    tel = SimpleNamespace(_checkpoint_path=telemetry_dir, _session_id=session_id)
    obj = SimpleNamespace(_flow=SimpleNamespace(_telemetry=tel), _spent={})
    obj._correlate_from_jsonl = _CampaignInstance._correlate_from_jsonl.__get__(obj)
    return obj, tel


# ── results collector ─────────────────────────────────────────────────────────

class _ResultsCollector:
    def __init__(self, ctrl: ScoreController):
        self._ctrl = ctrl
        self.results: list[WorkflowResult] = []

    def on_complete(self, result: WorkflowResult) -> None:
        self._ctrl.on_complete(result)
        self.results.append(result)


# ── campaign runner ───────────────────────────────────────────────────────────

async def run_campaign(telemetry_dir: str, goal: int, max_concurrent: int) -> _ResultsCollector:
    collector = _ResultsCollector(ScoreController(goal_analyses=goal))

    spec = CampaignSpec(
        name="dummy-campaign",
        budget=CampaignBudget(
            max_concurrent_workflows=max_concurrent,
            max_concurrency_per_workflow=2,
        ),
        controller=collector._ctrl,
        workflows=[
            WorkflowConfig(
                name="sim_worker",
                block=_wf.sim_worker,
                score_directions={"score": ScoreDirection.HIGHER},
                stale_timeout_s=30.0,
                launch_on_start=True,
                on_completion=collector.on_complete,
            ),
            WorkflowConfig(
                name="analysis_worker",
                block=_wf.analysis_worker,
                launch_on_start=False,
                on_completion=collector.on_complete,
            ),
        ],
        dependencies=[
            WorkflowDependency(
                source="sim_worker",
                target="analysis_worker",
                condition=lambda r: r.returncode == 0,
            ),
        ],
        policy_interval_s=0.5,
        shutdown_timeout_s=10.0,
        use_asyncflow_blocks=True,
    )

    cm = CampaignManager(
        enable_telemetry=True,
        telemetry_checkpoint_dir=telemetry_dir,
    )
    cm.submit_campaign(spec)
    await cm.run()
    return collector


# ── comparison ────────────────────────────────────────────────────────────────

def compare(collector: _ResultsCollector, telemetry_dir: str) -> list[dict]:
    jsonl_files = list(Path(telemetry_dir).glob("asyncflow.session.*.telemetry.jsonl"))
    if not jsonl_files:
        print("WARNING: no JSONL found — Path 2 will be empty", file=sys.stderr)
        return []

    jsonl = max(jsonl_files, key=lambda p: p.stat().st_mtime)
    session_id = ".".join(jsonl.name.split(".")[:3])  # asyncflow.session.XXXXXXXX
    print(f"JSONL: {jsonl.name}  session_id={session_id}")

    correlator, tel = _make_correlator(telemetry_dir, session_id)

    rows = []
    for result in collector.results:
        fm = result.final_metrics or {}
        p2, _ = correlator._correlate_from_jsonl(result.workflow_id, result.wall_time_s, tel)

        # P1: independent psutil monitor thread (returned via head-node emit).
        # Falls back to resource_profile when profiler_cpu_avg not present.
        p1_cpu = fm.get("profiler_cpu_avg")
        p1_n   = fm.get("profiler_n", 0)
        if p1_cpu is not None:
            p1_src = "profiler_thread"
        else:
            rp = result.resource_profile
            p1_cpu = rp.get("cpu_percent_avg")
            p1_n   = rp.get("n_samples", 0)
            p1_src = rp.get("source", "absent") if p1_cpu is not None else "absent"

        row = {
            "wf_id":   result.workflow_id,
            "wf_type": result.workflow_type,
            "wall_s":  round(result.wall_time_s, 3),
            "rc":      result.returncode,
            "p1_source":   p1_src,
            "p1_cpu_avg":  p1_cpu,
            "p1_n":        p1_n,
            "p2_source":   p2.get("source", "none"),
            "p2_cpu_avg":  p2.get("cpu_percent_avg"),
            "p2_cpu_peak": p2.get("cpu_percent_peak"),
            "p2_mem_avg":  p2.get("memory_percent_avg"),
            "p2_nodes":    p2.get("nodes", []),
            "p2_n":        p2.get("n_samples", 0),
            "p2_dur":      p2.get("duration_s"),
        }
        both = row["p1_cpu_avg"] is not None and row["p2_cpu_avg"] is not None
        row["status"] = "ok" if both else (
            "p1-only" if row["p1_cpu_avg"] is not None else
            "p2-only" if row["p2_cpu_avg"] is not None else "none"
        )
        rows.append(row)

        p1v = f"{row['p1_cpu_avg']:.1f}%" if row["p1_cpu_avg"] is not None else "—"
        p2v = f"{row['p2_cpu_avg']:.1f}%" if row["p2_cpu_avg"] is not None else "—"
        print(f"  {result.workflow_id[-20:]:>20}  {result.workflow_type:16}  "
              f"wall={result.wall_time_s:.2f}s  "
              f"P1({p1_src})={p1v:>6}  P2({row['p2_source']})={p2v:>6}  [{row['status']}]")

    return rows


# ── main ──────────────────────────────────────────────────────────────────────

# ── post-hoc mode: read results.jsonl saved by a prior run ───────────────────

def load_saved_results(telemetry_dir: str) -> list[dict]:
    """Read results.jsonl written by run_campaign.py's _ResultsWriter."""
    path = Path(telemetry_dir) / "results.jsonl"
    if not path.exists():
        raise FileNotFoundError(
            f"results.jsonl not found in {telemetry_dir}\n"
            "Make sure the campaign was run with --telemetry-dir set."
        )
    results = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            results.append(json.loads(line))
    print(f"Loaded {len(results)} results from {path}")
    return results


def compare_post(saved_results: list[dict], telemetry_dir: str) -> list[dict]:
    """Post-hoc compare: saved resource_profile (P1) vs JSONL correlation (P2)."""
    jsonl_files = list(Path(telemetry_dir).glob("asyncflow.session.*.telemetry.jsonl"))
    if not jsonl_files:
        print("WARNING: no JSONL found — Path 2 will be empty", file=sys.stderr)
        return []

    jsonl = max(jsonl_files, key=lambda p: p.stat().st_mtime)
    session_id = ".".join(jsonl.name.split(".")[:3])
    print(f"JSONL: {jsonl.name}  session_id={session_id}")

    correlator, tel = _make_correlator(telemetry_dir, session_id)

    rows = []
    for rec in saved_results:
        wf_id  = rec["workflow_id"]
        wall_s = rec["wall_time_s"]
        p2, _ = correlator._correlate_from_jsonl(wf_id, wall_s, tel)

        # P1: independent psutil monitor thread stored in final_metrics.
        # Falls back to resource_profile for older runs without the thread.
        fm     = rec.get("final_metrics") or {}
        p1_cpu = fm.get("profiler_cpu_avg")
        p1_n   = fm.get("profiler_n", 0)
        if p1_cpu is not None:
            p1_src = "profiler_thread"
        else:
            rp     = rec.get("resource_profile") or {}
            p1_cpu = rp.get("cpu_percent_avg")
            p1_n   = rp.get("n_samples", 0)
            p1_src = rp.get("source", "absent") if p1_cpu is not None else "absent"

        row = {
            "wf_id":   wf_id,
            "wf_type": rec["workflow_type"],
            "wall_s":  round(wall_s, 3),
            "rc":      rec["returncode"],
            "p1_source":   p1_src,
            "p1_cpu_avg":  p1_cpu,
            "p1_n":        p1_n,
            "p2_source":   p2.get("source", "absent"),
            "p2_cpu_avg":  p2.get("cpu_percent_avg"),
            "p2_cpu_peak": p2.get("cpu_percent_peak"),
            "p2_mem_avg":  p2.get("memory_percent_avg"),
            "p2_nodes":    p2.get("nodes", []),
            "p2_n":        p2.get("n_samples", 0),
            "p2_dur":      p2.get("duration_s"),
        }
        both = row["p1_cpu_avg"] is not None and row["p2_cpu_avg"] is not None
        row["status"] = "ok" if both else (
            "p1-only" if row["p1_cpu_avg"] is not None else
            "p2-only" if row["p2_cpu_avg"] is not None else "none"
        )
        rows.append(row)

        p1v = f"{row['p1_cpu_avg']:.1f}%" if row["p1_cpu_avg"] is not None else "—"
        p2v = f"{row['p2_cpu_avg']:.1f}%" if row["p2_cpu_avg"] is not None else "—"
        print(f"  {wf_id[-20:]:>20}  {rec['workflow_type']:16}  "
              f"wall={wall_s:.2f}s  P1({p1_src})={p1v:>6}  P2={p2v:>6}  [{row['status']}]")
    return rows


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Compare Path 1 vs Path 2 resource attribution")
    ap.add_argument("--post", action="store_true",
                    help="post-hoc mode: read results.jsonl + JSONL from --telemetry-dir")
    ap.add_argument("--telemetry-dir", default=None,
                    help="telemetry directory (required for --post; auto-created for live mode)")
    ap.add_argument("--output", default=None,
                    help="write comparison JSON here (default: /tmp/path_comparison.json)")
    ap.add_argument("--goal-analyses", type=int, default=5)
    ap.add_argument("--max-concurrent", type=int, default=3)
    ap.add_argument("--n-iters",        type=int,   default=60,
                    help="live mode: iterations per sim block (60 × 0.15 s ≈ 9 s)")
    ap.add_argument("--iter-sleep",     type=float, default=0.15,
                    help="live mode: sleep per iteration")
    ap.add_argument("--analysis-steps", type=int,   default=10)
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)-8s %(name)-30s %(message)s",
    )

    if args.post:
        # ── post-hoc mode ──────────────────────────────────────────────────
        if not args.telemetry_dir:
            ap.error("--post requires --telemetry-dir")
        saved = load_saved_results(args.telemetry_dir)
        rows  = compare_post(saved, args.telemetry_dir)
        config_info = {"mode": "post", "telemetry_dir": args.telemetry_dir}
    else:
        # ── live mode ──────────────────────────────────────────────────────
        _wf.configure(
            n_iters=args.n_iters,
            iter_sleep_s=args.iter_sleep,
            analysis_steps=args.analysis_steps,
        )
        print(f"Block timing: sim ≈ {args.n_iters * args.iter_sleep:.1f}s · "
              f"ana ≈ {args.analysis_steps * args.iter_sleep:.1f}s")

        ctx = tempfile.TemporaryDirectory(prefix="aucam-cmp-") \
              if args.telemetry_dir is None else None
        tmpdir = ctx.name if ctx else args.telemetry_dir
        if ctx is None:
            Path(tmpdir).mkdir(parents=True, exist_ok=True)

        print(f"Telemetry dir: {tmpdir}")
        t0 = time.monotonic()
        collector = asyncio.run(run_campaign(tmpdir, args.goal_analyses, args.max_concurrent))
        elapsed = time.monotonic() - t0
        print(f"\nCampaign done in {elapsed:.1f}s — "
              f"{len(collector.results)} results collected\n")

        rows = compare(collector, tmpdir)
        if ctx:
            ctx.cleanup()
        config_info = {
            "mode":           "live",
            "n_iters":        args.n_iters,
            "iter_sleep":     args.iter_sleep,
            "analysis_steps": args.analysis_steps,
            "goal_analyses":  args.goal_analyses,
        }

    # ── shared summary ─────────────────────────────────────────────────────
    both  = [r for r in rows if r["status"] == "ok"]
    p1_ok = [r for r in rows if r["p1_cpu_avg"] is not None]
    p2_ok = [r for r in rows if r["p2_cpu_avg"] is not None]
    print(f"\nSummary: {len(rows)} blocks · "
          f"{len(p1_ok)} have P1 · {len(p2_ok)} have P2 · {len(both)} have both")
    if both:
        diffs = [abs(r["p1_cpu_avg"] - r["p2_cpu_avg"]) for r in both]
        print(f"  |P1 - P2| mean={sum(diffs)/len(diffs):.1f}%  max={max(diffs):.1f}%")

    out_data = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "config": config_info,
        "rows": rows,
    }
    out_path = args.output or "/tmp/path_comparison.json"
    Path(out_path).write_text(json.dumps(out_data, indent=2))
    print(f"\nComparison JSON saved: {out_path}")


if __name__ == "__main__":
    main()
