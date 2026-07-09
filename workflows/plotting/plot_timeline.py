#!/usr/bin/env python3
"""
Plot campaign timeline with optional Dreamer emulation statistics.

Long-campaign superset of ``plot_dep_timeline.py``: shares the same log parser,
Gantt chart, and resource-utilization row, and adds a third row of Dreamer
emulation metrics when dreamer-profiles/ are present. Rows 0–1 work for any
AsyncCampaignManager campaign; use ``plot_dep_timeline.py`` for shorter campaigns
where per-replica dependency arrows are the primary insight.

Reads:
  - A campaign log file (ANSI-colored CM output)
  - dreamer profile JSON files from dreamer-profiles/ (auto-detected next to log)

Produces a 3-row figure:
  Row 0  Gantt chart (wall-clock replica execution) + workflow config table
  Row 1  CPU / GPU resource utilization over wall-clock time
  Row 2  Dreamer simulation metrics (only when dreamer-profiles/ exist):
           2a  Simulated makespan per replica, grouped by workflow
           2b  Task ops distribution per workflow (box plots from profile JSONs)
           2c  Per-workflow summary statistics table

Usage:
    python plot_timeline.py log [--profiles-dir DIR] [--config FILE] [--out FILE] [--title STR]
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

try:
    import yaml

    _HAVE_YAML = True
except ImportError:
    _HAVE_YAML = False

import matplotlib
import matplotlib.ticker

matplotlib.use("Agg")
import matplotlib.gridspec as gridspec
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np

# ── workflow appearance ──────────────────────────────────────────────────────────

GROUP_COLORS = {
    "s1_ligand_filter": "#4C72B0",
    "s2_ml_affinity": "#DD8452",
    "s3_docking": "#55A868",
    "s4_md_refinement": "#C44E52",
    "s5_fep_ranking": "#8172B2",
}
GROUP_ORDER = [
    "s1_ligand_filter",
    "s2_ml_affinity",
    "s3_docking",
    "s4_md_refinement",
    "s5_fep_ranking",
]
GROUP_LABELS = {
    "s1_ligand_filter": "S1 Filter",
    "s2_ml_affinity": "S2 ML",
    "s3_docking": "S3 Dock",
    "s4_md_refinement": "S4 MD",
    "s5_fep_ranking": "S5 FEP",
}
DEFAULT_COLOR = "#9E9E9E"

_FALLBACK_COLORS = [
    "#9C27B0",
    "#F06292",
    "#4DB6AC",
    "#FFB74D",
    "#BA68C8",
    "#4DD0E1",
    "#AED581",
    "#FF8A65",
]


def _resolve_colors(groups: list) -> None:
    """Assign fallback colors to groups not already in GROUP_COLORS."""
    used = set(GROUP_COLORS.values())
    cycle = [c for c in _FALLBACK_COLORS if c not in used] or _FALLBACK_COLORS
    ci = 0
    for g in groups:
        if g not in GROUP_COLORS:
            GROUP_COLORS[g] = cycle[ci % len(cycle)]
            ci += 1


# ── Log regexes ───────────────────────────────────────────────────────────────

_TS_RE = re.compile(r"\x1b\[2m(\d{2}:\d{2}:\d{2}\.\d{3})\x1b\[0m")
_START_RE = re.compile(r"starting replica '(\S+)'")
_FINISH_RE = re.compile(r"Replica '(\S+)' finished")
_ERROR_RE = re.compile(r"Replica '(\S+)' raised")
_GROUP_RE = re.compile(
    r"Registered group '(\S+)': replicas=(\d+) priority=(\d+) "
    r"min=(\d+) max=(\d+) deps=\[([^\]]*)\] dep_threshold=(\d+) "
    r"resources=\(cpus=(\d+), gpus=(\d+)\)"
)
_USAGE_RE = re.compile(r"resources: cpus=(\d+)/(\d+)\s+gpus=(\d+)/(\d+)")
_AVAIL_RE = re.compile(r"available: cpus=(\d+)/(\d+)\s+gpus=(\d+)/(\d+)")
_TOTAL_RES_RE = re.compile(r"Resource pool: total_cpus=(\d+)\s+total_gpus=(\d+)")
_TRIGGER_RE = re.compile(r"trigger_dependent: '(\S+)' \+(\d+) replicas \(total=(\d+)\)")
_STALL_RE = re.compile(r"Group '(\S+)' stalled — waiting for resources")

# dreamer: 128 cores × 256 tasks → 256 completed  avg_exec=15.89  makespan=42.82  (strategy=random)
_DREAMER_RE = re.compile(
    r"\[(\S+)\] dreamer: (\d+) cores × (\d+) tasks → (\d+) completed"
    r"\s+avg_exec=([\d.]+)\s+makespan=([\d.]+)\s+\(strategy=(\w+)\)"
)


# ── Log parser ────────────────────────────────────────────────────────────────


def parse_log(path):
    starts = {}
    spans = []
    group_meta = {}
    resource_timeline = []
    signal_events = []  # (elapsed_s, tgt_group, n)
    dreamer_stats = {}  # replica_id → dict
    stall_events = []  # (elapsed_s, group)
    t0_dt = None
    total_cpus = total_gpus = 0

    _iso_re = re.compile(r"^(\d{4}-\d{2}-\d{2}) \d{2}:\d{2}:\d{2}")
    date_ref = "1970-01-01"
    with open(path) as fh:
        for raw in fh:
            if m := _iso_re.match(raw):
                date_ref = m.group(1)
                break

    with open(path) as fh:
        for raw in fh:
            if m := _GROUP_RE.search(raw):
                name = m.group(1)
                deps = [
                    d.strip().strip("'\"") for d in m.group(6).split(",") if d.strip().strip("'\"")
                ]
                group_meta[name] = {
                    "replicas": int(m.group(2)),
                    "priority": int(m.group(3)),
                    "min": int(m.group(4)),
                    "max": int(m.group(5)),
                    "deps": deps,
                    "dep_threshold": int(m.group(7)),
                    "cpus": int(m.group(8)),
                    "gpus": int(m.group(9)),
                }
            if m := _TOTAL_RES_RE.search(raw):
                total_cpus, total_gpus = int(m.group(1)), int(m.group(2))
            if m := _DREAMER_RE.search(raw):
                dreamer_stats[m.group(1)] = {
                    "num_cores": int(m.group(2)),
                    "num_tasks": int(m.group(3)),
                    "tasks_completed": int(m.group(4)),
                    "avg_exec": float(m.group(5)),
                    "makespan": float(m.group(6)),
                    "strategy": m.group(7),
                }

            ts_m = _TS_RE.search(raw)
            if ts_m is None:
                continue
            dt = datetime.strptime(f"{date_ref} {ts_m.group(1)}", "%Y-%m-%d %H:%M:%S.%f")
            if t0_dt is None:
                t0_dt = dt
            elapsed = (dt - t0_dt).total_seconds()

            if m := _USAGE_RE.search(raw):
                uc, tc, ug, tg = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
                resource_timeline.append((elapsed, uc, tc, ug, tg))
            elif m := _AVAIL_RE.search(raw):
                ac, tc, ag, tg = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
                resource_timeline.append((elapsed, tc - ac, tc, tg - ag, tg))

            if m := _TRIGGER_RE.search(raw):
                signal_events.append((elapsed, m.group(1), int(m.group(2))))
            if m := _STALL_RE.search(raw):
                stall_events.append((elapsed, m.group(1)))

            if m := _START_RE.search(raw):
                starts[m.group(1)] = dt
            elif m := _FINISH_RE.search(raw):
                rid = m.group(1)
                if rid in starts:
                    spans.append((rid, _group(rid), starts.pop(rid), dt, True))
            elif m := _ERROR_RE.search(raw):
                rid = m.group(1)
                if rid in starts:
                    spans.append((rid, _group(rid), starts.pop(rid), dt, False))

    for rid, start in starts.items():
        spans.append((rid, _group(rid), start, start, None))

    resource_timeline.sort(key=lambda x: x[0])
    signal_events.sort(key=lambda x: x[0])

    t0 = min(s[2] for s in spans) if spans else t0_dt
    return (
        spans,
        group_meta,
        resource_timeline,
        signal_events,
        dreamer_stats,
        stall_events,
        t0,
        (total_cpus, total_gpus),
    )


def _group(rid):
    return rid.rsplit("_", 1)[0]


# ── Profile JSON loader ───────────────────────────────────────────────────────


def load_profiles(profiles_dir):
    profiles = {}
    pdir = Path(profiles_dir)
    if not pdir.exists():
        return profiles
    for fpath in sorted(pdir.rglob("*.json")):
        try:
            data = json.loads(fpath.read_text())
            rid = data.get("replica_id") or fpath.stem
            profiles[rid] = data
        except Exception:
            pass
    return profiles


def parse_config(path):
    """Parse group metadata from config.yaml (flat or plan format)."""
    if not _HAVE_YAML:
        return {}
    with open(path) as fh:
        cfg = yaml.safe_load(fh)
    out = {}

    if isinstance(cfg.get("workflows"), list):
        # cm-prototype plan format (list of stage dicts with "id" keys)
        _pilot_res = {
            "cpu": {"cpus": 16, "gpus": 0},
            "gpu": {"cpus": 4, "gpus": 1},
            "mpi+gpu": {"cpus": 16, "gpus": 2},
            "largemem": {"cpus": 8, "gpus": 1},
        }
        workflow_ids = {s["id"] for s in cfg.get("workflows", [])}
        scale = float(cfg.get("cm", {}).get("concurrency_scale", 1.0))
        for s in cfg.get("workflows", []):
            sid = s["id"]
            upstream = s.get("upstream", "")
            deps = [upstream] if upstream in workflow_ids else []
            cap = int(s.get("concurrency_cap", 0))
            # Accept legacy max_replicas key from old plan files.
            max_r = int(s.get("max_replicas", max(1, round(cap * scale)) if cap else 0))
            pilot = s.get("pilot", {})
            res = _pilot_res.get(pilot.get("partition", "cpu").lower(), {"cpus": 4, "gpus": 0})
            out[sid] = {
                "replicas": int(s.get("replicas", 0 if deps else 1)),
                "priority": int(s.get("priority", 0)),
                "min": 0,
                "max": max_r,
                "deps": deps,
                "dep_threshold": int(s.get("dependency_threshold", 1)),
                "cpus": res["cpus"],
                "gpus": res["gpus"],
            }
    else:
        # Legacy flat format
        for name, wf in cfg.get("workflows", {}).items():
            has_deps = bool(wf.get("dependencies", []))
            out[name] = {
                "replicas": int(wf.get("replicas", 0 if has_deps else 1)),
                "priority": int(wf.get("priority", 0)),
                # Accept both new and legacy keys.
                "min": int(wf.get("concurrency_floor", wf.get("min_replicas", 0))),
                "max": int(wf.get("concurrency_cap", wf.get("max_replicas", 0))),
                "deps": list(wf.get("dependencies", [])),
                "dep_threshold": int(wf.get("dependency_threshold", 1)),
                "cpus": int(wf.get("required_cpus", 0)),
                "gpus": int(wf.get("required_gpus", 0)),
            }
    return out


# ── Figure builder ────────────────────────────────────────────────────────────


def plot(
    spans,
    group_meta,
    resource_timeline,
    signal_events,
    dreamer_stats,
    stall_events,
    profiles,
    t0,
    total_resources,
    out_path,
    title="Campaign Timeline",
):
    if not spans:
        print("No replica events found.", file=sys.stderr)
        return

    # workflow ordering
    present = {s[1] for s in spans}
    ordered = [s for s in GROUP_ORDER if s in present] + sorted(present - set(GROUP_ORDER))
    _resolve_colors(ordered)

    def _sort(s):
        rid, grp, *_ = s
        return (ordered.index(grp) if grp in ordered else 999, int(rid.rsplit("_", 1)[-1]))

    spans.sort(key=_sort)

    n_workflows = len(ordered)
    total_cpus, total_gpus = total_resources
    has_resources = bool(resource_timeline)
    has_dreamer = bool(dreamer_stats) or bool(profiles)

    # ── Figure layout ────────────────────────────────────────────────────────
    # Row 0  summary table   (under title, full width)
    # Row 1  workflow-level concurrency Gantt
    # Row 2  resource util   (CPU/GPU)
    # Row 3  makespan dist + ops dist (dreamer metrics, 2 equal panels)
    summ_h = 1.5 if has_dreamer else 0
    gantt_h = max(3.5, n_workflows * 0.75)
    res_h = 2.6 if has_resources else 0
    drm_h = 4.2 if has_dreamer else 0
    fig_h = summ_h + gantt_h + res_h + drm_h + 1.2

    fig = plt.figure(figsize=(22, fig_h))

    hr = []
    if summ_h:
        hr.append(summ_h)
    hr.append(gantt_h)
    if has_resources:
        hr.append(res_h)
    if has_dreamer:
        hr.append(drm_h)
    outer = gridspec.GridSpec(
        len(hr), 1, height_ratios=hr, hspace=0.38, top=0.93, bottom=0.03, left=0.06, right=0.97
    )

    ri = 0
    ax_summary = None
    if summ_h:
        ax_summary = fig.add_subplot(outer[ri])
        ri += 1

    ax_gantt = fig.add_subplot(outer[ri])
    ri += 1

    ax_res = None
    if has_resources:
        ax_res = fig.add_subplot(outer[ri])
        ri += 1

    ax_mspan = ax_ops = None
    if has_dreamer:
        inner_d = gridspec.GridSpecFromSubplotSpec(
            1, 2, subplot_spec=outer[ri], width_ratios=[1, 1], wspace=0.30
        )
        ax_mspan = fig.add_subplot(inner_d[0, 0])
        ax_ops = fig.add_subplot(inner_d[0, 1])

    # ─────────────────────────────────────────────────────────────────────────
    # 0. workflow SUMMARY TABLE  (full-width row directly under suptitle)
    # ─────────────────────────────────────────────────────────────────────────
    if ax_summary is not None and dreamer_stats:
        ax_summary.axis("off")

        agg: dict = {}
        for rid, ds in dreamer_stats.items():
            g = _group(rid)
            a = agg.setdefault(g, {"n": 0, "tasks": [], "mk": [], "ae": [], "strat": set()})
            a["n"] += 1
            a["tasks"].append(ds.get("tasks_completed", ds.get("num_tasks", 0)))
            a["mk"].append(ds.get("makespan", 0))
            a["ae"].append(ds.get("avg_exec", 0))
            a["strat"].add(ds.get("strategy", "?"))

        _short = {"smallest_to_fastest": "s→fast", "largest_to_fastest": "l→fast", "random": "rand"}

        ch = [
            "workflow",
            "Facility / partition",
            "Budget\n(node-h)",
            "Cap",
            "Reps",
            "Tasks/rep",
            "Makespan\n(avg sim)",
            "AvgExec\n(avg sim)",
            "Edge profile",
            "Strategy",
        ]
        tbl_rows, tbl_cols = [], []

        # Enrich with config metadata when available
        cfg_workflows = {}
        if _HAVE_YAML:
            try:
                import yaml as _yaml

                # locate config.yaml next to wherever the script is being called from
                _cfg_path = Path(sys.argv[0]).parent / "config.yaml"
                if _cfg_path.exists():
                    _raw = _yaml.safe_load(_cfg_path.read_text())
                    _edges = {e["upstream"]: e for e in _raw.get("edges", [])}
                    for _s in _raw.get("workflows", []):
                        _sid = _s["id"]
                        _pilot = _s.get("pilot", {})
                        _edge = _edges.get(_sid, {})
                        cfg_workflows[_sid] = {
                            "facility": _pilot.get("facility", "—"),
                            "partition": _pilot.get("partition", "—"),
                            "budget": _s.get("budget_node_hours", "—"),
                            "cap": _s.get("concurrency_cap", "—"),
                            "profile": _edge.get("profile", "—"),
                        }
            except Exception:
                pass

        for s in ordered:
            if s not in agg:
                continue
            a = agg[s]
            st = "/".join(_short.get(x, x) for x in sorted(a["strat"]))
            cm = cfg_workflows.get(s, {})
            fac_part = f"{cm.get('facility', '—')} / {cm.get('partition', '—')}"
            tbl_rows.append(
                [
                    GROUP_LABELS.get(s, s),
                    fac_part,
                    f"{cm.get('budget', '—'):,}"
                    if isinstance(cm.get("budget"), (int, float))
                    else "—",
                    f"{cm.get('cap', '—'):,}" if isinstance(cm.get("cap"), (int, float)) else "—",
                    str(a["n"]),
                    f"{np.mean(a['tasks']):.0f}",
                    f"{np.mean(a['mk']):.1f}",
                    f"{np.mean(a['ae']):.1f}",
                    cm.get("profile", "—"),
                    st,
                ]
            )
            tbl_cols.append([GROUP_COLORS.get(s, DEFAULT_COLOR)] + ["#f5f5f5"] * (len(ch) - 1))

        if tbl_rows:
            tbl = ax_summary.table(
                cellText=tbl_rows,
                colLabels=ch,
                cellColours=tbl_cols,
                loc="center",
                cellLoc="center",
            )
            tbl.auto_set_font_size(False)
            tbl.set_fontsize(8)
            tbl.scale(1.0, 1.35)
            for j in range(len(ch)):
                tbl[0, j].set_facecolor("#333333")
                tbl[0, j].set_text_props(color="white", fontweight="bold")

    # ─────────────────────────────────────────────────────────────────────────
    # 1. GANTT CHART  —  actual wall-clock concurrency per workflow
    #
    # X-axis = elapsed wall-clock seconds (from log).
    # Source: replica start/finish timestamps parsed from the CM log.
    #
    # For each workflow the concurrency step function is derived directly from
    # the log: events (+1 at replica start, -1 at replica finish) are merged
    # and integrated.  This faithfully reflects the streaming pipeline where
    # workflows overlap in wall-clock time (S1 fires S2 as its first replicas
    # finish, S3 fires while S2 is still running, etc.).
    #
    # Bar HEIGHT ∝ peak concurrency / global peak so S4/S5 (peak=1) appear
    # visibly thinner than S1 (peak=500), with a 30% minimum so they remain
    # readable.
    # ─────────────────────────────────────────────────────────────────────────

    # Build per-workflow replica spans in elapsed seconds
    spans_by_workflow: dict[str, list] = {}
    for _rid, grp, s_dt, e_dt, ok in spans:
        if grp not in ordered:
            continue
        s_el = (s_dt - t0).total_seconds()
        e_el = (e_dt - t0).total_seconds()
        spans_by_workflow.setdefault(grp, []).append((s_el, e_el, ok))

    def _log_sf(workflow):
        """Concurrency step function from log spans (wall-clock seconds)."""
        events = []
        for s, e, _ in spans_by_workflow.get(workflow, []):
            events.append((s, +1))
            events.append((e, -1))
        if not events:
            return [], []
        events.sort()
        xs, ys = [0.0], [0]
        running = 0
        for t_ev, delta in events:
            xs.append(t_ev)
            ys.append(running)
            running += delta
            xs.append(t_ev)
            ys.append(running)
        return xs, ys

    workflow_sf: dict[str, tuple] = {}
    for workflow in ordered:
        xs, ys = _log_sf(workflow)
        if not xs:
            continue
        wc_start = min(spans_by_workflow.get(workflow, [(0, 0, None)])[0][:1] or [0])
        wc_start = min(s for s, e, _ in spans_by_workflow.get(workflow, [(0, 0, None)]))
        wc_end = max(e for s, e, _ in spans_by_workflow.get(workflow, [(0, 0, None)]))
        workflow_sf[workflow] = (xs, ys, wc_start, wc_end)

    wc_total = max((e for _, _, _, e in workflow_sf.values()), default=1.0) or 1.0

    global_max_c = max((max(ys) for _, (_, ys, _, _) in workflow_sf.items() if ys), default=1) or 1

    # ── Draw ─────────────────────────────────────────────────────────────────
    for row_idx, workflow in enumerate(ordered):
        if workflow not in workflow_sf:
            continue
        xs, ys, wc_start, wc_end = workflow_sf[workflow]

        color = GROUP_COLORS.get(workflow, DEFAULT_COLOR)
        meta = group_meta.get(workflow, {})
        dur = wc_end - wc_start

        workflow_max_c = max(ys) or 1
        n_reps = len(spans_by_workflow.get(workflow, []))
        n_err = sum(1 for _, _, ok in spans_by_workflow.get(workflow, []) if ok is False)

        row_bot = row_idx - 0.45
        row_h = 0.9
        min_frac = 0.30  # minimum bar height so single-replica workflows stay visible

        def _scale(y, _smc=workflow_max_c, _rb=row_bot, _rh=row_h, _mf=min_frac):
            if y == 0:
                return _rb
            raw = y / global_max_c * _rh
            return _rb + max(raw, _mf * _rh * y / _smc)

        ys_sc = [_scale(y) for y in ys]
        peak_y = _scale(workflow_max_c)

        ax_gantt.fill_between(xs, row_bot, ys_sc, color=color, alpha=0.60, zorder=2)
        ax_gantt.plot(xs, ys_sc, color=color, lw=1.2, alpha=0.9, zorder=3)
        ax_gantt.barh(
            row_idx,
            dur,
            left=wc_start,
            height=row_h,
            fill=False,
            edgecolor=color,
            lw=0.6,
            alpha=0.25,
            zorder=1,
        )
        ax_gantt.hlines(peak_y, wc_start, wc_end, color=color, lw=0.6, ls="--", alpha=0.4, zorder=1)

        err_s = f"  ✗{n_err}" if n_err else ""
        ann = (
            f"n={n_reps}{err_s}  peak={workflow_max_c}"
            f"  {dur:.1f}s"
            f"  cpu={meta.get('cpus', 0)} gpu={meta.get('gpus', 0)}"
        )
        ax_gantt.text(
            wc_total * 1.005,
            row_idx,
            ann,
            va="center",
            ha="left",
            fontsize=7,
            color=color,
            clip_on=True,
        )

        if row_idx % 2 == 0:
            ax_gantt.axhspan(row_idx - 0.5, row_idx + 0.5, color="grey", alpha=0.04, lw=0)

    # Trigger-signal markers on the Gantt
    for t_sig, tgt, _ in signal_events:
        if tgt in ordered:
            ax_gantt.axvline(
                t_sig, color=GROUP_COLORS.get(tgt, "#888"), lw=0.6, ls=":", alpha=0.4, zorder=1
            )

    ax_gantt.set_yticks(range(n_workflows))
    ax_gantt.set_yticklabels(
        [GROUP_LABELS.get(s, s) for s in ordered], fontsize=10, fontweight="bold"
    )
    ax_gantt.set_xlabel("Elapsed wall-clock time (s)", fontsize=9)
    ax_gantt.set_title("Streaming Pipeline Activity (wall-clock)", fontweight="bold", fontsize=10)
    ax_gantt.invert_yaxis()
    ax_gantt.grid(axis="x", ls="--", alpha=0.35)
    ax_gantt.set_xlim(left=0)

    legend_handles = [
        mpatches.Patch(color=GROUP_COLORS.get(s, DEFAULT_COLOR), label=GROUP_LABELS.get(s, s))
        for s in ordered
    ] + [mpatches.Patch(fc="white", ec="red", lw=1.2, label="error")]
    ax_gantt.legend(handles=legend_handles, loc="upper right", fontsize=7, framealpha=0.8)

    # ─────────────────────────────────────────────────────────────────────────
    # 2. RESOURCE UTILIZATION
    # ─────────────────────────────────────────────────────────────────────────
    if ax_res is not None and resource_timeline:
        times = [t for t, *_ in resource_timeline]
        used_gpu = [ug for _, _, _, ug, _ in resource_timeline]
        used_cpu = [uc for _, uc, *_ in resource_timeline]
        tot_gpu = [tg for _, _, _, _, tg in resource_timeline]
        tot_cpu = [tc for _, _, tc, *_ in resource_timeline]

        ax_res.step(times, used_gpu, where="post", color="#4C72B0", lw=2, label="GPU used")
        ax_res.fill_between(times, used_gpu, step="post", color="#4C72B0", alpha=0.15)
        if any(t > 0 for t in tot_gpu):
            ax_res.step(
                times,
                tot_gpu,
                where="post",
                color="#4C72B0",
                lw=0.9,
                ls="--",
                alpha=0.5,
                label="GPU total",
            )

        ax_res.set_ylabel("GPUs in use", color="#4C72B0", fontsize=8)
        ax_res.tick_params(axis="y", labelcolor="#4C72B0", labelsize=7)
        ax_res.set_ylim(bottom=0)

        for t_sig, tgt, _ in signal_events:
            ax_res.axvline(t_sig, color=GROUP_COLORS.get(tgt, "#888"), lw=0.8, alpha=0.4, ls=":")

        ax_cpu = ax_res.twinx()
        ax_cpu.step(times, used_cpu, where="post", color="#DD8452", lw=2, label="CPU used")
        ax_cpu.fill_between(times, used_cpu, step="post", color="#DD8452", alpha=0.12)
        if any(t > 0 for t in tot_cpu):
            ax_cpu.step(
                times,
                tot_cpu,
                where="post",
                color="#DD8452",
                lw=0.9,
                ls="--",
                alpha=0.5,
                label="CPU total",
            )
        ax_cpu.set_ylabel("CPUs in use", color="#DD8452", fontsize=8)
        ax_cpu.tick_params(axis="y", labelcolor="#DD8452", labelsize=7)
        ax_cpu.set_ylim(bottom=0)

        ax_res.set_xlabel("Elapsed wall-clock time (s)", fontsize=8)
        ax_res.set_title("Resource Utilization (CPU / GPU)", fontweight="bold", fontsize=9)
        ax_res.grid(axis="x", ls="--", alpha=0.35)
        l1, lb1 = ax_res.get_legend_handles_labels()
        l2, lb2 = ax_cpu.get_legend_handles_labels()
        ax_res.legend(l1 + l2, lb1 + lb2, fontsize=7, loc="upper right", framealpha=0.8)

    # ─────────────────────────────────────────────────────────────────────────
    # 3a. SIMULATED MAKESPAN DISTRIBUTION (box plots per workflow)
    # ─────────────────────────────────────────────────────────────────────────
    if ax_mspan is not None and dreamer_stats:
        mspan_by: dict = {s: [] for s in ordered}
        for rid, ds in dreamer_stats.items():
            g = _group(rid)
            if g in mspan_by:
                mspan_by[g].append(ds["makespan"])

        plot_stgs = [s for s in ordered if mspan_by.get(s)]
        if plot_stgs:
            box_data = [mspan_by[s] for s in plot_stgs]
            pos = list(range(len(plot_stgs)))

            bp = ax_mspan.boxplot(
                box_data,
                positions=pos,
                widths=0.55,
                patch_artist=True,
                showfliers=False,
                medianprops=dict(color="white", lw=2.5),
                boxprops=dict(lw=1),
                whiskerprops=dict(lw=1),
                capprops=dict(lw=1),
            )
            for patch, s in zip(bp["boxes"], plot_stgs, strict=False):
                patch.set_facecolor(GROUP_COLORS.get(s, DEFAULT_COLOR))
                patch.set_alpha(0.78)

            # Jittered individual points (sample ≤ 300 per workflow)
            rng = np.random.default_rng(42)
            for pi, (s, vals) in enumerate(zip(plot_stgs, box_data, strict=False)):
                sample = rng.choice(vals, size=min(300, len(vals)), replace=False)
                jitter = rng.uniform(-0.18, 0.18, size=len(sample))
                ax_mspan.scatter(
                    pi + jitter,
                    sample,
                    s=4,
                    alpha=0.30,
                    color=GROUP_COLORS.get(s, DEFAULT_COLOR),
                    zorder=3,
                )

            # n + median annotation beside each box
            for pi, (_s, vals) in enumerate(zip(plot_stgs, box_data, strict=False)):
                med = float(np.median(vals))
                ax_mspan.text(
                    pi + 0.34,
                    med,
                    f"n={len(vals)}\nmed={med:.0f}",
                    ha="left",
                    va="center",
                    fontsize=6.5,
                    color="#333",
                )

            ax_mspan.set_xticks(pos)
            ax_mspan.set_xticklabels([GROUP_LABELS.get(s, s) for s in plot_stgs], fontsize=8)
            ax_mspan.set_ylabel("Simulated makespan (dreamer time units)", fontsize=8)
            ax_mspan.set_title("Makespan Distribution per workflow", fontweight="bold", fontsize=9)
            ax_mspan.grid(axis="y", ls="--", alpha=0.35)

    # ─────────────────────────────────────────────────────────────────────────
    # 3b. TASK OPS DISTRIBUTION (box plots per workflow)
    # ─────────────────────────────────────────────────────────────────────────
    if ax_ops is not None:
        ops_by: dict = {s: [] for s in ordered}
        for rid, prof in profiles.items():
            g = _group(rid)
            if g in ops_by:
                for t in prof.get("tasks", []):
                    v = t.get("ops")
                    if v is not None:
                        ops_by[g].append(float(v))
        # Fallback: use avg_exec as ops proxy when no profiles
        if not any(ops_by.values()):
            for rid, ds in dreamer_stats.items():
                g = _group(rid)
                if g in ops_by:
                    ops_by[g].extend([ds["avg_exec"]] * ds.get("tasks_completed", 1))

        plot_stgs = [s for s in ordered if ops_by.get(s)]
        if plot_stgs:
            box_data = [ops_by[s] for s in plot_stgs]
            pos = list(range(len(plot_stgs)))

            bp = ax_ops.boxplot(
                box_data,
                positions=pos,
                widths=0.55,
                patch_artist=True,
                showfliers=False,
                medianprops=dict(color="white", lw=2.5),
                boxprops=dict(lw=1),
                whiskerprops=dict(lw=1),
                capprops=dict(lw=1),
            )
            for patch, s in zip(bp["boxes"], plot_stgs, strict=False):
                patch.set_facecolor(GROUP_COLORS.get(s, DEFAULT_COLOR))
                patch.set_alpha(0.78)

            # Jittered points (sample ≤ 300 per workflow)
            rng = np.random.default_rng(42)
            for pi, (s, ops) in enumerate(zip(plot_stgs, box_data, strict=False)):
                sample = rng.choice(ops, size=min(300, len(ops)), replace=False)
                jitter = rng.uniform(-0.2, 0.2, size=len(sample))
                ax_ops.scatter(
                    pi + jitter,
                    sample,
                    s=3,
                    alpha=0.30,
                    color=GROUP_COLORS.get(s, DEFAULT_COLOR),
                    zorder=3,
                )

            # Median annotation
            for pi, ops in enumerate(box_data):
                med = float(np.median(ops))
                ax_ops.text(
                    pi + 0.35, med, f"{med:.0f}", ha="left", va="center", fontsize=6.5, color="#333"
                )

            ax_ops.set_yscale("log")
            ax_ops.set_xticks(pos)
            ax_ops.set_xticklabels([GROUP_LABELS.get(s, s) for s in plot_stgs], fontsize=8)
            ax_ops.set_ylabel("Task ops (log scale, dreamer units)", fontsize=8)
            ax_ops.set_title("Task Ops Distribution per workflow", fontweight="bold", fontsize=9)
            ax_ops.grid(axis="y", ls="--", alpha=0.35)

    # ─────────────────────────────────────────────────────────────────────────
    fig.suptitle(title, fontsize=13, fontweight="bold")
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved → {out_path}")


# ── CLI ───────────────────────────────────────────────────────────────────────


def main():
    ap = argparse.ArgumentParser(
        description="Plot dreamer campaign timeline and simulation statistics"
    )
    ap.add_argument("log", help="Campaign log file")
    ap.add_argument(
        "--profiles-dir",
        default=None,
        help="dreamer-profiles/ directory (default: auto-detect next to log)",
    )
    ap.add_argument("--config", default=None, help="config.yaml (default: auto-detect next to log)")
    ap.add_argument("--out", default=None, help="Output PNG path")
    ap.add_argument(
        "--title", default=None, help="Figure suptitle (default: Campaign Timeline — <stem>)"
    )
    args = ap.parse_args()

    log_dir = Path(args.log).parent
    stem = Path(args.log).stem
    if args.out is None:
        args.out = f"plots/timeline_{stem}.png"
    if args.config is None:
        c = log_dir / "config.yaml"
        if c.exists():
            args.config = str(c)
    if args.profiles_dir is None:
        args.profiles_dir = str(log_dir / "dreamer-profiles")

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)

    (
        spans,
        group_meta_log,
        resource_timeline,
        signal_events,
        dreamer_stats,
        stall_events,
        t0,
        total_resources,
    ) = parse_log(args.log)

    group_meta_cfg = parse_config(args.config) if args.config else {}
    group_meta = group_meta_cfg if group_meta_cfg else group_meta_log
    if args.config:
        print(f"workflow config from: {args.config}")
    profiles = load_profiles(args.profiles_dir)

    print(
        f"Parsed: {len(spans)} replica spans | {len(group_meta)} workflows | "
        f"{len(resource_timeline)} resource events | {len(signal_events)} triggers | "
        f"{len(dreamer_stats)} dreamer records | {len(profiles)} profile JSONs"
    )

    plot(
        spans,
        group_meta,
        resource_timeline,
        signal_events,
        dreamer_stats,
        stall_events,
        profiles,
        t0,
        total_resources,
        args.out,
        title=args.title or f"Campaign Timeline — {stem}",
    )


if __name__ == "__main__":
    main()
