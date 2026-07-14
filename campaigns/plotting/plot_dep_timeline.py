#!/usr/bin/env python3
"""
Plot replica execution timeline from a campaign SLURM log.

Generic (campaign-agnostic) timeline: Gantt chart of replica execution + a
CPU/GPU resource-utilization row, for any AsyncCampaignManager run. Draws
per-replica dependency arrows showing which upstream replica triggered each
downstream one. For longer campaigns with Dreamer emulation data use
``plot_timeline.py`` instead — it adds a third row of simulation statistics
(makespan, task-ops box plots, per-workflow stats).

Usage:
    python plot_dep_timeline.py slurm-XXXXXX.out [--out plots/dep_timeline.png]
"""

import argparse
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

matplotlib.use("Agg")
import matplotlib.gridspec as gridspec
import matplotlib.lines as mlines
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

_TS_RE = re.compile(r"\x1b\[2m(\d{2}:\d{2}:\d{2}\.\d{3})\x1b\[0m")
_START_RE = re.compile(r"starting replica '(\w+)'")
_FINISH_RE = re.compile(r"Replica '(\w+)' finished")
_ERROR_RE = re.compile(r"Replica '(\w+)' raised")
_GROUP_RE = re.compile(
    r"Registered group '(\w+)': replicas=(\d+) priority=(\d+) "
    r"min=(\d+) max=(\d+) deps=\[([^\]]*)\] dep_threshold=(\d+) "
    r"resources=\(cpus=(\d+), gpus=(\d+)\)"
)
_GPU_ASSIGN_RE = re.compile(r"GPU assign: '(\w+)' → GPU\(s\) \[([^\]]*)\]")
_USAGE_RE = re.compile(r"resources: cpus=(\d+)/(\d+)\s+gpus=(\d+)/(\d+)")
_AVAIL_RE = re.compile(r"available: cpus=(\d+)/(\d+)\s+gpus=(\d+)/(\d+)")
_TOTAL_RES_RE = re.compile(r"Resource pool: total_cpus=(\d+)\s+total_gpus=(\d+)")

# Signal events — new dependency model
# "signal_done":     'md' signaled done → +1 replica for ['miniapps']
# "trigger_dep":     trigger_dependent: 'dummy' +1 replicas (total=3)
_SIGNAL_DONE_RE = re.compile(r"'(\w+)' signaled done.*?\+(\d+) replica.*?\['([\w,\s]*)'\]")
_TRIGGER_DEP_RE = re.compile(r"trigger_dependent: '(\w+)' \+(\d+) replicas \(total=(\d+)\)")

GROUP_COLORS = {
    "inference": "#4C72B0",
    "md": "#DD8452",
    "miniapps": "#55A868",
    "dummy": "#C44E52",
}
# Fallback palette for groups not in the hardcoded set above.
_FALLBACK_COLORS = [
    "#8172B2",
    "#F06292",
    "#4DB6AC",
    "#FFB74D",
    "#BA68C8",
    "#4DD0E1",
    "#AED581",
    "#FF8A65",
]

GROUP_ORDER = ["md", "miniapps", "inference", "dummy"]


def _resolve_colors(groups: list[str]) -> None:
    """Assign distinct colors to any group not already in GROUP_COLORS."""
    used = set(GROUP_COLORS.values())
    cycle = [c for c in _FALLBACK_COLORS if c not in used] or _FALLBACK_COLORS
    ci = 0
    for g in groups:
        if g not in GROUP_COLORS:
            GROUP_COLORS[g] = cycle[ci % len(cycle)]
            ci += 1


def _find_policy_sections(path: str) -> dict[str, tuple[int, int]]:
    """Scan a benchmark SLURM log for 'Policy: X' section headers.
    Returns {policy_name: (start_line, end_line)} where end is exclusive."""
    _sep_re = re.compile(r"^={10,}\s*$")
    _policy_re = re.compile(r"^Policy:\s+(\w+)\s*$")
    order: list[str] = []
    starts_map: dict[str, int] = {}

    with open(path) as fh:
        lines = fh.readlines()

    for i, line in enumerate(lines):
        if _sep_re.match(line) and i + 1 < len(lines):
            pm = _policy_re.match(lines[i + 1])
            if pm:
                policy = pm.group(1)
                if policy not in starts_map:
                    starts_map[policy] = i
                    order.append(policy)

    result: dict[str, tuple[int, int]] = {}
    for idx, policy in enumerate(order):
        start = starts_map[policy]
        end = starts_map[order[idx + 1]] if idx + 1 < len(order) else len(lines)
        result[policy] = (start, end)
    return result


def parse_log(path: str, line_range: tuple[int, int] | None = None):
    """Parse SLURM log; return spans, group_meta, resource_timeline,
    gpu_assignments, signal_events.  line_range restricts to a slice of
    the file (e.g. a single policy section in a benchmark log)."""
    with open(path) as fh:
        all_lines = fh.readlines()
    lines = all_lines[line_range[0] : line_range[1]] if line_range else all_lines

    starts: dict[str, datetime] = {}
    spans = []
    group_meta = {}
    gpu_assignments = {}
    resource_timeline = []
    # (elapsed_s, source_group, target_groups, n_replicas, kind)
    # kind: "signal_done" | "trigger_dep"
    signal_events = []
    t0_dt = None
    total_cpus = total_gpus = 0

    _iso_re = re.compile(r"^(\d{4}-\d{2}-\d{2}) \d{2}:\d{2}:\d{2}")
    date_ref = "1970-01-01"
    for raw in lines:
        if m := _iso_re.match(raw):
            date_ref = m.group(1)
            break

    for raw in lines:
        if m := _GROUP_RE.search(raw):
            name = m.group(1)
            deps_raw = m.group(6)
            deps = [d.strip().strip("'\"") for d in deps_raw.split(",") if d.strip().strip("'\"")]
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
            total_cpus = int(m.group(1))
            total_gpus = int(m.group(2))

        ts_m = _TS_RE.search(raw)
        if ts_m is None:
            continue
        dt = datetime.strptime(f"{date_ref} {ts_m.group(1)}", "%Y-%m-%d %H:%M:%S.%f")
        if t0_dt is None:
            t0_dt = dt
        elapsed = (dt - t0_dt).total_seconds()

        if m := _GPU_ASSIGN_RE.search(raw):
            rid = m.group(1)
            gpu_str = m.group(2).strip()
            gpu_ids = [int(x) for x in gpu_str.split(",") if x.strip()] if gpu_str else []
            gpu_assignments[rid] = gpu_ids

        if m := _USAGE_RE.search(raw):
            uc, tc, ug, tg = (
                int(m.group(1)),
                int(m.group(2)),
                int(m.group(3)),
                int(m.group(4)),
            )
            resource_timeline.append((elapsed, uc, tc, ug, tg))
        elif m := _AVAIL_RE.search(raw):
            ac, tc, ag, tg = (
                int(m.group(1)),
                int(m.group(2)),
                int(m.group(3)),
                int(m.group(4)),
            )
            resource_timeline.append((elapsed, tc - ac, tc, tg - ag, tg))

        # Signal events — new dependency model
        if m := _SIGNAL_DONE_RE.search(raw):
            src = m.group(1)
            n = int(m.group(2))
            targets_raw = m.group(3)
            targets = [t.strip().strip("'\"") for t in targets_raw.split(",") if t.strip()]
            for tgt in targets:
                signal_events.append((elapsed, src, tgt, n, "signal_done"))

        if m := _TRIGGER_DEP_RE.search(raw):
            tgt = m.group(1)
            n = int(m.group(2))
            total = int(m.group(3))  # total queued after this trigger
            # Source unknown from log line — mark as "trigger_dep"
            # total lets us identify the exact replica created: {tgt}_{total-1}
            signal_events.append((elapsed, None, tgt, n, "trigger_dep", total))

        if m := _START_RE.search(raw):
            starts[m.group(1)] = dt
        elif m := _FINISH_RE.search(raw):
            rid = m.group(1)
            if rid in starts:
                group = rid.rsplit("_", 1)[0]
                spans.append((rid, group, starts.pop(rid), dt, True))
        elif m := _ERROR_RE.search(raw):
            rid = m.group(1)
            if rid in starts:
                group = rid.rsplit("_", 1)[0]
                spans.append((rid, group, starts.pop(rid), dt, False))

    for rid, start in starts.items():
        group = rid.rsplit("_", 1)[0]
        spans.append((rid, group, start, start, None))

    resource_timeline.sort(key=lambda x: x[0])
    signal_events.sort(key=lambda x: x[0])

    t0 = min(s[2] for s in spans) if spans else t0_dt
    return (
        spans,
        group_meta,
        resource_timeline,
        gpu_assignments,
        signal_events,
        t0,
        (total_cpus, total_gpus),
    )


def plot(
    spans,
    group_meta,
    resource_timeline,
    gpu_assignments,
    signal_events,
    t0,
    total_resources,
    out_path,
):
    if not spans:
        print("No replica events found.", file=sys.stderr)
        return

    def sort_key(s):
        rid, group, *_ = s
        idx = int(rid.rsplit("_", 1)[-1])
        g_idx = GROUP_ORDER.index(group) if group in GROUP_ORDER else len(GROUP_ORDER)
        return (g_idx, idx)

    spans.sort(key=sort_key)

    has_resources = len(resource_timeline) > 0
    total_cpus, total_gpus = total_resources

    n_rows = len(spans)
    gantt_h = max(6, n_rows * 0.38)

    fig = plt.figure(figsize=(20, gantt_h + (3 if has_resources else 0) + 1))

    if has_resources:
        gs = gridspec.GridSpec(
            2,
            2,
            height_ratios=[gantt_h, 2.5],
            width_ratios=[3, 1],
            hspace=0.2,
            wspace=0.18,
        )
        ax_gantt = fig.add_subplot(gs[0, 0])
        ax_info = fig.add_subplot(gs[0, 1])
        ax_res = fig.add_subplot(gs[1, 0])
        ax_leg = fig.add_subplot(gs[1, 1])
        ax_leg.axis("off")
    else:
        gs = gridspec.GridSpec(1, 2, width_ratios=[3, 1], wspace=0.18)
        ax_gantt = fig.add_subplot(gs[0, 0])
        ax_info = fig.add_subplot(gs[0, 1])
        ax_res = None

    # ── Gantt chart ──────────────────────────────────────────────────────────
    yticks, ylabels = [], []
    group_row_ranges = {}

    prev_group = None
    for row, (rid, group, start, end, ok) in enumerate(spans):
        t_start = (start - t0).total_seconds()
        t_end = (end - t0).total_seconds() if end != start else t_start + 0.5
        bar_w = t_end - t_start

        color = GROUP_COLORS.get(group, "#888888")
        edgecolor = "red" if ok is False else "none"
        lw = 1.5 if ok is False else 0
        alpha = 0.45 if ok is None else 0.88

        if group != prev_group and prev_group is not None:
            ax_gantt.axhline(row - 0.5, color="grey", lw=0.6, alpha=0.5, linestyle="--")
        prev_group = group

        if group not in group_row_ranges:
            group_row_ranges[group] = [row, row]
        else:
            group_row_ranges[group][1] = row

        g_idx = GROUP_ORDER.index(group) if group in GROUP_ORDER else len(GROUP_ORDER)
        if g_idx % 2 == 0:
            ax_gantt.axhspan(row - 0.5, row + 0.5, color="grey", alpha=0.04, linewidth=0)

        ax_gantt.barh(
            row,
            bar_w,
            left=t_start,
            height=0.72,
            color=color,
            edgecolor=edgecolor,
            linewidth=lw,
            alpha=alpha,
        )

        gpu_ids = gpu_assignments.get(rid, [])
        meta = group_meta.get(group, {})
        if gpu_ids:
            ann_txt = f"gpu:{','.join(str(g) for g in gpu_ids)}"
        elif meta.get("cpus", 0) > 0:
            ann_txt = f"{meta['cpus']} cpu(s)"
        else:
            ann_txt = ""

        if ann_txt and bar_w > 0.5:
            ax_gantt.text(
                t_start + bar_w / 2,
                row,
                ann_txt,
                ha="center",
                va="center",
                fontsize=5.5,
                color="white",
                fontweight="bold",
                clip_on=True,
            )

        yticks.append(row)
        ylabels.append(rid)

    # Group section labels on the right
    for group, (r0, r1) in group_row_ranges.items():
        meta = group_meta.get(group, {})
        mid = (r0 + r1) / 2
        pri = meta.get("priority", "?")
        cpus = meta.get("cpus", 0)
        gpus = meta.get("gpus", 0)
        mode = "dep." if meta.get("deps") else "indep."
        info = f"priority={pri}\ncpu={cpus}  gpu={gpus}\n{mode}"
        ax_gantt.text(
            1.002,
            1.0 - (mid + 0.5) / n_rows,
            info,
            transform=ax_gantt.transAxes,
            va="center",
            ha="left",
            fontsize=6.5,
            color=GROUP_COLORS.get(group, "#888888"),
            fontweight="bold",
        )

    # Build group_spans lookup: group -> sorted list of (row, start_dt, end_dt, ok)
    group_spans: dict[str, list] = {}
    for row, (_, group, start, end, ok) in enumerate(spans):
        group_spans.setdefault(group, []).append((row, start, end, ok))

    # ── Dependency signal arrows ─────────────────────────────────────────────
    # Each signal event gets its own arrow: signal point → triggered replica start.
    consumed_tgt_rows: set[int] = set()
    consumed_src_rows: set[int] = set()
    # Round-robin counters for signal_done — distributes signals evenly across
    # parallel replicas when the log doesn't record which replica sent each signal.
    signal_done_rr: dict[str, int] = {}
    # available target replicas per group, sorted by start time
    available: dict[str, list] = {
        g: sorted(sl, key=lambda s: s[1]) for g, sl in group_spans.items()
    }

    for sig_event in signal_events:
        sig_elapsed, src_group, tgt_group, _n, kind = sig_event[:5]
        trig_total = sig_event[5] if len(sig_event) > 5 else None  # trigger_dep only

        if tgt_group not in available:
            continue

        # Find the target replica.
        # For trigger_dep events: log records "total=N" meaning the replica just
        # created is {tgt_group}_{N-1}.  Use that directly instead of time heuristics
        # (time-based matching mis-assigns triggers when initial replicas > 0).
        # For signal_done: fall back to time-based search.
        tgt_span = None
        if kind == "trigger_dep" and trig_total is not None:
            target_rid = f"{tgt_group}_{trig_total - 1}"
            for s in available[tgt_group]:
                rid = spans[s[0]][0]
                if rid == target_rid and s[0] not in consumed_tgt_rows:
                    tgt_span = s
                    break
        if tgt_span is None:
            # signal_done or trigger_dep without total: earliest unconsumed replica
            # starting at or after the signal
            for s in available[tgt_group]:
                if (
                    s[0] not in consumed_tgt_rows
                    and (s[1] - t0).total_seconds() >= sig_elapsed - 0.5
                ):
                    tgt_span = s
                    break
        if tgt_span is None:
            continue
        consumed_tgt_rows.add(tgt_span[0])

        tgt_row = tgt_span[0]
        tgt_start_t = (tgt_span[1] - t0).total_seconds()

        # Determine the row to use for the signal source.
        # For trigger_dep events src_group is None — infer from tgt_group's own
        # dependency list (what tgt_group depends ON, not who depends on it).
        resolved_src = src_group
        if not resolved_src:
            tgt_deps = group_meta.get(tgt_group, {}).get("deps", [])
            resolved_src = tgt_deps[0] if tgt_deps else None

        if resolved_src and resolved_src in group_row_ranges:
            r0, r1 = group_row_ranges[resolved_src]
            src_spans = group_spans.get(resolved_src, [])

            if kind == "trigger_dep":
                # Signal fires from on_replica_done — the source replica may not
                # yet have its "finished" line in the log.  Find the closest
                # unconsumed source replica by finish time.  If all source rows
                # are consumed (more triggers than source replicas), fall back to
                # the closest row regardless — keeps the diamond on a real bar.
                candidates = [s for s in src_spans if s[0] not in consumed_src_rows]
                if not candidates:
                    candidates = src_spans  # reuse closest rather than float to center
                if candidates:
                    best = min(
                        candidates,
                        key=lambda s: abs((s[2] - t0).total_seconds() - sig_elapsed),
                    )
                    src_row = best[0]
                    consumed_src_rows.add(best[0])
                else:
                    src_row = (r0 + r1) / 2
            else:
                # signal_done fires from within run() — multiple replicas may be
                # running in parallel and the log doesn't record which sent it.
                # Distribute signals round-robin across the running replicas.
                running = [
                    s
                    for s in src_spans
                    if (s[1] - t0).total_seconds()
                    <= sig_elapsed
                    <= (s[2] - t0).total_seconds() + 0.5
                ]
                if running:
                    rr_idx = signal_done_rr.get(resolved_src, 0)
                    src_row = running[rr_idx % len(running)][0]
                    signal_done_rr[resolved_src] = rr_idx + 1
                else:
                    src_row = (r0 + r1) / 2

            src_color = GROUP_COLORS.get(resolved_src, "#888888")
        else:
            src_row = tgt_row - 1.5
            src_color = GROUP_COLORS.get(tgt_group, "#888888")

        # Draw diamond marker at signal point on source row
        ax_gantt.plot(
            sig_elapsed,
            src_row,
            "D",
            markersize=5,
            color=src_color,
            zorder=5,
            markeredgecolor="white",
            markeredgewidth=0.5,
        )

        # Draw arrow from signal diamond to triggered replica start
        ax_gantt.annotate(
            "",
            xy=(tgt_start_t, tgt_row),
            xytext=(sig_elapsed, src_row),
            arrowprops=dict(
                arrowstyle="->",
                color="#555555",
                lw=1.1,
                connectionstyle="arc3,rad=0.25",
            ),
            annotation_clip=False,
        )

    # Fall back to a single structural arrow for deps with no logged signals
    # (e.g. log truncated or dep_threshold path)
    drawn_dep_pairs: set[tuple[str, str]] = set()
    for ev in signal_events:
        _, src, tgt, _, kind = ev[:5]
        if src:
            drawn_dep_pairs.add((src, tgt))
        elif kind == "trigger_dep":
            # src is None for trigger_dep log lines — resolve from tgt's dep list
            tgt_deps = group_meta.get(tgt, {}).get("deps", [])
            if tgt_deps:
                drawn_dep_pairs.add((tgt_deps[0], tgt))
    for group, _ in group_row_ranges.items():
        meta = group_meta.get(group, {})
        for dep_name in meta.get("deps", []):
            if (dep_name, group) in drawn_dep_pairs:
                continue
            if dep_name not in group_spans or group not in group_spans:
                continue
            dep_first = group_spans[dep_name][0]
            grp_first = group_spans[group][0]
            dep_row, dep_start, dep_end, _ = dep_first
            grp_row, grp_start, _, _ = grp_first
            dep_mid_t = (dep_start - t0).total_seconds()
            if dep_end != dep_start:
                dep_mid_t += (dep_end - dep_start).total_seconds() / 2
            grp_start_t = (grp_start - t0).total_seconds()
            ax_gantt.annotate(
                "",
                xy=(grp_start_t, grp_row),
                xytext=(dep_mid_t, dep_row),
                arrowprops=dict(
                    arrowstyle="->",
                    color="#888888",
                    lw=1.0,
                    connectionstyle="arc3,rad=0.35",
                    linestyle="dashed",
                ),
                annotation_clip=False,
            )

    ax_gantt.set_yticks(yticks)
    ax_gantt.set_yticklabels(ylabels, fontsize=7)
    ax_gantt.set_xlabel("Elapsed time (s)", fontsize=9)
    ax_gantt.set_title("Campaign Manager Timeline", fontweight="bold", fontsize=12)
    ax_gantt.invert_yaxis()
    ax_gantt.grid(axis="x", linestyle="--", alpha=0.35)

    seen_groups = dict.fromkeys(s[1] for s in spans)  # insertion-ordered, deduped
    legend_patches = [
        mpatches.Patch(color=GROUP_COLORS[g], label=g) for g in seen_groups if g in GROUP_COLORS
    ]
    legend_patches += [
        mpatches.Patch(facecolor="white", edgecolor="red", linewidth=1.2, label="error"),
        mpatches.Patch(color="grey", alpha=0.45, label="still running"),
        mlines.Line2D(
            [0],
            [0],
            marker="D",
            color="w",
            markerfacecolor="#666666",
            markersize=6,
            label="signal / trigger",
        ),
    ]
    ax_gantt.legend(handles=legend_patches, loc="lower right", fontsize=7, framealpha=0.8)

    # ── Group info + dependency table ────────────────────────────────────────
    ax_info.axis("off")
    ax_info.set_title("Campaign Manager Config", fontweight="bold", fontsize=9, pad=4)

    if group_meta:
        info_groups = [g for g in GROUP_ORDER if g in group_meta] + [
            g for g in group_meta if g not in GROUP_ORDER
        ]

        col_labels = ["Workflow", "Priority", "CPUs", "GPUs", "min/max", "Mode", "Deps"]
        rows_data, row_colors = [], []
        for gname in info_groups:
            m = group_meta[gname]
            deps = ", ".join(m.get("deps", [])) or "—"
            mode = "dep." if m.get("deps") else "indep."
            rows_data.append(
                [
                    gname,
                    str(m.get("priority", "?")),
                    str(m.get("cpus", 0)),
                    str(m.get("gpus", 0)),
                    f"{m.get('min', 0)}/{m.get('max', 0)}",
                    mode,
                    deps,
                ]
            )
            c = GROUP_COLORS.get(gname, "#888888")
            row_colors.append([c] + ["#f5f5f5"] * (len(col_labels) - 1))

        tbl = ax_info.table(
            cellText=rows_data,
            colLabels=col_labels,
            cellColours=row_colors,
            loc="upper center",
            cellLoc="center",
        )
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(7.5)
        tbl.scale(1.0, 1.5)

        for j in range(len(col_labels)):
            tbl[0, j].set_facecolor("#333333")
            tbl[0, j].set_text_props(color="white", fontweight="bold")

        # Signal-based dependency graph
        dep_lines = []
        # Collect unique dep relationships with signal counts
        sig_counts: dict[tuple[str, str], int] = {}
        for _ev in signal_events:
            _, src, tgt, n, _kind = _ev[:5]
            if src:
                sig_counts[(src, tgt)] = sig_counts.get((src, tgt), 0) + n

        for gname in info_groups:
            m = group_meta[gname]
            for dep_name in m.get("deps", []):
                count = sig_counts.get((dep_name, gname), 0)
                count_str = f" ×{count}" if count else ""
                dep_lines.append(f"  {dep_name} —signals→ {gname}{count_str}")

        if dep_lines:
            dep_str = "Dependency graph (signals):\n" + "\n".join(dep_lines)
            ax_info.text(
                0.5,
                0.22,
                dep_str,
                transform=ax_info.transAxes,
                va="bottom",
                ha="center",
                fontsize=8,
                family="monospace",
                bbox=dict(boxstyle="round,pad=0.5", facecolor="#f0f4ff", edgecolor="#aabbdd"),
            )

        sched_note = (
            "Dependency model:\n"
            "  Independent: starts on cm.start()\n"
            "  Dependent: waits for upstream signal\n"
            "    ◆ _signal_done() → +1 replica per\n"
            "      downstream group in dependencies\n"
            "    ◆ _trigger_dependent() → explicit N\n"
            "\n"
            "Scheduler:\n"
            "  Pass 1: guarantee min replicas (priority)\n"
            "  Pass 2: fill up to max replicas (priority)"
        )
        ax_info.text(
            0.5,
            0.52,
            sched_note,
            transform=ax_info.transAxes,
            va="bottom",
            ha="center",
            fontsize=8,
            bbox=dict(boxstyle="round,pad=0.5", facecolor="#fffbe6", edgecolor="#ccaa00"),
        )

    # ── Resource utilization subplot ─────────────────────────────────────────
    if ax_res is not None and resource_timeline:
        times = [t for t, *_ in resource_timeline]
        used_gpus = [ug for _, _, _, ug, _ in resource_timeline]
        used_cpus = [uc for _, uc, *_ in resource_timeline]
        tot_gpus = [tg for _, _, _, _, tg in resource_timeline]
        tot_cpus = [tc for _, _, tc, *_ in resource_timeline]

        ax_res.step(times, used_gpus, where="post", color="#4C72B0", lw=1.8, label="GPUs used")
        ax_res.fill_between(times, used_gpus, step="post", color="#4C72B0", alpha=0.15)
        if any(t > 0 for t in tot_gpus):
            ax_res.step(
                times,
                tot_gpus,
                where="post",
                color="#4C72B0",
                lw=0.8,
                linestyle="--",
                alpha=0.55,
                label="GPU total",
            )

        ax_res.set_ylabel("GPUs in use", color="#4C72B0", fontsize=8)
        ax_res.tick_params(axis="y", labelcolor="#4C72B0", labelsize=7)
        ax_res.set_ylim(bottom=0)

        # Mark signal events on resource plot
        for _ev in signal_events:
            sig_elapsed, src, tgt, _n, _kind = _ev[:5]
            color = GROUP_COLORS.get(src or tgt, "#888888")
            ax_res.axvline(sig_elapsed, color=color, lw=0.7, alpha=0.5, linestyle=":")

        ax_cpu = ax_res.twinx()
        ax_cpu.step(times, used_cpus, where="post", color="#DD8452", lw=1.8, label="CPUs used")
        ax_cpu.fill_between(times, used_cpus, step="post", color="#DD8452", alpha=0.12)
        if any(t > 0 for t in tot_cpus):
            ax_cpu.step(
                times,
                tot_cpus,
                where="post",
                color="#DD8452",
                lw=0.8,
                linestyle="--",
                alpha=0.55,
                label="CPU total",
            )

        ax_cpu.set_ylabel("CPUs in use", color="#DD8452", fontsize=8)
        ax_cpu.tick_params(axis="y", labelcolor="#DD8452", labelsize=7)
        ax_cpu.set_ylim(bottom=0)

        ax_res.set_xlabel("Elapsed time (s)", fontsize=8)
        ax_res.set_title("Resource Utilization (GPU / CPU)", fontweight="bold", fontsize=9)
        ax_res.grid(axis="x", linestyle="--", alpha=0.35)

        lines1, lbl1 = ax_res.get_legend_handles_labels()
        lines2, lbl2 = ax_cpu.get_legend_handles_labels()
        ax_res.legend(lines1 + lines2, lbl1 + lbl2, fontsize=7, loc="upper right", framealpha=0.8)

    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved → {out_path}")


def parse_config(path: str) -> dict:
    """Load group_meta from a campaign config.yaml."""
    if not _HAVE_YAML:
        print("PyYAML not installed — falling back to log-parsed group metadata", file=sys.stderr)
        return {}
    with open(path) as fh:
        cfg = yaml.safe_load(fh)
    group_meta = {}
    for name, wf in cfg.get("workflows", {}).items():
        has_deps = bool(wf.get("dependencies", []))
        default_replicas = 0 if has_deps else 1
        group_meta[name] = {
            "replicas": int(wf.get("replicas", default_replicas)),
            "priority": int(wf.get("priority", 0)),
            # Accept both new (concurrency_floor / concurrency_cap) and legacy
            # (min_replicas / max_replicas) keys so old benchmark configs render.
            "min": int(wf.get("concurrency_floor", wf.get("min_replicas", 0))),
            "max": int(wf.get("concurrency_cap", wf.get("max_replicas", 0))),
            "deps": list(wf.get("dependencies", [])),
            "dep_threshold": int(wf.get("dependency_threshold", 1)),
            "cpus": int(wf.get("required_cpus", 0)),
            "gpus": int(wf.get("required_gpus", 0)),
        }
    return group_meta


def main():
    parser = argparse.ArgumentParser(description="Plot campaign manager replica timeline")
    parser.add_argument("log", help="SLURM output file")
    parser.add_argument(
        "--config",
        default=None,
        help="Campaign config.yaml (auto-detected as config.yaml next to log if not given)",
    )
    parser.add_argument(
        "--out", default=None, help="Output PNG (default: plots/dep_timeline_<run>.png)"
    )
    parser.add_argument(
        "--policy",
        default=None,
        help="Policy section to plot when the log contains a multi-policy benchmark "
        "(e.g. none, rule, bandit, llm). Omit to plot the whole log.",
    )
    args = parser.parse_args()

    # ── Detect policy sections ────────────────────────────────────────────────
    sections = _find_policy_sections(args.log)
    line_range = None
    if sections:
        available = list(sections.keys())
        if args.policy is None:
            print(
                f"Benchmark log detected — contains policies: {available}\n"
                f"  Use --policy <name> to plot a specific section.\n"
                f"  Plotting first section ({available[0]!r}) by default."
            )
            args.policy = available[0]
        elif args.policy not in sections:
            raise SystemExit(f"Policy {args.policy!r} not found. Available: {available}")
        line_range = sections[args.policy]
        print(f"Extracting policy section: {args.policy!r} (lines {line_range[0]}–{line_range[1]})")

    if args.out is None:
        stem = Path(args.log).stem
        suffix = f"_{args.policy}" if args.policy else ""
        m = re.search(r"(\d+)", stem)
        run_num = m.group(1) if m else stem
        Path("plots").mkdir(parents=True, exist_ok=True)
        args.out = f"plots/dep_timeline_{run_num}{suffix}.png"

    if args.config is None:
        candidate = Path(args.log).parent / "config.yaml"
        if candidate.exists():
            args.config = str(candidate)

    (
        spans,
        group_meta_log,
        resource_timeline,
        gpu_assignments,
        signal_events,
        t0,
        total_resources,
    ) = parse_log(args.log, line_range=line_range)

    if args.config:
        group_meta = parse_config(args.config)
        print(f"Loaded group metadata from config: {args.config}")
    else:
        group_meta = group_meta_log
        print("No config.yaml found — using group metadata parsed from log")

    _resolve_colors(list(group_meta.keys()))

    print(
        f"Parsed {len(spans)} replica spans, "
        f"{len(group_meta)} groups, "
        f"{len(resource_timeline)} resource events, "
        f"{len(gpu_assignments)} GPU assignments, "
        f"{len(signal_events)} signal events"
    )
    plot(
        spans,
        group_meta,
        resource_timeline,
        gpu_assignments,
        signal_events,
        t0,
        total_resources,
        args.out,
    )


if __name__ == "__main__":
    main()
