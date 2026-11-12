#!/usr/bin/env python3
"""
Figures for the AuCaM docs, from real (short, synthetic) AuCaM runs.

  budget   — how AuCaM keeps a campaign within its spending limit: (A) with
             per-workflow limits, (B) without, where the campaign limit
             cancels running workflows.  → docs/figures/aucam_budget_burn.png
  bandit   — one campaign, two workflow types; the bandit learns which one
             completes and explores less as the budget is used up.
             → docs/figures/aucam_bandit_budget.png
  runlimit — per-run spending limits: one workflow type sizes its work to fit
             its limit, the other ignores it; AuCaM cancels every run of it that
             spends past the limit.  → docs/figures/aucam_run_limit.png

Run (about 1 minute, needs matplotlib):
    cd $WORK_DIR/campaign_manager
    python campaigns/hpc_campaign/figures/doc_figures.py              # run all, write JSON + PNGs
    python campaigns/hpc_campaign/figures/doc_figures.py --plot-only  # redraw from the JSON
    python campaigns/hpc_campaign/figures/doc_figures.py --only runlimit  # one experiment
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from src.aucam import (  # noqa: E402
    BanditSchedulingPolicy,
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    PolicyResult,
    WorkflowConfig,
    WorkflowReporter,
    WorkflowResult,
)

FIG_DIR = ROOT / "docs" / "figures"
DATA = Path(__file__).parent / "doc_figures.json"
H = 3600.0  # seconds per hour: costs are written in CPU-seconds, AuCaM counts CPU-hours


def cpu_s(hours: float) -> float:
    return hours * H


def declared(cpus: float, wall_s: float) -> dict[str, float]:
    return {"wall_hours": wall_s / H, "cpu_hours": cpus * wall_s / H}


# ── 1. Campaign spending limit ─────────────────────────────────────────────────


async def _budget_case(with_estimate: bool) -> dict:
    """One campaign, limit 24 CPU·s, up to 3 workflows of 2 CPUs at once.

    Each workflow wants to work 0.7–2.5 s (1.4–5 CPU·s) and reports its CPU·s
    every 0.1 s.
      with_estimate — declared at 1 s (2 CPU·s): every workflow gets a limit
                      (2.5 CPU·s, then raised after cancels, then learned),
                      held against the campaign; AuCaM cancels one that
                      passes it.
      without       — no estimate, so no limit of its own: when actual spending
                      passes the campaign limit, AuCaM cancels the running
                      workflows (on_limit="cancel").
    """
    t0 = time.monotonic()
    samples: list[dict] = []
    runs: list[dict] = []
    cpus, step = 2.0, 0.1

    async def sim(reporter: WorkflowReporter) -> None:
        dur = random.uniform(0.7, 2.5)
        start = time.monotonic()
        while (el := time.monotonic() - start) < dur:
            await asyncio.sleep(step)
            await reporter.emit(score=1.0, metrics={"cpu_hours": cpus * (el + step) / H})

    class Ctrl(CampaignController):
        def decide(self, obs: dict) -> PolicyResult:
            b = obs["budget"]
            spent = cpu_s(b["spent"].get("cpu_hours", 0.0))
            used = spent + sum(cpu_s(w.get("cpu_hours", 0.0)) for w in obs["wf_spend"].values())
            samples.append(
                {
                    "t": time.monotonic() - t0,
                    "spent": spent,
                    "in_flight": cpu_s(b["in_flight"].get("cpu_hours", 0.0)),
                    "used": used,
                }
            )
            return super().decide(obs)

        def on_complete(self, result: WorkflowResult) -> None:
            super().on_complete(result)
            end = time.monotonic() - t0
            runs.append(
                {
                    "start": end - result.wall_time_s,
                    "end": end,
                    "rc": result.returncode,
                    "over_budget": result.over_budget,
                    "cpu_s": cpu_s(result.spent.get("cpu_hours", 0.0)),
                }
            )

    limit = 24.0
    spec = CampaignSpec(
        name="budget-demo",
        budget=CampaignBudget(
            max_concurrent_workflows=3,
            max_concurrency_per_workflow=3,
            spending={"cpu_hours": limit / H},
            on_limit="cancel",
        ),
        controller=Ctrl(launch=["sim"]),
        workflows=[
            WorkflowConfig(
                name="sim",
                block=sim,
                declared_spending=declared(cpus, 1.0) if with_estimate else {},
            )
        ],
        policy_interval_s=0.1,
        # B: nothing learned during the run either, so no workflow has a limit.
        min_cost_samples=3 if with_estimate else 1000,
    )
    o = await CampaignManager().run(spec)
    return {
        "limit": limit,
        "samples": samples,
        "runs": runs,
        "exit_reason": o.exit_reason,
        "elapsed": time.monotonic() - t0,
        "spent": cpu_s(o.spent.get("cpu_hours", 0.0)),
        "budget_cancels": o.budget_cancels,
    }


async def exp_budget() -> dict:
    """Panel A: every workflow gets a limit; panel B: no estimate, the campaign limit cancels."""
    return {"a": await _budget_case(True), "b": await _budget_case(False)}


# ── 2. Bandit and budget ───────────────────────────────────────────────────────


async def exp_bandit() -> dict:
    """good: completes 90 %; poor: fails 75 %.  One slot, 1 CPU × 0.35 s each,
    limit enough for ~45 runs."""
    t0 = time.monotonic()
    picks: list[dict] = []

    async def good(reporter: WorkflowReporter) -> None:
        await asyncio.sleep(0.35)
        if random.random() < 0.10:
            raise RuntimeError("failed")
        await reporter.emit(score=1.0)

    async def poor(reporter: WorkflowReporter) -> None:
        await asyncio.sleep(0.35)
        if random.random() < 0.75:
            raise RuntimeError("failed")
        await reporter.emit(score=1.0)

    class RecordingBandit(BanditSchedulingPolicy):
        def select(self, candidates, ctx):
            chosen = super().select(candidates, ctx)
            used = 1.0 - ctx.budget_remaining_fraction
            for req in chosen:
                picks.append(
                    {
                        "t": time.monotonic() - t0,
                        "type": req.workflow_type,
                        "used": used,
                        "c_eff": self._exploration * max(0.0, 1.0 - used) ** 0.5,
                    }
                )
            return chosen

    n_runs = 45
    limit = n_runs * 0.35
    spec = CampaignSpec(
        name="bandit-demo",
        budget=CampaignBudget(
            max_concurrent_workflows=1,
            max_concurrency_per_workflow=1,
            spending={"cpu_hours": limit / H},
        ),
        controller=CampaignController(launch=["good", "poor"]),
        workflows=[
            WorkflowConfig(name="good", block=good, declared_spending=declared(1, 0.35)),
            WorkflowConfig(name="poor", block=poor, declared_spending=declared(1, 0.35)),
        ],
        scheduling_policy=RecordingBandit(exploration=2.0),
        policy_interval_s=0.2,
    )
    o = await CampaignManager().run(spec)
    return {"picks": picks, "exploration": 2.0, "exit_reason": o.exit_reason}


# ── 3. Per-run spending limit ──────────────────────────────────────────────────


async def exp_runlimit() -> dict:
    """Two workflow types, 2 CPUs each, both with a 3 CPU·s limit per run.

    steady  — plans 1.2 s of work (2.4 CPU·s) and stops before passing its limit.
    runaway — runs 0.6–3 s (1.2–6 CPU·s) and ignores its limit; AuCaM cancels
              the runs that pass it.
    Every run reports its CPU·s so far every 0.1 s.
    """
    cpus, step_s, limit = 2.0, 0.1, 3.0
    t_campaign = time.monotonic()
    traj: dict[str, list[list[float]]] = {}
    plans: dict[str, dict] = {}  # wf_id -> {"start": s since campaign start, "plan": s}
    runs: list[dict] = []

    def make(kind: str):
        async def wf(reporter: WorkflowReporter, context: dict) -> None:
            lim = context["spending_limit"]["cpu_hours"] * H  # CPU·s
            plan = 1.2 if kind == "steady" else random.uniform(0.6, 3.0)
            t0, used = time.monotonic(), 0.0
            plans[reporter.wf_id] = {"start": t0 - t_campaign, "plan": plan}
            pts = traj.setdefault(reporter.wf_id, [[0.0, 0.0]])
            while time.monotonic() - t0 < plan:
                if kind == "steady" and used + cpus * step_s > lim + 1e-9:
                    break  # stay within the limit AuCaM gave this run
                await asyncio.sleep(step_s)
                used = cpus * (time.monotonic() - t0)
                pts.append([time.monotonic() - t0, used])
                await reporter.emit(score=1.0, metrics={"cpu_hours": used / H})

        wf.__name__ = kind
        return wf

    class Ctrl(CampaignController):
        def on_complete(self, result: WorkflowResult) -> None:
            super().on_complete(result)
            runs.append(
                {
                    "id": result.workflow_id,
                    "type": result.workflow_type,
                    "rc": result.returncode,
                    "over_budget": result.over_budget,
                    "spent": cpu_s(result.spent.get("cpu_hours", 0.0)),
                }
            )

        def decide(self, obs: dict) -> PolicyResult:
            res = super().decide(obs)
            res.terminate = res.terminate or len(runs) >= 16
            return res

    lim = {"cpu_hours": limit / H}
    spec = CampaignSpec(
        name="runlimit-demo",
        budget=CampaignBudget(max_concurrent_workflows=4, max_concurrency_per_workflow=2),
        controller=Ctrl(launch=["steady", "runaway"]),
        workflows=[
            WorkflowConfig(
                name=k, block=make(k), declared_spending=declared(cpus, 1.2), spending_limit=lim
            )
            for k in ("steady", "runaway")
        ],
        policy_interval_s=0.1,
    )
    o = await CampaignManager().run(spec)
    return {
        "limit": limit,
        "cpus": cpus,
        "traj": traj,
        "plans": plans,
        "runs": runs,
        "budget_cancels": o.budget_cancels,
    }


# ── Plots ──────────────────────────────────────────────────────────────────────

C_SPENT, C_FLIGHT, C_LIMIT, C_ANN = "#4a6fa5", "#9fc0e6", "#cf222e", "#57606a"
C_GOOD, C_POOR = "#1a7f37", "#cf222e"


def _plot_budget_panel(ax, d: dict, title: str) -> None:
    t = [s["t"] for s in d["samples"]]
    ax.stackplot(
        t,
        [s["spent"] for s in d["samples"]],
        [s["in_flight"] for s in d["samples"]],
        colors=[C_SPENT, C_FLIGHT],
        step="post",
        labels=["spent (charged)", "in-flight (held for running workflows)"],
    )
    ax.step(
        t,
        [s["used"] for s in d["samples"]],
        where="post",
        color="black",
        lw=1.2,
        label="actual: spent + used so far by running workflows",
    )
    limit = d["limit"]
    ax.axhline(limit, color=C_LIMIT, ls="--", lw=1.5)
    ax.text(0.05, limit + 0.5, f"campaign limit = {limit:.0f} CPU·s", color=C_LIMIT, fontsize=9)
    for r in d["runs"]:
        ax.plot([r["start"]], [-1.0], marker="|", color=C_ANN, ms=8)
        if r["over_budget"]:
            ax.plot([r["end"]], [-1.0], marker="x", color=C_LIMIT, ms=6)
        elif r["rc"] == -1:
            ax.plot([r["end"]], [-1.0], marker="o", mfc="none", color=C_LIMIT, ms=6)
    end = d["elapsed"]
    ax.axvline(end, color=C_ANN, ls=":", lw=1)
    final = d["spent"]
    ax.text(
        end - 0.05,
        limit * 1.18,
        f"ends: {d['exit_reason']}\nfinal spent {final:.1f} CPU·s",
        fontsize=8.5,
        color=C_ANN,
        ha="right",
        va="center",
    )
    ax.set_xlim(0, end + 0.3)
    ax.set_ylim(-2.6, limit * 1.32)
    ax.set_ylabel("CPU-seconds")
    ax.set_title(title, loc="left", fontsize=10.5)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)


def plot_budget(d: dict, plt) -> None:
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(10, 8.4))
    a, b = d["a"], d["b"]
    _plot_budget_panel(
        a1,
        a,
        "A — every workflow gets a limit (estimate + 25 %, raised 25 % after each cancel at it, "
        "then\nlearned cost + 25 %), held against the campaign; one that passes it is cancelled (×)",
    )
    _plot_budget_panel(
        a2,
        b,
        "B — safety net: workflows with no estimate (and nothing learned yet) have no limit of their "
        'own;\nwhen actual spending passes the campaign limit, AuCaM cancels them (on_limit="cancel")',
    )
    from matplotlib.lines import Line2D  # noqa: PLC0415

    handles, labels = a1.get_legend_handles_labels()
    handles += [
        Line2D([], [], marker="|", ls="none", color=C_ANN, ms=8),
        Line2D([], [], marker="x", ls="none", color=C_LIMIT, ms=6),
        Line2D([], [], marker="o", mfc="none", ls="none", color=C_LIMIT, ms=6),
    ]
    labels += ["a workflow starts", "cancelled at its own limit", "cancelled at the campaign limit"]
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=3,
        fontsize=8.5,
        frameon=False,
        bbox_to_anchor=(0.5, 0.0),
    )
    a2.set_xlabel("time (s)")
    fig.suptitle(
        "Campaign spending limit — workflows of 2 CPUs × 0.7–2.5 s, up to 3 at once",
        x=0.01,
        ha="left",
        fontsize=11.5,
    )
    fig.tight_layout(rect=(0, 0.07, 1, 1))
    fig.savefig(FIG_DIR / "aucam_budget_burn.png", dpi=150)


def plot_bandit(d: dict, plt) -> None:
    picks = d["picks"]
    n = range(1, len(picks) + 1)
    fig, (a1, a2) = plt.subplots(
        2, 1, figsize=(10, 5.6), sharex=True, gridspec_kw={"height_ratios": [1.1, 1]}
    )
    for kind, color, y in (("good", C_GOOD, 1), ("poor", C_POOR, 0)):
        xs = [i for i, p in zip(n, picks, strict=True) if p["type"] == kind]
        a1.scatter(xs, [y] * len(xs), color=color, s=45, marker="s")
        a1.text(len(picks) + 0.8, y, f"{len(xs)} runs", va="center", fontsize=9, color=color)
    a1.set_yticks([0, 1], ["poor\n(fails 75 %)", "good\n(fails 10 %)"])
    a1.set_ylim(-0.6, 1.6)
    a1.set_title(
        "Bandit scheduling — which type the bandit launched, in order", loc="left", fontsize=11
    )
    a2.plot(list(n), [p["c_eff"] for p in picks], color=C_SPENT, lw=2, label="exploration weight")
    a2.set_ylabel("exploration weight")
    a2.set_ylim(0, d["exploration"] * 1.1)
    a3 = a2.twinx()
    a3.plot(
        list(n),
        [100 * p["used"] for p in picks],
        color=C_LIMIT,
        lw=1.5,
        ls="--",
        label="budget used",
    )
    a3.set_ylabel("budget used (%)", color=C_LIMIT)
    a3.set_ylim(0, 110)
    a2.set_xlabel("launch number")
    a2.set_title(
        "…and how much it explores as the campaign's budget is used up", loc="left", fontsize=11
    )
    lines = a2.get_lines() + a3.get_lines()
    a2.legend(lines, [ln.get_label() for ln in lines], loc="center left", fontsize=9, frameon=False)
    for ax in (a1, a2):
        ax.spines["top"].set_visible(False)
    a1.spines["right"].set_visible(False)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "aucam_bandit_budget.png", dpi=150)


def plot_runlimit(d: dict, plt) -> None:
    """Timeline: one bar per run; red bars were cancelled by AuCaM at the run's limit."""
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch

    by_id = {r["id"]: r for r in d["runs"]}
    rows = []
    for wf_id, info in d["plans"].items():
        r = by_id.get(wf_id)
        # Skip runs still going, or stopped, when the campaign ended.
        if r is None or (r["rc"] == -1 and not r["over_budget"]):
            continue
        end = d["traj"][wf_id][-1][0]
        rows.append((info["start"], end, info["plan"], r))
    rows.sort(key=lambda x: x[0])
    t_lim = d["limit"] / d["cpus"]  # run time at which a run reaches its limit
    fig, ax = plt.subplots(figsize=(10, 0.32 * len(rows) + 1.8))
    n = {"steady": 0, "ok": 0, "cut": 0}
    for i, (start, dur, plan, r) in enumerate(rows):
        y = len(rows) - i
        if r["type"] == "steady":
            color, key = C_GOOD, "steady"
        elif r["over_budget"]:
            color, key = C_LIMIT, "cut"
            # The work this run still wanted to do when AuCaM cancelled it.
            ax.barh(
                y,
                plan - dur,
                left=start + dur,
                height=0.6,
                color="none",
                edgecolor=C_LIMIT,
                hatch="///",
                lw=0.6,
            )
            ax.plot(start + dur, y, marker="x", color=C_LIMIT, ms=8, mew=2)
        else:
            color, key = "#e8a33d", "ok"
        n[key] += 1
        ax.barh(y, dur, left=start, height=0.6, color=color)
        ax.plot([start + t_lim] * 2, [y - 0.38, y + 0.38], color=C_ANN, lw=1.2)
    ax.set_yticks([])
    ax.set_xlabel("time since the campaign started (s)")
    ax.set_title(
        f"Per-run spending limit: {d['limit']:.0f} CPU·s per run (2 CPUs, so {t_lim:.1f} s of run time)",
        loc="left",
        fontsize=11,
    )
    handles = [
        Patch(color=C_GOOD, label=f"steady: sizes its work to fit its limit ({n['steady']} runs)"),
        Patch(color="#e8a33d", label=f"runaway that finished below its limit ({n['ok']})"),
        Patch(color=C_LIMIT, label=f"runaway cancelled by AuCaM at its limit ({n['cut']})"),
        Patch(
            facecolor="none",
            edgecolor=C_LIMIT,
            hatch="///",
            label="work it had left when cancelled",
        ),
        Line2D([], [], color=C_ANN, lw=1.2, label="the run's limit"),
    ]
    ax.legend(
        handles=handles,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.1),
        ncol=2,
        fontsize=8.5,
        frameon=False,
    )
    for sp in ("top", "right", "left"):
        ax.spines[sp].set_visible(False)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "aucam_run_limit.png", dpi=150, bbox_inches="tight")


def plot_all(data: dict) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update({"font.size": 10})
    for name, fn in (("budget", plot_budget), ("bandit", plot_bandit), ("runlimit", plot_runlimit)):
        if name in data:
            fn(data[name], plt)
    print(f"figures → {FIG_DIR}")


EXPERIMENTS = {"budget": exp_budget, "bandit": exp_bandit, "runlimit": exp_runlimit}


async def run_all(only: list[str] | None = None) -> dict:
    random.seed(11)
    out = {}
    for name, fn in EXPERIMENTS.items():
        if only and name not in only:
            continue
        print(f"── {name}")
        out[name] = await fn()
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate the AuCaM doc figures")
    ap.add_argument("--plot-only", action="store_true", help=f"redraw from {DATA.name}")
    ap.add_argument(
        "--only", nargs="+", choices=list(EXPERIMENTS), help="run only these experiments"
    )
    args = ap.parse_args()
    logging.basicConfig(level=logging.CRITICAL)  # the 'poor' workflows fail on purpose
    if not args.plot_only:
        data = json.loads(DATA.read_text()) if args.only and DATA.exists() else {}
        data.update(asyncio.run(run_all(args.only)))
        DATA.write_text(json.dumps(data, indent=1))
        print(f"data → {DATA}")
    plot_all(json.loads(DATA.read_text()))
