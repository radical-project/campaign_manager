#!/usr/bin/env python3
"""
Figures for the AuCaM docs, from real (short, synthetic) AuCaM runs.

  budget   — one campaign with a spending limit: spent + in-flight vs. the
             limit over time; launching stops at the limit, the campaign ends
             with exit reason spending_limit.  → docs/figures/aucam_budget_burn.png
  bandit   — one campaign, two workflow types; the bandit learns which one
             completes and explores less as the budget is used up.
             → docs/figures/aucam_bandit_budget.png
  global   — a driver submits four campaigns under a global spending limit:
             two run side by side, one is rejected, one is admitted after an
             earlier campaign finishes.  → docs/figures/aucam_global_budget.png

Run (about 1.5 minutes, needs matplotlib):
    cd $WORK_DIR/campaign_manager
    python campaigns/demo_campaign/doc_figures.py              # run all, write JSON + PNGs
    python campaigns/demo_campaign/doc_figures.py --plot-only  # redraw from the JSON
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

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.aucam import (  # noqa: E402
    BanditSchedulingPolicy,
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    GlobalBudget,
    PolicyResult,
    WorkflowConfig,
    WorkflowReporter,
    WorkflowResult,
)

FIG_DIR = ROOT / "docs" / "figures"
DATA = Path(__file__).parent / "doc_figures.json"
H = 3600.0   # seconds per hour: costs are written in CPU-seconds, AuCaM counts CPU-hours


def cpu_s(hours: float) -> float:
    return hours * H


def declared(cpus: float, wall_s: float) -> dict[str, float]:
    return {"wall_hours": wall_s / H, "cpu_hours": cpus * wall_s / H}


async def instance_of(cm: CampaignManager, name: str):
    """The running campaign object (to read its exit_reason after it ends)."""
    while name not in cm._active_instances:
        await asyncio.sleep(0.01)
    return cm._active_instances[name]


async def run_with_driver(cm: CampaignManager, driver) -> None:
    cm.expect_driver()

    async def wrapped() -> None:
        try:
            await driver()
        finally:
            cm.driver_done()

    task = asyncio.create_task(wrapped())
    await cm.run()
    await task


# ── 1. Campaign spending limit ─────────────────────────────────────────────────

async def exp_budget() -> dict:
    """Workflows of 2 CPUs × ~1 s (declared 1 s), limit 24 CPU·s, 3 at once."""
    t0 = time.monotonic()
    samples: list[dict] = []
    runs: list[dict] = []

    async def sim(reporter: WorkflowReporter) -> None:
        dur = random.uniform(0.7, 1.4)
        await asyncio.sleep(dur)
        await reporter.emit(score=1.0)

    class Ctrl(CampaignController):
        def decide(self, obs: dict) -> PolicyResult:
            b = obs["budget"]
            samples.append({"t": time.monotonic() - t0,
                            "spent": cpu_s(b["spent"].get("cpu_hours", 0.0)),
                            "in_flight": cpu_s(b["in_flight"].get("cpu_hours", 0.0)),
                            "running": sum(obs["running"].values())})
            return super().decide(obs)

        def on_complete(self, result: WorkflowResult) -> None:
            super().on_complete(result)
            end = time.monotonic() - t0
            runs.append({"start": end - result.wall_time_s, "end": end, "rc": result.returncode,
                         "cpu_s": cpu_s(result.spent.get("cpu_hours", 0.0))})

    limit = 24.0
    spec = CampaignSpec(
        name="budget-demo",
        budget=CampaignBudget(max_concurrent_workflows=3, max_concurrency_per_workflow=3,
                              spending={"cpu_hours": limit / H}),
        controller=Ctrl(launch=["sim"]),
        workflows=[WorkflowConfig(name="sim", block=sim, declared_spending=declared(2, 1.0))],
        policy_interval_s=0.1,
    )
    outcome = {}

    async def driver() -> None:
        res = await cm.submit(spec)
        inst = await instance_of(cm, spec.name)
        o = await res.wait()
        outcome.update(exit_reason=inst.exit_reason,
                       elapsed=time.monotonic() - t0, spent=cpu_s(o.spent.get("cpu_hours", 0.0)))

    cm = CampaignManager()
    await run_with_driver(cm, driver)
    return {"limit": limit, "samples": samples, "runs": runs, "outcome": outcome,
            "exit_reason": outcome["exit_reason"], "declared": 2.0}


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
                picks.append({"t": time.monotonic() - t0, "type": req.workflow_type,
                              "used": used,
                              "c_eff": self._exploration * max(0.0, 1.0 - used) ** 0.5})
            return chosen

    n_runs = 45
    limit = n_runs * 0.35
    spec = CampaignSpec(
        name="bandit-demo",
        budget=CampaignBudget(max_concurrent_workflows=1, max_concurrency_per_workflow=1,
                              spending={"cpu_hours": limit / H}),
        controller=CampaignController(launch=["good", "poor"]),
        workflows=[WorkflowConfig(name="good", block=good, declared_spending=declared(1, 0.35)),
                   WorkflowConfig(name="poor", block=poor, declared_spending=declared(1, 0.35))],
        scheduling_policy=RecordingBandit(exploration=2.0),
        policy_interval_s=0.2,
    )

    info = {}

    async def driver() -> None:
        res = await cm.submit(spec)
        inst = await instance_of(cm, spec.name)
        await res.wait()
        info["exit_reason"] = inst.exit_reason

    cm = CampaignManager()
    await run_with_driver(cm, driver)
    return {"picks": picks, "exploration": 2.0, "exit_reason": info["exit_reason"]}


# ── 3. Global budget and admission ─────────────────────────────────────────────

async def exp_global() -> dict:
    """Global limit 32 CPU·s.  A (limit 12) and B (limit 12) run side by side;
    C (limit 12) is rejected; after A finishes having spent ~6, D (limit 12)
    fits (6 + 12 + 12 ≤ 32) and is admitted."""
    t0 = time.monotonic()
    cap = 32.0
    events: list[dict] = []
    ledger: list[dict] = []

    async def sim(reporter: WorkflowReporter) -> None:
        await asyncio.sleep(1.0)
        await reporter.emit(score=1.0)

    def spec(name: str, goal: int, limit: float) -> CampaignSpec:
        return CampaignSpec(
            name=name,
            budget=CampaignBudget(max_concurrent_workflows=1, max_concurrency_per_workflow=1,
                                  spending={"cpu_hours": limit / H}),
            controller=CampaignController(launch=["sim"], goal_type="sim", goal_n=goal),
            workflows=[WorkflowConfig(name="sim", block=sim, declared_spending=declared(2, 1.0))],
            policy_interval_s=0.2,
        )

    cm = CampaignManager(global_budget=GlobalBudget(spending={"cpu_hours": cap / H}))
    stop = asyncio.Event()

    async def sampler() -> None:
        while not stop.is_set():
            snap = cm._allocator.snapshot()
            open_limits = sum(cpu_s(c["limits"].get("cpu_hours", 0.0)) for c in snap["campaigns"].values())
            open_spent = sum(cpu_s(c["spent"].get("cpu_hours", 0.0)) for c in snap["campaigns"].values())
            ledger.append({"t": time.monotonic() - t0,
                           "closed_spent": cpu_s(snap["global_spent"].get("cpu_hours", 0.0)),
                           "open_limits": open_limits, "open_spent": open_spent})
            await asyncio.sleep(0.1)

    async def submit(name: str, goal: int, limit: float):
        res = await cm.submit(spec(name, goal, limit))
        events.append({"name": name, "t_submit": time.monotonic() - t0, "accepted": res.accepted,
                       "limit": limit, "reason": res.reason})
        return res

    async def finish(name: str, res) -> None:
        o = await res.wait()
        for e in events:
            if e["name"] == name:
                e.update(t_end=time.monotonic() - t0, spent=cpu_s(o.spent.get("cpu_hours", 0.0)))

    async def driver() -> None:
        samp = asyncio.create_task(sampler())
        a = await submit("A", 3, 12.0)
        b = await submit("B", 8, 12.0)
        wait_b = asyncio.create_task(finish("B", b))
        await asyncio.sleep(1.0)
        await submit("C", 4, 12.0)                     # 12 + 12 + 12 > 32: rejected
        await finish("A", a)
        await asyncio.sleep(0.3)
        d = await submit("D", 4, 12.0)                 # A spent ~6: 6 + 12 + 12 ≤ 32
        await finish("D", d)
        await wait_b
        stop.set()
        await samp

    await run_with_driver(cm, driver)
    return {"cap": cap, "events": events, "ledger": ledger}


# ── Plots ──────────────────────────────────────────────────────────────────────

C_SPENT, C_FLIGHT, C_LIMIT, C_ANN = "#4a6fa5", "#9fc0e6", "#cf222e", "#57606a"
C_GOOD, C_POOR = "#1a7f37", "#cf222e"


def plot_budget(d: dict, plt) -> None:
    fig, ax = plt.subplots(figsize=(10, 4.6))
    t = [s["t"] for s in d["samples"]]
    spent = [s["spent"] for s in d["samples"]]
    inflight = [s["in_flight"] for s in d["samples"]]
    ax.stackplot(t, spent, inflight, colors=[C_SPENT, C_FLIGHT], step="post",
                 labels=["spent (charged)", "in-flight (expected cost of running workflows)"])
    ax.axhline(d["limit"], color=C_LIMIT, ls="--", lw=1.5)
    ax.text(0.1, d["limit"] + 0.4, f"campaign spending limit = {d['limit']:.0f} CPU·s",
            color=C_LIMIT, fontsize=9)
    last_start = max(r["start"] for r in d["runs"])
    for r in d["runs"]:
        ax.plot([r["start"]], [-0.9], marker="|", color=C_ANN, ms=8)
    ax.axvline(last_start, color=C_ANN, ls=":", lw=1)
    top = d["limit"] * 1.22
    ax.text(last_start - 0.05, top, "last launch: spent + in-flight\nreached the limit", fontsize=8.5,
            color=C_ANN, ha="right", va="center")
    end = d["outcome"]["elapsed"]
    ax.axvline(end, color=C_ANN, ls=":", lw=1)
    ax.text(end + 0.05, top, f"ends:\n{d['exit_reason']}", fontsize=8.5, color=C_ANN, va="center")
    final = d["outcome"]["spent"]
    if final > d["limit"]:
        ax.text(end - 0.1, final + 0.3, f"final {final:.1f} CPU·s: runs took longer than declared",
                fontsize=8, color=C_ANN, ha="right", va="bottom")
    ax.text(0.1, -2.2, "| = workflow launches", fontsize=8, color=C_ANN)
    ax.set_xlim(0, end + 0.9)
    ax.set_ylim(-2.8, d["limit"] * 1.32)
    ax.set_xlabel("time (s)")
    ax.set_ylabel("CPU-seconds")
    ax.set_title(f"Campaign spending limit — {len(d['runs'])} workflows of 2 CPUs × 0.7–1.4 s, "
                 "up to 3 at once", loc="left", fontsize=11)
    ax.legend(loc="center left", fontsize=9, frameon=False, bbox_to_anchor=(0, 0.62))
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "aucam_budget_burn.png", dpi=150)


def plot_bandit(d: dict, plt) -> None:
    picks = d["picks"]
    n = range(1, len(picks) + 1)
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(10, 5.6), sharex=True,
                                 gridspec_kw={"height_ratios": [1.1, 1]})
    for kind, color, y in (("good", C_GOOD, 1), ("poor", C_POOR, 0)):
        xs = [i for i, p in zip(n, picks, strict=True) if p["type"] == kind]
        a1.scatter(xs, [y] * len(xs), color=color, s=45, marker="s")
        a1.text(len(picks) + 0.8, y, f"{len(xs)} runs", va="center", fontsize=9, color=color)
    a1.set_yticks([0, 1], ["poor\n(fails 75 %)", "good\n(fails 10 %)"])
    a1.set_ylim(-0.6, 1.6)
    a1.set_title("Bandit scheduling — which type the bandit launched, in order", loc="left", fontsize=11)
    a2.plot(list(n), [p["c_eff"] for p in picks], color=C_SPENT, lw=2, label="exploration weight")
    a2.set_ylabel("exploration weight")
    a2.set_ylim(0, d["exploration"] * 1.1)
    a3 = a2.twinx()
    a3.plot(list(n), [100 * p["used"] for p in picks], color=C_LIMIT, lw=1.5, ls="--",
            label="budget used")
    a3.set_ylabel("budget used (%)", color=C_LIMIT)
    a3.set_ylim(0, 110)
    a2.set_xlabel("launch number")
    a2.set_title("…and how much it explores as the campaign's budget is used up", loc="left", fontsize=11)
    lines = a2.get_lines() + a3.get_lines()
    a2.legend(lines, [ln.get_label() for ln in lines], loc="center left", fontsize=9, frameon=False)
    for ax in (a1, a2):
        ax.spines["top"].set_visible(False)
    a1.spines["right"].set_visible(False)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "aucam_bandit_budget.png", dpi=150)


def plot_global(d: dict, plt) -> None:
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(10, 5.8), sharex=True,
                                 gridspec_kw={"height_ratios": [1, 1.3]})
    ev = d["events"]
    end_t = max(e.get("t_end", e["t_submit"]) for e in ev) + 0.5
    for i, e in enumerate(ev):
        y = len(ev) - i
        if e["accepted"]:
            a1.barh(y, e["t_end"] - e["t_submit"], left=e["t_submit"], color=C_SPENT, height=0.55)
            a1.text(e["t_end"] + 0.1, y, f"spent {e['spent']:.0f} of {e['limit']:.0f} CPU·s",
                    va="center", fontsize=8.5, color=C_ANN)
        else:
            a1.plot(e["t_submit"], y, marker="x", color=C_LIMIT, ms=11, mew=2.5)
            a1.text(e["t_submit"] + 0.2, y, f"rejected: 12 + 12 + 12 > {d['cap']:.0f} CPU·s", va="center",
                    fontsize=8.5, color=C_LIMIT)
    a1.set_yticks([len(ev) - i for i in range(len(ev))], [f"campaign {e['name']}" for e in ev])
    a1.set_title("Campaigns submitted by one driver under a global spending limit", loc="left", fontsize=11)
    a1.set_xlim(0, end_t + 1.8)
    led = d["ledger"]
    t = [x["t"] for x in led]
    closed = [x["closed_spent"] for x in led]
    held = [x["open_limits"] for x in led]
    a2.stackplot(t, closed, held, colors=[C_SPENT, C_FLIGHT], step="post",
                 labels=["spent by finished campaigns", "limits of running campaigns (reserved)"])
    a2.plot(t, [c + x["open_spent"] for c, x in zip(closed, led, strict=True)], color="#1f2d3d",
            lw=1.2, label="actually spent so far")
    a2.axhline(d["cap"], color=C_LIMIT, ls="--", lw=1.5)
    a2.text(0.1, d["cap"] + 0.6, f"global spending limit = {d['cap']:.0f} CPU·s", color=C_LIMIT, fontsize=9)
    a2.set_ylim(0, d["cap"] * 1.35)
    a2.set_ylabel("CPU-seconds")
    a2.set_xlabel("time (s)")
    a2.legend(loc="upper right", fontsize=8.5, frameon=False)
    for ax in (a1, a2):
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "aucam_global_budget.png", dpi=150)


def plot_all(data: dict) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({"font.size": 10})
    plot_budget(data["budget"], plt)
    plot_bandit(data["bandit"], plt)
    plot_global(data["global"], plt)
    print(f"figures → {FIG_DIR}")


async def run_all() -> dict:
    random.seed(11)
    out = {}
    for name, fn in (("budget", exp_budget), ("bandit", exp_bandit), ("global", exp_global)):
        print(f"── {name}")
        out[name] = await fn()
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate the AuCaM doc figures")
    ap.add_argument("--plot-only", action="store_true", help=f"redraw from {DATA.name}")
    args = ap.parse_args()
    logging.basicConfig(level=logging.CRITICAL)   # the 'poor' workflows fail on purpose
    if not args.plot_only:
        data = asyncio.run(run_all())
        DATA.write_text(json.dumps(data, indent=1))
        print(f"data → {DATA}")
    plot_all(json.loads(DATA.read_text()))
