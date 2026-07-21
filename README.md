# Campaign Manager

An `asyncio`-native orchestrator for multi-stage HPC campaigns. It runs pools of workflow instances concurrently inside a single event loop, routing results from one stage to the next and stopping automatically when a configurable goal is met.

---

## Core idea

A **campaign** is a directed acyclic graph (DAG) of **workflow groups**. Each group is a pool of instances that all run the same workflow class. When an instance finishes it can signal the next group, triggering more work downstream.

```
ddsim_a ──┐
           ├──► analysis   (stop after 15 analyses)
ddsim_b ──┘
```

The DAG is wired entirely through a YAML config — chains, fan-out, fan-in joins, and diamonds all work without changing workflow code.

---

## What the CM does

- **Schedules** instances across groups using a two-pass greedy algorithm (floor guarantees first, then best-priority fill), gated by a shared resource pool (CPUs, GPUs, memory).
- **Routes** results between groups: `_signal_done()` or `_trigger_dependent()` in a workflow's `on_replica_done` hook adds instances to the next group at runtime.
- **Stops** when a goal is met — `campaign_target: N` on any group, or an ADR-layer goal expression.

Optional features extend the baseline with backpressure control, priority-ranked batching (sharder), candidate triage, surrogate scoring, budget tracking, and drift alerts. All are toggled per-campaign in config.

---

## ADR adaptive scheduling

The ADR (Autonomous Decision Runtime) layer runs an Observe → Decide → Act loop alongside the live CM. Each tick it reads a structured observation of the current campaign state and nudges group priorities, proposes or revokes stopping goals, or takes custom actions (launch a child operator, set a triage cutoff, trigger new replicas). The CM still owns scheduling, execution, and resources — ADR only observes and advises.

Four built-in policies cover the common cases (`rule`, `bandit`, `llm`, hierarchical), and custom policies extend `Policy` for campaign-specific logic. The ddsim campaign is the reference implementation — see [`campaigns/ddsim_campaign/README.md`](campaigns/ddsim_campaign/README.md) for a full walkthrough of every ADR feature.

---

## Quick start

```bash
pip install -e ".[adr]"

cd campaigns/ddsim_campaign
python run_campaign.py --config config.yaml --policy rule
```

Requires Python ≥ 3.10. No HPC runtime needed for local testing — the concurrent backend runs entirely in asyncio.

---

## Layout

```
src/campaign/          # orchestrator core (scheduler, executor, optional features)
src/campaign/adr/      # ADR adaptive scheduling layer
campaigns/
  ddsim_campaign/      # reference campaign — all ADR features demonstrated
src/utils/             # config loading, logging setup
tests/                 # pytest suite
```
