# Campaign Manager

An `asyncio`-native orchestrator for multi-stage HPC campaigns. It runs pools of workflow instances concurrently inside a single event loop, routing results from one stage to the next and stopping automatically when a configurable goal is met.

---

## Core idea

A **campaign** is a set of **workflow groups** wired by dependencies and runtime triggers. Each group is a pool of instances that all run the same workflow class. When an instance finishes it can signal the next group, triggering more work downstream.

```
ddsim_a ──┐
          ├──► analysis   (stop after 15 analyses)
ddsim_b ──┘
```

Simple workflows are declared in YAML; campaigns can also grow dynamically as replicas finish.

---

## What the CM does

- **Schedules** instances across groups using a two-pass greedy algorithm (floor guarantees first, then best-priority fill), gated by a shared resource pool (CPUs, GPUs, memory).
- **Routes** results between groups: `_signal_done()` or `_trigger_dependent()` in a workflow's `on_replica_done` hook adds instances to the next group at runtime.
- **Stops** when a goal is met — `campaign_target: N` on any group, or an operator-defined goal expression.

Optional features extend the baseline with backpressure control, priority-ranked batching (sharder), candidate triage, surrogate scoring, budget tracking, and drift alerts. All are toggled per-campaign in config.

---

## Adaptive scheduling

An optional scheduling layer runs an Observe → Decide → Act loop alongside the live CM. Each tick it reads a structured snapshot of the current campaign state and can nudge group priorities, propose stopping goals, or take custom actions — launching child operators, tightening triage cutoffs, triggering new replicas. The CM still owns scheduling, execution, and resources; the scheduling layer only observes and advises.

Three built-in policies cover the common cases (`rule`, `bandit`, `llm`), and custom policies extend `Policy` for campaign-specific logic. The ddsim campaign is the reference implementation — see [`campaigns/ddsim_campaign/README.md`](campaigns/ddsim_campaign/README.md) for a full walkthrough of every scheduling feature.

---

## Quick start

```bash
pip install -e .

cd campaigns/dreamer_campaign
python run_campaign.py --config config.yaml
```

Requires Python ≥ 3.10. No HPC runtime needed for local testing — the concurrent backend runs entirely in asyncio.

---

## Layout

```
src/campaign/          # orchestrator core (scheduler, executor, optional features)
src/campaign/adr/      # adaptive scheduling layer
campaigns/
  ddsim_campaign/      # reference benchmark suite — all scheduling features demonstrated
  dreamer_campaign/    # drug-discovery funnel emulation (radical.dreamer stubs)
  esm2_ddsim_campaign/ # dual-pipeline HPC campaign on Delta GPU nodes
  orbit_campaign/      # remote task execution via ORBIT broker + rhapsody
  nested_campaign/     # nested-loop demo — sequential outer rounds, SNAPSHOT messaging
  plotting/            # shared Gantt and timeline visualisation scripts
src/utils/             # config loading, logging setup
tests/                 # pytest suite
CONTRIBUTING.md        # workflow authoring guide — hooks, ClassVar pattern, API reference
```

---

## Documentation

| Document                                                                         | What it covers                                                                                                                                                                        |
|----------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`src/campaign/README.md`](src/campaign/README.md)                               | Seven SPLASH-inspired design patterns: decision tracing, forward-compatible checkpoints, abort signalling, developer guide enforcement, Gantt plotting, concurrency ceiling, artifact lineage |
| [`campaigns/README.md`](campaigns/README.md)                                     | Campaign index — topology, operator table, and decorator breakdown for all five campaigns                                                                                              |
| [`campaigns/ddsim_campaign/README.md`](campaigns/ddsim_campaign/README.md)       | Full scheduling feature walkthrough via the ddsim benchmark suite (four benchmarks, all policy types)                                                                                 |
| [`campaigns/nested_campaign/README.md`](campaigns/nested_campaign/README.md)     | Nested-loop pattern in detail: architecture diagram, operator concepts with code snippets, design decision log, and eight possible extensions                                          |
| [`campaigns/orbit_campaign/README.md`](campaigns/orbit_campaign/README.md)       | Remote task execution via ORBIT + rhapsody; prerequisites and deployment notes                                                                                                        |
| [`campaigns/plotting/README.md`](campaigns/plotting/README.md)                   | `plot_gantt.py` (metrics JSON → replica Gantt), `plot_dep_timeline.py` and `plot_timeline.py` (SLURM log → timeline), `plot_telemetry.sh` (asyncflow dashboard)                      |
| [`CONTRIBUTING.md`](CONTRIBUTING.md)                                             | CM lifecycle API, workflow authoring guide — hooks, ClassVar FIFO pattern, `_trigger_dependent` / `_trigger_batch` / `_signal_done`, checkpoint custom state                          |
