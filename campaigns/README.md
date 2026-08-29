# Campaign Manager — Campaign Index

Five campaigns developed in this repository, spanning from local simulation
benchmarks to real HPC drug-discovery pipelines.  Each entry below covers the
scientific purpose, the topology, and — most importantly — which ADR agent
constructs are used.

---

## 1. ddsim_campaign

**Purpose:** ADR scheduling benchmark suite.  Isolates four dimensions of ADR
behaviour using a simulated ddsim workload so that policies can be compared on
equal footing without requiring real HPC allocation.

**Topology:** Shared CPU pools feeding analysis stages; exact shape varies per
benchmark (see below).

**Benchmarks:**

| ID      | Config                         | What it tests                                                                  |
|---------|--------------------------------|--------------------------------------------------------------------------------|
| bmark1  | `config_consensus_3stage.yaml` | 3-stage resource contention; rule vs bandit vs LLM vs consensus policies       |
| bmark1b | `config_consensus_3stage.yaml` | Same topology; isolated bandit warm-start effect                               |
| bmark2  | `config_hierarchical.yaml`     | Two-pipeline isolation; flat global policy vs per-pipeline backpressure        |
| bmark3  | `config_temporal.yaml`         | Temporal adaptation; reactive vs proactive phase detection                     |
| bmark4  | `config_bmark4.yaml`           | Multi-operator + hierarchical + budget control; 4 shared CPUs, 30 CPU-s budget |

**Agent implementation:**

- **bmark1–3:** Base `CampaignOperator` with swappable policies (`none | rule |
  bandit | llm | consensus`).  Uses the base class `@goals` unchanged —
  stop when `n_target` completions are reached (value read from config).
  No custom `@observe`; the default campaign snapshot is sufficient.

- **bmark4 — three conditions compared:**

  | Condition           | Operator class                                                       | ADR decorators                                                                                                                                                        |
  |---------------------|----------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
  | `flat_rule`         | `CampaignOperator`                                                   | `@goals` only                                                                                                                                                         |
  | `multi_specialized` | `FastWorkflowOperator` + `SlowWorkflowOperator` via `asyncio.gather` | each has independent `@observe` / `@decide`; slow op owns `@goals`                                                                                                    |
  | `hier_parent`       | `SimParentBudgetPolicy` (parent) + `AnalysisChildOperator × 2`       | parent: `@observe` tracks cumulative CPU-time, `@decide` manages sim priorities and budget demotion; children: `@decide` scoped to own workflow's analysis stage only |

**Entry point:** `benchmark.py --benchmark bmark4 --runs 5`

---

## 2. dreamer_campaign

**Purpose:** SPHERICAL drug-discovery funnel emulation.  Models a large-scale
HPC virtual screen at local scale using `radical.dreamer` stubs.
Demonstrates ADR policy comparison (bandit, LLM, rule) over a multi-stage
cascade where each stage applies a score gate before passing candidates forward.

**Topology:** Linear 5-stage funnel with tightening score gates — only
candidates whose computed score meets or exceeds the stage threshold proceed
to the next stage.  The gates grow progressively stricter so the population
narrows at each transition; roughly 3% of s1 candidates reach s5.

```
library: 10 000 compounds (s1 is the CM root group, pre-loaded with 10 000 replicas)
  → s1_ligand_filter  (cap 30, 0.5 s/rep, score ≥ 0.60 → ~40% pass → ~4 000 enter s2)
  → s2_ml_affinity    (cap 16, 0.5 s/rep, score ≥ 0.65 → ~75% pass → ~3 000 enter s3)
  → s3_docking        (cap 12, 1.0 s/rep, score ≥ 0.70 → ~85% pass → ~2 550 enter s4)
  → s4_md_refinement  (cap  8, 2.0 s/rep, score ≥ 0.75 → ~80% pass → ~2 040 enter s5)
  → s5_fep_ranking    (cap  6, 2.0 s/rep)  ← terminal; campaign stops at 5 completions
```

Each stage refines the score with small added noise, so high-scoring candidates
tend to remain high-scoring across stages.  The ADR agent competes on how
quickly it drives 5 s5 completions through this narrowing funnel.

Engine: `concurrent` (asyncio, local).

**Agent implementation — `DreamerCampaignOperator`:**

| Decorator  | Implementation                                                                                                        |
|------------|-----------------------------------------------------------------------------------------------------------------------|
| `@goals`   | Single goal: `n_hits ≥ 4.5` (5 s5_fep_ranking completions). Campaign stops as soon as 5 leads emerge from the funnel. |
| `@observe` | None — uses base `CampaignView.observe()` snapshot unchanged.                                                         |
| `@decide`  | Delegated entirely to the swappable policy.                                                                           |

Default policy: `BanditSchedulingPolicy` with depth-based warm-start priors.
Policy options at runtime: `none | rule | downstream_first | bandit | llm`.
LLM backend: HuggingFace router, `Llama-3.3-70B`, 2 s tick, rule fallback on
timeout.

**Entry point:** `run_campaign.py --policy bandit`

---

## 3. esm2_ddsim_campaign

**Purpose:** Real HPC dual-pipeline campaign on Delta GPU nodes.  Two
independent pipelines (ESM2 protein inference + DDSim, and MD simulations +
MiniApps analysis) share 8 GPUs and compete for slots at every ADR tick.
Demonstrates that a well-designed operator can eliminate idle GPUs that the
`none` policy leaves stranded.

**Topology:** Two parallel pipelines, resource-coupled.

```
Pipeline A:  inference (8 rep, 1 GPU ea)  →  dummy/DDSim (dep, 0 GPU)
Pipeline B:  md        (2 rep, 1 GPU ea)  →  miniapps    (dep, 1 GPU)
```

Dependency routing uses `_trigger_dependent` / `_signal_done` at runtime (not
config `dependencies`), which means the logical DAG is invisible to the base
CM view and must be reconstructed by the operator.  Engine: `dragon`
(radical.asyncflow DragonExecutionBackendV3, real HPC).

**Agent implementation — `DDSimCampaignOperator`:**

| Decorator                | Implementation                                                                                                                                                                                                                                                                                                                                                                                                                      |
|--------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `@observe` — `extract()` | Reconstructs the logical pipeline topology that the config cannot express (inference/md = sources; miniapps depends on md; dummy depends on inference+miniapps). Recomputes `starved` correctly for each stage. Flattens per-stage finished counts as top-level obs keys (`miniapps_finished`, etc.) so Goal metrics can reference them. Also merges live hardware telemetry (`gpu_util`, `cpu_util`, `mem_used_gb`) from `asyncflow.start_telemetry()` via `TelemetrySubscriber` — the only campaign where the operator sees real GPU utilisation during scheduling decisions. |
| `@goals`                 | Three goals — scientific, efficiency, and operational health (see below).                                                                                                                                                                                                                                                                                                                                                           |
| `@decide`                | Delegated to the swappable policy.                                                                                                                                                                                                                                                                                                                                                                                                  |

**Three goals:**

| Goal                   | Metric              | Threshold | What it catches                                                     |
|------------------------|---------------------|-----------|---------------------------------------------------------------------|
| `md_pipeline_complete` | `miniapps_finished` | ≥ 3.5     | All 4 MD trajectories analyzed by ML                                |
| `gpus_fully_utilized`  | `free_gpus`         | < 0.5     | No idle GPU slots (policy=none leaves 1 GPU idle after MD finishes) |
| `low_task_failures`    | `task_fail_rate`    | < 0.05    | Hardware / config health check                                      |

Policy options: `none | rule | downstream_first | bandit | llm`.
LLM backend: HuggingFace router, `Llama-3.3-70B`, 10 s tick (matched to real
HPC cadence).

**Entry point:** `run_campaing.py --policy rule` (launched via `delta_gpu_batch.sh`)

---

## 4. orbit_campaign

**Purpose:** Proof-of-concept for remote task execution via the ORBIT broker +
rhapsody endpoint.  A two-stage search → refine pipeline where each replica
fans out simulation tasks to a live HPC compute node over a WebSocket
connection.  Not an ADR benchmark — demonstrates the CM's ability to drive
real remote compute without an HPC scheduler in the loop.

**Topology:**

```
search (10 rep, cap 2)  — score mean(|y|); candidates below threshold trigger refine
  → refine (dep, cap 2)  ← campaign stops when 5 refine replicas complete
```

Each replica submits `num_sims=5` tasks to ORBIT in one batch; ORBIT runs them
in parallel on the remote endpoint so per-replica wall time ≈ one simulation's
duration (~15 s).

**Agent implementation:** None.  This campaign uses `AsyncCampaignManager`
directly with no ADR operator, no policies, and no `@goals` / `@observe` /
`@decide` decorators.  Stopping is handled by `campaign_target: 5` on the
refine group in config.

**Prerequisites:** ORBIT broker running on the login node + rhapsody endpoint
running inside the allocation.

**Entry point:** `run_campaign.py --config config.yaml`

---

---

## 5. nested_campaign

**Purpose:** Demonstration of sequential outer rounds each containing an inner
`sim → analysis` loop.  Each round tightens the analysis score gate cutoff using
the previous round's best score, delivered to the parent via SNAPSHOT messaging.

**Topology:** N sequential outer rounds; each round triggers `n_sim` sim
replicas that each draw a random score and individually trigger one analysis
replica. A score gate compares each sim's score against a per-round cutoff
— sims whose score falls below the cutoff are dropped before triggering analysis.  Engine:
`concurrent` (local, no config file).

**Operators:** `InnerOperator` (child — `@goals` + `@observe`) tracks hits and
best score per round. `OuterOperator` (parent — `@observe` + `@act`) sequences
rounds, reads the child's score via SNAPSHOT, and tightens the score gate cutoff
between rounds.  `OuterPolicy` (`@decide`) is a three-state machine: launch →
wait → stop.

**Key patterns:** hierarchical `child.start()` / SNAPSHOT messaging; fresh child
operator per round; score gate cutoff tightened between rounds; manual
infrastructure wiring (score tracking, candidate injection) required when
bypassing `from_config`.

See [`nested_campaign/README.md`](nested_campaign/README.md) for the full
architecture diagram, operator table, design decisions, and possible extensions.

**Entry point:** `run_campaign.py [--n-rounds N] [--n-sim N] [--tick S]`

---

## Quick reference

| Campaign              | Domain                | Engine     | Operator class                         | Key ADR decorators                                         |
|-----------------------|-----------------------|------------|----------------------------------------|------------------------------------------------------------|
| `ddsim_campaign`      | Scheduling benchmark  | concurrent | `CampaignOperator` / bmark4 subclasses | `@goals`; bmark4 adds `@observe` + `@decide` per operator  |
| `dreamer_campaign`    | Drug discovery funnel | concurrent | `DreamerCampaignOperator`              | `@goals`                                                   |
| `esm2_ddsim_campaign` | Dual-pipeline HPC     | dragon     | `DDSimCampaignOperator`                | `@observe` (topology + starved + obs keys) + `@goals` (×3) |
| `orbit_campaign`      | Remote task execution | concurrent | — (no ADR)                             | —                                                          |
| `nested_campaign`     | Nested-loop demo      | concurrent | `InnerOperator` + `OuterOperator`      | `@goals` + `@observe` + `@act` (both levels)               |
