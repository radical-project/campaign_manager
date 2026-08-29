# DDSim Campaign — ADR Feature Reference

The DDSim campaign is the reference implementation for ADR (Autonomous Decision Runtime) features in the Campaign Manager. It models a two-pool molecular dynamics screening workflow: fast/cheap `ddsim_a` (or `fast_sim`) simulations run in parallel with slow/fine `ddsim_b` (or `slow_sim`), feeding one or two downstream analysis stages. The same workflow code is reused across four structured benchmarks, each isolating a different dimension of ADR behaviour.

---

## Table of Contents

1. [Campaign topology](#1-campaign-topology)
2. [Quick-start — exploratory runs](#2-quick-start--exploratory-runs)
3. [Benchmark 1 — Resource-contention stall recovery](#3-benchmark-1--resource-contention-stall-recovery)
4. [Benchmark 1b — LLM timeout resilience](#4-benchmark-1b--llm-timeout-resilience)
5. [Benchmark 2 — Pipeline isolation](#5-benchmark-2--pipeline-isolation)
6. [Benchmark 3 — Temporal adaptation to stage depletion](#6-benchmark-3--temporal-adaptation-to-stage-depletion)
7. [Benchmark 4 — Multi-operator + hierarchical + budget control](#7-benchmark-4--multi-operator--hierarchical--budget-control)
8. [Phased campaigns](#8-phased-campaigns)
9. [Offline policy replay](#9-offline-policy-replay)
10. [HPC submission (Delta SLURM)](#10-hpc-submission-delta-slurm)
11. [File reference](#11-file-reference)
12. [ADR features demonstrated](#12-adr-features-demonstrated)

---

## 1. Campaign topology

The base topology used in most benchmarks is a fan-in with two simulation pools sharing a CPU-constrained scheduler:

```
fast_sim  (fast / coarse)  ──┐
                             ├──► analysis_fast
                             │
slow_sim  (slow / fine)    ──┘──► analysis_slow
```

`DdSimWorkflow` drives both simulation pools. Each replica draws a score from a Gaussian and passes that score to `_trigger_dependent("analysis_*", replicas=1)`, which enqueues a downstream analysis job. The score travels with the trigger so the sharder can use it for dynamic priority ranking of pending analysis jobs. `AnalysisWorkflow` simulates the analysis compute time and records completion; the refined score it computes is logged for observability but is not consumed by the benchmark ADR policies. The campaign stops when a goal is met — a target count of analyses, a compute budget, or a quality threshold, whichever fires first.

---

## 2. Quick-start — exploratory runs

### Flat campaign (both pools concurrent)

```bash
cd campaigns/ddsim_campaign

python run_campaign.py --config config.yaml
python run_campaign.py --config config.yaml --policy rule
python run_campaign.py --config config.yaml --policy bandit
python run_campaign.py --config config.yaml --policy llm   # needs HF_TOKEN or OPENROUTER_API_KEY
```

**Config:** `config.yaml`
- `ddsim_a`: 10 replicas, 0.10 s/rep, σ=0.15 (fast/coarse)
- `ddsim_b`: 10 replicas, 0.30 s/rep, σ=0.05 (slow/fine)
- Shared `analysis` stage; `total_cpus: 0` (unlimited)

### Full-features flat campaign

```bash
python run_campaign.py --config config_features.yaml
```

**Config:** `config_features.yaml`
- Enables all CM features: backpressure, sharder, score-aware slack (`backpressure_score_slack`), triage cutoff
- **Score-aware slack (`backpressure_score_slack`):** backpressure normally throttles a stage when its queue depth exceeds a fixed `high_water` mark. Score-aware slack makes that mark dynamic: if the sharder's buffer currently holds high-quality candidates (score well above the historical mean), the effective `high_water` is raised by up to `score_slack × 100 %`, so the CM tolerates a longer queue rather than cutting off good work. Example: `high_water=10, score_slack=0.3, score_quality=0.8` → `effective_high = int(10 × (1 + 0.8 × 0.3)) = 12`.
- Used for verifying that `is_source` (no upstream deps → pipeline entry) and `_infer_terminal` (no downstream deps → pipeline exit) are correctly auto-detected from config-level `dependencies` keys, score-aware backpressure, and budget-controller integration

---

## 3. Benchmark 1 — Resource-contention stall recovery

**Question:** when a slow simulation pool monopolises shared CPU slots, can ADR detect and correct the stall?

**Design file:** `ddsim_operator.py`

### Topology

```
ddsim_a (0.30 s, 30 rep)  ──┐
                            ├──► analysis   (cap=2, goal: 15 analyses)
ddsim_b (0.10 s, 30 rep)  ──┘
total_cpus = 5   (5 slots contested by 2 sims + analysis)
```

**Config:** `config_consensus_3stage.yaml`

Key blind spot: `ddsim_b` is the fast feeder in this config (0.10 s vs 0.30 s), but the hard-coded rule policy always assigns `ddsim_a` the higher priority, wasting the first ~3 s on the wrong pool.

**Fairness note:** this is not a level playing field. The rule is intentionally misconfigured (wrong feeder bias) while the bandit is given a warm-start prior that already encodes the correct answer (Beta(5,1) for `ddsim_b`). The benchmark therefore measures two things at once: algorithmic adaptivity (bandit can learn from data) and prior quality (bandit was told the right answer to begin with). A stricter comparison would use a neutral Beta(1,1) prior for both arms — the bandit would still converge to the correct arm from the reward signal, just ~2× slower. The warm start is a realistic design choice (a practitioner who knows the config can encode it), but it should not be confused with pure algorithmic superiority over the rule.

### Policies under test

| Policy      | Operator / class            | Behaviour                                                                      |
|-------------|-----------------------------|--------------------------------------------------------------------------------|
| `rule`      | `DdSim3RulePolicy`          | Hard-coded: a=108, b=105; wrong feeder bias; stalls during STARVED             |
| `bandit`    | `BanditSchedulingPolicy`    | Thompson-sampling; Beta(5,1) warm-start for b, Beta(1,3) for a — prior already encodes the correct feeder order |
| `llm`       | `LLMSchedulingPolicy`       | System prompt describes b as fast feeder; reacts per-cycle                     |
| `consensus` | `ConsensusSchedulingPolicy` | 2/3 majority (bandit + llm); overrides rule's bias from cycle 1                |

### Run

```bash
python benchmark.py --config config_consensus_3stage.yaml --runs 5 \
    --policies rule bandit llm consensus \
    --out stall_benchmark_results_3stage.json

python make_benchmark_plots.py --data stall_benchmark_results_3stage.json --out bmark1.png
```

**Results JSON:** `stall_benchmark_results_3stage.json`
**Plot:** `bmark1.png`

**Expected ordering (lower ttt = better):** `consensus ≈ bandit ≈ llm  <<  rule`

**Observed (job 20880053):** consensus ≈ 2.90 s · bandit ≈ 2.88 s · llm ≈ 2.94 s · rule ≈ 4.07 s

---

## 4. Benchmark 1b — LLM timeout resilience

**Question:** how does each policy degrade as the LLM API response time worsens?

**Same operator as Benchmark 1**; varied via `--llm-timeout`.

### Policies under test

Same four as Benchmark 1: `none`, `rule`, `bandit`, `llm`, `consensus`.

`consensus` is the key subject. When the LLM misses its deadline, `LLMSchedulingPolicy` silently returns its own internal rule fallback — so consensus receives three votes: bandit + rule + LLM-as-rule = bandit vs 2×rule. A naive majority would pick the wrong feeder. What saves consensus is performance-mode obs scoring: instead of counting votes, it selects the policy whose decision best matches the live observation signal (`avg_duration_s`, `starved` state). Since the obs signal confirms `ddsim_b` is faster, the bandit's decision wins even when outvoted 2-to-1. The expected result is that `consensus` tracks the bandit baseline regardless of LLM timeout, while standalone `llm` degrades as its timeout tightens.

### Run

```bash
# Reference (full-capability, 8 s timeout)
python benchmark.py --config config_consensus_3stage.yaml --runs 5 \
    --policies none rule bandit llm consensus \
    --out stall_benchmark_results_t8s.json
python make_benchmark_plots.py --data stall_benchmark_results_t8s.json \
    --benchmark bmark1b --out bmark1b_t8s.png

# Tight timeout sweeps
python benchmark.py ... --llm-timeout 2.0 --out stall_benchmark_results_t2s.json
python benchmark.py ... --llm-timeout 1.0 --out stall_benchmark_results_t1s.json
python benchmark.py ... --llm-timeout 0.5 --out stall_benchmark_results_t0s5.json

python make_benchmark_plots.py --data stall_benchmark_results_t2s.json --benchmark bmark1b --out bmark1b_t2s.png
python make_benchmark_plots.py --data stall_benchmark_results_t1s.json --benchmark bmark1b --out bmark1b_t1s.png
python make_benchmark_plots.py --data stall_benchmark_results_t0s5.json --benchmark bmark1b --out bmark1b_t0s5.png
```

**Results JSONs:** `stall_benchmark_results_t8s.json`, `_t2s.json`, `_t1s.json`, `_t0s5.json`
**Plots:** `bmark1b_t8s.png`, `bmark1b_t2s.png`, `bmark1b_t1s.png`, `bmark1b_t0s5.png`

**Expected:** `llm` ttt grows with tighter timeout; `consensus` stays flat near `bandit` baseline.

---

## 5. Benchmark 2 — Pipeline isolation

**Question:** when two independent simulation→analysis pipelines share a CPU pool, can per-pipeline operator scope prevent cross-pipeline interference?

**Design file:** `ddsim_operator_hierarchical.py`

### Topology

```
Pipeline 1:  fast_sim (0.10 s, 200 rep, cap=3) ──► analysis_fast (0.05 s, cap=1)
Pipeline 2:  slow_sim (0.30 s,  30 rep, cap=3) ──► analysis_slow (0.05 s, cap=1)
total_cpus = 4,  n_target = 30 per pipeline
```

**Config:** `config_bmark2.yaml`

**Key blind spot (flat):** fast_sim produces tasks faster than analysis_fast can drain them (3 CPUs × 10/s vs 1 CPU × 20/s). THROTTLE fires and a flat policy demotes **both** sims — Pipeline 2's slow_sim stalls even though analysis_slow is fine.

**Isolation fix:** Pipeline 1's policy reacts only to analysis_fast THROTTLE; Pipeline 2's policy ignores Pipeline 1. Both pipelines run concurrently at full speed.

### Policies under test

| Policy                  | Class                      | Scope                                                                       |
|-------------------------|----------------------------|-----------------------------------------------------------------------------|
| `flat_global`           | `FlatGlobalRulePolicy`     | Global: any THROTTLE demotes both sims                                      |
| `isolated_delegated`    | `IsolatedRulePolicy`       | Each pipeline reacts to its own analysis stage only                         |
| `isolated_centralized`  | `IsolatedCentralizedPolicy`| Per-pipeline backpressure + cross-pipeline rebalancing every cycle when hit-count imbalance \|fast_hits − slow_hits\| / n_target > 5% |
| `isolated_debounced`     | `IsolatedDebouncedPolicy`   | Same but rebalances only when hit-count imbalance > 20% for ≥2 consecutive cycles — avoids thrashing on transient spikes that centralized overreacts to |

### Run

```bash
python benchmark.py --benchmark bmark2 --config config_bmark2.yaml --runs 5 \
    --policies flat_global isolated_delegated isolated_centralized isolated_debounced \
    --out bmark2_results.json

python make_benchmark_plots.py --data bmark2_results.json --out bmark2.png
```

**Results JSON:** `bmark2_results.json`
**Plot:** `bmark2.png`

**Expected ordering (lower ttt = better):** `isolated_debounced ≈ isolated_centralized ≈ isolated_delegated  <<  flat_global`

**Observed (job 20883017):** flat_global ≈ 10.56 s · isolated_delegated ≈ 4.97 s · isolated_centralized ≈ 4.03 s · isolated_debounced ≈ 4.22 s (2.1–2.6× speedup)

### Variant configs

#### bmark2-noisy — centralized policy fails under high-jitter imbalance

**Question:** when two pipelines are structurally identical but differ only in measurement noise, does centralized rebalancing help or hurt?

**Why it's interesting:** `IsolatedCentralizedPolicy` fires REBALANCE whenever the hit-count gap exceeds 5%. With 70% CV jitter (σ=0.14 s on a 0.20 s mean), that threshold is breached nearly every cycle — but the imbalance is pure noise, not a real structural difference. Each REBALANCE cycle one sim is demoted to priority 95, filling 0 CPUs for an entire tick. Because the imbalance is noise-driven, the policy immediately flips direction next tick, stalling the *other* pipeline instead. `IsolatedDebouncedPolicy` (20% threshold + streak ≥ 2) never triggers, staying in NORMAL mode throughout. This is the clearest case where a more sensitive policy is strictly worse.

**Expected ordering (lower ttt = better):** `isolated_debounced ≈ isolated_delegated  <  isolated_centralized`

**Result data:** `bmark2_noisy_results.json`

```bash
python benchmark.py --benchmark bmark2 --config config_bmark2_noisy.yaml --runs 5 \
    --policies flat_global isolated_delegated isolated_centralized isolated_debounced \
    --out bmark2_noisy_results.json
python make_benchmark_plots.py --data bmark2_noisy_results.json --out bmark2_noisy.png
```

#### bmark2-osc — centralized policy fails under oscillating imbalance

**Question:** when two pipelines oscillate in antiphase (each takes turns being faster), does centralized rebalancing recover the lag or amplify it?

**Why it's interesting:** antiphase sinusoidal speed variation (amplitude=0.08 s, period=1.5 s) produces a sustained hit-count gap of ~8% — above centralized's 5% threshold but below debounced's 20%. Centralized fires REBALANCE on every p1-lead peak, demoting fast_sim to priority 95. Because fast_sim has `concurrency_cap=1`, the combined analysis×2 + slow_sim×2 already fills all 4 CPUs, leaving fast_sim 0 slots. Pipeline 1 stalls for ~0.2 s per peak (~3 stalls per run, ~1–2 s total penalty). The gap then reverses as slow_sim accelerates; centralized fires again in the opposite direction — a no-op this time. `IsolatedDebouncedPolicy` never fires; both pipelines run at their natural oscillation with no added stalls.

**Expected ordering (lower ttt = better):** `isolated_debounced ≈ isolated_delegated  <  isolated_centralized`

**Result data:** `bmark2_osc_results.json`

```bash
python benchmark.py --benchmark bmark2 --config config_bmark2_osc.yaml --runs 5 \
    --policies isolated_delegated isolated_centralized isolated_debounced \
    --out bmark2_osc_results.json
python make_benchmark_plots.py --data bmark2_osc_results.json --out bmark2_osc.png
```

---

## 6. Benchmark 3 — Temporal adaptation to stage depletion

**Question:** when one simulation pool exhausts its replicas mid-campaign, can ADR detect (or predict) the depletion and shift compute before the downstream analysis queue starves?

**Design file:** `ddsim_operator_temporal.py`

### Topology

```
fast_sim (0.10 s, 200 rep, cap=3) ──► analysis_fast (0.20 s, cap=2)
slow_sim (0.50 s,  30 rep, cap=3) ──► analysis_slow (0.05 s, cap=1)   ← target
total_cpus = 4,  n_target = 30 analysis_slow completions
```

**Config:** `config_temporal.yaml`

**Phase dynamics:** fast_sim produces 30 tasks/s; analysis_fast handles only 10 tasks/s → a ~67-task backlog builds up. fast_sim depletes all 200 replicas at t ≈ 6.7 s. After depletion, analysis_fast backlog drains at 10 tasks/s for another ~6.7 s, occupying 2 CPUs and starving slow_sim unless the policy reacts.

### Policies under test

| Policy          | Class                | Mechanism                                                                                 |
|-----------------|----------------------|-------------------------------------------------------------------------------------------|
| `rule_static`   | `RuleStaticPolicy`   | Always: analysis_fast=110, slow_sim=90. No adaptation.                                    |
| `adr_reactive`  | `AdrReactivePolicy`  | Reads `fast_depleted` from `@observe`; rebalances the moment fast_sim exhausts.           |
| `adr_proactive` | `AdrProactivePolicy` | Reads `fast_eta_s` from `@observe`; rebalances ~2 s *before* depletion via ETA estimate. |

`@observe` enrichment (both ADR operators):
```python
obs["fast_depleted"] = (pending == 0 and running == 0)
obs["fast_eta_s"]    = (pending + running) * dur / max(running, 1)
```

### Run

```bash
python benchmark.py --benchmark bmark3 --config config_temporal.yaml --runs 5 \
    --policies rule_static adr_reactive adr_proactive \
    --out temporal_benchmark_results.json

python make_benchmark_plots.py --data temporal_benchmark_results.json --out bmark3.png
```

**Results JSON:** `temporal_benchmark_results.json`
**Plot:** `bmark3.png`

**Expected ordering (lower ttt = better):** `rule_static > adr_reactive > adr_proactive`

The earlier the policy rebalances, the more CPU cycles slow_sim gains at the critical phase.

---

## 7. Benchmark 4 — Multi-operator + hierarchical + budget control

**Question:** on two quality-asymmetric workflows sharing a tight CPU pool, which operator composition maximises high-quality analysis completions within a fixed CPU-time budget?

**Design file:** `ddsim_operator_bmark4.py`

### Topology

```
Workflow 1:  fast_sim (0.10 s, 200 rep, score_mean=0.55) ──► analysis_fast (0.05 s)
Workflow 2:  slow_sim (0.40 s,  30 rep, score_mean=0.80) ──► analysis_slow (0.05 s)  ← target
total_cpus = 4
CPU-time budget = 30 CPU-s  (≈ 70% of full campaign cost ≈ 43.5 CPU-s)
n_target = 30 analysis_slow completions
```

**Config:** `config_bmark4.yaml`

**Quality asymmetry:** slow workflow score_mean=0.80 (89% above threshold 0.70); fast workflow score_mean=0.55 (below threshold). Maximising high-quality yield means maximising slow workflow throughput within the budget.

**Primary metric:** `quality_yield` = number of analysis_slow completions at the moment cumulative CPU-time crosses 30 CPU-s. CPU-time is approximated as Σ(stage_done × avg_duration_s × required_cpus) from the observation dict each tick.

### Conditions under test

#### `flat_rule` — single operator, global scope

**Operator:** `FlatRuleBmark4Operator`  
**Policy:** `FlatRuleBmark4Policy`

All 4 stages are managed by one operator. Any backpressure (analysis_fast pending > cap) triggers a global THROTTLE that demotes **both** fast_sim and slow_sim. Fast-workflow congestion (fast_sim feeds 20 tasks/s into an analysis_fast that handles only 20 tasks/s at cap=1) fires THROTTLE almost immediately and starves slow_sim for the duration.

```python
class FlatRuleBmark4Operator(CampaignOperator):
    @goals
    def _goals(self):
        return Goal(metric="analysis_slow_hits", threshold=n_target - 0.5)
    
    @observe
    def _obs(self, obs):
        obs["analysis_slow_hits"] = ...   # count from stages
        obs["budget_exceeded"] = _cpu_time_from_obs(obs) >= CPU_BUDGET_S
```

#### `multi_specialized` — two workflow-specialized operators (asyncio.gather)

**Operators:** `FastChainOperator` (secondary) + `SlowChainOperator` (primary)  
**Policies:** `FastWorkflowRulePolicy`, `SlowWorkflowRulePolicy`

Each operator owns its full workflow. `FastWorkflowRulePolicy` reacts only to analysis_fast backpressure; `SlowWorkflowRulePolicy` reacts only to analysis_slow backpressure. Fast-workflow THROTTLE cannot demote slow_sim. Both workflows run concurrently.

Stopping: `SlowChainOperator` is the **primary** (owns `@goals`; calls `cm.stop()` when done). `FastChainOperator` is a **secondary** (max_cycles=10000; shuts down when `cm.wait()` returns).

**vs bmark2 `isolated_delegated`:** both achieve the same per-workflow backpressure isolation — THROTTLE on one workflow cannot stall the other. The structural difference is *where* the isolation lives:

| | bmark2 `isolated_delegated` | bmark4 `multi_specialized` |
|---|---|---|
| Isolation mechanism | Policy scope: single operator, policy ignores other pipeline's state | Operator decomposition: two separate ADR loops, each blind to the other |
| Cross-workflow rebalancing | Possible — both pipelines visible in one obs dict (used by `isolated_centralized`, `isolated_debounced`) | Not possible — operators share no observation; each only sees its own stages |
| Stopping | Symmetric: both pipelines must hit `n_target` | Asymmetric: primary (`slow`) stops the campaign; secondary runs until then |
| Per-workflow policy | Same policy class handles both | Different policy class (and optionally different algorithm) per workflow |

`multi_specialized` is the right pattern when sub-systems need different stopping conditions or different ADR policies (e.g. bandit on one, rule on another). `isolated_delegated` is simpler and also enables cross-workflow rebalancing via the shared obs dict.

```python
# benchmark.py — _run_once_bmark4
await _drive_multi_with_timeout(
    cm,
    primary=slow_op,
    secondaries=[fast_op],
    timeout=TIMEOUT_S)
```

#### `hier_parent` — parent + 2 analysis child operators (hierarchical)

**Operators:** `SimParentBmark4Operator` (primary) + `AnalysisChildOperator×2` (secondaries)  
**Policies:** `SimParentBudgetPolicy` (parent), `AnalysisOnlyPolicy` (children)

The parent owns fast_sim + slow_sim priorities and CPU-time budget tracking. Children each own their own analysis stage's priority, intra-workflow.

Parent `@observe` derives budget state:
```python
@observe
def _obs(self, obs):
    cpu = _cpu_time_from_obs(obs)           # Σ(done × cost_per_task)
    obs["cpu_time_s"]      = cpu
    obs["budget_exceeded"] = cpu >= CPU_BUDGET_S
```

Parent `@decide` responds at budget crossing:
```python
@decide  # SimParentBudgetPolicy
def _decide(self, obs, priority):
    if obs.get("budget_exceeded"):
        priority["fast_sim"] = _P_SIM_KILLED   # 10 — near-zero; all CPUs shift to slow workflow
    else:
        priority["slow_sim"] = _P_SIM_BOOST    # 109 — above fast_sim from cycle 1
        priority["fast_sim"] = _P_SIM_NORMAL   # 106
```

Children use `AnalysisOnlyPolicy`: a fixed-priority anchor that pins analysis to 110 (`_P_ANALYSIS`) every cycle — no backpressure detection, no rebalancing. This is a deliberate simplification so the parent's budget-control logic is the sole moving part; in a realistic campaign each child would carry its own logic (THROTTLE detection, concurrency adjustment, quality triage). The parent never touches analysis priorities; children never touch sim priorities — division of responsibility is enforced by scope.

### Priority constants

```python
_P_ANALYSIS   = 110   # analysis stages — always highest
_P_SIM_BOOST  = 109   # slow_sim in Phase 1 (above fast_sim)
_P_SIM_HIGH   = 108   # reserved
_P_SIM_NORMAL = 106   # fast_sim baseline
_P_SIM_DEMOTE = 95    # demoted (flat throttle)
_P_SIM_KILLED = 10    # fast_sim after budget exhaustion (hier_parent)
```

### CPU-time cost map

`_cpu_time_from_obs` estimates total CPU-seconds from live telemetry: `Σ(finished × avg_duration_s)` across all stages. `avg_duration_s` is the mean wall-time of finished replicas, updated each tick by the view layer — so the estimate adapts to actual durations under jitter rather than relying on a static cost map. All stages use `required_cpus=1`, so `avg_duration_s` equals CPU-seconds per task directly. This sum drives the `budget_exceeded` flag that `SimParentBudgetPolicy` uses to kill fast_sim.

### Run

```bash
python benchmark.py --benchmark bmark4 --config config_bmark4.yaml --runs 5 \
    --policies flat_rule multi_specialized hier_parent \
    --out bmark4_results.json

python make_bmark4_plot.py --data bmark4_results.json --out bmark4.png
```

**Results JSON:** `bmark4_results.json`
**Plot:** `bmark4.png`

The plot shows cumulative analysis_slow throughput curves (mean + min/max band), a vertical dashed budget line at mean budget_crossing_t for conditions that hit the budget, a filled circle at (budget_t, quality_yield), and a star marker at campaign end for conditions that finish under budget.

**Results (job 20931307, 5 runs each):**

| Condition           | quality_yield | budget_crossing_t | wall_time |
|---------------------|---------------|-------------------|-----------|
| `flat_rule`         | 4.4 / 30      | 7.89 s            | 11.79 s   |
| `multi_specialized` | 30 / 30       | never             | 6.31 s    |
| `hier_parent`       | 30 / 30       | never             | 4.99 s    |

**ADR features demonstrated:**
- `asyncio.gather` multi-operator pattern (workflow isolation)
- `@observe` enriching obs with derived state (`cpu_time_s`, `budget_exceeded`)
- Per-pipeline isolated `@decide` separating sim-level and analysis-level scope
- Dynamic budget response (priority demotion at threshold crossing)

---

## 8. Phased campaigns

The phased campaign demonstrates a two-phase ADR hierarchy where Phase 1 screens cheap simulations and Phase 2 refines on the best candidates, with the Phase 2 triage cutoff set from Phase 1's actual results.

**Design file:** `ddsim_operator_phased.py`  
**Runner:** `run_campaign_phased.py`

```
PhasedParentOperator
  ├── Phase1Operator  (supervises ddsim_a + analysis)
  └── Phase2Operator  (supervises ddsim_b + analysis)
```

Phase 2's triage cutoff is computed from Phase 1's `best_score_p50` via SNAPSHOT messaging:
```python
cutoff = max(0.10, best_score_p50 * 0.85)   # 15% margin; floor at 0.10
```

### Configs

| Config                    | Purpose                                                                                            |
|---------------------------|----------------------------------------------------------------------------------------------------|
| `config_phased.yaml`      | Standard phased campaign                                                                           |
| `config_phased_edge.yaml` | Edge case: exactly 5 Phase 1 replicas (score_p50 minimum-sample boundary)                         |
| `config_phased_neg.yaml`  | Negative path: all scores below triage cutoff → zero analysis output; campaign still exits cleanly |

### Run

```bash
# Standard
python run_campaign_phased.py --config config_phased.yaml

# Resume from checkpoint (after SLURM preemption or manual stop)
python run_campaign_phased.py --config config_phased.yaml \
    --resume adr-logs/checkpoint_<job>.json

# Edge / negative tests
python run_campaign_phased.py --config config_phased_edge.yaml
python run_campaign_phased.py --config config_phased_neg.yaml
```

Every run saves:
- **Checkpoint:** `adr-logs/checkpoint_<job>.json` — full decision state; survives SLURM preemption
- **Trace:** `adr-logs/trace_<job>.jsonl` — per-cycle observation + decision log for offline replay

---

## 9. Offline policy replay

`replay_policies.py` loads a phased-campaign trace and re-drives two alternative policies against it — no engine, no tasks, no HPC.

```bash
python replay_policies.py adr-logs/trace_<job>.jsonl
python replay_policies.py adr-logs/trace_<job>.jsonl --score-gate 0.65 --preempt 0.45
```

Three policies are compared:

| Policy         | Behaviour                                                                              |
|----------------|----------------------------------------------------------------------------------------|
| `ORIGINAL`     | Launches Phase 2 as soon as Phase 1 is DONE                                            |
| `CONSERVATIVE` | Requires Phase 1 DONE **and** `stage1_score ≥ SCORE_GATE` (default 0.70)              |
| `AGGRESSIVE`   | Pre-emptively launches Phase 2 when `stage1_score ≥ PREEMPT_THRESHOLD` (default 0.50) |

Output is a cycle-by-cycle comparison table with `◄` markers where policies diverge, plus a summary of when each policy would have launched Phase 2.

---

## 10. HPC submission (Delta SLURM)

**Script:** `delta_sbatch.sh`

**Prerequisites (one-time):**

```bash
# 1. SLURM account and scratch path
export SBATCH_ACCOUNT=<project>-delta-cpu
export SCRATCH=/scratch/<allocation>

# 2. LLM API key — required for bmark1 and bmark1b (llm / consensus policies)
export ANTHROPIC_API_KEY=<your-key>   # or HF_TOKEN / OPENROUTER_API_KEY depending on config

# 3. Create the virtualenv
bash env_setup.sh
```

`env_setup.sh` creates a venv at `/u/$USER/ve/ddsim_campaign` with `python-pptx`, `matplotlib`, `numpy`, and all CM/ADR dependencies. Run once per user account; subsequent jobs reuse it via `${ENV_DIR:-/u/$USER/ve/ddsim_campaign}`.

**Submit:**

```bash
sbatch delta_sbatch.sh bmark1
sbatch delta_sbatch.sh bmark1b
sbatch delta_sbatch.sh bmark2
sbatch delta_sbatch.sh bmark3
sbatch delta_sbatch.sh bmark4
```

Each job runs the full benchmark pipeline (Python benchmark + plot generation). `PYTHONASYNCIODEBUG=1` is set to surface slow asyncio callbacks in stderr.

---

## 11. File reference

### Workflow and operator files

| File                             | Role                                                                                             |
|----------------------------------|--------------------------------------------------------------------------------------------------|
| `ddsim_workflow.py`              | `DdSimWorkflow` + `AnalysisWorkflow` — simulation and analysis task logic                        |
| `ddsim_operator.py`              | Flat-campaign ADR operator: goals, observe, scheduling regimes, dynamic quality gate (bmark1/1b) |
| `ddsim_operator_hierarchical.py` | Per-pipeline isolation policies for concurrent-pipeline benchmark (bmark2)                       |
| `ddsim_operator_temporal.py`     | Reactive / proactive temporal-adaptation policies for depletion benchmark (bmark3)               |
| `ddsim_operator_bmark4.py`       | Multi-operator + hierarchical + budget operators (bmark4)                                        |
| `ddsim_operator_phased.py`       | Two-phase hierarchical operator: Phase1, Phase2, PhasedParent with SNAPSHOT + checkpoint         |

### Runner scripts

| File                     | Role                                                                            |
|--------------------------|---------------------------------------------------------------------------------|
| `run_campaign.py`        | Flat campaign runner: loads any config, optional `--policy` flag                |
| `run_campaign_phased.py` | Phased campaign runner: wires checkpoint save, trace recording, `--resume`      |
| `benchmark.py`           | Unified benchmark harness for bmark1–4; `--benchmark` selects mode; `--runs N` |
| `replay_policies.py`     | Offline A/B policy comparison against a recorded phased-campaign trace          |

### Config files

| File                           | Used by                    | Description                                                         |
|--------------------------------|----------------------------|---------------------------------------------------------------------|
| `config.yaml`                  | `run_campaign.py`          | Flat baseline: ddsim_a + ddsim_b → analysis, unlimited CPUs         |
| `config_consensus_3stage.yaml` | `benchmark.py` (bmark1/1b) | 3-pool, 5 CPUs; ddsim_b is fast feeder; rule has a-first blind spot |
| `config_bmark2.yaml`     | `benchmark.py` (bmark2)    | Two pipelines, 4 CPUs; fast_sim floods analysis_fast                |
| `config_temporal.yaml`         | `benchmark.py` (bmark3)    | fast_sim depletes at ~6.7 s; two analysis stages; n_target=30 slow  |
| `config_bmark4.yaml`           | `benchmark.py` (bmark4)    | Quality-asymmetric workflows; 4 CPUs; 30 CPU-s budget               |
| `config_phased.yaml`           | `run_campaign_phased.py`   | Two-phase hierarchical: ddsim_a screens → ddsim_b refines           |
| `config_phased_edge.yaml`      | `run_campaign_phased.py`   | Edge: 5-replica Phase 1; score_p50 minimum-sample boundary          |
| `config_phased_neg.yaml`       | `run_campaign_phased.py`   | Negative path: all scores below triage cutoff; zero analyses        |

### Plot and result files

| File                                  | Produced by               | Content                                                                   |
|---------------------------------------|---------------------------|---------------------------------------------------------------------------|
| `bmark1.png`                          | `make_benchmark_plots.py` | Benchmark 1 TTT comparison (rule/bandit/llm/consensus)                    |
| `bmark1b.png` / `bmark1b_t*.png`      | `make_benchmark_plots.py` | Benchmark 1b LLM timeout degradation sweep                                |
| `bmark2.png`                          | `make_benchmark_plots.py` | Benchmark 2 TTT comparison (flat_rule vs per-pipeline isolation policies) |
| `bmark3.png`                          | `make_benchmark_plots.py` | Benchmark 3 TTT comparison (rule_static vs reactive vs proactive)         |
| `bmark4.png`                          | `make_bmark4_plot.py`     | Benchmark 4 quality-yield throughput curves                               |
| `stall_benchmark_results_3stage.json` | `benchmark.py`            | Bmark1 raw results (5 runs × 4 policies)                                  |
| `stall_benchmark_results_t*.json`     | `benchmark.py`            | Bmark1b results at each timeout setting                                   |
| `bmark2_results.json`         | `benchmark.py`            | Bmark2 raw results                                                        |
| `bmark2_noisy_results.json`   | `benchmark.py`            | Bmark2 noisy variant results                                              |
| `bmark2_osc_results.json`     | `benchmark.py`            | Bmark2 oscillating variant results                                        |
| `temporal_benchmark_results.json`     | `benchmark.py`            | Bmark3 raw results                                                        |
| `bmark4_results.json`                 | `benchmark.py`            | Bmark4 raw results (5 runs × 3 conditions)                                |

### Infrastructure

| File              | Role                                                                               |
|-------------------|------------------------------------------------------------------------------------|
| `delta_sbatch.sh` | SLURM batch script; `sbatch delta_sbatch.sh <bmark1|bmark1b|bmark2|bmark3|bmark4>` |
| `env_setup.sh`    | One-time venv creation on Delta (runs as `bash env_setup.sh`)                      |
| `adr-logs/`       | Per-run checkpoints (`checkpoint_<job>.json`) and ADR traces (`trace_<job>.jsonl`) |
| `prompts/`        | LLM system prompts used by `LLMSchedulingPolicy`                                   |

---

## 12. ADR features demonstrated

### Goals and stopping conditions

**Feature: `@goals` — static stopping criteria**
```python
# ddsim_operator.py
@goals
def criteria(self):
    return AnyGoal([
        Goal(name="analyses_done", metric="n_hits",  threshold=n_target - 0.5),
        Goal(name="budget_out",    metric="n_sims",  threshold=n_budget - 0.5),
    ])
```
`AnyGoal` gives OR semantics. The first of the two sub-goals to fire wins. Used in the flat campaign and all benchmarks.

**Feature: runtime goal proposal — AND requirement added mid-campaign**
```python
# ddsim_operator.py — on first THROTTLE event
return Decision(goals=[
    Goal(name="quality_gate", metric="score_p50_analysis",
         threshold=0.35, direction="maximize")
])
```
A new name adds an independent AND requirement. The campaign must now satisfy both AnyGoal and quality_gate before stopping.

**Feature: goal upsert — tightening in place**
```python
# Re-propose with the SAME name → replaces threshold, no duplicate
return Decision(goals=[
    Goal(name="quality_gate", metric="score_p50_analysis", threshold=0.45)
])
```
Progressive threshold tightening (0.35 → 0.45 → 0.55) as evidence accumulates. The goal list stays clean.

**Feature: goal removal**
```python
return Decision(remove_goals=["quality_gate"])
```
When analyses are within 5 of target, the quality gate has served its purpose. Removing it lets AnyGoal fire cleanly on the count.

---

### Observation enrichment

**Feature: `@observe` — base obs + campaign-specific metrics**
```python
# ddsim_operator.py
@observe
def extract(self, snapshot) -> dict:
    obs = base.extract(self, snapshot)
    obs["score_p50_analysis"] = stages.get("analysis", {}).get("score_p50")
    return obs
```

**Feature: `@observe` with derived state (bmark3 / bmark4)**
```python
# ddsim_operator_temporal.py — temporal depletion prediction
obs["fast_depleted"] = (pending == 0 and running == 0)
obs["fast_eta_s"]    = (pending + running) * dur / max(running, 1)

# ddsim_operator_bmark4.py — CPU-time budget tracking
obs["cpu_time_s"]      = _cpu_time_from_obs(obs)   # Σ(done × cost)
obs["budget_exceeded"] = obs["cpu_time_s"] >= CPU_BUDGET_S
```

---

### Scheduling policy

**Feature: `@decide` — per-cycle priority adjustment**
```python
# ddsim_operator_bmark4.py — SimParentBudgetPolicy
@decide
def _decide(self, obs, priority):
    if obs.get("budget_exceeded"):
        priority["fast_sim"] = _P_SIM_KILLED   # 10
    else:
        priority["slow_sim"] = _P_SIM_BOOST    # 109
        priority["fast_sim"] = _P_SIM_NORMAL   # 106
```

---

### Multi-operator patterns

**Feature: asyncio.gather multi-operator (workflow isolation, bmark4)**
```python
# benchmark.py — _drive_multi_with_timeout
# primary owns @goals; calls cm.stop() on exit
# secondaries use max_cycles=10000; shut down when cm.wait() returns
await _drive_multi_with_timeout(
    cm, primary=slow_op, secondaries=[fast_op], timeout=TIMEOUT_S)
```

**Feature: per-pipeline isolation — scoped policy loops (bmark2)**

Bmark2: `IsolatedRulePolicy` — each policy reacts only to its own pipeline's analysis stage. No cross-pipeline THROTTLE interference.

**Feature: hierarchical operators — parent + child ADR loops (bmark4)**

Bmark4: `SimParentBmark4Operator` owns sim priorities and budget; `AnalysisChildOperator×2` each own their analysis stage. No cross-workflow interference; no cross-level priority collision.

---

### Phased campaign (hierarchical operator lifecycle)

**Feature: `child.start()` — launching an ADR operator as an AsyncFlow block**
```python
# PhasedParentOperator
self._phase1 = Phase1Operator(view, engine=engine, n_ddsim_a=n_ddsim_a)
object.__setattr__(self._phase1, "parent", self)   # wire SNAPSHOT channel
```
`child.start()` returns a Future tracked via `state.task_futures`. Phase ordering is enforced at the policy level: when `phase1_status == "DONE"`, the policy fires `launch_phase2`.

**Feature: SNAPSHOT messaging — child-to-parent data flow**
```python
# Phase1Operator publishes best_score_p50 in state.objectives each cycle.
# Parent reads it each cycle:
p50_now = snapshot.objectives.get("child:Phase1Operator:obj:best_score_p50") or 0.0
self.state.artifacts["phase1_best_score"] = max(p50_now, stored_best)
```

**Feature: information passing — data-driven Phase 2 triage cutoff**
```python
p50 = self.state.artifacts.get("phase1_best_score", 0.0) or 0.0
cutoff = round(max(0.10, p50 * 0.85), 3)
self.view.set_score_cutoff("analysis", cutoff)
```

**Feature: checkpoint save / load — preemption resilience**
```python
finally:
    parent.save_checkpoint_full(str(ckpt_path))   # always saves, even on SIGTERM
```
`save_checkpoint_full()` calls the radical.adr base `save_checkpoint()` then writes any campaign-specific state returned by `save_extra()` to a sidecar `.extra.json` file. On resume, `load_checkpoint_full()` checks the checkpoint version (warns on mismatch rather than aborting) then calls `load_extra()` with the sidecar data if present. Campaign subclasses override `save_extra()` / `load_extra()` to persist state that lives outside radical.adr's `artifacts`/`runtime` dicts.

Checkpoint restores `cycle`, `objectives`, `artifacts`, and `task_context`. LOST tasks are re-launched by the policy:
```python
if phase1_status == "LOST":
    return Decision(actions=[self._act.launch_phase1()])
```

**Feature: record & replay — offline A/B policy evaluation**

Every phased run writes `adr-logs/trace_<job>.jsonl` (one JSON line per cycle: obs + decision). `replay_policies.py` re-drives alternative policies against it without any HPC resources. `◄` markers show exactly which observation caused policies to diverge.

---

### ADR features at a glance

| Feature               | Without ADR                  | With ADR                                                                  |
|-----------------------|------------------------------|---------------------------------------------------------------------------|
| Stopping condition    | Fixed `campaign_target: N`   | Count / budget / quality in any AND/OR combination                        |
| Priorities            | Static config numbers        | Adjusted every tick to keep the pipeline flowing                          |
| Quality gate          | Hard-coded threshold or none | Proposed on data signal, tightened as evidence grows, revoked when served |
| Phase 2 triage cutoff | Fixed in config              | Computed from Phase 1's actual best score                                 |
| Phase ordering        | Not applicable               | Parent/child hierarchy; each phase has its own goal + policy stack        |
| Post-run debugging    | Read SLURM stdout            | Replay any alternative policy against the exact observation sequence      |
| Preemption recovery   | Restart from zero            | Checkpoint captures full decision state; resume mid-phase                 |
| CPU-time budget       | Not applicable               | `@observe` tracks cumulative cost; `@decide` demotes at crossing          |
| Workflow isolation       | Global flat scope            | Per-workflow operator; THROTTLE on one workflow cannot starve another     |
