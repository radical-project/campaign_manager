# Plotting Tools

Visualization scripts for campaign benchmarks, timelines, and ADR policy analysis.
All scripts use `matplotlib` unless noted. Run from any directory — paths default to the current working directory.

---

## Campaign Timelines

### `plot_cm_timeline.py` — Generic campaign Gantt chart

Parses a SLURM output file and produces a two-panel figure: replica start/end Gantt chart and CPU/GPU resource utilization over time.

**Usage**
```bash
python plotting/plot_cm_timeline.py slurm-XXXXXX.out \
    [--config config.yaml] \
    [--out cm_timeline.png]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `log` (positional) | required | SLURM output file (`slurm-XXXXXX.out`) |
| `--config` | none | Campaign YAML config; adds a config-summary table to the figure |
| `--out` | `cm_timeline_<run>.png` | Output PNG path |

---

### `plot_dreamer_timeline.py` — Dreamer campaign timeline (extended)

Superset of `plot_cm_timeline.py` with an extra row showing Dreamer-specific simulation statistics (task distributions, stage-level throughput) read from per-replica profile JSON files.

**Usage**
```bash
python plotting/plot_dreamer_timeline.py campaign.log \
    [--profiles-dir dreamer-profiles/] \
    [--config config.yaml] \
    [--out dreamer_timeline.png]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `log` (positional) | required | Campaign log file |
| `--profiles-dir` | none | Directory of per-replica Dreamer profile JSONs |
| `--config` | none | Campaign YAML config for the summary table |
| `--out` | auto-named PNG | Output PNG path |

---

## Benchmark Comparisons

### `plot_optimizations.py` — Feature-flag benchmark comparison

Reads a benchmark results JSON (produced by `dreamer_campaign/benchmark.py`) and produces **7 PNG files** comparing configurations with different feature flags enabled.

**Output files** (written to `--out-dir`):
`wall_time.png`, `pipeline_gantt.png`, `cascade_funnel.png`, `gpu_utilization.png`, `shard_dispatch.png`, `bandit_convergence.png`, `time_to_target.png`

**Usage**
```bash
python plotting/plot_optimizations.py \
    [--results benchmark_results.json] \
    [--out-dir plots/optimizations]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--results` | `benchmark_results.json` | Benchmark JSON from `benchmark.py` |
| `--out-dir` | `plots/optimizations` | Directory for output PNGs (created if missing) |

---

### `plot_adr_optimizations.py` — ADR policy benchmark comparison

Reads an ADR benchmark results JSON (from `benchmark_adr.py`) and produces **4 PNG files** comparing `rule`, `bandit`, and `llm` scheduling policies.

**Output files** (written to `--out-dir`):
`wall_time.png`, `pipeline_gantt.png`, `cascade_funnel.png`, `time_to_target.png`

> **Note**: imports helper functions from `plot_optimizations.py` — both scripts must be in the same directory.

**Usage**
```bash
python plotting/plot_adr_optimizations.py \
    [--results benchmark_adr_results.json] \
    [--out-dir plots/adr]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--results` | `benchmark_adr_results.json` | ADR benchmark JSON from `benchmark_adr.py` |
| `--out-dir` | `plots/adr` | Directory for output PNGs (created if missing) |

---

### `plot_budget_control.py` — Triage / BudgetController narrative

Three-panel figure illustrating how the BudgetController adapts Triage cutoffs over a campaign run: ADVANCE skip rate, score-cutoff trajectory, and burn-ratio vs. plan.

**Usage**
```bash
python plotting/plot_budget_control.py \
    [--results benchmark_results.json] \
    [--out plots/diagrams]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--results` | `benchmark_results.json` | Benchmark JSON from `benchmark.py` |
| `--out` | `plots/diagrams` | Output directory |

---

### `plot_deadline_yield.py` — Leads-within-deadline bar chart

Bar chart showing the fraction of leads completed within a deadline window for each ADR policy configuration.

**Usage**
```bash
python plotting/plot_deadline_yield.py \
    [--results benchmark_deadline.json] \
    [--out plots/deadline_yield.png] \
    [--deadline 3600.0]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--results` | `benchmark_deadline.json` | Deadline benchmark JSON from `benchmark_adr.py` |
| `--out` | `plots/deadline_yield.png` | Output PNG path |
| `--deadline` | from results | Override deadline cutoff in seconds |

---

## ADR Policy Analysis

### `plot_policy_comparison.py` — Priority trace per ADR policy

Line plot showing how each ADR policy changes group priorities over scheduling cycles, one panel per policy. Reads JSONL decision logs written by `PolicyRecorder`.

**Usage**
```bash
python plotting/plot_policy_comparison.py run1_decisions.jsonl run2_decisions.jsonl \
    [--out plots/policy_comparison.png]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `logs` (positional, one or more) | required | `PolicyRecorder` JSONL decision log files |
| `--out` | `plots/policy_comparison.png` | Output PNG path |

---

### `plot_replica_timeline.py` — Replica Gantt per ADR policy

Gantt chart of replica start/end times grouped by ADR policy (`rule`, `bandit`, `llm`, `none`). Reads the GPU ADR benchmark JSON.

**Usage**
```bash
python plotting/plot_replica_timeline.py \
    [--input benchmark_adr_gpu.json] \
    [--out plots/replica_timeline.png] \
    [--policies rule bandit llm] \
    [--dpi 150]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--input` | `benchmark_adr_gpu.json` | GPU ADR benchmark JSON from `benchmark_adr_gpu.py` |
| `--out` | `plots/replica_timeline.png` | Output PNG path |
| `--policies` | all in file | Subset of policies to plot |
| `--dpi` | 150 | Figure DPI |

---

## Presentation

### `make_presentation.py` — PowerPoint deck

Assembles a PowerPoint presentation (`spherical_benchmark.pptx`) from the benchmark figures. Requires `python-pptx`.

**Dependencies**
```bash
pip install python-pptx
```

**Usage**
```bash
python plotting/make_presentation.py [--out spherical_benchmark.pptx]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--out` | `spherical_benchmark.pptx` | Output PPTX file path |

---

## Typical Workflow

```bash
cd workflows

# 1. Run the dreamer benchmark
python dreamer_campaign/benchmark.py --config dreamer_campaign/config.yaml \
    --runs 5 --out benchmark_results.json

# 2. Run the ADR policy sweep
python dreamer_campaign/benchmark_adr.py --config dreamer_campaign/config.yaml \
    --out benchmark_adr_results.json

# 3. Generate all plots
python plotting/plot_optimizations.py --results benchmark_results.json
python plotting/plot_adr_optimizations.py --results benchmark_adr_results.json
python plotting/plot_budget_control.py --results benchmark_results.json

# 4. Visualize a live or finished campaign from its SLURM log
python plotting/plot_cm_timeline.py slurm-17715157.out --config dreamer_campaign/config.yaml

# 5. Build the presentation
python plotting/make_presentation.py
```
