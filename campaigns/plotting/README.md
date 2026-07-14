# Plotting Tools

Shared visualization scripts for campaign timelines.
All scripts use `matplotlib`. Run from any directory — paths default to the current working directory.

Campaign-specific benchmark plots live alongside each campaign:
- `dreamer_campaign/plot_benchmark.py` — policy comparison for the Dreamer campaign
- `esm2_ddsim_campaign/plot_benchmark.py` — policy comparison for the ESM2/DDSim campaign

---

## `plot_dep_timeline.py` — Short campaign: dependency arrows

Parses a SLURM output file and produces a two-panel figure: per-replica Gantt chart
with upstream→downstream dependency arrows + CPU/GPU resource utilization.

Best for campaigns with a small number of replicas where inter-workflow triggers
are the primary insight (e.g. ESM2/DDSim, dummy campaigns).

```bash
python plotting/plot_dep_timeline.py slurm-XXXXXX.out \
    [--config config.yaml] \
    [--policy none|rule|bandit|llm] \
    [--out cm_timeline.png]
```

| Argument            | Default                 | Description                                                      |
|---------------------|-------------------------|------------------------------------------------------------------|
| `log` (positional)  | required                | SLURM output file                                                |
| `--config`          | none                    | Campaign YAML; adds a config-summary table to the figure         |
| `--policy`          | auto (first section)    | Select a policy section when the log contains a multi-run benchmark |
| `--out`             | `plots/dep_timeline_<run>.png` | Output PNG path                                           |

**Output**: one PNG with two rows — Gantt (one bar per replica, arrows for triggered replicas) + CPU/GPU utilization step chart.

---

## `plot_timeline.py` — Long campaign: simulation statistics

Parses a SLURM output file and produces a three-panel figure: concurrency-weighted
Gantt + resource utilization + optional Dreamer emulation statistics.

Best for longer campaigns (hundreds to thousands of replicas per stage) run with the
Dreamer emulation backend, where aggregate concurrency shapes and simulation metrics
are the primary insight.

```bash
python plotting/plot_timeline.py slurm-XXXXXX.out \
    [--config config.yaml] \
    [--profiles-dir dreamer-profiles/] \
    [--title "My Campaign"] \
    [--out timeline.png]
```

| Argument            | Default                        | Description                                               |
|---------------------|--------------------------------|-----------------------------------------------------------|
| `log` (positional)  | required                       | SLURM or campaign log file                                |
| `--config`          | `config.yaml` next to log      | Campaign YAML; enriches the per-workflow stats table      |
| `--profiles-dir`    | `dreamer-profiles/` next to log | Per-replica JSON profiles from Dreamer                   |
| `--title`           | `Campaign Timeline — <stem>`   | Figure suptitle                                           |
| `--out`             | `plots/timeline_<stem>.png`    | Output PNG path                                           |

**Output**: one PNG with two or three rows:
- Row 0: Gantt — concurrency step-function per workflow over wall-clock time
- Row 1: CPU/GPU resource utilization
- Row 2 *(only when `dreamer-profiles/` present)*: simulated makespan + task-ops distributions + per-workflow stats table

---

## `plot_telemetry.sh` — AsyncFlow task-level telemetry dashboard

Wraps `radical.asyncflow`'s `plot_workflow_dashboard.py` to produce a multi-panel
PNG from the JSONL telemetry checkpoint written by the asyncflow engine during a run.

The workflow name is inferred automatically from the grandparent of the telemetry
directory (`<wf_name>/telemetry-output/<file>.jsonl → plots/<wf_name>/`), so no
extra arguments are needed for the common case.

```bash
bash plotting/plot_telemetry.sh <telemetry.jsonl> \
    [--out-dir DIR] \
    [--split]
```

| Argument              | Default                                        | Description                                      |
|-----------------------|------------------------------------------------|--------------------------------------------------|
| `<telemetry.jsonl>`   | required                                       | JSONL checkpoint from asyncflow telemetry        |
| `--out-dir DIR`       | `plots/<wf_name>/` next to this script         | Output directory                                 |
| `--split`             | off                                            | Save each subplot as a separate PNG instead of one combined dashboard |

**Output (combined)**: `workflow_dashboard_<YYYYMMDD_HHMMSS>.png` in `OUT_DIR`

**Output (`--split`)**: one `<stem>.<panel>.png` per subplot in `OUT_DIR`

> **Dependency**: requires `radical.asyncflow` with the telemetry extras installed at
> `$SCRATCH/$USER/radical.asyncflow`. The plot script is resolved at
> `radical.asyncflow/examples/telemetry/plot_workflow_dashboard.py`.

---

## Typical Workflow

```bash
cd workflows

# ── Short campaign (ESM2/DDSim) ───────────────────────────────────────────────

# 1. Run the campaign
sbatch esm2_ddsim_campaign/delta_run_sbatch.sh

# 2. Visualize the SLURM log with dependency arrows
python plotting/plot_dep_timeline.py slurm-XXXXXX.out \
    --config esm2_ddsim_campaign/config.yaml

# 3. (If benchmark log with multiple policy sections)
python plotting/plot_dep_timeline.py slurm-XXXXXX.out --policy rule

# ── Long campaign (Dreamer benchmark) ────────────────────────────────────────

# 1. Run the benchmark
sbatch dreamer_campaign/delta_benchmark_sbatch.sh

# 2. Plot benchmark outcomes (multi-policy comparison)
python dreamer_campaign/plot_benchmark.py \
    --results dreamer_campaign/benchmark_results.json \
    --out-dir dreamer_campaign/plots/

# 3. Visualize a single run with simulation statistics
python plotting/plot_timeline.py slurm-XXXXXX.out \
    --config dreamer_campaign/config.yaml \
    --profiles-dir dreamer_campaign/dreamer-profiles/

# ── AsyncFlow telemetry dashboard (any campaign) ─────────────────────────────

# Combined dashboard (single PNG, timestamped)
bash plotting/plot_telemetry.sh esm2_ddsim_campaign/telemetry-results/out.jsonl

# Per-panel PNGs in a custom directory
bash plotting/plot_telemetry.sh esm2_ddsim_campaign/telemetry-results/out.jsonl \
    --out-dir esm2_ddsim_campaign/plots/telemetry --split
```
