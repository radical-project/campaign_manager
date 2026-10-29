# ddsim_aucam Campaign

Runs the DeepDriveSim miniapps workflow under AuCaM — a bandit-driven campaign manager that selects between fast and slow simulation arms.

## Prerequisites

Two repos must exist side-by-side under `$WORK_DIR`:

```
$WORK_DIR/
  campaign_manager/       # this repo
  DeepDriveSim/           # github.com/radical-project/DeepDriveSim
  workflow-mini-apps/     # github.com/radical-cybertools/workflow-mini-apps
```

## 1 — Create the virtual environment (once)

```bash
export WORK_DIR=/work/nvme/bdyk/<project>/<username>
bash campaigns/ddsim_aucam/delta_env_setup.sh
```

This creates `$WORK_DIR/ve/ddsim_aucam` with all dependencies:
AuCaM, DDSim, mpi4py (built against Cray MPICH), and wfMiniAPI.

## 2 — Run from the command line (login node, concurrent engine)

```bash
export WORK_DIR=/work/nvme/bdyk/<project>/<username>
source campaigns/ddsim_aucam/delta_env_setup.sh   # sets env vars + activates VE

cd campaigns/ddsim_aucam
python run_campaign.py --engine concurrent \
    --global-config   campaign_configs/global.yaml \
    --campaign-config campaign_configs/campaign.yaml \
    --telemetry-dir   aucam-tel-local
```

Uses `campaign.yaml` (small scale: `total_num_sim=5`, `num_step=50`).
No GPU required; runs on CPU.

## 3 — Run via SLURM (GPU node, HPC scale)

```bash
export WORK_DIR=/work/nvme/bdyk/<project>/<username>
export SBATCH_ACCOUNT=<project>-delta-gpu
mkdir -p campaigns/ddsim_aucam/logs

# Concurrent engine (GPU node):
ENGINE=concurrent sbatch campaigns/ddsim_aucam/delta_sbatch.sh

# Dragon engine (CPU, single-node):
ENGINE=dragon sbatch campaigns/ddsim_aucam/delta_sbatch.sh
```

Uses `campaign_hpc.yaml` (`total_num_sim=20`, `num_step=1000`).
Logs land in `campaigns/ddsim_aucam/logs/ddsim_<jobid>.out`.
Telemetry lands in `campaigns/ddsim_aucam/aucam-tel-<jobid>/`.

## Configuration

| File | Purpose |
|------|---------|
| `campaign_configs/global.yaml` | Global defaults; `--verbose` and `--telemetry-dir` override at runtime |
| `campaign_configs/campaign.yaml` | Small-scale run parameters (local dev) |
| `campaign_configs/campaign_hpc.yaml` | Full-scale run parameters (SLURM) |

Key knobs in `campaign.yaml` / `campaign_hpc.yaml`:

- `controller.goal_n` — stop after N workflow completions
- `miniapps.total_num_sim` — simulations per replica
- `miniapps.max_sim_batch` — max concurrent sims at once
- `miniapps.num_step` — timesteps per simulation (slow arm = `× slow_step_multiplier`)
