#!/bin/sh -l
#
# SPHERICAL Dreamer Campaign — SLURM batch script (CPU-only, no GPU needed)
#
#SBATCH -A ***
#SBATCH --partition=RM
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --time=01:00:00
#SBATCH --job-name=dreamer_campaign
#SBATCH --mail-user=***
#SBATCH --mail-type=END,FAIL

# ── Environment ───────────────────────────────────────────────────────────────
export SPHERICAL_DIR="/scratch/bblj/${USER}/SPHERICAL"
export DREAMER_DIR="/scratch/bblj/${USER}/radical.dreamer"
export ENV_DIR="/u/${USER}/ve/dreamer_campaign"

export DREAMER_DIR="${DREAMER_DIR}"   # picked up by dreamer_workflow.py

unset SLURM_EXPORT_ENV
module load anaconda3 2>/dev/null || true

source "${ENV_DIR}/bin/activate"

# ── Run campaign ──────────────────────────────────────────────────────────────
CAMPAIGN_DIR="${SPHERICAL_DIR}/workflows/run_campaign/dreamer_campaign"
cd "${CAMPAIGN_DIR}"

rm -rf dreamer-profiles telemetry-results

python run_campaign.py --config config.yaml
