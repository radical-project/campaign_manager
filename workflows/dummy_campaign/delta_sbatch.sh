#!/bin/sh -l
#
# SPHERICAL Dummy Campaign — SLURM batch script (CPU-only, no GPU needed)
#
#SBATCH -A ***-delta-cpu
#SBATCH --partition=cpu
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --time=00:30:00
#SBATCH --job-name=dummy_campaign
#SBATCH --mail-user=***
#SBATCH --mail-type=END,FAIL

# ── Environment ───────────────────────────────────────────────────────────────
export SPHERICAL_DIR="/scratch/***/${USER}/SPHERICAL"
export ENV_DIR="/u/${USER}/ve/campaign"

unset SLURM_EXPORT_ENV
module load anaconda3 2>/dev/null || true

source "${ENV_DIR}/bin/activate"

# ── Run campaign ──────────────────────────────────────────────────────────────
CAMPAIGN_DIR="${SPHERICAL_DIR}/workflows/dummy_campaign"
cd "${CAMPAIGN_DIR}"

#python run_campaign.py --config config.yaml --policy rule

python benchmark.py --config config.yaml --runs 5 --out benchmark_results.json

echo "=== Benchmark done: $(date) ==="

# Regenerate plots
python plot_benchmark.py --results benchmark_results.json --out-dir plots

echo "=== Plots written: $(date) ==="

rm -rf asyncflow.session*