#!/bin/sh -l

#SBATCH -A bblj-delta-gpu
#SBATCH --partition=gpuA100x4
#SBATCH --nodes=1
#SBATCH --gpus-per-node=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --time=06:00:00
#SBATCH --job-name=dreamer_bench
#SBATCH --mail-user=mariya.goliyad@rutgers.edu
#SBATCH --mail-type=ALL

export SPHERICAL_DIR="/scratch/bblj/${USER}/SPHERICAL"
export DREAMER_DIR="/scratch/bblj/${USER}/radical.dreamer"
export ENV_DIR="/u/${USER}/ve/dreamer_campaign"

source "${ENV_DIR}/bin/activate"

CAMPAIGN_DIR="${SPHERICAL_DIR}/workflows/run_campaign/dreamer_campaign"
cd "${CAMPAIGN_DIR}"
rm -rf dreamer-profiles telemetry-results

echo "=== Dreamer benchmark: $(date) === Node: ${SLURMD_NODENAME}"
echo "Config: 10000 s1, target=20 s5, 5 runs x 4 configs"

python benchmark.py --config config.yaml --runs 5 --out benchmark_results.json

python plot_optimizations.py --results benchmark_results.json --out-dir plots/optimizations

echo "=== Done: $(date) ==="
