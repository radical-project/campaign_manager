#!/bin/sh -l
#
# SPHERICAL Dreamer Campaign — Delta CPU benchmark
#
# All stages GPU-bound: s1=2s(3000 reps), s2=5s, s3=15s, s4=30s, s5=22s.
# s1 GPU-limited at 24 concurrent → 250s (4.2min); all stages overlap (3× GPU oversubscription).
# Expected runtimes per run: baseline ~25min, optimised ~15min.
# Total benchmark (5 runs × 4 configs): ~5.4h → 7h walltime gives 1.6h safety margin.
#
# Submit: sbatch delta_cpu_sbatch.sh
# Logs:   slurm-<jobid>.out  (stdout+stderr, streamed live)
#
#SBATCH -A bblj-delta-cpu
#SBATCH --partition=cpu
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --time=05:00:00
#SBATCH --job-name=dreamer_bench
#SBATCH --mail-user=mariya.goliyad@rutgers.edu
#SBATCH --mail-type=ALL
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.out

# ── Paths ─────────────────────────────────────────────────────────────────────
export SPHERICAL_DIR="/scratch/bblj/${USER}/SPHERICAL"
export DREAMER_DIR="/scratch/bblj/${USER}/radical.dreamer"
export ENV_DIR="/u/${USER}/ve/dreamer_campaign"

# ── Activate venv ─────────────────────────────────────────────────────────────
source "${ENV_DIR}/bin/activate"

# ── Run ───────────────────────────────────────────────────────────────────────
CAMPAIGN_DIR="${SPHERICAL_DIR}/workflows/run_campaign/dreamer_campaign"
cd "${CAMPAIGN_DIR}"

# Clean stale artifacts from previous runs
rm -rf dreamer-profiles telemetry-results

echo "=== Dreamer benchmark: $(date) ==="
echo "    Node: ${SLURMD_NODENAME}  CPUs: ${SLURM_CPUS_PER_TASK}"
echo "    Config: config.yaml  Runs: 5"
echo "    Stage durations: s1=2s(1500 reps) s2=5s s3=15s s4=30s s5=22s (all GPU-bound)"
echo "    Workload: s2=787, s3=110, s4=55, s5=33 replicas (identical across all configs)"
echo "    Expected: ~10min/run, total ~3.5h (5 runs x 4 configs)"

python benchmark.py --config config.yaml --runs 5 --out benchmark_results.json

echo "=== Benchmark done: $(date) ==="

# Regenerate plots
python plot_optimizations.py --results benchmark_results.json --out-dir plots/optimizations

echo "=== Plots written: $(date) ==="

rm -rf asyncflow.session*