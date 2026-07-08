#!/bin/sh -l
#
# SPHERICAL Dreamer Campaign — Delta CPU benchmark
#
# All stages run in dreamer stub mode (total_gpus=0, no real GPU use):
#   s1=0.5s(10000 reps, cap=30), s2=0.5s(cap=16), s3=1.0s(cap=12),
#   s4=2.0s(cap=8), s5=2.0s(cap=6, campaign_target=5).
# Campaign early-stops when s5 produces 5 leads; all stages overlap in pipeline.
# Pipeline latency s1→s5: ~6s; measured wall time per run: ~8-10s.
# Total benchmark (5 runs × 3 policies: none/rule/bandit): ~3min.
#
# Submit: sbatch delta_benchmark_sbatch.sh
# Logs:   slurm-<jobid>.out  (stdout+stderr, streamed live)
#
#SBATCH -A ***-delta-cpu
#SBATCH --partition=cpu
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --time=00:15:00
#SBATCH --job-name=dreamer_bench
#SBATCH --mail-user=mariya.goliyad@rutgers.edu
#SBATCH --mail-type=ALL
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.out

# ── Paths ─────────────────────────────────────────────────────────────────────
export SPHERICAL_DIR="/scratch/***/${USER}/SPHERICAL"
export DREAMER_DIR="/scratch/***/${USER}/radical.dreamer"
export ENV_DIR="/u/${USER}/ve/dreamer_campaign"

# ── Activate venv ─────────────────────────────────────────────────────────────
source "${ENV_DIR}/bin/activate"

# ── Run ───────────────────────────────────────────────────────────────────────
CAMPAIGN_DIR="${SPHERICAL_DIR}/workflows/dreamer_campaign"
cd "${CAMPAIGN_DIR}"

# Clean stale artifacts from previous runs
rm -rf dreamer-profiles telemetry-results

echo "=== Dreamer benchmark: $(date) ==="
echo "    Node: ${SLURMD_NODENAME}  CPUs: ${SLURM_CPUS_PER_TASK}"
echo "    Config: config.yaml  Runs: 5"
echo "    Stage durations (stub): s1=0.5s(10000 reps) s2=0.5s s3=1.0s s4=2.0s s5=2.0s"
echo "    Campaign early-stops when s5 completes 5 leads (campaign_target=5)"
echo "    Expected: ~9s/run, total ~3min (5 runs x 3 policies: none/rule/bandit)"

python benchmark.py --config config.yaml --runs 5 --out benchmark_results.json

echo "=== Benchmark done: $(date) ==="

# Regenerate plots
python plot_benchmark.py --results benchmark_results.json --out-dir plots

echo "=== Plots written: $(date) ==="

rm -rf asyncflow.session*