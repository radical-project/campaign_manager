#!/bin/sh -l
#
# SPHERICAL Dreamer Campaign — Delta CPU benchmark
#
# All stages run in dreamer stub mode (total_gpus=0, no real GPU use):
#   s1=0.5s(10000 reps, cap=30), s2=0.5s(cap=16), s3=1.0s(cap=12),
#   s4=2.0s(cap=8), s5=2.0s(cap=6, cm.adr.goals.n_target=5).
# Campaign early-stops when s5 produces 5 leads; all stages overlap in pipeline.
# Pipeline latency s1→s5: ~6s; measured wall time per run: ~8-10s.
# Total benchmark (1 run × 4 policies: none/rule/bandit/llm): ~1min.
#
# Submit: sbatch delta_benchmark_sbatch.sh
# Logs:   slurm-<jobid>.out  (stdout+stderr, streamed live)
#
# Account: set SBATCH_ACCOUNT=<project>-delta-cpu before calling sbatch
#SBATCH --partition=cpu
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --time=01:00:00
#SBATCH --job-name=dreamer_bench
#SBATCH --mail-user=mariya.goliyad@rutgers.edu
#SBATCH --mail-type=ALL
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.out

# ── Paths ─────────────────────────────────────────────────────────────────────
if [ -z "${SBATCH_ACCOUNT:-}${SLURM_JOB_ACCOUNT:-}" ]; then
    echo "WARNING: SBATCH_ACCOUNT is not set — job may be charged to default account."
    echo "         Set it with: export SBATCH_ACCOUNT=<project>-delta-cpu"
fi
echo "Account: ${SLURM_JOB_ACCOUNT:-unknown}"

if [ -z "${SCRATCH:-}" ]; then
    echo "ERROR: SCRATCH is not set."
    echo "       export SCRATCH=/scratch/<allocation> && sbatch delta_benchmark_sbatch.sh"
    exit 1
fi

export CM_DIR="${CM_DIR:-${SCRATCH}/${USER}/campaign_manager}"
export DREAMER_DIR="${DREAMER_DIR:-${SCRATCH}/${USER}/radical.dreamer}"
export ENV_DIR="${ENV_DIR:-/u/${USER}/ve/dreamer_campaign}"

# ── Activate venv ─────────────────────────────────────────────────────────────
source "${ENV_DIR}/bin/activate"

# ── Run ───────────────────────────────────────────────────────────────────────
CAMPAIGN_DIR="${CM_DIR}/campaigns/dreamer_campaign"
cd "${CAMPAIGN_DIR}"

# Clean stale artifacts from previous runs
rm -rf dreamer-profiles telemetry-results

echo "=== Dreamer benchmark: $(date) ==="
echo "    Node: ${SLURMD_NODENAME}  CPUs: ${SLURM_CPUS_PER_TASK}"
echo "    Config: config.yaml  Runs: 1 per policy  Policies: none/rule/bandit/llm"
echo "    Stage durations (stub): s1=0.5s(10000 reps) s2=0.5s s3=1.0s s4=2.0s s5=2.0s"
echo "    Campaign early-stops when s5 completes 5 leads (cm.adr.goals.n_target=5)"
echo "    Expected: ~9s/run, total ~1min (1 run x 4 policies: none/rule/bandit/llm)"

python benchmark.py --config config_stall.yaml 
#--runs 1 --out benchmark_results.json

echo "=== Benchmark done: $(date) ==="

# Regenerate plots
python plot_benchmark.py --results benchmark_results.json --out-dir plots

echo "=== Plots written: $(date) ==="

rm -rf asyncflow.session*