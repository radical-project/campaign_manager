#!/bin/sh -l
#
# Dreamer Campaign — SLURM batch script (CPU-only, no GPU needed)
#
# Account: set SBATCH_ACCOUNT=<project>-delta-cpu before calling sbatch
#SBATCH --partition=cpu
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --time=01:00:00
#SBATCH --job-name=dreamer_campaign
#SBATCH --mail-user=mariya.goliyad@rutgers.edu
#SBATCH --mail-type=END,FAIL

# ── Environment ───────────────────────────────────────────────────────────────
if [ -z "${SBATCH_ACCOUNT:-}${SLURM_JOB_ACCOUNT:-}" ]; then
    echo "WARNING: SBATCH_ACCOUNT is not set — job may be charged to default account."
    echo "         Set it with: export SBATCH_ACCOUNT=<project>-delta-cpu"
fi
echo "Account: ${SLURM_JOB_ACCOUNT:-unknown}"

if [ -z "${SCRATCH:-}" ]; then
    echo "ERROR: SCRATCH is not set."
    echo "       export SCRATCH=/scratch/<allocation> && sbatch delta_sbatch.sh"
    exit 1
fi

export CM_DIR="${CM_DIR:-${SCRATCH}/${USER}/campaign_manager}"
export DREAMER_DIR="${DREAMER_DIR:-${SCRATCH}/${USER}/radical.dreamer}"
export ENV_DIR="${ENV_DIR:-/u/${USER}/ve/dreamer_campaign}"

unset SLURM_EXPORT_ENV
module load anaconda3 2>/dev/null || true

source "${ENV_DIR}/bin/activate"

# ── Run campaign ──────────────────────────────────────────────────────────────
CAMPAIGN_DIR="${CM_DIR}/campaigns/dreamer_campaign"
cd "${CAMPAIGN_DIR}"

rm -rf dreamer-profiles telemetry-results

python run_campaign.py --config config.yaml

echo "=== Dreamer campaign done: $(date) ==="

rm -rf asyncflow.session*
