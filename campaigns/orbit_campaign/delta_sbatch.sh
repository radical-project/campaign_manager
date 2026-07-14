#!/bin/sh -l
#
# SPHERICAL Dummy Campaign — SLURM batch script (CPU-only, no GPU needed)
#
# Account: set SBATCH_ACCOUNT=<project>-delta-cpu before calling sbatch
#SBATCH --partition=cpu
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --time=00:30:00
#SBATCH --job-name=orbit_campaign
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
ENV_DIR="${ENV_DIR:-/u/${USER}/ve/orbit_campaign}"

unset SLURM_EXPORT_ENV
module load anaconda3 2>/dev/null || true

source "${ENV_DIR}/bin/activate"

# ── Run campaign ──────────────────────────────────────────────────────────────
CAMPAIGN_DIR="${CM_DIR}/campaigns/orbit_campaign"
cd "${CAMPAIGN_DIR}"

# Set RADICAL_ORBIT_BROKER_URL before submitting to match whichever login node
# hosts the broker (e.g. export RADICAL_ORBIT_BROKER_URL=https://dt-login03...:8020)
if [ -z "${RADICAL_ORBIT_BROKER_URL:-}" ]; then
    echo "ERROR: RADICAL_ORBIT_BROKER_URL is not set."
    echo "       export RADICAL_ORBIT_BROKER_URL=https://<login-node>.delta.ncsa.illinois.edu:8020"
    exit 1
fi
echo "Broker: ${RADICAL_ORBIT_BROKER_URL}"
python run_campaign.py --config config.yaml

echo "=== DDSim campaign done: $(date) ==="

rm -rf asyncflow.session*