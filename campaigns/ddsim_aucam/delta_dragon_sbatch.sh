#!/bin/bash
#
# DDSim AuCaM Campaign — Dragon run (Delta HPC, CPU)
#
# Parallel driver: submits all NUM_CAMPAIGNS campaigns simultaneously.
# MAX_CONCURRENT controls how many run at once (GlobalBudget cap).
#
# Usage:
#   export WORK_DIR=/work/nvme/bdyk/$USER
#   export SBATCH_ACCOUNT=<project>-delta-cpu
#   mkdir -p $WORK_DIR/campaign_manager/campaigns/ddsim_aucam/logs
#   sbatch campaigns/ddsim_aucam/delta_dragon_sbatch.sh
#
# Optional env overrides:
#   TELEMETRY_DIR=<path>          (default: campaigns/ddsim_aucam/aucam-tel-<jobid>)
#   NUM_CAMPAIGNS=4               (total parallel campaigns; default: 4)
#   MAX_CONCURRENT=2              (max running simultaneously; default: 2)
#   SPENDING_CPU_HOURS=0.150      (per-campaign soft cap; 0 = unlimited)
#
# The driver submits NUM_CAMPAIGNS configs at once, each with a different
# score_delta so they explore different starting conformations in parallel.
# After all finish a cross-campaign comparison table is printed to stderr.
#
#SBATCH --partition=cpu
#SBATCH --nodes=2
#SBATCH --tasks-per-node=4
#SBATCH --cpus-per-task=16
#SBATCH --exclusive
#SBATCH --time=00:45:00
#SBATCH --job-name=ddsim_dragon
#SBATCH --mail-user=mgoliyad@gmail.com
#SBATCH --mail-type=ALL
#SBATCH --output=logs/ddsim_dragon_%j.out
#SBATCH --error=logs/ddsim_dragon_%j.err
#SBATCH --exclude=cn132

set -euo pipefail

trap 'dragon-cleanup -s 2>/dev/null || true' EXIT

# ── Sanity checks ─────────────────────────────────────────────────────────────
if [ -z "${SBATCH_ACCOUNT:-}${SLURM_JOB_ACCOUNT:-}" ]; then
    echo "WARNING: SBATCH_ACCOUNT is not set — job may be charged to default account."
fi
echo "Account  : ${SLURM_JOB_ACCOUNT:-unknown}"
echo "Job ID   : ${SLURM_JOB_ID:-unknown}"
echo "Nodes    : ${SLURM_NNODES:-?} (${SLURM_JOB_NODELIST:-?})"

if [ -z "${WORK_DIR:-}" ]; then
    echo "ERROR: WORK_DIR is not set."
    echo "       export WORK_DIR=/work/nvme/bdyk/\$USER && sbatch ..."
    exit 1
fi

# ── Paths ─────────────────────────────────────────────────────────────────────
CM_DIR="${CM_DIR:-${WORK_DIR}/campaign_manager}"
VE="${VE:-${WORK_DIR}/ve/small_mol}"
CAMPAIGN_DIR="${CM_DIR}/campaigns/ddsim_aucam"
TELEMETRY_DIR="${TELEMETRY_DIR:-${CAMPAIGN_DIR}/aucam-tel-${SLURM_JOB_ID:-local}}"

NUM_CAMPAIGNS="${NUM_CAMPAIGNS:-4}"
MAX_CONCURRENT="${MAX_CONCURRENT:-2}"
SPENDING_CPU_HOURS="${SPENDING_CPU_HOURS:-0.150}"

mkdir -p "${TELEMETRY_DIR}"

# ── System libraries (Dragon on Delta) ───────────────────────────────────────
export CUDA_HOME=/opt/nvidia/hpc_sdk/Linux_x86_64/25.3/cuda/12.8
export MPI_LIB=/opt/cray/pe/mpich/8.1.32/ofi/gnu/11.2/lib-abi-mpich
export FAB_LIB=/opt/cray/libfabric/1.22.0/lib64
export LD_LIBRARY_PATH="${CUDA_HOME}/lib64:${MPI_LIB}:${FAB_LIB}:${LD_LIBRARY_PATH:-}"

# ── Activate venv ─────────────────────────────────────────────────────────────
unset SLURM_EXPORT_ENV
export PYTHONUNBUFFERED=1
source "${VE}/bin/activate"
dragon-config add --ofi-runtime-lib="${FAB_LIB}"

echo ""
echo "Python   : $(python --version)"
echo "asyncflow: $(python -c 'import radical.asyncflow; print(radical.asyncflow.__version__)')"
echo "rhapsody : $(python -c 'import rhapsody; print(rhapsody.__version__)')"
echo "dragon   : $(python -c 'import dragon; print(dragon.__version__)')"
echo ""
echo "TELEMETRY_DIR   : ${TELEMETRY_DIR}"
echo "NUM_CAMPAIGNS   : ${NUM_CAMPAIGNS}"
echo "MAX_CONCURRENT  : ${MAX_CONCURRENT}"
echo "GLOBAL_CONFIG   : config/global_hpc.yaml"
echo "CAMPAIGN_CONFIG : config/campaign_hpc.yaml"
echo ""

# ── Run ───────────────────────────────────────────────────────────────────────
cd "${CAMPAIGN_DIR}"
rm -f ddict_orc* asyncflow.session*.telemetry.jsonl 2>/dev/null || true

if [ "${SLURM_NNODES:-1}" -gt 1 ]; then
    # dragon -m fails on Delta: gethostname() returns FQDNs but SLURM NodeName is short.
    # Workaround: generate network-config yaml and use SSH + TCP transport.
    NET_DIR="${TELEMETRY_DIR}/dragon_net"
    mkdir -p "${NET_DIR}"
    (cd "${NET_DIR}" && dragon-network-config --output-to-yaml --no-stdout)
    DRAGON_MODE="-w ssh --network-config ${NET_DIR}/slurm.yaml -t tcp"
    echo "Network config : ${NET_DIR}/slurm.yaml"
else
    DRAGON_MODE="-s"
fi

echo "Running: dragon ${DRAGON_MODE} run_campaign.py ..."

SPENDING_FLAGS=""
if [ "${SPENDING_CPU_HOURS:-0}" != "0" ]; then
    SPENDING_FLAGS="--spending-cpu-hours ${SPENDING_CPU_HOURS}"
fi

dragon ${DRAGON_MODE} run_campaign.py \
    --global-config   config/global_hpc.yaml \
    --campaign-config config/campaign_hpc.yaml \
    --telemetry-dir   "${TELEMETRY_DIR}" \
    --num-campaigns   "${NUM_CAMPAIGNS}" \
    --max-concurrent-campaigns "${MAX_CONCURRENT}" \
    ${SPENDING_FLAGS} || true

echo ""
echo "=== DDSim Dragon campaign done: $(date) ==="
echo "Telemetry : ${TELEMETRY_DIR}"
ls -lh "${TELEMETRY_DIR}"/asyncflow.session*.telemetry.jsonl 2>/dev/null || echo "(no telemetry file found)"
