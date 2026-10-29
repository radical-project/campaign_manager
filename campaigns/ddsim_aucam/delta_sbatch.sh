#!/bin/bash
#
# DDSim AuCaM Campaign — Delta HPC
#
# Usage:
#   export WORK_DIR=<your_work_dir>
#   export SBATCH_ACCOUNT=<project>-delta-gpu   # or -delta-cpu for Dragon/CPU
#   mkdir -p $WORK_DIR/campaign_manager/campaigns/ddsim_aucam/logs
#   sbatch campaigns/ddsim_aucam/delta_sbatch.sh
#
# ENGINE=dragon (default) uses CPU partition; ENGINE=concurrent uses GPU.
# Run parameters live in campaign_configs/global_hpc.yaml + campaign_hpc.yaml.
#
#SBATCH --partition=gpuA100x4
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --gpus-per-node=1
#SBATCH --time=04:00:00
#SBATCH --job-name=aucam_ddsim
#SBATCH --mail-user=your@email.com
#SBATCH --mail-type=ALL
#SBATCH --output=logs/ddsim_%j.out
#SBATCH --error=logs/ddsim_%j.err

set -euo pipefail

ENGINE="${ENGINE:-dragon}"

if [ "${ENGINE}" = "dragon" ]; then
    trap 'dragon-cleanup -s 2>/dev/null || true' EXIT
fi

# ── Sanity checks ─────────────────────────────────────────────────────────────
if [ -z "${SBATCH_ACCOUNT:-}${SLURM_JOB_ACCOUNT:-}" ]; then
    echo "WARNING: SBATCH_ACCOUNT is not set — job may be charged to default account."
fi
echo "Account  : ${SLURM_JOB_ACCOUNT:-unknown}"
echo "Job ID   : ${SLURM_JOB_ID:-unknown}"
echo "Nodes    : ${SLURM_NNODES:-?} (${SLURM_JOB_NODELIST:-?})"
echo "ENGINE   : ${ENGINE}"

if [ -z "${WORK_DIR:-}" ]; then
    echo "ERROR: WORK_DIR is not set."
    echo "       export WORK_DIR=<your_work_dir> && sbatch ..."
    exit 1
fi

# ── Paths ─────────────────────────────────────────────────────────────────────
CM_DIR="${CM_DIR:-${WORK_DIR}/campaign_manager}"
CAMPAIGN_DIR="${CM_DIR}/campaigns/ddsim_aucam"
# shellcheck source=delta_env_setup.sh
source "${CAMPAIGN_DIR}/delta_env_setup.sh"
if [ ! -d "${VE}" ]; then
    echo "ERROR: VE not found at ${VE}."
    echo "       Run: bash ${CAMPAIGN_DIR}/delta_env_setup.sh"
    exit 1
fi
TELEMETRY_DIR="${TELEMETRY_DIR:-${CAMPAIGN_DIR}/aucam-tel-${SLURM_JOB_ID:-local}}"

# HPC YAMLs on SLURM, dev YAMLs for local runs.
if [ -n "${SLURM_JOB_ID:-}" ]; then
    GLOBAL_CONFIG="${GLOBAL_CONFIG:-campaign_configs/global_hpc.yaml}"
    CAMPAIGN_CONFIG="${CAMPAIGN_CONFIG:-campaign_configs/campaign_hpc.yaml}"
else
    GLOBAL_CONFIG="${GLOBAL_CONFIG:-campaign_configs/global.yaml}"
    CAMPAIGN_CONFIG="${CAMPAIGN_CONFIG:-campaign_configs/campaign.yaml}"
fi

mkdir -p "${TELEMETRY_DIR}"

# ── System libraries ──────────────────────────────────────────────────────────
export CUDA_HOME=/opt/nvidia/hpc_sdk/Linux_x86_64/25.3/cuda/12.8
export MPI_LIB=/opt/cray/pe/mpich/8.1.32/ofi/gnu/11.2/lib-abi-mpich
export FAB_LIB=/opt/cray/libfabric/1.22.0/lib64
export LD_LIBRARY_PATH="${CUDA_HOME}/lib64:${MPI_LIB}:${FAB_LIB}:${LD_LIBRARY_PATH:-}"

# ── Activate venv ─────────────────────────────────────────────────────────────
unset SLURM_EXPORT_ENV
export PYTHONUNBUFFERED=1
source "${VE}/bin/activate"

if [ "${ENGINE}" = "dragon" ]; then
    dragon-config add --ofi-runtime-lib="${FAB_LIB}"
fi

echo ""
echo "Python      : $(python --version)"
echo "asyncflow   : $(python -c 'import radical.asyncflow; print(radical.asyncflow.__version__)')"
echo "rhapsody    : $(python -c 'import rhapsody; print(rhapsody.__version__)')"
if [ "${ENGINE}" = "dragon" ]; then
    echo "dragon      : $(python -c 'import dragon; print(dragon.__version__)')"
fi
echo ""
echo "DDSIM_DIR       : ${DDSIM_DIR}"
echo "MINIAPPS_VE     : ${MINIAPPS_VE}"
echo "TELEMETRY_DIR   : ${TELEMETRY_DIR}"
echo "GLOBAL_CONFIG   : ${GLOBAL_CONFIG}"
echo "CAMPAIGN_CONFIG : ${CAMPAIGN_CONFIG}"
echo ""

# ── Run ───────────────────────────────────────────────────────────────────────
cd "${CAMPAIGN_DIR}"
rm -f ddict_orc* asyncflow.session*.telemetry.jsonl 2>/dev/null || true

COMMON_ARGS=(
    --global-config   "${GLOBAL_CONFIG}"
    --campaign-config "${CAMPAIGN_CONFIG}"
    --telemetry-dir   "${TELEMETRY_DIR}"
)

if [ "${ENGINE}" = "dragon" ]; then
    echo "Running: dragon -s run_campaign.py ..."
    dragon -s run_campaign.py "${COMMON_ARGS[@]}" || true
else
    echo "Running: python run_campaign.py --engine concurrent ..."
    python run_campaign.py --engine concurrent "${COMMON_ARGS[@]}" || true
fi

echo ""
echo "=== DDSim campaign done (ENGINE=${ENGINE}): $(date) ==="
echo "Telemetry : ${TELEMETRY_DIR}"
ls -lh "${TELEMETRY_DIR}"/asyncflow.session*.telemetry.jsonl 2>/dev/null || echo "(no telemetry file found)"
