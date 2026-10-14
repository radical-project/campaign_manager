#!/bin/bash
# Run Dragon multi-node campaign in an interactive SLURM allocation.
# Usage: bash run_dragon_interactive.sh
#
# Start an interactive job first, e.g.:
#   salloc --partition=cpu --nodes=2 --ntasks=2 --account=<account> --time=00:15:00

set -euo pipefail

WORK_DIR="${WORK_DIR:-/work/nvme/bdyk/mgoliyad1}"
VE="${VE:-${WORK_DIR}/ve/small_mol}"
CAMPAIGN_DIR="${WORK_DIR}/campaign_manager/campaigns/dummy_campaign"
TELEMETRY_DIR="${TELEMETRY_DIR:-${CAMPAIGN_DIR}/aucam-tel}"
GOAL_ANALYSES="${GOAL_ANALYSES:-5}"
MAX_CONCURRENT="${MAX_CONCURRENT:-4}"

export CUDA_HOME=/opt/nvidia/hpc_sdk/Linux_x86_64/25.3/cuda/12.8
export MPI_LIB=/opt/cray/pe/mpich/8.1.32/ofi/gnu/11.2/lib-abi-mpich
export FAB_LIB=/opt/cray/libfabric/1.22.0/lib64
export LD_LIBRARY_PATH="${CUDA_HOME}/lib64:${MPI_LIB}:${FAB_LIB}:${LD_LIBRARY_PATH:-}"

unset SLURM_EXPORT_ENV
export PYTHONUNBUFFERED=1
source "${VE}/bin/activate"
dragon-config add --ofi-runtime-lib="${FAB_LIB}"

echo "Nodes : ${SLURM_JOB_NUM_NODES:-?} (${SLURM_JOB_NODELIST:-?})"
echo "dragon: $(python -c 'import dragon; print(dragon.__version__)')"

mkdir -p "${TELEMETRY_DIR}"
cd "${CAMPAIGN_DIR}"

if [ "${SLURM_NNODES:-1}" -gt 1 ]; then
    NET_DIR="/tmp/dragon_net_${SLURM_JOB_ID}"
    mkdir -p "${NET_DIR}"
    # --output-to-yaml writes <wlm>.yaml in cwd; --no-stdout suppresses the JSON dump
    (cd "${NET_DIR}" && dragon-network-config --output-to-yaml --no-stdout)
    DRAGON_MODE="-w ssh --network-config ${NET_DIR}/slurm.yaml -t tcp"
else
    DRAGON_MODE="-s"
fi

echo "Running: dragon ${DRAGON_MODE} ..."

dragon ${DRAGON_MODE} run_campaign.py \
    --engine dragon \
    --asyncflow-blocks \
    --telemetry-dir "${TELEMETRY_DIR}" \
    --goal-analyses "${GOAL_ANALYSES}" \
    --max-concurrent "${MAX_CONCURRENT}"
