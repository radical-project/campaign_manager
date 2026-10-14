#!/bin/bash
#
# Demo Campaign — Delta HPC
#
# Usage:
#   export WORK_DIR=/work/nvme/bdyk/$USER
#   export SBATCH_ACCOUNT=<project>-delta-cpu
#   mkdir -p $WORK_DIR/campaign_manager/campaigns/demo_campaign/logs
#   sbatch campaigns/demo_campaign/delta_sbatch.sh
#
# Optional env overrides:
#   ENGINE=dragon             (default) | concurrent
#   TELEMETRY_DIR=<path>      (default: campaigns/demo_campaign/aucam-tel-<jobid>)
#   GLOBAL_CONFIG=<path>      overrides the YAML chosen by ENGINE
#   CAMPAIGN_CONFIG=<path>    overrides the YAML chosen by ENGINE
#
# Spending limits are set in campaign_hpc.yaml (budget.spending.cpu_hours).
#
# ENGINE=dragon   — launches via Dragon (multi-node DDict backend, asyncflow blocks)
# ENGINE=concurrent — launches via plain Python (in-process asyncio, local dict backend)
#                     Useful for single-node smoke tests on a login or compute node.
#
# To demo rejection: set num_campaigns=3 and lower the global cpu_hours cap to 0.120
# so campaign c (requesting 0.042) exceeds the ledger after a+b complete.
#
#SBATCH --partition=cpu
#SBATCH --nodes=2
#SBATCH --tasks-per-node=4
#SBATCH --cpus-per-task=16
#SBATCH --exclusive
#SBATCH --time=00:30:00
#SBATCH --job-name=aucam_demo
#SBATCH --mail-user=your@email.com
#SBATCH --mail-type=ALL
#SBATCH --output=logs/demo_%j.out
#SBATCH --error=logs/demo_%j.err
#SBATCH --exclude=cn132,cn043

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
echo "Nodes    : ${SLURM_JOB_NUM_NODES:-?} (${SLURM_JOB_NODELIST:-?})"
echo "ENGINE   : ${ENGINE}"

if [ -z "${WORK_DIR:-}" ]; then
    echo "ERROR: WORK_DIR is not set."
    echo "       export WORK_DIR=/work/nvme/bdyk/\$USER && sbatch ..."
    exit 1
fi

# ── Paths ─────────────────────────────────────────────────────────────────────
CM_DIR="${CM_DIR:-${WORK_DIR}/campaign_manager}"
VE="${VE:-${WORK_DIR}/ve/ddsim}"
CAMPAIGN_DIR="${CM_DIR}/campaigns/demo_campaign"
TELEMETRY_DIR="${TELEMETRY_DIR:-${CAMPAIGN_DIR}/aucam-tel-${SLURM_JOB_ID:-local}}"

# Config YAML selection — HPC YAMLs for dragon, local YAMLs for concurrent.
if [ "${ENGINE}" = "dragon" ]; then
    GLOBAL_CONFIG="${GLOBAL_CONFIG:-campaign_configs/global_hpc.yaml}"
    CAMPAIGN_CONFIG="${CAMPAIGN_CONFIG:-campaign_configs/campaign_hpc.yaml}"
else
    GLOBAL_CONFIG="${GLOBAL_CONFIG:-campaign_configs/global.yaml}"
    CAMPAIGN_CONFIG="${CAMPAIGN_CONFIG:-campaign_configs/campaign.yaml}"
fi

mkdir -p "${TELEMETRY_DIR}"

# ── System libraries (required by Dragon; harmless to set for concurrent) ─────
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
echo "Python   : $(python --version)"
echo "asyncflow: $(python -c 'import radical.asyncflow; print(radical.asyncflow.__version__)')"
echo "rhapsody : $(python -c 'import rhapsody; print(rhapsody.__version__)')"
if [ "${ENGINE}" = "dragon" ]; then
    echo "dragon   : $(python -c 'import dragon; print(dragon.__version__)')"
fi
echo ""
echo "TELEMETRY_DIR  : ${TELEMETRY_DIR}"
echo "GLOBAL_CONFIG  : ${GLOBAL_CONFIG}"
echo "CAMPAIGN_CONFIG: ${CAMPAIGN_CONFIG}"
echo ""

# ── Run ───────────────────────────────────────────────────────────────────────
cd "${CAMPAIGN_DIR}"
rm -f ddict_orc* asyncflow.session*.telemetry.jsonl 2>/dev/null || true

COMMON_ARGS=(
    --global-config   "${GLOBAL_CONFIG}"
    --campaign-config "${CAMPAIGN_CONFIG}"
    --telemetry-dir   "${TELEMETRY_DIR}"
    --engine          "${ENGINE}"
    --driver
)

if [ "${ENGINE}" = "dragon" ]; then
    # dragon -m fails on Delta: gethostname() returns FQDNs but SLURM NodeName is short
    # (e.g. gpub081.delta.internal vs gpub081) so srun --nodelist is unsatisfiable.
    # Workaround: generate network config yaml, then launch via SSH + TCP transport.
    if [ "${SLURM_NNODES:-1}" -gt 1 ]; then
        NET_DIR="${TELEMETRY_DIR}/dragon_net"
        mkdir -p "${NET_DIR}"
        (cd "${NET_DIR}" && dragon-network-config --output-to-yaml --no-stdout)
        DRAGON_MODE="-w ssh --network-config ${NET_DIR}/slurm.yaml -t tcp"
        echo "Network config: ${NET_DIR}/slurm.yaml"
    else
        DRAGON_MODE="-s"
    fi
    echo "Running: dragon ${DRAGON_MODE} run_campaign.py ..."
    dragon ${DRAGON_MODE} run_campaign.py "${COMMON_ARGS[@]}" || true
else
    echo "Running: python run_campaign.py --engine concurrent ..."
    python run_campaign.py --engine concurrent "${COMMON_ARGS[@]}" || true
fi

echo ""
echo "=== Demo campaign done (ENGINE=${ENGINE}): $(date) ==="
echo "Telemetry: ${TELEMETRY_DIR}"
ls -lh "${TELEMETRY_DIR}"/asyncflow.session*.telemetry.jsonl 2>/dev/null || echo "(no telemetry file found)"
