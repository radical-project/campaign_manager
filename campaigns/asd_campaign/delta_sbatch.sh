#!/bin/bash
#
# ASD prototype campaign (design -> label -> ROME fine-tune -> promote) on Delta.
#
# Usage:
#   export WORK_DIR=<your_work_dir>
#   export SBATCH_ACCOUNT=<project>-delta-gpu
#   export ASD_DESIGN_SPEC=$WORK_DIR/IMPRESS/agent/campaigns/delta-small-molecule.yaml
#   export ASD_LABEL_SPEC=$WORK_DIR/IMPRESS/agent/campaigns/delta-label.yaml
#   mkdir -p $WORK_DIR/campaign_manager/campaigns/asd_campaign/logs
#   sbatch campaigns/asd_campaign/delta_sbatch.sh
#
# A CPU smoke run of the same control flow, no GPU and no Dragon:
#   export SBATCH_ACCOUNT=<project>-delta-cpu
#   export ASD_DESIGN_SPEC=$WORK_DIR/IMPRESS/agent/campaigns/mock-small-molecule-binding.yaml
#   export ASD_LABEL_SPEC=$ASD_DESIGN_SPEC
#   export ENGINE=concurrent TRAIN_IMPL=fake CONFIG=campaign_configs/campaign_smoke.yaml
#   sbatch --partition=cpu --gpus-per-node=0 campaigns/asd_campaign/delta_sbatch.sh
#
# ENGINE=dragon     → dragon -s run_campaign.py   (GPU partition; ROME needs Dragon)
# ENGINE=concurrent → python run_campaign.py      (CPU partition; TRAIN_IMPL=fake)
#
#SBATCH --partition=gpuA100x4
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --gpus-per-node=4
#SBATCH --time=04:00:00
#SBATCH --job-name=asd_campaign
#SBATCH --output=logs/asd_%j.out
#SBATCH --error=logs/asd_%j.err

set -euo pipefail

ENGINE="${ENGINE:-dragon}"
TRAIN_IMPL="${TRAIN_IMPL:-rome}"

if [ "${ENGINE}" = "dragon" ]; then
    # After every Dragon program, including a crash or a timeout: leftovers stop
    # the next run from starting.  Either name may be the one this install ships.
    trap 'dragon-cleanup-deprecated 2>/dev/null || dragon-cleanup -s 2>/dev/null || true' EXIT
fi

# ── Sanity checks ─────────────────────────────────────────────────────────────
if [ -z "${SBATCH_ACCOUNT:-}${SLURM_JOB_ACCOUNT:-}" ]; then
    echo "WARNING: SBATCH_ACCOUNT is not set — the job may be charged to the default account."
fi
echo "Account : ${SLURM_JOB_ACCOUNT:-unknown}"
echo "Job ID  : ${SLURM_JOB_ID:-unknown}"
echo "Nodes   : ${SLURM_NNODES:-?} (${SLURM_JOB_NODELIST:-?})"
echo "ENGINE  : ${ENGINE}   TRAIN_IMPL: ${TRAIN_IMPL}"

: "${WORK_DIR:?WORK_DIR must be set — export WORK_DIR=<your_work_dir>}"
: "${ASD_DESIGN_SPEC:?ASD_DESIGN_SPEC must be set (an IMPRESS-A design YAML)}"
: "${ASD_LABEL_SPEC:?ASD_LABEL_SPEC must be set (an IMPRESS-A label YAML)}"

if [ "${ENGINE}" != "dragon" ] && [ "${TRAIN_IMPL}" = "rome" ]; then
    echo "ERROR: TRAIN_IMPL=rome needs ENGINE=dragon — 'import rome' requires the"
    echo "       Dragon runtime.  Use TRAIN_IMPL=fake for a non-Dragon run."
    exit 1
fi

# ── Paths ─────────────────────────────────────────────────────────────────────
CM_DIR="${CM_DIR:-${WORK_DIR}/campaign_manager}"
CAMPAIGN_DIR="${CM_DIR}/campaigns/asd_campaign"
# shellcheck source=delta_env_setup.sh
source "${CAMPAIGN_DIR}/delta_env_setup.sh"
if [ ! -d "${VE}" ]; then
    echo "ERROR: VE not found at ${VE}.  Run: bash ${CAMPAIGN_DIR}/delta_env_setup.sh"
    exit 1
fi

# Everything the campaign writes, on the shared filesystem: the corpus is the
# handoff between workflows and the checkpoints are read by a later launch, so
# neither may live in a node-local directory.
ASD_ROOT="${ASD_ROOT:-${WORK_DIR}/asd-runs/${SLURM_JOB_ID:-local}}"
RUN_ROOT="${ASD_ROOT}/runs"
CORPUS="${CORPUS:-${ASD_ROOT}/corpus}"          # keep across jobs to carry records over
CHECKPOINTS="${CHECKPOINTS:-${ASD_ROOT}/checkpoints}"
AUCAM_CKPT="${ASD_ROOT}/aucam_ckpt"
CONFIG="${CONFIG:-campaign_configs/campaign.yaml}"
mkdir -p "${RUN_ROOT}" "${CORPUS}" "${CHECKPOINTS}" "${AUCAM_CKPT}"

# ── System libraries ──────────────────────────────────────────────────────────
export CUDA_HOME=/opt/nvidia/hpc_sdk/Linux_x86_64/25.3/cuda/12.8
export MPI_LIB=/opt/cray/pe/mpich/8.1.32/ofi/gnu/11.2/lib-abi-mpich
export FAB_LIB=/opt/cray/libfabric/1.22.0/lib64
export LD_LIBRARY_PATH="${CUDA_HOME}/lib64:${MPI_LIB}:${FAB_LIB}:${LD_LIBRARY_PATH:-}"

# ── Thread limits ─────────────────────────────────────────────────────────────
export OPENBLAS_NUM_THREADS=1
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1

unset SLURM_EXPORT_ENV
export PYTHONUNBUFFERED=1
source "${VE}/bin/activate"

if [ "${ENGINE}" = "dragon" ]; then
    dragon-config add --ofi-runtime-lib="${FAB_LIB}"
fi

echo ""
echo "Python     : $(python --version)"
echo "asyncflow  : $(python -c 'import radical.asyncflow; print(radical.asyncflow.__version__)')"
echo "impress-a  : $(python -c 'import impress_a; print(getattr(impress_a, "__version__", "installed"))')"
echo ""
echo "DESIGN_SPEC : ${ASD_DESIGN_SPEC}"
echo "LABEL_SPEC  : ${ASD_LABEL_SPEC}"
echo "CONFIG      : ${CONFIG}"
echo "ASD_ROOT    : ${ASD_ROOT}"
echo "CORPUS      : ${CORPUS}  ($(ls "${CORPUS}"/records.jsonl 2>/dev/null | wc -l) existing record file(s))"
echo ""

# ── Run ───────────────────────────────────────────────────────────────────────
cd "${CAMPAIGN_DIR}"
rm -f ddict_orc* asyncflow.session*.telemetry.jsonl 2>/dev/null || true

ARGS=(
    --design-spec    "${ASD_DESIGN_SPEC}"
    --label-spec     "${ASD_LABEL_SPEC}"
    --config         "${CONFIG}"
    --run-root       "${RUN_ROOT}"
    --corpus         "${CORPUS}"
    --checkpoints    "${CHECKPOINTS}"
    --checkpoint-dir "${AUCAM_CKPT}"
    --train-impl     "${TRAIN_IMPL}"
    --name           "asd-${SLURM_JOB_ID:-local}"
    --verbose
)

# `|| true`: the summary below is worth printing even when the campaign ends badly.
if [ "${ENGINE}" = "dragon" ]; then
    echo "Running: dragon -s run_campaign.py ..."
    dragon -s run_campaign.py "${ARGS[@]}" || true
else
    echo "Running: python run_campaign.py ..."
    python run_campaign.py "${ARGS[@]}" || true
fi

echo ""
echo "=== ASD campaign done (ENGINE=${ENGINE}): $(date) ==="
echo "Records  : $(wc -l < "${CORPUS}/records.jsonl" 2>/dev/null || echo 0) labelled design(s)"
echo "Consumed : $(python -c "
import json, pathlib
p = pathlib.Path('${CORPUS}/records.consumed')
print(len(json.loads(p.read_text())) if p.exists() else 0)
" 2>/dev/null || echo '?') already trained on"
echo "Checkpoints:"
find "${CHECKPOINTS}" -name 'checkpoint*' -o -name 'train_complete' 2>/dev/null | sort | tail -10
echo "Runs     : ${RUN_ROOT}"
