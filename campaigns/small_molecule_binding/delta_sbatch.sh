#!/bin/sh -l
#
# Small Molecule Binding Campaign — SLURM batch script (Delta HPC / GPU)
#
# Set before calling sbatch (only SBATCH_ACCOUNT and SCRATCH are required;
# the rest default to the standard Delta locations):
#   export SBATCH_ACCOUNT=<project>-delta-gpu
#   export SCRATCH=/scratch/<allocation>
#
# Example:
#   sbatch delta_sbatch.sh
#
#SBATCH --partition=gpuA40x4
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --gpus-per-node=4
#SBATCH --mem=220G
#SBATCH --time=00:30:00
#SBATCH --job-name=sm_binding_campaign
#SBATCH --mail-user=mg2347@soe.rutgers.edu
#SBATCH --mail-type=ALL
#SBATCH --output=runs/slurm-%j.out
#SBATCH --error=runs/slurm-%j.err
# NOTE: runs/ must exist before sbatch is called.  Create it once with:
#   mkdir -p <campaign_dir>/runs

# ── Sanity checks ─────────────────────────────────────────────────────────────
if [ -z "${SBATCH_ACCOUNT:-}${SLURM_JOB_ACCOUNT:-}" ]; then
    echo "WARNING: SBATCH_ACCOUNT is not set — job may be charged to default account."
fi
echo "Account: ${SLURM_JOB_ACCOUNT:-unknown}"

if [ -z "${SCRATCH:-}" ]; then
    echo "ERROR: SCRATCH is not set."
    echo "       export SCRATCH=/scratch/<allocation> && sbatch delta_sbatch.sh"
    exit 1
fi

# ── Directory and environment setup ──────────────────────────────────────────
export CM_DIR="${CM_DIR:-${SCRATCH}/${USER}/campaign_manager}"
export ENV_DIR="${ENV_DIR:-/u/${USER}/ve/impress}"

# Path to IMPRESS repo root — defaults to the standard Delta scratch location.
export IMPRESS_SRC="${IMPRESS_SRC:-${SCRATCH}/${USER}/IMPRESS}"

# IMPRESS examples dir for the small molecule binding use-case.
export SM_BINDING_EXAMPLES_DIR="${SM_BINDING_EXAMPLES_DIR:-${SCRATCH}/${USER}/IMPRESS/examples/small_molecule_binding}"

unset SLURM_EXPORT_ENV
source "${ENV_DIR}/bin/activate"

# ── Dragon system library paths (Delta-specific) ──────────────────────────────
export CUDA_HOME=/opt/nvidia/hpc_sdk/Linux_x86_64/25.3/cuda/12.8
export MPI_LIB=/opt/cray/pe/mpich/8.1.32/ofi/gnu/11.2/lib-abi-mpich
export FAB_LIB=/opt/cray/libfabric/1.22.0/lib64
export LD_LIBRARY_PATH=${CUDA_HOME}/lib64:${MPI_LIB}:${FAB_LIB}:${LD_LIBRARY_PATH:-}
dragon-config add --ofi-runtime-lib="${FAB_LIB}"

# PYTHONASYNCIODEBUG=1  # disabled: floods .err with per-task subprocess traces

# ── Tool paths (adjust for your allocation) ────────────────────────────────────
# These are read by SmallMoleculeBindingPipeline via kwargs or env vars.
export MPNN_PATH="${MPNN_PATH:-/work/hdd/bdyk/hooten1/LigandMPNN}"
export COLABFOLD_PATH="${COLABFOLD_PATH:-/work/hdd/bdyk/hooten1/localcolabfold}"

# ColabFold model weights cache — kept on scratch to avoid home quota exhaustion.
# Pre-download once on login node:
#   export COLABFOLD_CACHE_DIR=/scratch/bblj/${USER}/.cache/colabfold
#   python -c "from colabfold.download import download_alphafold_params; \
#              download_alphafold_params('alphafold2', '${COLABFOLD_CACHE_DIR}')"
export COLABFOLD_CACHE_DIR="${COLABFOLD_CACHE_DIR:-${SCRATCH}/${USER}/.cache/colabfold}"
mkdir -p "${COLABFOLD_CACHE_DIR}"

export FOUNDRY_SIF_PATH="${FOUNDRY_SIF_PATH:-/work/hdd/bdyk/hooten1/foundry.sif}"
echo "FOUNDRY_SIF_PATH: ${FOUNDRY_SIF_PATH}"

# ── Run campaign ──────────────────────────────────────────────────────────────
CAMPAIGN_DIR="${CM_DIR}/campaigns/small_molecule_binding"
cd "${CAMPAIGN_DIR}"

# -s = single-node Dragon runtime; -m = multi-node (uses MPI/OFI fabric).
if [ "${SLURM_NNODES:-1}" -gt 1 ]; then
    DRAGON_MODE="-m"
else
    DRAGON_MODE="-s"
fi

CONFIG="${1:-config.yaml}"
echo "Running: dragon ${DRAGON_MODE} run_campaign.py --config ${CONFIG}  (nodes=${SLURM_NNODES:-1})"
dragon ${DRAGON_MODE} run_campaign.py --config "${CONFIG}"

echo "=== Small Molecule Binding campaign done: $(date) ==="

rm -rf asyncflow.session*
