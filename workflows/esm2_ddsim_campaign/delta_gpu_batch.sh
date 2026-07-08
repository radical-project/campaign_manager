#!/bin/sh -l
#
# SPHERICAL ESM2/DDSim Campaign — SLURM GPU batch script (Dragon backend)
#
# GPU stress scenario: inference (priority 9) competes with md (priority 4) for
# 4 A40 GPUs.  inference.cap=4 × required_gpus=1 fills the entire pool under the
# 'none' baseline, starving Pipeline B (md → miniapps) completely.
#
#   --gpus=4  matches config.yaml resources.total_gpus=4
#
# Compare policies by changing cm.adr.policy in config.yaml:
#   none   → miniapps=0  (inference monopolizes all 4 GPUs)
#   rule   → miniapps>0  (ADR boosts md into pass-2 GPU allocation)
#   bandit → miniapps>0  (converges within 3–5 cycles)
#   llm    → miniapps>0  (if HF_TOKEN is valid)
#
#SBATCH -A ***-delta-gpu
#SBATCH --partition=gpuA40x4
#SBATCH --nodes=1
#SBATCH --tasks-per-node=4
#SBATCH --cpus-per-task=16
#SBATCH --gpus=4
#SBATCH --exclusive
#SBATCH --export=NONE
#SBATCH --time=01:30:00
#SBATCH --job-name=campaign-gpu
#xSBATCH --mail-user=${USER}@institution.edu
#SBATCH --mail-type=ALL

# HuggingFace token — set before submitting:  export HF_TOKEN=<token> && sbatch ...
[ -z "${HF_TOKEN}" ] && echo "WARNING: HF_TOKEN not set — LLM policy will fail" >&2

# ── System library paths (Delta-specific) ────────────────────────────────────
export CUDA_HOME=/opt/nvidia/hpc_sdk/Linux_x86_64/25.3/cuda/12.8
export MPI_LIB=/opt/cray/pe/mpich/8.1.32/ofi/gnu/11.2/lib-abi-mpich
export FAB_LIB=/opt/cray/libfabric/1.22.0/lib64
export LD_LIBRARY_PATH=${CUDA_HOME}/lib64:${MPI_LIB}:${FAB_LIB}:${LD_LIBRARY_PATH}

export TF_FORCE_GPU_ALLOW_GROWTH=true
export JAX_PLATFORMS=cpu

# ── Project paths (adjust base dirs if layout differs) ───────────────────────
export SPHERICAL_DIR=/scratch/***/${USER}/SPHERICAL
export DDSIM_DIR=/scratch/***/${USER}/DeepDriveSim
export VE_HOME=/u/${USER}/ve

export MD_DIR=${DDSIM_DIR}/workflows/ddmd_workflow
export MINAPPS_DIR=${DDSIM_DIR}/workflows/miniapps_workflow
export DUMMY_DIR=${DDSIM_DIR}/workflows/dummy_workflow
export INF_DIR=${SPHERICAL_DIR}/workflows/esm2_inference

export MD_HOME=${DDSIM_DIR}/workflows/ddmd_workflow
export MD_INPUT=${MD_HOME}/data
export SGDES_DIR=/scratch/***/${USER}/SGDES

export WORK_DIR=${SPHERICAL_DIR}/workflows/esm2_ddsim_campaign

cd ${WORK_DIR}

# ── Clean previous run artifacts ─────────────────────────────────────────────
rm -rf DDMD* telemetry-results nvml-telemetry asyncflow.session*

# ── Activate campaign environment and configure Dragon ───────────────────────
source ${VE_HOME}/campaign/bin/activate
dragon-config add --ofi-runtime-lib=${FAB_LIB}

# ── Launch ───────────────────────────────────────────────────────────────────
GPUS_PER_NODE=${SLURM_GPUS_PER_NODE:-4}
export TOTAL_GPUS=$(( SLURM_NNODES * GPUS_PER_NODE ))
echo "Nodes: ${SLURM_NNODES}  GPUs/node: ${GPUS_PER_NODE}  Total GPUs: ${TOTAL_GPUS}"

if [ "${SLURM_NNODES}" -gt 1 ]; then
    dragon -m benchmark.py --config config.yaml \
        --timeout 1200 --policies none rule bandit llm --runs 1 --out benchmark.json
else
    dragon -s benchmark.py --config config.yaml \
        --timeout 1200 --policies none rule bandit llm --runs 1 --out benchmark.json
fi

echo "=== Benchmark done: $(date) ==="

python plot_benchmark.py --results benchmark.json --out-dir plots

echo "=== Plots written: $(date) ==="
