#!/bin/sh -l
#
# SPHERICAL ESM2/DDSim Campaign — SLURM GPU batch script (Dragon backend)
#
# Telemetry benchmark: rule vs rule_telemetry.
#   inference: priority=6, replicas=16, cap=4 → 4 running + 12 queued throughout
#   miniapps:  priority=5, triggered by inference (1 per result, first 4); cap=4
#   rule:           mini=102 > inf=101 (margin=1); batch-dispatch can occasionally
#                   allocate extra slots to inference → miniapps slightly delayed
#   rule_telemetry: mini=106 > inf=101 (margin=5+); miniapps wins every slot contest
#                   → time_to_last_miniapps reliably lower than rule
#
# GPU warm-up bias fix:
#   A discarded warmup run fires before timing starts (--no-warmup to skip).
#   Runs are interleaved (rule/rule_telemetry alternate each round) so both
#   conditions see equivalent GPU thermal state (--no-interleave for block order).
#
# Account: set SBATCH_ACCOUNT=<project>-delta-gpu before calling sbatch
#SBATCH --partition=gpuA40x4
#SBATCH --nodes=1
#SBATCH --tasks-per-node=4
#SBATCH --cpus-per-task=16
#SBATCH --gpus=4
#SBATCH --exclusive
#SBATCH --time=02:00:00
#SBATCH --job-name=campaign-gpu-tel
#xSBATCH --mail-user=${USER}@institution.edu
#SBATCH --mail-user=mg2347@soe.rutgers.edu
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
export TF_CPP_MIN_LOG_LEVEL=3   # suppress TF/XLA C++ log noise (cuInit probe at import time)

# ── Environment ───────────────────────────────────────────────────────────────
if [ -z "${SBATCH_ACCOUNT:-}${SLURM_JOB_ACCOUNT:-}" ]; then
    echo "WARNING: SBATCH_ACCOUNT is not set — job may be charged to default account."
    echo "         Set it with: export SBATCH_ACCOUNT=<project>-delta-gpu"
fi
echo "Account: ${SLURM_JOB_ACCOUNT:-unknown}"

if [ -z "${SCRATCH:-}" ]; then
    echo "ERROR: SCRATCH is not set."
    echo "       export SCRATCH=/scratch/<allocation> && sbatch delta_gpu_batch.sh"
    exit 1
fi

# ── Project paths (adjust base dirs if layout differs) ───────────────────────
export CM_DIR="${CM_DIR:-${SCRATCH}/${USER}/campaign_manager}"
export DDSIM_DIR="${DDSIM_DIR:-${SCRATCH}/${USER}/DeepDriveSim}"
export SPHERICAL_DIR="${SPHERICAL_DIR:-${SCRATCH}/${USER}/SPHERICAL}"
export VE_HOME=/u/${USER}/ve

export MD_DIR=${DDSIM_DIR}/workflows/ddmd_workflow
export MINAPPS_DIR=${DDSIM_DIR}/workflows/miniapps_workflow
export DUMMY_DIR=${DDSIM_DIR}/workflows/dummy_workflow
export INF_DIR=${SPHERICAL_DIR}/workflows/esm2_inference

export MD_HOME=${DDSIM_DIR}/workflows/ddmd_workflow
export MD_INPUT=${MD_HOME}/data
export CONDA_ENV=${VE_HOME}   # on Delta venvs live under VE_HOME; ddmd-openmm/ddmd-keras used by ddsim config
export SGDES_DIR="${SGDES_DIR:-${SCRATCH}/${USER}/SGDES}"

export WORK_DIR=${CM_DIR}/campaigns/esm2_ddsim_campaign

cd ${WORK_DIR}

# ── Clean previous run artifacts ─────────────────────────────────────────────
rm -rf DDMD* telemetry-results nvml-telemetry asyncflow.session*

# ── Activate campaign environment and configure Dragon ───────────────────────
source ${VE_HOME}/esm2_ddsim_campaign/bin/activate
dragon-config add --ofi-runtime-lib=${FAB_LIB}

# ── Launch ───────────────────────────────────────────────────────────────────
GPUS_PER_NODE=${SLURM_GPUS_PER_NODE:-4}
export TOTAL_GPUS=$(( SLURM_NNODES * GPUS_PER_NODE ))
echo "Nodes: ${SLURM_NNODES}  GPUs/node: ${GPUS_PER_NODE}  Total GPUs: ${TOTAL_GPUS}"

if [ "${SLURM_NNODES}" -gt 1 ]; then
    dragon -m benchmark_telemetry.py --config config.yaml \
        --runs 4 --timeout 1100 --out telemetry_benchmark_results.json
else
    dragon -s benchmark_telemetry.py --config config.yaml \
        --runs 4 --timeout 1100 --out telemetry_benchmark_results.json
fi

echo "=== Telemetry benchmark done: $(date) ==="

# Original policy comparison benchmark (uncomment to run instead):
# dragon -s benchmark.py --config config_stress_gpu.yaml \
#     --timeout 1200 --policies none rule bandit llm --runs 1 --out benchmark.json
