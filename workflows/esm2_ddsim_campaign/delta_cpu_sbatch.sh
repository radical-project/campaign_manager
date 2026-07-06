#!/bin/sh -l

# ── Cluster settings (adjust per allocation) ─────────────────────────────────
#SBATCH -A ***-delta-gpu
#SBATCH --partition=gpuA40x4
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --gpus-per-node=4
#SBATCH --time=00:30:00
#SBATCH --job-name=campaign
#xSBATCH --mail-user=${USER}@institution.edu
#SBATCH --mail-user=mariya.goliyad@rutgers.edu
#SBATCH --mail-type=ALL

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

export WORK_DIR=${SPHERICAL_DIR}/workflows/run_campaign

cd ${WORK_DIR}

# ── Clean previous run artifacts ─────────────────────────────────────────────
rm -rf DDMD*

# ── Activate campaign environment and configure Dragon ───────────────────────
source ${VE_HOME}/campaign/bin/activate
dragon-config add --ofi-runtime-lib=${FAB_LIB}

# ── Launch ───────────────────────────────────────────────────────────────────
# Environment variable substitution (${DUMMY_DIR}, ${VE_HOME}, etc.) is handled
# by run_campaing.py at load time — no sed or cp needed.
GPUS_PER_NODE=${SLURM_GPUS_PER_NODE:-1}
export TOTAL_GPUS=$(( SLURM_NNODES * GPUS_PER_NODE ))
echo "Nodes: ${SLURM_NNODES}  GPUs/node: ${GPUS_PER_NODE}  Total GPUs: ${TOTAL_GPUS}"

if [ "${SLURM_NNODES}" -gt 1 ]; then
    dragon -m run_campaing.py --config config.yaml
else
    dragon -s run_campaing.py --config config.yaml
fi
