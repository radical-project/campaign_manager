#!/bin/sh -l

#SBATCH -A ***-delta-gpu
#SBATCH --partition=gpuA40x4
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --gpus-per-node=4
#SBATCH --time=00:30:00
#SBATCH --job-name=esm2_inf
#SBATCH --mail-user=mariya.goliyad@rutgers.edu
#SBATCH --mail-type=ALL

export CUDA_HOME=/opt/nvidia/hpc_sdk/Linux_x86_64/25.3/cuda/12.8
export LD_LIBRARY_PATH=$CUDA_HOME/lib64:$LD_LIBRARY_PATH

export TF_FORCE_GPU_ALLOW_GROWTH=true

export SPHERICAL_DIR=/scratch/***/${USER}/SPHERICAL
export WORK_DIR=${SPHERICAL_DIR}/workflows/esm2_inference

export VE_HOME=/u/${USER}/ve
cd ${WORK_DIR}

# ── Clean previous run artifacts ──────────────────────────────────────────────
rm -rf data/outputs_test

source ${VE_HOME}/esm2/bin/activate
dragon-config add --ofi-runtime-lib=/opt/cray/libfabric/1.22.0/lib64

# Compute total GPUs and choose single- vs multi-node Dragon launch.
GPUS_PER_NODE=${SLURM_GPUS_PER_NODE:-1}
export TOTAL_GPUS=$(( SLURM_NNODES * GPUS_PER_NODE ))
echo "Nodes: ${SLURM_NNODES}  GPUs/node: ${GPUS_PER_NODE}  Total GPUs: ${TOTAL_GPUS}"

if [ "${SLURM_NNODES}" -gt 1 ]; then
    dragon -m run_esm2_infern.py --config_file config.yaml
else
    dragon -s run_esm2_infern.py --config_file config.yaml
fi
