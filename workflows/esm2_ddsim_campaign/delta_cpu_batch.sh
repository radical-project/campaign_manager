#!/bin/sh -l
#
# SPHERICAL ESM2/DDSim Campaign — SLURM CPU batch script (concurrent backend)
#
# Runs the campaign without Dragon/GPU — useful for functional testing and
# development. Workflows execute via the asyncflow ConcurrentExecutionBackend.
#
#SBATCH -A ***-delta-cpu
#SBATCH --partition=cpu
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --time=00:30:00
#SBATCH --job-name=campaign-cpu
#xSBATCH --mail-user=${USER}@institution.edu
#SBATCH --mail-type=ALL

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

export WORK_DIR=${SPHERICAL_DIR}/workflows/esm2_ddsim_campaign

cd ${WORK_DIR}

# ── Clean previous run artifacts ─────────────────────────────────────────────
rm -rf DDMD* telemetry-results asyncflow.session*

# ── Activate campaign environment ─────────────────────────────────────────────
source ${VE_HOME}/campaign/bin/activate

# ── Launch (concurrent backend, no Dragon) ───────────────────────────────────
python run_campaing.py --config config.yaml --engine concurrent
