#!/bin/sh -l
#
# SPHERICAL ESM2/DDSim Campaign — SLURM CPU batch script (local/concurrent backend)
#
#SBATCH -A bblj-delta-cpu
#SBATCH --partition=cpu
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --time=04:25:00
#SBATCH --job-name=spher
#SBATCH --mail-user=mariya.goliyad@rutgers.edu
#SBATCH --mail-type=ALL

export HF_TOKEN="${HF_TOKEN}"

# ── Paths ─────────────────────────────────────────────────────────────────────
export SPHERICAL_DIR="/scratch/bblj/${USER}/SPHERICAL"
export DDSIM_DIR="/scratch/bblj/${USER}/DeepDriveSim"
export ENV_DIR="/u/${USER}/ve/campaign"
export VE_HOME="/u/${USER}/ve"

export WORK_DIR="${DDSIM_DIR}/workflows/ddmd_workflow"
export INPUT_DIR="${WORK_DIR}/data"
export DUMMY_DIR="${DDSIM_DIR}/workflows/dummy_workflow"
export INF_DIR="${SPHERICAL_DIR}/workflows/esm2_inference"
export MINAPPS_DIR="${DDSIM_DIR}/workflows/miniapps_workflow"
export MD_DIR="${WORK_DIR}"
export MD_HOME="${WORK_DIR}"
export MD_INPUT="${WORK_DIR}/data"

#WARNING: this directory has to be empty before running new experiment!
export EXPRMNT_DIR="${WORK_DIR}/ddmd_test_experiments"
rm -rf "${EXPRMNT_DIR}"

# ── Generate per-workflow configs from templates ──────────────────────────────
cp  "${INPUT_DIR}/lassen-keras-dbscan.yaml" "${INPUT_DIR}/new_lassen-keras-dbscan.yaml"
sed -i "s|\${EXPRMNT_DIR}|${EXPRMNT_DIR}|g" "${INPUT_DIR}/new_lassen-keras-dbscan.yaml"
sed -i "s|\${MD_HOME}|${MD_HOME}|g"         "${INPUT_DIR}/new_lassen-keras-dbscan.yaml"
sed -i "s|\${CONDA_ENV}|${VE_HOME}|g"       "${INPUT_DIR}/new_lassen-keras-dbscan.yaml"

# ── Environment ───────────────────────────────────────────────────────────────
unset SLURM_EXPORT_ENV
module load anaconda3 2>/dev/null || true

source "${ENV_DIR}/bin/activate"

export LD_LIBRARY_PATH=${CUDA_HOME:+$CUDA_HOME/lib64:}$LD_LIBRARY_PATH

# ── Run campaign ──────────────────────────────────────────────────────────────
CAMPAIGN_DIR="${SPHERICAL_DIR}/workflows/run_campaign/esm2_ddsim_campaign"
cd "${CAMPAIGN_DIR}"

rm -rf DDMD-* telemetry-results nvml-telemetry

#python run_campaing.py --config config.yaml --engine concurrent


python benchmark_adr.py --config config_stress.yaml --mode deadline-yield --deadline 90 \
    --policies  none rule bandit llm --runs 1 --out benchmark_adr_cpu.json