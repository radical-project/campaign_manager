#!/bin/sh -l
#
# SPHERICAL ESM2/DDSim Campaign — SLURM GPU batch script (Dragon backend)
#
# GPU stress scenario: inference (priority 9) competes with md (priority 4) for
# 4 A40 GPUs.  inference.cap=4 × required_gpus=1 fills the entire pool under the
# 'none' baseline, starving Pipeline B (md → miniapps) completely.
#
#   --gpus=4  matches config_stress_gpu.yaml resources.total_gpus=4
#             (== inference.concurrency_cap × required_gpus = 4 × 1)
#             Any more GPUs would let md run even under 'none'; any fewer would
#             prevent inference from reaching full concurrency.
#
# Compare policies by changing cm.adr.policy in config_stress_gpu.yaml:
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
#SBATCH --time=01:30:00   # 4 policies × ≤11 min each + overhead (16 inf × 114 s / 3)
#SBATCH --job-name=sphr-stress
#SBATCH --mail-user=mg2347@soe.rutgers.edu
#SBATCH --mail-type=ALL

# HuggingFace token — set before submitting:  export HF_TOKEN=<token> && sbatch ...
[ -z "${HF_TOKEN}" ] && echo "WARNING: HF_TOKEN not set — LLM policy will fail" >&2

# ── Paths ─────────────────────────────────────────────────────────────────────
export SPHERICAL_DIR="/scratch/***/${USER}/SPHERICAL"
export DDSIM_DIR="/scratch/***/${USER}/DeepDriveSim"
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

# ── Environment ───────────────────────────────────────────────────────────────
unset SLURM_EXPORT_ENV
module load anaconda3 2>/dev/null || true

source "${ENV_DIR}/bin/activate"

export CUDA_HOME=/opt/packages/cuda/v12.6.1
export LD_LIBRARY_PATH=$CUDA_HOME/lib64:$LD_LIBRARY_PATH
# miniapps venv lib: contains libmpi.so.12 symlink (→ Cray MPICH) needed by mpi4py.
# Must be in LD_LIBRARY_PATH before Dragon spawns simulation subprocesses.
export LD_LIBRARY_PATH="${VE_HOME}/miniapps/lib:${LD_LIBRARY_PATH}"
export TF_FORCE_GPU_ALLOW_GROWTH=true

# ── Generate per-workflow configs from templates ──────────────────────────────
cp  "${INPUT_DIR}/lassen-keras-dbscan.yaml" "${INPUT_DIR}/new_lassen-keras-dbscan.yaml"
sed -i "s|\${EXPRMNT_DIR}|${EXPRMNT_DIR}|g" "${INPUT_DIR}/new_lassen-keras-dbscan.yaml"
sed -i "s|\${MD_HOME}|${MD_HOME}|g"         "${INPUT_DIR}/new_lassen-keras-dbscan.yaml"
sed -i "s|\${CONDA_ENV}|${VE_HOME}|g"       "${INPUT_DIR}/new_lassen-keras-dbscan.yaml"

# All workflow configs (md, miniapps, dummy, inference) are committed and
# self-templating — env-var refs (${DDSIM_DIR}, etc.) are expanded at load
# time by _expand_env(), so no cp/sed step is needed.

# ── Run campaign ──────────────────────────────────────────────────────────────
CAMPAIGN_DIR="${SPHERICAL_DIR}/workflows/esm2_ddsim_campaign"
cd "${CAMPAIGN_DIR}"

rm -rf DDMD-* telemetry-results nvml-telemetry

# benchmark.py runs all 4 workflows on real GPU hardware via Dragon.
# Primary metric: time_to_first_miniapps_s (lower = better).
# All policies complete all 4 workflows (md.floor=1 guarantees md runs).
# --timeout 1200 is a safety cap; campaigns finish naturally before that.
dragon -s benchmark.py --config config_stress_gpu.yaml \
    --timeout 1200 --policies none rule bandit llm --runs 1 --out benchmark.json

echo "=== Benchmark done: $(date) ==="

# Regenerate plots
python plot_benchmark.py --results benchmark_results.json --out-dir plots

echo "=== Plots written: $(date) ==="

rm -rf asyncflow.session*

# dragon run_campaing.py --config config.yaml