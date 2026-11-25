#!/bin/bash
#
# ASD prototype campaign (design -> label -> ROME fine-tune -> promote) on Delta.
#
# Usage:
#   export WORK_DIR=<your_work_dir>
#   export SBATCH_ACCOUNT=<project>-delta-gpu
#   export ASD_DESIGN_SPEC=$WORK_DIR/IMPRESS/agent/campaigns/delta-small-molecule.yaml
#   export ASD_LABEL_SPEC=$WORK_DIR/IMPRESS/agent/campaigns/asd-labels-md.yaml
#
# `delta-label.yaml` was named here, in README.md and in run_campaign.py until
# 2026-11-25 and has never existed on any branch (asteer item 35). The label
# campaign is `asd-labels-md.yaml` on IMPRESS `asd-steering`, with
# `asd-labels-md-mock.yaml` as its laptop twin. Using the real one needs
# `label_input_param: md_label.design_dir` and a `score_key` among its three
# objectives; campaign.yaml still carries the design chain's
# `score_designs.input_dir` / `complex_plddt`, which match neither file.
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
# --cpus-per-task IS THE CONCURRENCY CEILING, not a CPU budget. Dragon's Batch
# starts `Node.num_cpus // 2` workers per node, lowered to the physical cores
# the job's affinity actually allows (rhapsody backends/execution/dragon.py,
# _workers_per_node_for_affinity). One never-returning task holds one worker,
# and each resident stream is one. Measured on a gpuA100x4 node, job 24157731:
# 64 CPUs gives 32 workers and 28 free under a long hold; this line said 16,
# which is 8 physical cores and therefore 8 workers, leaving 5 once the three
# surrogate streams are up. A round submitted into a full pool is never placed
# while asyncflow still reports RUNNING -- the probe reproduced that failure on
# purpose at pool size 4. Raised to a whole node 2026-11-22.
#SBATCH --partition=gpuA100x4
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --cpus-per-task=64
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
# Resolved by asteer, not written down here. These three paths were hardcoded
# until 2026-11-25, when all three turned out to name directories Delta had
# removed -- HPC SDK 25.3/CUDA 12.8, MPICH 8.1.32, libfabric 1.22.0 -- and a
# stale FAB_LIB is only ever discovered as a hang, because `dragon-config add`
# does not validate the path it stores. `asteer` is the repo all five
# checkouts already share, so the search and its fatal check live there and
# every launcher sources the one copy.
# shellcheck source=/dev/null
source "${ASTEER_DIR}/site/delta_paths.sh"

# ── Thread limits ─────────────────────────────────────────────────────────────
export OPENBLAS_NUM_THREADS=1
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1

unset SLURM_EXPORT_ENV
# Dragon does not pass --overlap to the steps it issues, so without this its
# steps collide with the batch step at "job step creation temporarily
# disabled". The M0/M2 probes have carried it since job 24400895; this script
# never had it, and has also never run.
export SLURM_OVERLAP=1
export PYTHONUNBUFFERED=1
source "${VE}/bin/activate"

if [ "${ENGINE}" = "dragon" ]; then
    asd_dragon_config_ofi
fi

echo ""
echo "Python     : $(python --version)"
# Origin rather than version, through the helper delta_env_setup.sh defines:
# both middleware placement branches declare 0.7.0, the release's own version,
# so a log line reading `asyncflow : 0.7.0` says nothing about which one ran.
# This log is what a measurement gets written up from, so it has to say.
echo "asyncflow  : $(asd_middleware_origin radical.asyncflow)"
echo "rhapsody   : $(asd_middleware_origin rhapsody-py)"
echo "dragon     : $(asd_middleware_origin dragonhpc)"
echo "impress-a  : $(python -c 'import impress_a; print(getattr(impress_a, "__version__", "installed"))')"
# THE MD STACK, ASKED ON THE COMPUTE NODE. delta_env_setup.sh asks these same
# three questions, but its verification block sits below the sourced-early-
# return, so a job that sources the file prints only the banner -- AMBERHOME,
# which is where tleap was *told* to be rather than where it resolved, and no
# platform list at all. A login node's platform list is not a compute node's:
# CUDA appears only where libcuda does. Asked here, this job log answers
# asteer rows 77 and 82 by itself, which is what M2 is for.
echo "openmm     : $(python -c 'import openmm; print(openmm.__version__)' 2>/dev/null || echo MISSING)"
echo "  platforms: $(python -c 'import openmm; print(", ".join(openmm.Platform.getPlatform(i).getName() for i in range(openmm.Platform.getNumPlatforms())))' 2>/dev/null || echo MISSING)"
echo "tleap      : $(
    if [ -n "${AMBERHOME:-}" ] && [ -x "${AMBERHOME}/bin/tleap" ]; then
        echo "${AMBERHOME}/bin/tleap (from AMBERHOME)"
    elif command -v tleap >/dev/null 2>&1; then
        echo "$(command -v tleap) (from PATH)"
    else
        echo "MISSING -- md_label cannot build a system"
    fi)"
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
