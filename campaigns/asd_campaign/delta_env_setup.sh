#!/bin/bash
# Create and configure the asd_campaign VE on Delta HPC.
#
# Run once to create the VE:
#   export WORK_DIR=<your_work_dir>
#   bash campaigns/asd_campaign/delta_env_setup.sh
#
# Sourced by delta_sbatch.sh to set path variables and activate (no creation).
# Any path can be overridden by exporting it first.
#
# Four repos are required under WORK_DIR (clone if missing):
#   campaign_manager — AuCaM (this repo)
#   IMPRESS          — the design and label campaigns (agent/)
#   ROME             — the fine-tuning half
#   autoagent_steered_diffusion — `asteer`, the contracts the three agree on
#
# asteer declares no dependencies, so installing it costs nothing but the path.
# It is installed from a path rather than pinned to a commit for now; the pin
# waits on that repo being reachable from CI.
#
# ROME is installed with --no-deps by default: the prototype trains with
# ROME's DummyTrainer, which needs only numpy/asyncflow/Dragon (already here),
# while ROME's declared dependencies pull torch, transformers, trl, peft,
# pandas and pyarrow — gigabytes for a round that sleeps. Export ROME_FULL=1
# when switching to a real trainer.

: "${WORK_DIR:?WORK_DIR must be set}"

VE="${VE:-${WORK_DIR}/ve/asd_campaign}"
CM_DIR="${CM_DIR:-${WORK_DIR}/campaign_manager}"
IMPRESS_DIR="${IMPRESS_DIR:-${WORK_DIR}/IMPRESS}"
ROME_DIR="${ROME_DIR:-${WORK_DIR}/ROME}"
ASTEER_DIR="${ASTEER_DIR:-${WORK_DIR}/autoagent_steered_diffusion}"
export VE CM_DIR IMPRESS_DIR ROME_DIR ASTEER_DIR

# mpi4py needs libmpi.so at runtime; Cray MPICH sets MPICH_DIR but not LD_LIBRARY_PATH.
if [ -n "${MPICH_DIR:-}" ]; then
    export LD_LIBRARY_PATH="${MPICH_DIR}/lib:${LD_LIBRARY_PATH:-}"
fi

# Prevent OpenBLAS/OMP from spawning a thread per core in every task.
export OPENBLAS_NUM_THREADS=1
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1

echo "VE          : ${VE}"
echo "CM_DIR      : ${CM_DIR}"
echo "IMPRESS_DIR : ${IMPRESS_DIR}"
echo "ROME_DIR    : ${ROME_DIR}"
echo "ASTEER_DIR  : ${ASTEER_DIR}"

# Sourced: activate and return.  Executed: fall through and build the VE.
[[ "${BASH_SOURCE[0]}" != "${0}" ]] && source "${VE}/bin/activate" && return 0

if [ -d "${VE}" ]; then
    echo "VE already exists — skipping creation."
    exit 0
fi

for dir in "${CM_DIR}" "${IMPRESS_DIR}" "${ROME_DIR}" "${ASTEER_DIR}"; do
    if [ ! -d "${dir}" ]; then
        echo "ERROR: not found: ${dir} — clone it under ${WORK_DIR} first."
        exit 1
    fi
done

echo ""
echo "Creating VE at ${VE} ..."
module load python/3.13.5-gcc13.3.1
# --copies so ${VE}/bin/python is a real binary Dragon can execute.
python3 -m venv --copies "${VE}"
source "${VE}/bin/activate"

pip install --upgrade pip --quiet
pip install --quiet -e "${CM_DIR}[dragon,telemetry]"
pip install --quiet -e "${IMPRESS_DIR}/agent[aucam,dragon,telemetry]"
pip install --quiet -e "${ASTEER_DIR}"
if [ "${ROME_FULL:-0}" = "1" ]; then
    echo "Installing ROME with its full dependency set (torch, transformers, trl) ..."
    pip install --quiet -e "${ROME_DIR}"
else
    pip install --quiet --no-deps -e "${ROME_DIR}"
fi

echo ""
echo "=== VE ready ==="
echo "Python      : $(python --version)"
echo "asyncflow   : $(python -c 'import radical.asyncflow; print(radical.asyncflow.__version__)')"
echo "rhapsody    : $(python -c 'import rhapsody; print(rhapsody.__version__)')"
echo "dragon      : $(python -c 'import dragon; print(dragon.__version__)')"
echo "impress-a   : $(python -c 'import impress_a; print(getattr(impress_a, "__version__", "installed"))')"
# `import rome` needs the Dragon runtime, so it is checked inside the job, not here.
echo "rome        : $(python -c 'import importlib.util; print("importable" if importlib.util.find_spec("rome") else "MISSING")')"
