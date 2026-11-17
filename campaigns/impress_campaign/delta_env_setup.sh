#!/bin/bash
# Create and configure the impress_campaign VE on Delta HPC.
#
# Run once to create the VE:
#   export WORK_DIR=<your_work_dir>
#   bash campaigns/impress_campaign/delta_env_setup.sh
#
# Sourced by delta_sbatch.sh to set path variables only (no VE creation).
# All paths can be overridden by exporting the variable before calling this script.
#
# Repos required under WORK_DIR (clone if missing):
#   IMPRESS   — github.com/stride-research/IMPRESS

: "${WORK_DIR:?WORK_DIR must be set}"

VE="${VE:-${WORK_DIR}/ve/impress_campaign}"
IMPRESS_DIR="${IMPRESS_DIR:-${WORK_DIR}/IMPRESS}"
export VE IMPRESS_DIR

# mpi4py needs libmpi.so at runtime; Cray MPICH sets MPICH_DIR but not LD_LIBRARY_PATH.
if [ -n "${MPICH_DIR}" ]; then
    export LD_LIBRARY_PATH="${MPICH_DIR}/lib:${LD_LIBRARY_PATH:-}"
fi

# Prevent OpenBLAS/OMP from spawning 64 threads per task under concurrency.
export OPENBLAS_NUM_THREADS=1
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1

echo "VE           : ${VE}"
echo "IMPRESS_DIR  : ${IMPRESS_DIR}"

# When sourced: activate the VE and return (no VE creation).
# When executed directly: fall through to VE creation.
[[ "${BASH_SOURCE[0]}" != "${0}" ]] && source "${VE}/bin/activate" && return 0

if [ -d "${VE}" ]; then
    echo "VE already exists — skipping creation."
    exit 0
fi

if [ ! -d "${IMPRESS_DIR}" ]; then
    echo "ERROR: IMPRESS_DIR not found: ${IMPRESS_DIR}"
    echo "       Clone it first: git clone <impress-repo> ${IMPRESS_DIR}"
    exit 1
fi

CM_DIR="${CM_DIR:-${WORK_DIR}/campaign_manager}"

echo ""
echo "Creating VE at ${VE} ..."
module load python/3.13.5-gcc13.3.1
# --copies so ${VE}/bin/python is a real binary Dragon can execute.
python3 -m venv --copies "${VE}"
source "${VE}/bin/activate"

pip install --upgrade pip --quiet
pip install --quiet -e "${CM_DIR}[dragon,telemetry]"
pip install --quiet -e "${IMPRESS_DIR}/agent[aucam,dragon,telemetry]"

echo ""
echo "=== VE ready ==="
echo "Python      : $(python --version)"
echo "asyncflow   : $(python -c 'import radical.asyncflow; print(radical.asyncflow.__version__)')"
echo "rhapsody    : $(python -c 'import rhapsody; print(rhapsody.__version__)')"
echo "dragon      : $(python -c 'import dragon; print(dragon.__version__)')"
echo "impress-a   : $(python -c 'import impress_a; print(getattr(impress_a, "__version__", "installed"))')"
