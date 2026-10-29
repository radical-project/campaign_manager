#!/bin/bash
# Create and configure the ddsim_aucam VE on Delta HPC.
#
# Run once to create the VE:
#   export WORK_DIR=<your_work_dir>
#   bash campaigns/ddsim_aucam/delta_env_setup.sh
#
# Sourced by delta_sbatch.sh to set path variables only (no VE creation).
# All paths can be overridden by exporting the variable before calling this script.
#
# Repos required under WORK_DIR (clone if missing):
#   DeepDriveSim        — github.com/radical-project/DeepDriveSim
#   workflow-mini-apps  — github.com/radical-cybertools/workflow-mini-apps

: "${WORK_DIR:?WORK_DIR must be set}"

VE="${VE:-${WORK_DIR}/ve/ddsim_aucam}"
DDSIM_DIR="${DDSIM_DIR:-${WORK_DIR}/DeepDriveSim}"
# VE is created with --copies so ${VE}/bin/python is a real binary Dragon can execute.
MINIAPPS_PYTHON="${MINIAPPS_PYTHON:-${VE}/bin/python}"
export VE DDSIM_DIR MINIAPPS_PYTHON

# mpi4py needs libmpi.so at runtime; Cray MPICH sets MPICH_DIR but not LD_LIBRARY_PATH.
if [ -n "${MPICH_DIR}" ]; then
    export LD_LIBRARY_PATH="${MPICH_DIR}/lib:${LD_LIBRARY_PATH:-}"
fi

# Prevent OpenBLAS/OMP from spawning 64 threads per sim task under concurrency.
export OPENBLAS_NUM_THREADS=1
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1

echo "VE           : ${VE}"
echo "DDSIM_DIR    : ${DDSIM_DIR}"

# When sourced: activate the VE and return (no VE creation).
# When executed directly: fall through to VE creation.
[[ "${BASH_SOURCE[0]}" != "${0}" ]] && source "${VE}/bin/activate" && return 0

if [ -d "${VE}" ]; then
    echo "VE already exists — skipping creation."
    exit 0
fi

if [ ! -d "${DDSIM_DIR}" ]; then
    echo "ERROR: DDSIM_DIR not found: ${DDSIM_DIR}"
    echo "       Clone it first: git clone <ddsim-repo> ${DDSIM_DIR}"
    exit 1
fi

CM_DIR="${CM_DIR:-${WORK_DIR}/campaign_manager}"
MINIAPPS_REQ="${DDSIM_DIR}/workflows/miniapps_workflow/requirements.txt"

echo ""
echo "Creating VE at ${VE} ..."
module load python/3.13.5-gcc13.3.1
python3 -m venv --copies "${VE}"
source "${VE}/bin/activate"

pip install --upgrade pip --quiet
pip install --quiet -e "${CM_DIR}[dragon,telemetry]"
pip install --quiet DDSim
# Build mpi4py from source so it links against the actual Cray MPICH (libmpi_gnu.so.12),
# not the PyPI wheel which expects libmpi.so.12.
MPICC="${MPICH_DIR}/bin/mpicc" pip install --quiet --no-binary mpi4py mpi4py
pip install --quiet -r "${MINIAPPS_REQ}"

echo ""
echo "=== VE ready ==="
echo "Python      : $(python --version)"
echo "asyncflow   : $(python -c 'import radical.asyncflow; print(radical.asyncflow.__version__)')"
echo "rhapsody    : $(python -c 'import rhapsody; print(rhapsody.__version__)')"
echo "dragon      : $(python -c 'import dragon; print(dragon.__version__)')"
echo "ddsim       : $(python -c 'import ddsim; print(getattr(ddsim, "__version__", "installed"))')"
echo "wfMiniAPI   : $(python -c 'import wfMiniAPI; print(getattr(wfMiniAPI, "__version__", "installed"))')"
