#!/bin/bash
# Create and configure the ddsim_aucam VE on Delta HPC.
#
# Run once to create the VE:
#   export WORK_DIR=<your_work_dir>
#   bash campaigns/ddsim_aucam/delta_env_setup.sh
#
# Sourced by delta_sbatch.sh to set VE, MINIAPPS_VE, DDSIM_DIR.
# All paths can be overridden by exporting the variable before calling this script.

: "${WORK_DIR:?WORK_DIR must be set}"

VE="${VE:-${WORK_DIR}/ve/ddsim_aucam}"
MINIAPPS_VE="${MINIAPPS_VE:-${WORK_DIR}/ve/miniapps}"
DDSIM_DIR="${DDSIM_DIR:-${WORK_DIR}/DeepDriveSim}"
export VE MINIAPPS_VE DDSIM_DIR

echo "VE          : ${VE}"
echo "MINIAPPS_VE : ${MINIAPPS_VE}"
echo "DDSIM_DIR   : ${DDSIM_DIR}"

# VE creation only runs when this script is executed directly, not when sourced.
# delta_sbatch.sh sources this file only to get the path variables above.
[[ "${BASH_SOURCE[0]}" != "${0}" ]] && return 0

if [ -d "${VE}" ]; then
    echo "VE already exists — skipping creation."
    exit 0
fi

CM_DIR="${CM_DIR:-${WORK_DIR}/campaign_manager}"

echo ""
echo "Creating VE at ${VE} ..."
module load python/3.13.5-gcc13.3.1
python3 -m venv "${VE}"
source "${VE}/bin/activate"

pip install --upgrade pip --quiet
pip install --quiet -e "${CM_DIR}[dragon,telemetry]"
pip install --quiet "rhapsody-py[dragon]"

echo ""
echo "=== VE ready ==="
echo "Python      : $(python --version)"
echo "asyncflow   : $(python -c 'import radical.asyncflow; print(radical.asyncflow.__version__)')"
echo "rhapsody    : $(python -c 'import rhapsody; print(rhapsody.__version__)')"
echo "dragon      : $(python -c 'import dragon; print(dragon.__version__)')"
