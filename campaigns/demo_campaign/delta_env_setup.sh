#!/bin/bash
# Create and configure the demo_campaign VE on Delta HPC.
#
# Run once to create the VE:
#   export WORK_DIR=<your_work_dir>
#   bash campaigns/demo_campaign/delta_env_setup.sh
#
# Sourced by delta_sbatch.sh to set VE.
# Override VE by exporting it before calling this script.

: "${WORK_DIR:?WORK_DIR must be set}"

VE="${VE:-${WORK_DIR}/ve/demo_aucam}"
export VE

echo "VE : ${VE}"

# VE creation only runs when this script is executed directly, not when sourced.
# delta_sbatch.sh sources this file only to get the path variables above.
[[ "${BASH_SOURCE[0]}" != "${0}" ]] && return 0

if [ -d "${VE}" ]; then
    echo "VE already exists — skipping creation."
    exit 0
fi

CM_DIR="${CM_DIR:-${WORK_DIR}/campaign_manager}"
RADEX_REPO="${RADEX_REPO:-${WORK_DIR}/radex}"

# ── Build radex (C++ core + Python bindings) if not already done ──────────────
RADEX_SO="${RADEX_REPO}/src/python/build/lib.linux-x86_64-cpython-313/radex/clients/core.cpython-313-x86_64-linux-gnu.so"
if [ ! -f "${RADEX_SO}" ]; then
    if [ ! -d "${RADEX_REPO}" ]; then
        echo "Cloning radex into ${RADEX_REPO} ..."
        git clone https://github.com/radical-cybertools/radex.git "${RADEX_REPO}"
    fi
    echo "Building radex (cmake) ..."
    cmake -S "${RADEX_REPO}" -B "${RADEX_REPO}/build" \
          -DCMAKE_INSTALL_PREFIX="${RADEX_REPO}/install"
    cmake --build "${RADEX_REPO}/build" --target install
else
    echo "radex already built — skipping cmake."
fi

echo ""
echo "Creating VE at ${VE} ..."
module load python/3.13.5-gcc13.3.1
python3 -m venv "${VE}"
source "${VE}/bin/activate"

pip install --upgrade pip --quiet
pip install --quiet -e "${CM_DIR}[dragon,telemetry]"
pip install --quiet "rhapsody-py[dragon]"
DRAGON_INCLUDE_DIR="$(python -c 'import dragon, os; print(os.path.join(os.path.dirname(dragon.__file__), "include"))')"
DRAGON_LIB_DIR="$(python -c 'import dragon, os; print(os.path.join(os.path.dirname(dragon.__file__), "lib"))')"
export DRAGON_INCLUDE_DIR DRAGON_LIB_DIR
pip install --quiet -e "${RADEX_REPO}/src/python"

echo ""
echo "=== VE ready ==="
echo "Python    : $(python --version)"
echo "asyncflow : $(python -c 'import radical.asyncflow; print(radical.asyncflow.__version__)')"
echo "rhapsody  : $(python -c 'import rhapsody; print(rhapsody.__version__)')"
echo "dragon    : $(python -c 'import dragon; print(dragon.__version__)')"
echo "radex     : $(python -c 'import radex; print(getattr(radex, "__version__", "installed"))')"
