#!/bin/bash
# =============================================================================
# SPHERICAL ESM2/DDSim Campaign — environment setup
#
# Creates a Python venv with SPHERICAL (esm2 extras), DeepDriveSim, and the
# async backend (rhapsody + radical.asyncflow).
#
# Usage:
#   bash env_setup.sh [--env-dir DIR] [--spherical-dir DIR] [--ddsim-dir DIR]
#
# Defaults:
#   ENV_DIR       = /u/$USER/ve/campaign
#   SPHERICAL_DIR = /scratch/bblj/$USER/SPHERICAL
#   DDSIM_DIR     = /scratch/bblj/$USER/DeepDriveSim
# =============================================================================
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
fi

ENV_DIR="${ENV_DIR:-/u/${USER}/ve/campaign}"
SPHERICAL_DIR="${SPHERICAL_DIR:-/scratch/bblj/${USER}/SPHERICAL}"
DDSIM_DIR="${DDSIM_DIR:-/scratch/bblj/${USER}/DeepDriveSim}"

while [[ $# -gt 0 ]]; do
    case $1 in
        --env-dir)       ENV_DIR="$2";       shift 2 ;;
        --spherical-dir) SPHERICAL_DIR="$2"; shift 2 ;;
        --ddsim-dir)     DDSIM_DIR="$2";     shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

echo "================================================================="
echo "  ENV_DIR       = ${ENV_DIR}"
echo "  SPHERICAL_DIR = ${SPHERICAL_DIR}"
echo "  DDSIM_DIR     = ${DDSIM_DIR}"
echo "================================================================="

# ── 0. Clone repositories ─────────────────────────────────────────────────────
echo ""
echo "── Step 0: Checking repositories ──"

if [ ! -d "${SPHERICAL_DIR}/.git" ]; then
    echo "Cloning SPHERICAL → ${SPHERICAL_DIR}"
    git clone git@github.com:radical-collaboration/SPHERICAL.git "${SPHERICAL_DIR}"
else
    echo "SPHERICAL already cloned at ${SPHERICAL_DIR}"
fi

if [ ! -d "${DDSIM_DIR}/.git" ]; then
    echo "Cloning DeepDriveSim → ${DDSIM_DIR}"
    git clone --branch campaign_manager --single-branch \
        https://github.com/radical-collaboration/DeepDriveSim.git "${DDSIM_DIR}"
else
    echo "DeepDriveSim already cloned at ${DDSIM_DIR}"
fi

# ── 1. Find Python 3.11 ───────────────────────────────────────────────────────
echo ""
echo "── Step 1: Locating Python ──"

BASE_PY=$(command -v python3.11 2>/dev/null || true)

if [ -z "${BASE_PY}" ]; then
    module load cray-python/3.11.7 2>/dev/null || true
    BASE_PY=$(command -v python3.11 2>/dev/null || true)
fi

if [ -z "${BASE_PY}" ]; then
    BASE_PY=/opt/cray/pe/python/3.11.7/bin/python3.11
    [ -x "${BASE_PY}" ] || BASE_PY=""
fi

if [ -z "${BASE_PY}" ]; then
    echo "ERROR: no Python 3.11 interpreter found."
    exit 1
fi

PY_VERSION=$("${BASE_PY}" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PY="${ENV_DIR}/bin/python${PY_VERSION}"
PIP="${ENV_DIR}/bin/pip"
echo "Using Python: ${BASE_PY} ($(${BASE_PY} --version))"

# ── 2. Create venv ────────────────────────────────────────────────────────────
echo ""
echo "── Step 2: Creating venv ──"

if [ ! -x "${PY}" ]; then
    echo "Creating venv at ${ENV_DIR}..."
    "${BASE_PY}" -m venv "${ENV_DIR}"
else
    echo "venv already exists at ${ENV_DIR}"
fi

ln -sf "${ENV_DIR}/bin/python${PY_VERSION}" "${ENV_DIR}/bin/python"  2>/dev/null || true
ln -sf "${ENV_DIR}/bin/python${PY_VERSION}" "${ENV_DIR}/bin/python3" 2>/dev/null || true

# ── 3. Bootstrap pip ──────────────────────────────────────────────────────────
echo ""
echo "── Step 3: Bootstrapping pip ──"
"${PY}" -m pip install -q --upgrade pip wheel
"${PIP}" install -q --force-reinstall "setuptools<71"

# ── 4. Async backend ─────────────────────────────────────────────────────────
echo ""
echo "── Step 4: Async backend (rhapsody + radical.asyncflow) ──"
"${PIP}" install -q \
    "rhapsody-py>=0.2.0" \
    "radical.asyncflow>=0.5.0" \
    "pyyaml" \
    "numpy>=1.26.3,<2.0.0"

# ── 5. DeepDriveSim ───────────────────────────────────────────────────────────
echo ""
echo "── Step 5: DeepDriveSim ──"
"${PIP}" install -q -e "${DDSIM_DIR}"

# ── 6. SPHERICAL (with esm2 extras) ──────────────────────────────────────────
echo ""
echo "── Step 6: SPHERICAL [dragon,dev,esm2] ──"
"${PIP}" install -q -e "${SPHERICAL_DIR}[dragon,dev,esm2]"

# ── 7. Campaign requirements ──────────────────────────────────────────────────
echo ""
echo "── Step 7: esm2_ddsim_campaign requirements ──"
CAMP_DIR="${SPHERICAL_DIR}/workflows/run_campaign/esm2_ddsim_campaign"
if [ -f "${CAMP_DIR}/requirements.txt" ]; then
    "${PIP}" install -q -r "${CAMP_DIR}/requirements.txt"
fi

# ── 8. Verify ────────────────────────────────────────────────────────────────
echo ""
echo "── Verifying installation ──"
_check() {
    local label="$1"; shift
    if out=$("$@" 2>&1); then
        echo "  ${label}: OK  (${out})"
    else
        echo "  WARNING: ${label} failed"
        echo "    ${out}" | head -3
    fi
}

_check "radical.asyncflow" "${PY}" -c "import radical.asyncflow; print('ok')"
_check "rhapsody"          "${PY}" -c "import rhapsody; print('ok')"
_check "spherical"         "${PY}" -c "import src.campaign; print('ok')"

echo ""
echo "================================================================="
echo "Setup complete."
echo ""
echo "Activate with:"
echo "  source ${ENV_DIR}/bin/activate"
echo ""
echo "Run the campaign:"
echo "  cd ${CAMP_DIR}"
echo "  python run_campaing.py --config config.yaml"
echo "================================================================="
