#!/bin/bash
# =============================================================================
# SPHERICAL Campaign Manager environment setup — Delta HPC (NCSA)
#
# Creates a Python venv with SPHERICAL and DeepDriveSim (campaign manager only).
# Workflow-specific environments (ddmd, miniapps, inference, etc.) are created
# by their own setup scripts; the CM uses the python executable specified in
# each workflow's config (e.g. executable: "/u/${USER}/ve/ddmd/bin/python").
#
# Usage:
#   bash delta_env_setup.sh [--env-dir DIR] [--spherical-dir DIR] [--ddsim-dir DIR]
#
# Defaults:
#   ENV_DIR       = /u/$USER/ve/campaign
#   SPHERICAL_DIR = /scratch/bblj/$USER/SPHERICAL
#   DDSIM_DIR     = /scratch/bblj/$USER/DeepDriveSim
# =============================================================================
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
fi

# ── Parse optional overrides ──────────────────────────────────────────────────
ENV_DIR="${ENV_DIR:-/u/${USER}/ve/campaign}"
SPHERICAL_DIR="${SPHERICAL_DIR:-/scratch/bblj/${USER}/tmp/SPHERICAL}"
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
echo "── Step 0: Cloning repositories ──"

if [ ! -d "${SPHERICAL_DIR}/.git" ]; then
    echo "Cloning SPHERICAL → ${SPHERICAL_DIR}"
    git clone git@github.com:radical-collaboration/SPHERICAL.git "${SPHERICAL_DIR}"
else
    echo "SPHERICAL already cloned at ${SPHERICAL_DIR}"
fi

if [ ! -d "${DDSIM_DIR}/.git" ]; then
    echo "Cloning DeepDriveSim → ${DDSIM_DIR}"
    git clone --branch origin/campaign_manager --single-branch \
        https://github.com/radical-collaboration/DeepDriveSim.git "${DDSIM_DIR}"
else
    echo "DeepDriveSim already cloned at ${DDSIM_DIR}"
fi

# ── 1. Create venv ────────────────────────────────────────────────────────────
echo ""
echo "── Step 1: Creating venv ──"

BASE_PY=$(command -v python3.11 2>/dev/null || true)

if [ -z "${BASE_PY}" ]; then
    echo "python3.11 not in PATH — trying cray-python/3.11.7..."
    module load cray-python/3.11.7 2>/dev/null || true
    BASE_PY=$(command -v python3.11 2>/dev/null || true)
fi

if [ -z "${BASE_PY}" ]; then
    echo "python3.11 not available — trying python3.10 via anaconda3..."
    module load anaconda3 2>/dev/null || true
    BASE_PY=$(command -v python3.10 2>/dev/null || true)
fi

if [ -z "${BASE_PY}" ]; then
    BASE_PY=$(command -v python3 2>/dev/null || true)
    [ -n "${BASE_PY}" ] && echo "Falling back to $(${BASE_PY} --version)"
fi

if [ -z "${BASE_PY}" ]; then
    echo "ERROR: no Python 3.10+ interpreter found."
    echo "       Try: module load anaconda3  or  module load cray-python/3.11.7"
    exit 1
fi

PY_VERSION=$("${BASE_PY}" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PY="${ENV_DIR}/bin/python${PY_VERSION}"
PIP="${ENV_DIR}/bin/pip"
echo "Using Python: ${BASE_PY} ($(${BASE_PY} --version))"

if [ ! -x "${PY}" ]; then
    echo "Creating venv at ${ENV_DIR}..."
    "${BASE_PY}" -m venv "${ENV_DIR}"
else
    echo "venv already exists at ${ENV_DIR}"
fi

ln -sf "${ENV_DIR}/bin/python${PY_VERSION}" "${ENV_DIR}/bin/python"  2>/dev/null || true
ln -sf "${ENV_DIR}/bin/python${PY_VERSION}" "${ENV_DIR}/bin/python3" 2>/dev/null || true

echo "Python: $("${PY}" --version)"

# ── 2. Bootstrap pip / setuptools ────────────────────────────────────────────
echo ""
echo "── Step 2: Bootstrapping pip ──"
"${PY}" -m pip install -q --upgrade pip wheel
"${PIP}" install -q --force-reinstall "setuptools<71"

# ── 3. Dragon / Rhapsody / Radical ────────────────────────────────────────────
echo ""
echo "── Step 3: Dragon HPC + Rhapsody + Radical ──"
"${PIP}" install -q \
    "dragonhpc>=0.13.2" \
    "rhapsody-py>=0.2.0" \
    "radical.asyncflow>=0.3.1" \
    "nvidia-ml-py" \
    "numpy>=1.26.3,<2.0.0" \
    "transformers>=4.30.0"

# ── 4. SPHERICAL (editable, campaign manager extras only) ────────────────────
echo ""
echo "── Step 4: SPHERICAL ──"
"${PIP}" install -q -e "${SPHERICAL_DIR}[dragon,dev,plotting]"

# ── 5. DeepDriveSim (editable) ───────────────────────────────────────────────
echo ""
echo "── Step 5: DeepDriveSim ──"
"${PIP}" install -q -e "${DDSIM_DIR}"

# ── 6. run_campaign extra requirements ───────────────────────────────────────
echo ""
echo "── Step 6: run_campaign requirements ──"
"${PIP}" install -q -r "${SPHERICAL_DIR}/workflows/run_campaign/requirements.txt"

# ── 7. Apply slurm patch ─────────────────────────────────────────────────────
echo ""
echo "── Step 7: Applying slurm patch ──"
"${PY}" "${SPHERICAL_DIR}/workflows/apply_slurm_patch.py"

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
_check "dragonhpc"         "${PY}" -c "import dragon; print('ok')"
_check "ddsim"             "${PY}" -c "import ddsim; print('ok')"
_check "spherical"         "${PY}" -c "import src.campaign; print('ok')"

echo ""
echo "================================================================="
echo "Setup complete."
echo ""
echo "Activate with:"
echo "  source ${ENV_DIR}/bin/activate"
echo ""
echo "Note: workflow environments (ddmd, miniapps, inference, etc.) must be"
echo "set up separately. The CM uses the python executable from each"
echo "workflow's config (e.g. executable: \"/u/\${USER}/ve/ddmd/bin/python\")."
echo "================================================================="
