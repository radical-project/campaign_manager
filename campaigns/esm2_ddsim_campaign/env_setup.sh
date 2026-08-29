#!/bin/bash
# =============================================================================
# ESM2 DDSim Campaign — environment setup — Delta HPC (NCSA)
#
# Creates a Python 3.11+ venv with campaign_manager and DeepDriveSim.
# Workflow-specific environments (ddmd, miniapps, inference, etc.) are created
# by their own setup scripts; the CM uses the python executable specified in
# each workflow's config (e.g. executable: "/u/${USER}/ve/ddmd/bin/python").
#
# Usage:
#   export SCRATCH=/scratch/<allocation>
#   bash env_setup.sh [--env-dir DIR] [--cm-dir DIR] [--ddsim-dir DIR] [--python PATH]
#
# Defaults:
#   ENV_DIR   = /u/$USER/ve/esm2_ddsim_campaign
#   CM_DIR    = $SCRATCH/$USER/campaign_manager
#   DDSIM_DIR = $SCRATCH/$USER/DeepDriveSim
#   python    = auto-detected (Python 3.11+ required for Dragon)
# =============================================================================
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
fi

# ── SCRATCH guard ─────────────────────────────────────────────────────────────
if [[ -z "${SCRATCH:-}" ]]; then
    echo "ERROR: SCRATCH is not set."
    echo "  export SCRATCH=/scratch/<allocation>"
    echo "  bash env_setup.sh"
    exit 1
fi

# ── Parse optional overrides ──────────────────────────────────────────────────
ENV_DIR="${ENV_DIR:-/u/${USER}/ve/esm2_ddsim_campaign}"
CM_DIR="${CM_DIR:-${SCRATCH}/${USER}/campaign_manager}"
DDSIM_DIR="${DDSIM_DIR:-${SCRATCH}/${USER}/DeepDriveSim}"
ADR_DIR="${ADR_DIR:-${SCRATCH}/${USER}/radical.adr}"
SPHERICAL_DIR="${SPHERICAL_DIR:-${SCRATCH}/${USER}/SPHERICAL}"
BASE_PY_OVERRIDE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --env-dir)       ENV_DIR="$2";           shift 2 ;;
        --cm-dir)        CM_DIR="$2";            shift 2 ;;
        --ddsim-dir)     DDSIM_DIR="$2";         shift 2 ;;
        --adr-dir)       ADR_DIR="$2";           shift 2 ;;
        --spherical-dir) SPHERICAL_DIR="$2";     shift 2 ;;
        --python)        BASE_PY_OVERRIDE="$2";  shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

PY="${ENV_DIR}/bin/python"
PIP="${ENV_DIR}/bin/pip"

echo "================================================================="
echo "  ENV_DIR       = ${ENV_DIR}"
echo "  CM_DIR        = ${CM_DIR}"
echo "  DDSIM_DIR     = ${DDSIM_DIR}"
echo "  ADR_DIR       = ${ADR_DIR}"
echo "  SPHERICAL_DIR = ${SPHERICAL_DIR}"
echo "================================================================="

# ── 0. Clone repositories ─────────────────────────────────────────────────────
echo ""
echo "── Step 0: Checking repositories ──"

if [ ! -d "${CM_DIR}/.git" ]; then
    echo "Cloning campaign_manager → ${CM_DIR}"
    git clone git@github.com:radical-collaboration/campaign_manager.git "${CM_DIR}"
else
    echo "campaign_manager already cloned at ${CM_DIR}"
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

_find_python() {
    for candidate in python3.13 python3.12 python3.11 python3 python; do
        local p
        p=$(command -v "${candidate}" 2>/dev/null) || continue
        local ver
        # Use major*100+minor so Python 3.11 → 311, 3.10 → 310, 3.9 → 309
        ver=$("${p}" -c "import sys; v=sys.version_info; print(v.major*100+v.minor)" 2>/dev/null) || continue
        [ "${ver}" -ge 311 ] && echo "${p}" && return 0
    done
    return 1
}

if [ -n "${BASE_PY_OVERRIDE}" ]; then
    BASE_PY="${BASE_PY_OVERRIDE}"
    echo "Using Python override: ${BASE_PY}"
else
    BASE_PY=$(_find_python || true)
    if [ -z "${BASE_PY}" ]; then
        echo "python3.11+ not in PATH — trying modules..."
        # Delta HPC module names — try most specific first
        for mod in python/3.13.5-gcc13.3.1 python/3.13.1 python/3.12.3 \
                   python/3.11.9 python/3.11 cray-python/3.11.7 \
                   anaconda3_gpu/23.9.0 anaconda3; do
            module load "${mod}" 2>/dev/null || true
            BASE_PY=$(_find_python || true)
            [ -n "${BASE_PY}" ] && echo "  loaded module: ${mod}" && break
        done
    fi
    if [ -z "${BASE_PY}" ]; then
        echo "ERROR: no Python 3.11+ interpreter found (Dragon requires >= 3.11)."
        echo "       Pass an explicit interpreter:  --python /path/to/python3.11"
        echo "       Or load a module manually:     module load python/3.11.9"
        exit 1
    fi
fi
echo "Using Python: ${BASE_PY} ($(${BASE_PY} --version))"

if [ ! -x "${PY}" ]; then
    "${BASE_PY}" -m venv "${ENV_DIR}"
else
    echo "venv already exists at ${ENV_DIR}"
fi

echo "Python: $("${PY}" --version)"

# ── 2. Bootstrap pip / setuptools ────────────────────────────────────────────
echo ""
echo "── Step 2: Bootstrapping pip ──"
"${PY}" -m pip install -q --upgrade pip wheel
"${PIP}" install -q --force-reinstall "setuptools<71"

# ── 3. Dragon / Rhapsody / Radical ────────────────────────────────────────────
echo ""
echo "── Step 3: Dragon HPC + Rhapsody + Radical ──"
"${PIP}" install -q "rhapsody-py[telemetry,dragon]"
"${PIP}" install -q \
    "radical.asyncflow" \
    "numpy>=1.26.3,<2.0.0" \
    "transformers>=4.30.0" \
    "openai" \
    "instructor" \
    "pyyaml"

# ── 4. campaign_manager (editable) ───────────────────────────────────────────
echo ""
echo "── Step 4: campaign_manager[dev,plotting] ──"
"${PIP}" install -q -e "${CM_DIR}[dev,plotting]"

# ── 5. DeepDriveSim (editable) ───────────────────────────────────────────────
echo ""
echo "── Step 5: DeepDriveSim ──"
"${PIP}" install -q -e "${DDSIM_DIR}"

# ── 6. radical.adr (local — not on PyPI) ────────────────────────────────────
echo ""
echo "── Step 6: radical.adr (local editable) ──"
"${PIP}" install -q -e "${ADR_DIR}"

# ── 7. SPHERICAL (ESM2 inference service + torch) ────────────────────────────
echo ""
echo "── Step 7: SPHERICAL[esm2] ──"
if [ -d "${SPHERICAL_DIR}" ]; then
    "${PIP}" install -q -e "${SPHERICAL_DIR}[esm2]"
else
    echo "  WARNING: SPHERICAL_DIR not found (${SPHERICAL_DIR}) — skipping"
fi

# ── 8. esm2_ddsim_campaign requirements ─────────────────────────────────────
echo ""
echo "── Step 8: esm2_ddsim_campaign requirements ──"
"${PIP}" install -q -r "${CM_DIR}/campaigns/esm2_ddsim_campaign/requirements.txt"

# ── 9. Verify ────────────────────────────────────────────────────────────────
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

_check "radical.asyncflow" "${PY}" -c "import radical.asyncflow; print(radical.asyncflow.__version__)"
_check "rhapsody"          "${PY}" -c "import rhapsody; print('ok')"
_check "dragonhpc"         "${PY}" -c "import dragon; print('ok')"
_check "ddsim"             "${PY}" -c "import ddsim; print('ok')"
_check "radical.adr"       "${PY}" -c "import radical.adr; print('ok')"
_check "torch"             "${PY}" -c "import torch; print(torch.__version__)"
_check "transformers"      "${PY}" -c "import transformers; print(transformers.__version__)"
_check "aiohttp"           "${PY}" -c "import aiohttp; print(aiohttp.__version__)"
_check "campaign_manager"  "${PY}" -c "from src.campaign import AsyncCampaignManager; print('ok')"
_check "pyyaml"            "${PY}" -c "import yaml; print(yaml.__version__)"

echo ""
echo "================================================================="
echo "Setup complete."
echo ""
echo "Activate with:"
echo "  source ${ENV_DIR}/bin/activate"
echo ""
echo "Note: ddmd/miniapps workflows use separate venvs. The CM uses the"
echo "python executable from each workflow's config. Inference and dummy"
echo "workflows are covered by this venv (ve/esm2 and ve/ddsim symlink here)."
echo "================================================================="
