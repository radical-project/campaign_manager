#!/bin/bash
# =============================================================================
# Dummy Campaign environment setup — Delta HPC (NCSA)
#
# Creates a Python 3.10+ venv and installs all dependencies.
# No GPU / Dragon required — runs on the asyncio concurrent backend.
#
# Usage:
#   export SCRATCH=/scratch/<allocation>
#   bash delta_env_setup.sh [--env-dir DIR] [--spherical-dir DIR] [--adr-dir DIR]
#
# Defaults:
#   ENV_DIR       = /u/$USER/ve/dummy_campaign
#   SPHERICAL_DIR = $SCRATCH/$USER/SPHERICAL
#   ADR_DIR       = $SCRATCH/$USER/radical.adr
# =============================================================================
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
fi

# ── Parse optional overrides ──────────────────────────────────────────────────
if [[ -z "${SCRATCH:-}" ]]; then
    echo "ERROR: set the SCRATCH env var to your allocation scratch root, e.g.:"
    echo "  export SCRATCH=/scratch/<allocation>"
    echo "  bash delta_env_setup.sh"
    exit 1
fi

ENV_DIR="${ENV_DIR:-/u/${USER}/ve/dummy_campaign}"
SPHERICAL_DIR="${SPHERICAL_DIR:-${SCRATCH}/${USER}/SPHERICAL}"
ADR_DIR="${ADR_DIR:-${SCRATCH}/${USER}/radical.adr}"

while [[ $# -gt 0 ]]; do
    case $1 in
        --env-dir)       ENV_DIR="$2";       shift 2 ;;
        --spherical-dir) SPHERICAL_DIR="$2"; shift 2 ;;
        --adr-dir)       ADR_DIR="$2";       shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

CM_DIR="${SPHERICAL_DIR}/../campaign_manager"
PY="${ENV_DIR}/bin/python"
PIP="${ENV_DIR}/bin/pip"

echo "================================================================="
echo "  ENV_DIR       = ${ENV_DIR}"
echo "  SPHERICAL_DIR = ${SPHERICAL_DIR}"
echo "  CM_DIR        = ${CM_DIR}"
echo "  ADR_DIR       = ${ADR_DIR}"
echo "================================================================="

# ── 1. Create venv ────────────────────────────────────────────────────────────
echo ""
echo "── Step 1: Creating venv ──"

_find_python() {
    for candidate in python3.11 python3.10 python3 python; do
        local p
        p=$(command -v "${candidate}" 2>/dev/null) || continue
        local ver
        ver=$("${p}" -c "import sys; v=sys.version_info; print(v.major*10+v.minor)" 2>/dev/null) || continue
        [ "${ver}" -ge 310 ] && echo "${p}" && return 0
    done
    return 1
}

BASE_PY=$(_find_python || true)
if [ -z "${BASE_PY}" ]; then
    echo "python3.10+ not in PATH — loading cray-python/3.11.7 module..."
    module load cray-python/3.11.7 2>/dev/null || true
    BASE_PY=$(_find_python || true)
fi
if [ -z "${BASE_PY}" ]; then
    echo "ERROR: no Python 3.10+ interpreter found."
    exit 1
fi
echo "Using Python: ${BASE_PY} ($(${BASE_PY} --version))"

if [ ! -x "${PY}" ]; then
    "${BASE_PY}" -m venv "${ENV_DIR}"
else
    echo "venv already exists at ${ENV_DIR}"
fi

echo "Python: $("${PY}" --version)"

# ── 2. Bootstrap pip ──────────────────────────────────────────────────────────
echo ""
echo "── Step 2: Bootstrapping pip ──"
"${PY}" -m pip install -q --upgrade pip wheel
"${PIP}" install -q --force-reinstall "setuptools<71"

# ── 3. campaign_manager + ADR extra ──────────────────────────────────────────
echo ""
echo "── Step 3: campaign_manager[adr] (editable) ──"
"${PIP}" install -q -e "${CM_DIR}[adr]"

# ── 4. radical.adr (local — not on PyPI) ─────────────────────────────────────
echo ""
echo "── Step 4: radical.adr (local editable) ──"
"${PIP}" install -q -e "${ADR_DIR}"

# ── 5. Plotting ───────────────────────────────────────────────────────────────
echo ""
echo "── Step 5: matplotlib ──"
"${PIP}" install -q matplotlib

# ── 6. Verify ─────────────────────────────────────────────────────────────────
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
_check "radical.adr"       "${PY}" -c "import radical.adr; print('ok')"
_check "campaign_manager"  "${PY}" -c "from src.campaign import AsyncCampaignManager; print('ok')"
_check "pyyaml"            "${PY}" -c "import yaml; print(yaml.__version__)"
_check "matplotlib"        "${PY}" -c "import matplotlib; print(matplotlib.__version__)"

echo ""
echo "================================================================="
echo "Setup complete."
echo ""
echo "Activate with:"
echo "  source ${ENV_DIR}/bin/activate"
echo ""
echo "Run the campaign:"
echo "  cd ${SPHERICAL_DIR}/workflows/dummy_campaign"
echo "  python run_campaign.py --config config.yaml --policy rule"
echo "================================================================="
