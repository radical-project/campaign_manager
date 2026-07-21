#!/bin/bash
# =============================================================================
# Dummy Orbit Campaign environment setup — Delta HPC (NCSA)
#
# Creates a Python 3.10+ venv with campaign_manager + radical.orbit + rhapsody.
# No Dragon required — runs on the ORBIT concurrent backend.
#
# Usage:
#   export SCRATCH=/scratch/<allocation>
#   bash delta_env_setup.sh [--env-dir DIR] [--cm-dir DIR] [--orbit-dir DIR] [--adr-dir DIR] [--python PATH]
#
# Defaults:
#   ENV_DIR   = /u/$USER/ve/orbit_campaign
#   CM_DIR    = $SCRATCH/$USER/campaign_manager
#   ORBIT_DIR = $SCRATCH/$USER/radical.orbit
#   ADR_DIR   = $SCRATCH/$USER/radical.adr
#   python    = auto-detected (python/3.11, python/3.10, cray-python/3.11.7, anaconda3)
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

ENV_DIR="${ENV_DIR:-/u/${USER}/ve/orbit_campaign}"
CM_DIR="${CM_DIR:-${SCRATCH}/${USER}/campaign_manager}"
ORBIT_DIR="${ORBIT_DIR:-${SCRATCH}/${USER}/radical.orbit}"
ADR_DIR="${ADR_DIR:-${SCRATCH}/${USER}/radical.adr}"
BASE_PY_OVERRIDE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --env-dir)   ENV_DIR="$2";          shift 2 ;;
        --cm-dir)    CM_DIR="$2";           shift 2 ;;
        --orbit-dir) ORBIT_DIR="$2";        shift 2 ;;
        --adr-dir)   ADR_DIR="$2";          shift 2 ;;
        --python)    BASE_PY_OVERRIDE="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

PY="${ENV_DIR}/bin/python"
PIP="${ENV_DIR}/bin/pip"

echo "================================================================="
echo "  ENV_DIR   = ${ENV_DIR}"
echo "  CM_DIR    = ${CM_DIR}"
echo "  ORBIT_DIR = ${ORBIT_DIR}"
echo "  ADR_DIR   = ${ADR_DIR}"
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

if [ -n "${BASE_PY_OVERRIDE}" ]; then
    BASE_PY="${BASE_PY_OVERRIDE}"
    echo "Using Python override: ${BASE_PY}"
else
    BASE_PY=$(_find_python || true)
    if [ -z "${BASE_PY}" ]; then
        echo "python3.10+ not in PATH — trying modules..."
        for mod in python/3.11 python/3.10 cray-python/3.11.7 anaconda3; do
            module load "${mod}" 2>/dev/null || true
            BASE_PY=$(_find_python || true)
            [ -n "${BASE_PY}" ] && echo "  loaded module: ${mod}" && break
        done
    fi
    # Last resort: accept any python3 in PATH regardless of version check
    if [ -z "${BASE_PY}" ]; then
        BASE_PY=$(which python3 2>/dev/null || true)
        [ -n "${BASE_PY}" ] && echo "  falling back to: ${BASE_PY} ($(${BASE_PY} --version))"
    fi
    if [ -z "${BASE_PY}" ]; then
        echo "ERROR: no Python 3.10+ interpreter found."
        echo "       Pass an explicit interpreter:  --python /path/to/python3.11"
        echo "       Or load a module manually before running this script."
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

# ── 2. Bootstrap pip ──────────────────────────────────────────────────────────
echo ""
echo "── Step 2: Bootstrapping pip ──"
"${PY}" -m pip install -q --upgrade pip wheel
"${PIP}" install -q --force-reinstall "setuptools<71"

# ── 3. radical.orbit (local editable) ────────────────────────────────────────
echo ""
echo "── Step 3: radical.orbit (local editable) ──"
"${PIP}" install -q -e "${ORBIT_DIR}"

# ── 4. rhapsody ──────────────────────────────────────────────────────────────
echo ""
echo "── Step 4: rhapsody ──"
# [telemetry] extra pulls in opentelemetry-sdk, which rhapsody 0.4+ imports
# unconditionally during session init despite it being listed as optional.
"${PIP}" install -q "rhapsody-py[telemetry,dragon]>=0.2.0" "radical.asyncflow>=0.3.1" openai instructor

# ── 5. radical.adr (local — not on PyPI, must come before campaign_manager[adr]) ──
echo ""
echo "── Step 5: radical.adr (local editable) ──"
"${PIP}" install -q -e "${ADR_DIR}"

# ── 6. campaign_manager + ADR extra ──────────────────────────────────────────
echo ""
echo "── Step 6: campaign_manager[adr] (editable) ──"
"${PIP}" install -q -e "${CM_DIR}[adr]"

# ── 7. Plotting ───────────────────────────────────────────────────────────────
echo ""
echo "── Step 7: matplotlib ──"
"${PIP}" install -q matplotlib

# ── 8. Verify ─────────────────────────────────────────────────────────────────
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

_check "radical.orbit"     "${PY}" -c "import radical.orbit; print('ok')"
_check "rhapsody"          "${PY}" -c "import rhapsody; print('ok')"
_check "radical.asyncflow" "${PY}" -c "import radical.asyncflow; print(radical.asyncflow.__version__)"
_check "radical.adr"       "${PY}" -c "import radical.adr; print('ok')"
_check "campaign_manager"  "${PY}" -c "from src.campaign import AsyncCampaignManager; print('ok')"
_check "pyyaml"            "${PY}" -c "import yaml; print(yaml.__version__)"

echo ""
echo "================================================================="
echo "Setup complete."
echo ""
echo "Activate with:"
echo "  source ${ENV_DIR}/bin/activate"
echo ""
echo "Prerequisites before running:"
echo "  # Terminal 1 (login node):"
echo "  export RADICAL_ORBIT_BROKER_TOKEN=<token>"
echo "  ./bin/radical-orbit-broker.py --port 8020"
echo ""
echo "  # Terminal 2 (compute node):"
echo "  export RADICAL_ORBIT_BROKER_URL=wss://dt-login04.delta.ncsa.illinois.edu:8020"
echo "  ./bin/radical-orbit-endpoint.py --name my-endpoint -p rhapsody"
echo ""
echo "Run the campaign:"
echo "  export RADICAL_ORBIT_BROKER_URL=wss://dt-login04.delta.ncsa.illinois.edu:8020"
echo "  cd ${CM_DIR}/campaigns/orbit_campaign"
echo "  python run_campaign.py --config config.yaml"
echo "================================================================="
