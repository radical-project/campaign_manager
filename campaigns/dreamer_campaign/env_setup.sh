#!/bin/bash
# =============================================================================
# Dreamer Campaign — environment setup
#
# Creates a Python 3.10+ venv with campaign_manager, radical.dreamer, and the
# async backend.  No GPU / Dragon required.
#
# Usage:
#   export SCRATCH=/scratch/<allocation>
#   bash env_setup.sh [--env-dir DIR] [--cm-dir DIR] [--dreamer-dir DIR] [--python PATH]
#
# Defaults:
#   ENV_DIR     = /u/$USER/ve/dreamer_campaign
#   CM_DIR      = $SCRATCH/$USER/campaign_manager
#   DREAMER_DIR = $SCRATCH/$USER/radical.dreamer
#   python      = auto-detected (python/3.11, python/3.10, cray-python/3.11.7, anaconda3)
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
ENV_DIR="${ENV_DIR:-/u/${USER}/ve/dreamer_campaign}"
CM_DIR="${CM_DIR:-${SCRATCH}/${USER}/campaign_manager}"
DREAMER_DIR="${DREAMER_DIR:-${SCRATCH}/${USER}/radical.dreamer}"
BASE_PY_OVERRIDE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --env-dir)     ENV_DIR="$2";           shift 2 ;;
        --cm-dir)      CM_DIR="$2";            shift 2 ;;
        --dreamer-dir) DREAMER_DIR="$2";       shift 2 ;;
        --python)      BASE_PY_OVERRIDE="$2";  shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

PY="${ENV_DIR}/bin/python"
PIP="${ENV_DIR}/bin/pip"

echo "================================================================="
echo "  ENV_DIR     = ${ENV_DIR}"
echo "  CM_DIR      = ${CM_DIR}"
echo "  DREAMER_DIR = ${DREAMER_DIR}"
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

if [ ! -d "${DREAMER_DIR}/.git" ]; then
    echo "Cloning radical.dreamer → ${DREAMER_DIR}"
    git clone https://github.com/radical-cybertools/radical.dreamer.git "${DREAMER_DIR}"
else
    echo "radical.dreamer already cloned at ${DREAMER_DIR}"
fi

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

# ── 3. Async backend ─────────────────────────────────────────────────────────
echo ""
echo "── Step 3: Async backend (rhapsody + radical.asyncflow) ──"
"${PIP}" install -q \
    "rhapsody-py[telemetry,dragon]>=0.2.0" \
    "radical.asyncflow>=0.3.1" \
    "pyyaml" \
    "numpy>=1.26.3,<2.0.0" \
    "openai" \
    "matplotlib" \
    "instructor"

# ── 4. radical.dreamer ────────────────────────────────────────────────────────
echo ""
echo "── Step 4: radical.dreamer ──"
"${PIP}" install -q -e "${DREAMER_DIR}"
# pip's in-tree editable install skips the setup.py develop step that copies
# VERSION into the package directory. radical.dreamer.__init__ calls
# radical.utils.get_version(dirname(__file__)) which needs that file.
printf '%s\n' "$(cat "${DREAMER_DIR}/VERSION")" > "${DREAMER_DIR}/src/radical/dreamer/VERSION"

# ── 5. campaign_manager ───────────────────────────────────────────────────────
echo ""
echo "── Step 5: campaign_manager ──"
"${PIP}" install -q -e "${CM_DIR}"

# ── 6. Dreamer campaign requirements ─────────────────────────────────────────
echo ""
echo "── Step 6: dreamer_campaign requirements ──"
CAMP_DIR="${CM_DIR}/campaigns/dreamer_campaign"
if [ -f "${CAMP_DIR}/requirements.txt" ]; then
    "${PIP}" install -q -r "${CAMP_DIR}/requirements.txt"
fi

# ── 7. Verify ────────────────────────────────────────────────────────────────
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
_check "radical.dreamer"   "${PY}" -c "import radical.dreamer; print('ok')"
_check "campaign_manager"  "${PY}" -c "from src.campaign import AsyncCampaignManager; print('ok')"
_check "pyyaml"            "${PY}" -c "import yaml; print(yaml.__version__)"

echo ""
echo "================================================================="
echo "Setup complete."
echo ""
echo "Activate with:"
echo "  source ${ENV_DIR}/bin/activate"
echo ""
echo "Run the campaign:"
echo "  cd ${CM_DIR}/campaigns/dreamer_campaign"
echo "  python run_campaign.py --config config.yaml"
echo "================================================================="
