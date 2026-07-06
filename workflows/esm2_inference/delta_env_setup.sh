#!/bin/bash
# =============================================================================
# ESM2 Inference environment setup — Delta HPC (NCSA)
#
# Creates a Python 3.11 (or 3.10) venv and installs SPHERICAL with ESM2
# inference dependencies: PyTorch, transformers, fair-esm, Dragon, and Rhapsody.
#
# Usage:
#   bash delta_env_setup.sh [--env-dir DIR] [--spherical-dir DIR]
#
# Defaults:
#   ENV_DIR       = /u/$USER/ve/esm2
#   SPHERICAL_DIR = /scratch/bblj/$USER/SPHERICAL
# =============================================================================
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
fi

# ── Parse optional overrides ──────────────────────────────────────────────────
ENV_DIR="${ENV_DIR:-/u/${USER}/ve/esm2}"
SPHERICAL_DIR="${SPHERICAL_DIR:-/scratch/bblj/${USER}/SPHERICAL}"

while [[ $# -gt 0 ]]; do
    case $1 in
        --env-dir)       ENV_DIR="$2";       shift 2 ;;
        --spherical-dir) SPHERICAL_DIR="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

echo "================================================================="
echo "  ENV_DIR       = ${ENV_DIR}"
echo "  SPHERICAL_DIR = ${SPHERICAL_DIR}"
echo "================================================================="

# ── 0. Clone repository ───────────────────────────────────────────────────────
echo ""
echo "── Step 0: Cloning repository ──"

if [ ! -d "${SPHERICAL_DIR}/.git" ]; then
    echo "Cloning SPHERICAL → ${SPHERICAL_DIR}"
    git clone git@github.com:radical-collaboration/SPHERICAL.git "${SPHERICAL_DIR}"
else
    echo "SPHERICAL already cloned at ${SPHERICAL_DIR}"
fi

# ── 1. Create venv ────────────────────────────────────────────────────────────
echo ""
echo "── Step 1: Creating venv ──"

# Delta's anaconda3 module exposes python3.10; cray-python/3.11.7 exposes 3.11.
# Try 3.11 first (preferred), fall back to 3.10.
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
"${PIP}" install -q --force-reinstall "setuptools==78.1.1"

# ── 3. PyTorch 2.5.1+cu121 ────────────────────────────────────────────────────
echo ""
echo "── Step 3: PyTorch 2.5.1+cu121 ──"
"${PIP}" install -q \
    torch==2.5.1+cu121 \
    torchvision==0.20.1+cu121 \
    torchaudio==2.5.1+cu121 \
    --index-url https://download.pytorch.org/whl/cu121

# ── 4. Dragon / Rhapsody / Radical ────────────────────────────────────────────
echo ""
echo "── Step 4: Dragon HPC + Rhapsody + Radical ──"
"${PIP}" install -q \
    "dragonhpc>=0.13.2" \
    "rhapsody-py>=0.2.0" \
    "radical.asyncflow>=0.3.1" \
    "nvidia-ml-py"

# ── 5. ESM2 / Inference dependencies ─────────────────────────────────────────
echo ""
echo "── Step 5: ESM2 / Inference deps ──"
"${PIP}" install -q \
    "transformers>=4.30.0" \
    "accelerate>=0.20.0" \
    "fair-esm>=2.0.0" \
    "sentencepiece>=0.1.99" \
    "huggingface_hub" \
    "safetensors" \
    "numpy>=1.26.3,<2.0.0" \
    "pandas" \
    "PyYAML>=6.0" \
    "aiohttp>=3.8.0" \
    "tqdm>=4.65.0" \
    "psutil>=5.9.0" \
    "matplotlib>=3.7.0" \
    "pydantic>=2.0.0" \
    "pydantic-settings"

# ── 6. SPHERICAL (editable) ───────────────────────────────────────────────────
echo ""
echo "── Step 6: SPHERICAL ──"
"${PIP}" install -q -e "${SPHERICAL_DIR}[esm2,dragon,dev,plotting]"

# ── 7. Re-pin critical versions ──────────────────────────────────────────────
echo ""
echo "── Step 7: Re-pinning critical versions ──"
"${PIP}" install -q --force-reinstall \
    torch==2.5.1+cu121 \
    torchvision==0.20.1+cu121 \
    torchaudio==2.5.1+cu121 \
    --index-url https://download.pytorch.org/whl/cu121

"${PIP}" install -q --force-reinstall \
    "numpy>=1.26.3,<2.0.0" \
    "setuptools==78.1.1" \
    "cloudpickle>=3.0" \
    "python-dateutil>=2.7" \
    "tornado>=6.1"

"${PIP}" uninstall -q -y pynvml 2>/dev/null || true

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

_check "numpy"             "${PY}" -c "import numpy as np; print(np.__version__)"
_check "torch"             "${PY}" -c "import torch; print(torch.__version__, 'cuda=' + str(torch.cuda.is_available()))"
_check "transformers"      "${PY}" -c "import transformers; print(transformers.__version__)"
_check "esm"               "${PY}" -c "import esm; print('ok')"
_check "radical.asyncflow" "${PY}" -c "import radical.asyncflow; print('ok')"
_check "rhapsody"          "${PY}" -c "import rhapsody; print('ok')"
_check "spherical"         "${PY}" -c "from src.inference.esm2_service import ESM2InferenceService; print('ok')"

echo ""
echo "================================================================="
echo "Setup complete."
echo ""
echo "Activate with:"
echo "  source ${ENV_DIR}/bin/activate"
echo ""
echo "Update service_python in config.yaml to:"
echo "  ${ENV_DIR}/bin/python${PY_VERSION}"
echo "================================================================="
