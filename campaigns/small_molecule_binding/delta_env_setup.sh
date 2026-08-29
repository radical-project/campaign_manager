#!/bin/bash
# =============================================================================
# Small Molecule Binding Campaign — environment setup — Delta HPC (NCSA)
#
# Creates a Python 3.11+ venv and installs all dependencies needed to run
# the SmallMoleculeBindingPipeline under Campaign Manager.
#
# Usage:
#   export SCRATCH=/scratch/<allocation>
#   bash delta_env_setup.sh [--env-dir DIR] [--impress-dir DIR] [--cm-dir DIR] [--python PATH]
#
# Defaults:
#   ENV_DIR     = /u/$USER/ve/impress
#   IMPRESS_DIR = $SCRATCH/$USER/IMPRESS
#   CM_DIR      = $SCRATCH/$USER/campaign_manager
#   python      = auto-detected (python3.12, python3.11, cray-python/3.11.7)
# =============================================================================
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
fi

# ── Require SCRATCH ───────────────────────────────────────────────────────────
if [[ -z "${SCRATCH:-}" ]]; then
    echo "ERROR: set the SCRATCH env var to your allocation scratch root, e.g.:"
    echo "  export SCRATCH=/scratch/<allocation>"
    echo "  bash delta_env_setup.sh"
    exit 1
fi

# ── Defaults / arg parsing ────────────────────────────────────────────────────
ENV_DIR="${ENV_DIR:-/u/${USER}/ve/impress}"
IMPRESS_DIR="${IMPRESS_DIR:-${SCRATCH}/${USER}/IMPRESS}"
CM_DIR="${CM_DIR:-${SCRATCH}/${USER}/campaign_manager}"
BASE_PY_OVERRIDE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --env-dir)      ENV_DIR="$2";      shift 2 ;;
        --impress-dir)  IMPRESS_DIR="$2";  shift 2 ;;
        --cm-dir)       CM_DIR="$2";       shift 2 ;;
        --python)       BASE_PY_OVERRIDE="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

PY="${ENV_DIR}/bin/python"
PIP="${ENV_DIR}/bin/pip"

echo "================================================================="
echo "  ENV_DIR       = ${ENV_DIR}"
echo "  IMPRESS_DIR   = ${IMPRESS_DIR}"
echo "  CM_DIR        = ${CM_DIR}"
echo "================================================================="

# ── 1. Create venv ────────────────────────────────────────────────────────────
echo ""
echo "── Step 1: Creating venv ──"

_find_python() {
    for candidate in python3.12 python3.11 python3 python; do
        local p
        p=$(command -v "${candidate}" 2>/dev/null) || continue
        local ver
        ver=$("${p}" -c "import sys; v=sys.version_info; print(v.major*10+v.minor)" 2>/dev/null) || continue
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
        for mod in python/3.12 python/3.11 cray-python/3.11.7 anaconda3; do
            module load "${mod}" 2>/dev/null || true
            BASE_PY=$(_find_python || true)
            [ -n "${BASE_PY}" ] && echo "  loaded module: ${mod}" && break
        done
    fi
    if [ -z "${BASE_PY}" ]; then
        echo "ERROR: no Python 3.11+ interpreter found."
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

# ── 3. radical.asyncflow (PyPI) ──────────────────────────────────────────────
echo ""
echo "── Step 3: radical-asyncflow (PyPI) ──"
"${PIP}" install -q radical-asyncflow

# ── 4. rhapsody-py[dragon] (PyPI) ────────────────────────────────────────────
echo ""
echo "── Step 4: rhapsody-py[dragon] (PyPI) ──"
"${PIP}" install -q "rhapsody-py[dragon,telemetry]"

# ── 5. IMPRESS (local editable) ───────────────────────────────────────────────
echo ""
echo "── Step 5: IMPRESS (editable) ──"
"${PIP}" install -q -e "${IMPRESS_DIR}"

# ── 6. Campaign Manager (local editable) ──────────────────────────────────────
echo ""
echo "── Step 6: Campaign Manager (editable) ──"
"${PIP}" install -q -e "${CM_DIR}"

# ── 7. PyTorch (CUDA 12.1) ───────────────────────────────────────────────────
echo ""
echo "── Step 7: PyTorch (cu121) ──"
"${PIP}" install -q torch --index-url https://download.pytorch.org/whl/cu121

# ── 8. Additional dependencies ───────────────────────────────────────────────
echo ""
echo "── Step 8: pandas + biopandas ──"
"${PIP}" install -q pandas biopandas

# ── 9. gemmi + LigandMPNN runtime deps ───────────────────────────────────────
# gemmi: RFDiffusion3 outputs .cif.gz; LigandMPNN's ProDy parsePDB() only
#        reads PDB format, so the mpnn task converts CIF.GZ → PDB via gemmi.
# prody, ml-collections, dm-tree: LigandMPNN run.py imports these directly;
#        they are not in the LigandMPNN .venv on Delta (permissions issue),
#        so they are installed here in the shared IMPRESS venv.
echo ""
echo "── Step 9: gemmi + LigandMPNN deps (prody, ml-collections, dm-tree) ──"
"${PIP}" install -q gemmi prody ml-collections dm-tree

# ── 10. ColabFold + JAX (GPU AlphaFold2 inference) ───────────────────────────
# colabfold[alphafold]: installs colabfold_batch and the AF2 inference stack.
# jax[cuda12]: GPU-accelerated JAX (requires CUDA 12 on the compute node).
# After install, download AF2 model weights once (login node has internet;
# weights cache to ~/.cache/colabfold/ and are reused from compute nodes).
echo ""
echo "── Step 10: ColabFold + JAX[cuda12] ──"
"${PIP}" install -q "colabfold[alphafold]"
"${PIP}" install -q "jax[cuda12]"

echo ""
echo "── Step 10b: Pre-download AlphaFold2 model weights ──"
# Keep weights on scratch (not home) to avoid /u quota exhaustion.
# COLABFOLD_CACHE_DIR defaults to $SCRATCH/$USER/.cache/colabfold.
_cf_cache="${COLABFOLD_CACHE_DIR:-${SCRATCH:+${SCRATCH}/${USER}/.cache/colabfold}}"
if [ -z "${_cf_cache}" ]; then
    _cf_cache="${HOME}/.cache/colabfold"
    echo "WARNING: SCRATCH not set — downloading weights to home (may hit quota)."
fi
_cf_params="${_cf_cache}/params"
_cf_marker="${_cf_params}/download_finished.txt"

if [ -f "${_cf_marker}" ]; then
    echo "AF2 weights already present at ${_cf_params} — skipping download."
else
    mkdir -p "${_cf_params}"
    echo "Downloading AF2 params (3.5GB) to ${_cf_params} ..."
    _af2_tar="${_cf_params}/alphafold_params_2021-07-14.tar"
    wget -q --show-progress \
        "https://storage.googleapis.com/alphafold/alphafold_params_2021-07-14.tar" \
        -O "${_af2_tar}"
    tar xf "${_af2_tar}" -C "${_cf_params}" && rm "${_af2_tar}"
    touch "${_cf_marker}"
    echo "AF2 weights downloaded."
fi

# ── 11. PyRosetta (via pyrosetta-installer) ───────────────────────────────────
# Used by packmin.sh, fastrelax.sh, filter_shape.sh.
echo ""
echo "── Step 11: PyRosetta ──"
"${PIP}" install -q pyrosetta-installer
"${PY}" -c "import pyrosetta_installer; pyrosetta_installer.install_pyrosetta()"

# ── 11. Verify ────────────────────────────────────────────────────────────────
echo ""
echo "── Step 11: Verifying installation ──"
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
_check "rhapsody-py"       "${PY}" -c "import rhapsody; print('ok')"
_check "impress"           "${PY}" -c "import impress; print('ok')"
_check "campaign-manager"  "${PY}" -c "from src.campaign import AsyncCampaignManager; print('ok')"
_check "torch"             "${PY}" -c "import torch; print(torch.__version__)"
_check "gemmi"             "${PY}" -c "import gemmi; print(gemmi.__version__)"
_check "prody"             "${PY}" -c "import prody; print('ok')"
_check "ml-collections"   "${PY}" -c "import ml_collections; print('ok')"
_check "dm-tree"          "${PY}" -c "import tree; print('ok')"
_check "pandas"            "${PY}" -c "import pandas; print(pandas.__version__)"
_check "colabfold"         "${PY}" -c "import colabfold; print(colabfold.__version__)"
_check "jax"               "${PY}" -c "import jax; print(jax.__version__)"
_check "pyrosetta"         "${PY}" -c "import pyrosetta; print('ok')"

echo ""
echo "================================================================="
echo "Setup complete."
echo ""
echo "Activate with:"
echo "  source ${ENV_DIR}/bin/activate"
echo ""
echo "Run the campaign:"
echo "  export SCRATCH=${SCRATCH}"
echo "  export SBATCH_ACCOUNT=bblj-delta-gpu"
echo "  cd ${CM_DIR}/campaigns/small_molecule_binding"
echo "  mkdir -p runs"
echo "  sbatch delta_sbatch.sh"
echo "================================================================="
