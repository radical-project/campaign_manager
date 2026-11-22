#!/bin/bash
# Create and configure the asd_campaign VE on Delta HPC.
#
# Run once to create the VE:
#   export WORK_DIR=<your_work_dir>
#   bash campaigns/asd_campaign/delta_env_setup.sh
#
# Sourced by delta_sbatch.sh to set path variables and activate (no creation).
# Any path can be overridden by exporting it first.
#
# FIVE repos are required under WORK_DIR (clone if missing):
#   campaign_manager — AuCaM (this repo)
#   IMPRESS          — the design and label campaigns (agent/)
#   ROME             — the fine-tuning half
#   autoagent_steered_diffusion — `asteer`, the contracts the three agree on
#   interface-targeting — `itgt`, which Model A's train wrapper imports
#
# It was four until 2026-11-22. The fifth is not optional for the ASD
# surrogates: ROME's `examples/impress/small_molecule_binding` Model A wrapper
# imports `itgt.common.nn.batch`, `itgt.common.scoring.base`,
# `itgt.method2_hotspots.data` and `.train`. Every heavy import in those
# wrappers is function-local, so a missing `itgt` is invisible until a training
# task is already running on a GPU node.
#
# asteer declares no dependencies, so installing it costs nothing but the path.
# It and itgt are installed from a path rather than pinned to a commit for now;
# the pin waits on those repos being reachable from CI.
#
# ROME is installed with --no-deps by default: the prototype trains with
# ROME's DummyTrainer, which needs only numpy/asyncflow/Dragon (already here),
# while ROME's declared dependencies pull torch, transformers, trl, peft,
# pandas and pyarrow — gigabytes for a round that sleeps. Export ROME_FULL=1
# when switching to a real trainer.

: "${WORK_DIR:?WORK_DIR must be set}"

VE="${VE:-${WORK_DIR}/ve/asd_campaign}"
CM_DIR="${CM_DIR:-${WORK_DIR}/campaign_manager}"
IMPRESS_DIR="${IMPRESS_DIR:-${WORK_DIR}/IMPRESS}"
ROME_DIR="${ROME_DIR:-${WORK_DIR}/ROME}"
ASTEER_DIR="${ASTEER_DIR:-${WORK_DIR}/autoagent_steered_diffusion}"
ITGT_DIR="${ITGT_DIR:-${WORK_DIR}/interface-targeting}"
export VE CM_DIR IMPRESS_DIR ROME_DIR ASTEER_DIR ITGT_DIR

# mpi4py needs libmpi.so at runtime; Cray MPICH sets MPICH_DIR but not LD_LIBRARY_PATH.
if [ -n "${MPICH_DIR:-}" ]; then
    export LD_LIBRARY_PATH="${MPICH_DIR}/lib:${LD_LIBRARY_PATH:-}"
fi

# Prevent OpenBLAS/OMP from spawning a thread per core in every task.
export OPENBLAS_NUM_THREADS=1
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1

echo "VE          : ${VE}"
echo "CM_DIR      : ${CM_DIR}"
echo "IMPRESS_DIR : ${IMPRESS_DIR}"
echo "ROME_DIR    : ${ROME_DIR}"
echo "ASTEER_DIR  : ${ASTEER_DIR}"
echo "ITGT_DIR    : ${ITGT_DIR}"

# Sourced: activate and return.  Executed: fall through and build the VE.
[[ "${BASH_SOURCE[0]}" != "${0}" ]] && source "${VE}/bin/activate" && return 0

if [ -d "${VE}" ]; then
    echo "VE already exists — skipping creation."
    exit 0
fi

for dir in "${CM_DIR}" "${IMPRESS_DIR}" "${ROME_DIR}" "${ASTEER_DIR}" "${ITGT_DIR}"; do
    if [ ! -d "${dir}" ]; then
        echo "ERROR: not found: ${dir} — clone it under ${WORK_DIR} first."
        exit 1
    fi
done

echo ""
echo "Creating VE at ${VE} ..."
module load python/3.13.5-gcc13.3.1
# --copies so ${VE}/bin/python is a real binary Dragon can execute.
python3 -m venv --copies "${VE}"
source "${VE}/bin/activate"

pip install --upgrade pip --quiet
pip install --quiet -e "${CM_DIR}[dragon,telemetry]"
# NOT `[aucam]`. That extra is a direct git URL pinned to a commit
# (IMPRESS/agent/pyproject.toml), and a direct-URL requirement differs from the
# local editable's direct_url, so pip replaces the AuCaM installed on the line
# above with a GitHub copy. It costs nothing today -- the pin is an ancestor of
# this branch and its src/ is identical -- but the campaign would then be
# running a distribution nobody is editing, and the day src/aucam diverges
# nothing would say so.
pip install --quiet -e "${IMPRESS_DIR}/agent[dragon,telemetry]"
pip install --quiet -e "${ASTEER_DIR}"

# itgt: Model A's train wrapper imports it. '[ml]' adds torch and
# scikit-learn; the two halves of the extra fail independently, so fall back to
# the bare install rather than leaving itgt uninstalled. Same shape as
# ROME's own examples/impress_r/delta_env_setup.sh.
pip install --quiet -e "${ITGT_DIR}[ml]" || pip install --quiet -e "${ITGT_DIR}"

# botorch: Model C's GP is botorch's SingleTaskGP. ROME declares neither it nor
# itgt, because the surrogates are an example rather than a library dependency
# -- which is exactly why this script has to.
pip install --quiet botorch
if [ "${ROME_FULL:-0}" = "1" ]; then
    echo "Installing ROME with its full dependency set (torch, transformers, trl) ..."
    pip install --quiet -e "${ROME_DIR}"
else
    pip install --quiet --no-deps -e "${ROME_DIR}"
fi

echo ""
echo "=== VE ready ==="
echo "Python      : $(python --version)"
echo "asyncflow   : $(python -c 'import radical.asyncflow; print(radical.asyncflow.__version__)')"
echo "rhapsody    : $(python -c 'import rhapsody; print(rhapsody.__version__)')"
echo "dragon      : $(python -c 'import dragon; print(dragon.__version__)')"
echo "impress-a   : $(python -c 'import impress_a; print(getattr(impress_a, "__version__", "installed"))')"
# itgt and botorch are what Models A and C import; a missing one is otherwise
# only discovered inside a training task on a GPU node.
echo "itgt        : $(python -c 'import importlib.util as u; print("importable" if u.find_spec("itgt") else "MISSING")')"
echo "botorch     : $(python -c 'import importlib.util as u; print("importable" if u.find_spec("botorch") else "MISSING")')"
# Where AuCaM came from: a path means this checkout, a URL means something
# replaced it (see the `[aucam]` note above).
echo "aucam from  : $(python -c 'import json,importlib.metadata as m; d=m.distribution("aucam").read_text("direct_url.json"); print(json.loads(d).get("url","?") if d else "?")' 2>/dev/null || echo "?")"
# `import rome` needs the Dragon runtime, so it is checked inside the job, not here.
echo "rome        : $(python -c 'import importlib.util; print("importable" if importlib.util.find_spec("rome") else "MISSING")')"
