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
# WHICH MIDDLEWARE THIS VE RUNS. By default, whatever the editables above
# resolve, which is the released line (radical.asyncflow 0.7.0, rhapsody-py
# 0.7.0). Export ASYNCFLOW_REF and/or RHAPSODY_REF to install a git ref over
# the top instead:
#
#   export ASYNCFLOW_REF=feature/policy-resource   # ResourcePolicy, Pool, NodeShape
#   export RHAPSODY_REF=feature/dragon-placement   # _placement_policy on the Dragon backend
#
# Those two branches are where per-task placement exists at all: the released
# 0.7.0 has no `policies` module and no `policy` keyword on
# `WorkflowEngine.create`, so a runner asking for a ResourcePolicy against it
# gets neither. EII.6 sets both; nothing else should need to.
#
# A REF CANNOT BE TOLD FROM THE RELEASE BY ITS VERSION. Both branches declare
# version 0.7.0 -- the same string as the release -- so `rhapsody.__version__`
# prints 0.7.0 either way and a check on it passes on both answers. What
# differs is `direct_url.json`, which pip writes only for a VCS install, and
# that is what the verification block below reads.
#
# The VE is built once: this script exits early when it already exists, so
# changing a ref means removing ${VE} and re-running.
#
# THE MD LABEL STACK (group K). `impress_a.md` is the md_label protocol:
# apo + holo OpenMM, a seeded replicate ensemble, and MM/GBSA over the frames.
# It imports openmm and mdtraj, both of which come from PyPI and are installed
# below. What does NOT come from PyPI is AmberTools, and the protocol cannot
# build a system without it -- see the next paragraph, which is the one thing
# in this script that is still an open question rather than a measurement.
#
# TLEAP IS REQUIRED AND IS NOT INSTALLABLE HERE. K2 builds both legs with
# tleap and K5 builds three dry topologies with it; `antechamber` and
# `parmchk2` are K1's and have already run, because ALR is typed ONCE into a
# committed artifact (IMPRESS/agent/campaigns/data/alr/md/). So what an
# allocation needs is tleap, from one of three places, in this order of
# preference:
#
#   1. an AmberTools module on the cluster  -> export AMBERHOME and the
#      protocol finds `${AMBERHOME}/bin/tleap` without any PATH change;
#   2. a conda-forge `ambertools` environment, with its bin/ on PATH;
#   3. a user build under ${WORK_DIR}.
#
# This script does not guess a module name. It reports where tleap resolved
# from, or MISSING, and the first real md_label run is what settles which of
# the three it is. A guessed `module load amber` that failed would print an
# error and carry on, which is how a setup script lies.
#
# pdbfixer is deliberately NOT installed. The K6 plan listed it, and nothing in
# `impress_a.md` imports it: tleap's `loadpdb` adds hydrogens and refuses a
# residue with missing heavy atoms rather than repairing it. If a design
# complex ever needs repair, that is a change in K2's builder and a line here,
# in that order -- not a package installed against a future.
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

# Empty means "whatever the editables resolve" -- the released line. See the header.
ASYNCFLOW_REF="${ASYNCFLOW_REF:-}"
RHAPSODY_REF="${RHAPSODY_REF:-}"
ASYNCFLOW_URL="${ASYNCFLOW_URL:-https://github.com/radical-cybertools/radical.asyncflow}"
RHAPSODY_URL="${RHAPSODY_URL:-https://github.com/radical-cybertools/rhapsody}"
export ASYNCFLOW_REF RHAPSODY_REF ASYNCFLOW_URL RHAPSODY_URL

# AmberTools, for the MD label protocol's build step. Exported rather than
# merely set: `impress_a.md.system._run_tleap` reads AMBERHOME before falling
# back to PATH, and md_label runs inside a task that inherits this environment
# rather than sourcing this file. Empty is honest -- the protocol then looks on
# PATH and refuses by name if it finds nothing.
AMBERHOME="${AMBERHOME:-}"
export AMBERHOME

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
echo "AMBERHOME   : ${AMBERHOME:-<unset: tleap must be on PATH>}"
echo "ASYNCFLOW   : ${ASYNCFLOW_REF:-<released>}"
echo "RHAPSODY    : ${RHAPSODY_REF:-<released>}"

# What a distribution was installed FROM, not just which version it calls
# itself. Defined above the sourced-return below so delta_sbatch.sh gets it by
# sourcing this file -- one implementation, two callers, because the job log is
# where a run records which middleware it actually had.
asd_middleware_origin() {
    python - "$1" <<'ASD_ORIGIN_PY'
import importlib.metadata as md
import json
import sys

dist = sys.argv[1]
try:
    version = md.version(dist)
except md.PackageNotFoundError:
    print("MISSING")
    raise SystemExit(0)
# pip writes direct_url.json only for a VCS or path install. Its absence is the
# only reliable sign of a PyPI release here, because both placement branches
# declare the release's own version number.
raw = md.distribution(dist).read_text("direct_url.json")
if not raw:
    print(f"{version} (released)")
    raise SystemExit(0)
info = json.loads(raw)
vcs = info.get("vcs_info") or {}
ref = vcs.get("requested_revision") or ""
commit = (vcs.get("commit_id") or "")[:12]
where = info.get("url", "?")
if info.get("dir_info") is not None:
    print(f"{version} (editable {where})")
else:
    print(f"{version} from {where}@{ref or commit or '?'}" + (f" ({commit})" if ref and commit else ""))
ASD_ORIGIN_PY
}

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

# The MD label protocol (group K). openmm is the engine and mdtraj reads the
# trajectories back; both are what `impress_a.md` imports, and neither is
# declared by IMPRESS because the protocol is a toolkit rather than a library
# dependency -- which is exactly why this script has to. Versions proved
# locally on 2026-11-23: openmm 8.6.1, mdtraj 1.11.2.
pip install --quiet openmm mdtraj

# botorch: Model C's GP is botorch's SingleTaskGP. ROME declares neither it nor
# itgt, because the surrogates are an example rather than a library dependency
# -- which is exactly why this script has to.
pip install --quiet botorch
# The middleware refs, last, so an explicit install is the last word on which
# asyncflow and rhapsody this VE holds.
#
# --force-reinstall IS LOAD-BEARING, and this was measured rather than assumed
# (2026-11-22, throwaway 3.13 VE). Both branches declare version 0.7.0, the
# release's own number, so without the flag pip finds the requirement already
# satisfied: it exits 0, prints nothing under --quiet, leaves the release
# installed, and `radical.asyncflow.policies` still does not exist. A run would
# then measure placement against a middleware that has no policy layer while
# this log said a branch had been asked for -- a silent success, which is the
# one failure mode worth a flag nobody can drop.
#
# --no-deps because the editables above already resolved the dependency set,
# including the dragonhpc==0.15.0 the branch's own [dragon] extra pins.
if [ -n "${ASYNCFLOW_REF}" ]; then
    echo "Installing radical.asyncflow from ${ASYNCFLOW_URL}@${ASYNCFLOW_REF} ..."
    pip install --quiet --force-reinstall --no-deps \
        "radical.asyncflow @ git+${ASYNCFLOW_URL}@${ASYNCFLOW_REF}"
fi
if [ -n "${RHAPSODY_REF}" ]; then
    echo "Installing rhapsody-py from ${RHAPSODY_URL}@${RHAPSODY_REF} ..."
    pip install --quiet --force-reinstall --no-deps \
        "rhapsody-py[dragon,telemetry] @ git+${RHAPSODY_URL}@${RHAPSODY_REF}"
fi

if [ "${ROME_FULL:-0}" = "1" ]; then
    echo "Installing ROME with its full dependency set (torch, transformers, trl) ..."
    pip install --quiet -e "${ROME_DIR}"
else
    pip install --quiet --no-deps -e "${ROME_DIR}"
fi

echo ""
echo "=== VE ready ==="
echo "Python      : $(python --version)"
# Origin, not just version: see the header on why 0.7.0 cannot answer this.
echo "asyncflow   : $(asd_middleware_origin radical.asyncflow)"
echo "rhapsody    : $(asd_middleware_origin rhapsody-py)"
# The distribution, never `dragon.__version__`: that attribute is absent in
# dragonhpc 0.13.1 and present in 0.15.0, so reading it turns an older Dragon
# into an AttributeError in the middle of a setup log.
echo "dragon      : $(asd_middleware_origin dragonhpc)"
# What the refs are FOR. Asked of the code rather than of a version string, so
# a run that meant to measure placement cannot report success without it.
echo "placement   : $(python -c '
import importlib.util as u
policies = u.find_spec("radical.asyncflow.policies") is not None
try:
    from radical.asyncflow import WorkflowEngine
    import inspect
    kwarg = "policy" in inspect.signature(WorkflowEngine.create).parameters
except Exception:
    kwarg = False
pin = False
try:
    from rhapsody.backends.execution import dragon as _d
    pin = hasattr(_d, "_placement_policy")
except Exception:
    pass
print("asyncflow policies=%s policy-kwarg=%s | rhapsody _placement_policy=%s"
      % (policies, kwarg, pin))')"
# The MD stack, asked three ways because three different things can be missing:
# the engine, the devices it can see, and the binary that builds a system.
echo "openmm      : $(python -c 'import openmm; print(openmm.__version__)' 2>/dev/null || echo MISSING)"
# A LOGIN NODE'S ANSWER IS NOT A COMPUTE NODE'S. This lists the platforms the
# plugins loaded here; CUDA appearing depends on libcuda being present, which
# on Delta's login nodes it may not be. The protocol runs on `CUDA` in a job
# and on `CPU` in every test, and the job log is where that gets confirmed.
echo "  platforms : $(python -c 'import openmm; print(", ".join(openmm.Platform.getPlatform(i).getName() for i in range(openmm.Platform.getNumPlatforms())))' 2>/dev/null || echo MISSING)"
echo "mdtraj      : $(python -c 'import mdtraj; print(mdtraj.__version__)' 2>/dev/null || echo MISSING)"
# tleap by where it came from, not merely whether it is there: AMBERHOME and
# PATH can disagree, and the protocol prefers AMBERHOME.
echo "tleap       : $(
    if [ -n "${AMBERHOME}" ] && [ -x "${AMBERHOME}/bin/tleap" ]; then
        echo "${AMBERHOME}/bin/tleap (from AMBERHOME)"
    elif command -v tleap >/dev/null 2>&1; then
        echo "$(command -v tleap) (from PATH)"
    else
        echo "MISSING -- md_label cannot build a system; see the header"
    fi)"
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
