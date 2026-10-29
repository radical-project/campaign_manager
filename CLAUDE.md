# CLAUDE.md

Guidance for Claude Code in this repository.

## Read first

- `docs/architecture-overview.md` — what AuCaM does and the
  **design decisions** behind it. Don't change a decision listed there without
  asking.
- `docs/known-gaps.md` — what is planned or not implemented yet, with how it
  should work. Read it before working on budgets, checkpointing, command-line
  reporting, cancellation or campaign exit; update it when you implement or
  fix an entry.
- `docs/budget.md` — budget and spending behaviour as implemented.
- The design originals are Google Docs owned by the team
  ([AuCaM Design](https://docs.google.com/document/d/16oeg4FGlew5XUsdDedk4V3qdui3LhJqB_qwxlIn4C8Q/edit),
  [Detailed Design](https://docs.google.com/document/d/1aIEElgo-jP7h95SJR1cfMa-pHe4bk-zvnHtNxuryWC8/edit)).
  If code or a request conflicts with the documented design, point out the
  conflict instead of silently diverging.

## Rules

- Budgets are spending only (resource-hours or any other dimension, one ledger:
  `BudgetAllocator`). Don't add a "resources in use at once" budget; capacity is
  limited by the allocation and the workflow-count limits.
- Only the `CampaignController` decides to cancel workflows (plus CM's
  automatic stale-reporter and wall-time cancels).
- One scheduling-policy instance per campaign; no cross-campaign learning.
  Results reach other campaigns only through the driver.
- Every workflow runs as one AsyncFlow block (`launch_cmd` = a block with one
  executable task).
- Terms: CampaignManager (CM), driver API (`submit()` → `SubmitResult` →
  `CampaignOutcome`), GlobalBudget, CampaignBudget, `declared_spending`,
  CampaignController, PolicyResult, WorkflowQueue, SchedulingPolicy,
  WorkflowReporter, DataBackend.

## Traps

- Imports are `from src.aucam import ...`.
- A `WorkflowDependency` target needs `launch_on_start=False`, or it starts
  immediately.
- A Python workflow gets `reporter`, `flow` and `context` only if its
  function declares parameters with exactly these names.
- `_on_done` (in `_launch`) is a synchronous done-callback: it cannot await.
- Command-line workflows can report only with the Redis or Dragon backend.
- The built-in `goal_type`/`goal_n` goal ends a campaign with
  `controller_terminate`; `goals_satisfied` is for `Goal` objects.
- `max_queue_depth` only warns.

## Commands

```bash
pip install -e ".[dev]"      # Python ≥ 3.11 (3.10 works without [dragon]); [redis] for Redis + aucam-report
pytest                       # tests needing unreleased radical.asyncflow features skip (known-gaps X1)
pytest -m "not slow"
ruff check --no-fix . && ruff format --check .   # lint (plain `ruff check .` edits files)
python docs/figures/src/architecture.py              # redraw the architecture figure
python campaigns/demo_campaign/doc_figures.py        # redraw the budget/bandit figures (~1 min)
```

`docs/code-walkthrough.md` has the module map and main code paths.

## Campaigns on Delta (NCSA)

`campaigns/ddsim_aucam/README.md` is the quickstart for running the DDSim
campaign. The key steps:

```bash
# 1 — set WORK_DIR; three sibling repos must exist under it:
#     campaign_manager/  DeepDriveSim/  workflow-mini-apps/
export WORK_DIR=/work/nvme/bdyk/<project>/<username>

# 2 — create the VE once (≈ 5 min; builds mpi4py from source)
bash campaigns/ddsim_aucam/delta_env_setup.sh

# 3 — activate (sets DDSIM_DIR, MINIAPPS_PYTHON, LD_LIBRARY_PATH, thread limits)
source campaigns/ddsim_aucam/delta_env_setup.sh

# 4a — run locally (concurrent engine, login node)
cd campaigns/ddsim_aucam
python run_campaign.py --engine concurrent \
    --global-config campaign_configs/global.yaml \
    --campaign-config campaign_configs/campaign.yaml \
    --telemetry-dir aucam-tel-local

# 4b — run on HPC via SLURM
export SBATCH_ACCOUNT=<project>-delta-gpu
mkdir -p campaigns/ddsim_aucam/logs
ENGINE=concurrent sbatch campaigns/ddsim_aucam/delta_sbatch.sh
```

**Delta-specific VE requirements** (already handled by `delta_env_setup.sh`):

- `python3 -m venv --copies` — Dragon cannot execute symlinked binaries on
  Lustre; `--copies` makes `${VE}/bin/python` a real binary.
- mpi4py must be built from source against Cray MPICH (PyPI wheel links
  against `libmpi.so.12`; Cray provides `libmpi_gnu.so.12`):
  `MPICC=${MPICH_DIR}/bin/mpicc pip install --no-binary mpi4py mpi4py`
- `LD_LIBRARY_PATH="${MPICH_DIR}/lib:..."` — Cray MPICH sets `MPICH_DIR` but
  does not add its lib to `LD_LIBRARY_PATH`; sourcing `delta_env_setup.sh`
  does this.
- `OPENBLAS_NUM_THREADS=1`, `OMP_NUM_THREADS=1`, `MKL_NUM_THREADS=1` —
  prevent thread exhaustion when many sim tasks run concurrently.

## Dragon on Delta (NCSA)

- Cold page faults on Lustre can freeze the event loop: use
  `asyncio.to_thread()` for blocking file I/O inside workflows.
- `dragon -m` fails on Delta (FQDN vs short node name). Multi-node launch:
  ```bash
  (cd $NET_DIR && dragon-network-config --output-to-yaml --no-stdout)
  dragon -w ssh --network-config $NET_DIR/slurm.yaml -t tcp run_campaign.py ...
  ```
- Python `logging` goes to stderr (`.err`), `print()` to stdout (`.out`); CM
  budget and spending logs are INFO → `.err`.
- Dragon re-hooks the root logger during `asyncio.run()`, removing any
  handlers added before `_make_backend()`. Fix: call `_reconfigure_logging()`
  after `_make_backend()` to re-add `StreamHandler(sys.stdout)` and
  `FileHandler`. See `campaigns/ddsim_aucam/run_campaign.py` for the pattern.
- Stop `TelemetryManager` before `WorkflowEngine.shutdown()`, or its
  background thread hangs the process. See `run_campaign.py` `try/finally`.
