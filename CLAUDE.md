# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Quick Start

### Installation

```bash
# Core installation
pip install -e .

# With ESM2 model support (torch + transformers)
pip install -e ".[esm2]"

# With Dragon/RADICAL HPC support
pip install -e ".[dragon]"

# Full dev setup (recommended)
pip install -e ".[esm2,dragon,dev]"
```

Requires **Python ≥ 3.10** (union type syntax, structural pattern matching).

### Common Commands

```bash
# Run all tests
pytest

# Run tests with coverage
pytest --cov=src --cov-report=html

# Run a single test file or test
pytest tests/test_campaign_manager.py
pytest tests/test_campaign_manager.py::TestClass::test_method

# Lint and format check
ruff check .
ruff format --check .

# Auto-format code
ruff format .

# Run linting and format with auto-fixes
ruff check . --fix
ruff format .
```

### Key Entry Points

- **Multi-workflow campaign**: `python workflows/run_campaign/esm2_ddsim_campaign/run_campaing.py --config workflows/run_campaign/esm2_ddsim_campaign/config.yaml` *(note: `run_campaing.py` — intentional typo in filename)*
- **Dreamer campaign (single run)**: `python workflows/run_campaign/dreamer_campaign/run_campaign.py --config workflows/run_campaign/dreamer_campaign/config.yaml`
- **Dreamer benchmark (multi-config, N runs)**: `python workflows/run_campaign/dreamer_campaign/benchmark.py --config workflows/run_campaign/dreamer_campaign/config.yaml --runs 5 --out benchmark_results.json`
- **ESM2 inference (standalone)**: `python workflows/esm2_inference/run_esm2_infern.py --config workflows/esm2_inference/config.yaml --mode local`

---

## Architecture Overview

SPHERICAL is an **async-native HPC workflow orchestrator** built on `radical.asyncflow`. The core innovation is **AsyncCampaignManager**, which orchestrates multiple heterogeneous workflow groups (replicas) inside a single Python asyncio event loop with sophisticated multi-stage dependency signalling, adaptive resource scheduling, and optional adaptive batching.

### High-Level Design

```
AsyncCampaignManager (campaign_manager.py)
├── SchedulerMixin (scheduler.py)
│   └── Two-pass greedy scheduler: guarantee concurrency_floor, fill to concurrency_cap
├── ExecutorMixin (executor.py)
│   └── Replica lifecycle: launch, monitor, completion, GPU assignment
├── MonitorMixin (monitor_mixin.py)
│   └── Periodic health checks + drift detection
│
├── BaseWorkflow (base_workflow.py)
│   └── User-defined workflow classes subclass this; override run() or start()
│
└── Optional Features (feature flags in config)
    ├── BackpressureNegotiator (backpressure.py) — per-edge queue depth controller
    ├── Sharder (sharder.py) — batches upstream triggers before downstream dispatch
    ├── Monitor (monitor.py) — detects pass-through & budget burn drift
    └── CandidateLog (candidate_log.py) — tracks upstream results for sharder ranking
```

Adaptive cross-stage scheduling priority is driven by the ADR layer
(src/campaign/adr), not an in-CM bandit — see "ADR bridge" below.

### Core Concepts

#### AsyncCampaignManager

Orchestrates workflow groups with dependencies and resource constraints:

- **Groups**: Named pools of replicas of the same workflow class. Each group has:
  - `replicas`: total count (0 = dependent, wait for trigger)
  - `concurrency_floor` / `concurrency_cap`: concurrent caps
  - `priority`: scheduling priority (higher = first)
  - `required_cpus` / `required_gpus`: per-replica resource reservation
  - `dependencies`: upstream groups that must signal before this group starts

- **Two signalling modes**:
  - `_signal_done()`: broadcast to all downstream groups (topology-driven)
  - `_trigger_dependent(name, replicas=N)`: explicit queue N replicas to a named group

- **Scheduler**: Runs on every state change (replica finish, signal received). Two-pass greedy:
  1. Pass 1: guarantee `concurrency_floor` for all eligible groups (highest priority first)
  2. Pass 2: fill remaining capacity up to `concurrency_cap` (highest priority first)

A group is **eligible** when its dependencies are **ready**:
  - Workflow-driven: dependency called `_signal_done()` (sets `group.ready = True`)
  - Count-based fallback: `dep.finished_replicas >= dep.dependency_threshold`

#### BaseWorkflow

All user workflows subclass `BaseWorkflow`. The CM injects six objects at construction:

| Attribute | Type | Purpose |
|-----------|------|---------|
| `config` | dict | per-group config (CM scheduling keys stripped) |
| `_cm` | AsyncCampaignManager | reference to running CM (`None` in unit tests) |
| `_group_name` | str | name of this group (used by `_signal_done()`) |
| `asyncflow` | WorkflowEngine | shared radical.asyncflow engine |
| `policies` | list[Policy] | Dragon `Policy` per assigned GPU (empty on concurrent) |
| `engine_dragon` | object | Dragon backend handle (`None` on concurrent) |

When GPUs are assigned, two extra config keys are injected:
- `assigned_gpu_ids`: list of GPU IDs for this replica
- `group_gpu_ids`: all GPUs held by the group right now

**Workflow entry points**: Define **either** `async def run()` or `def start()`, not both. The CM detects which is overridden and raises `ValueError` if both or neither are defined. Async coroutines are awaited directly; sync functions run via `asyncio.to_thread`.

Optional hook: `on_replica_done(replica_id, cm, final_state)` — called after entry-point returns/raises; can be async or sync.

#### ResourcePool

Tracks available CPU cores and GPU slots:

```python
pool = ResourcePool(total_cpus=128, total_gpus=4)
pool.can_fit(cpus=4, gpus=1)  # → True/False
pool.allocate(cpus=4, gpus=1)
pool.release(cpus=4, gpus=1)
pool.usage_str()  # → "cpus=20/128  gpus=1/4"
```

Setting total to 0 disables tracking (unlimited).

---

## Campaign Configuration

All campaigns use YAML config files with two sections:

### Resources & Engine

```yaml
engine: concurrent  # or "dragon" for HPC
resources:
  total_cpus: 128
  total_gpus: 4
```

### Workflow Groups

```yaml
workflows:
  sim:
    replicas: 8              # independent: starts immediately
    concurrency_floor: 2
    concurrency_cap: 4
    priority: 10
    required_cpus: 4
    required_gpus: 1
    # other keys forwarded to workflow.config

  analysis:
    priority: 8
    concurrency_floor: 1
    concurrency_cap: 4
    required_cpus: 4
    required_gpus: 1
    dependencies: [sim]      # dependent: starts at 0 replicas
                             # sim's _signal_done() adds replicas at runtime
```

**Key insight**: Switch a group between independent/dependent modes purely through config, no workflow code changes needed.

### Optional Feature Flags

```yaml
cm:
  features:
    backpressure: true   # hysteresis queue depth controller per edge
    sharder: true        # adaptive batch dispatch from trigger buffer
    monitor: true        # periodic health checks + drift alerts
  # Cross-stage scheduling priority is driven by the ADR layer (cm.adr), not an
  # in-CM bandit; the scheduler orders eligible groups by group.priority.
  monitor_interval_s: 30   # tick interval for monitor
  telemetry:
    collect_telemetry: true
    telemetry_dir: telemetry-results
  workflow_registry:
    sim: my_module.SimWorkflow
    analysis: my_module.AnalysisWorkflow
```

---

## Optional Features Deep Dive

### Backpressure (backpressure.py)

Per-edge hysteresis state machine that throttles downstream queue depth:

```yaml
workflows:
  downstream:
    backpressure_high: 200   # queue ≥ 200 → THROTTLE (block new starts)
    backpressure_low: 100    # queue ≤ 100 → WIDEN (dispatch more)
```

Three states (HOLD → THROTTLE → WIDEN → HOLD):
- **HOLD**: normal, neither throttling nor widening
- **THROTTLE**: queue too deep, sharder dispatch returns 0
- **WIDEN**: queue drained, dispatch multiplier increases

### Sharder (sharder.py)

Buffers upstream trigger signals and batch-dispatches downstream, with optional priority ranking:

```yaml
workflows:
  downstream:
    sharding:
      target_size: 100         # nominal batch size
      min_size: 10
      max_size: 200
      stratify: soft           # soft | strict | off
      dispatch_cap: 500        # max replicas to dispatch (drop low-priority candidates)
```

**Stratify modes**:
- `off`: dispatch exactly 1 trigger per cycle
- `soft`: adaptive sizing with tail dispatch (partial batches acceptable)
- `strict`: hold buffer until target_size or upstream done (chemical diversity, etc.)

**Candidate ranking** (via ProfileWeights):
- Score, surrogate prediction, uncertainty, age, diversity (scaffold novelty)

### Monitor (monitor.py, monitor_mixin.py)

Periodic health checks and drift detection:

```yaml
cm:
  features:
    monitor: true
  monitor_interval_s: 30
  replan:
    budget_burn_deviation_pct: 20    # alert if spend > expected + 20%
    pass_through_deviation_pct: 25   # alert if pass-through ratio deviates 25%
    surrogate_recall_floor: 0.90     # alert if surrogate recall < 90%
```

Two monitoring paths:
1. **Reactive** (per replica finish) — low-latency drift check
2. **Periodic** (background task) — full health table, stall detection

### Bandit (bandit.py)

Thompson-sampling multi-armed bandit. `SchedulingBandit` (one Beta arm per
stage; reward = downstream BP state quality) is **no longer wired into the CM
scheduler** — it now runs in the ADR layer as `BanditSchedulingPolicy` (see the
ADR bridge section). The scheduler orders eligible groups purely by
`group.priority`, which the ADR policy drives.

`bandit.py` is retained because the ADR policy imports `SchedulingBandit` /
`BanditArm`; the legacy `shard_bandit` / `resource_bandit` factories are no
longer used by the core (the sharder uses a fixed BP→multiplier mapping).

### Triage + Surrogate (triage.py, surrogate.py)

Per-candidate gate that runs **before** any compute is spent. A `Surrogate`
model (`surrogate.py`: `NullSurrogate`, `RandomSurrogate`, `CorrelatedSurrogate`
— `surrogate_pred ≈ score × 0.9 + noise`) cheaply predicts a candidate's
downstream score. `Triage` (triage.py) then returns one of:

- **RUN** — execute normally
- **DISCARD** — drop candidates below the surrogate cutoffs before they consume resources
- **ADVANCE** — fast-forward high-confidence leads (score ≥ `advance_threshold`), skipping expensive stages

The DISCARD cutoffs live in the stage's `SurrogateSpec` (CM-adjustable); the
ADVANCE bar is `Triage.advance_threshold` (default `inf` → ADVANCE off):

```yaml
# structured plan (plan/schema.py): per-stage surrogate + triage
stages:
  - id: s2_ml_affinity
    advance_threshold: 0.88        # surrogate score above which a lead skips compute
    surrogate:
      score_cutoff: 0.40           # DISCARD: reject upstream score < this
      uncertainty_cutoff: 0.30     # DISCARD: reject surrogate σ > this
      score_cutoff_nudge_bounds: [0.0, 0.6]   # bounds BudgetController may nudge within
```

`RecallTracker` (in surrogate.py) monitors how often the surrogate's ADVANCE
calls would have been correct, feeding the `surrogate_recall_floor` drift check.

### BudgetController (budget_controller.py)

Proportional feedback loop that keeps a stage's spend on plan by nudging its
Triage **score cutoff**. Each finished replica updates `burn_ratio = actual /
(budget × progress)`; if it drifts outside the band the controller raises or
lowers the cutoff (bounded by plan-set `nudge_bounds`).

```yaml
cm:
  # ...
workflows:
  s2_ml_affinity:
    downstream_input_target: 200   # BudgetController denominator (planned throughput)
    budget_kp: 0.002               # proportional gain
    budget_warmup_min: 20          # min finished replicas before nudging starts
```

- `downstream_input_target` — the BudgetController denominator (planned input volume).
- `campaign_target` — **separate** early-stop trigger; the campaign ends when a
  stage reaches this many completions (0 = never early-stop on this stage).

When the cutoff stays bound-locked for K cycles, the Monitor raises a
`BUDGET_LOCKED` drift event → `ReplanningController` (see below).

### Replanning (replanning.py)

`ReplanningController` consumes `DriftEvent`s from the Monitor and decides
whether to request a replan (re-deriving stage priorities, budgets, or
concurrency from current state). Drives the reactive arm of the monitor loop.

### Structured plan schema (plan/)

Campaigns can be expressed either as the legacy flat `workflows:` dict or as a
typed `CampaignPlan` (`plan/schema.py`: `StageSpec`, `EdgeSpec`,
`SurrogateSpec`, `BackpressureEdge`, `RetryPolicy`, `PilotSpec`,
`ReplanThresholds`). `load_plan()` (`plan/loader.py`) auto-detects the shape;
`plan_to_workflows_dict()` flattens a structured plan to the registration form.
(Depth-based warm-start priors `Beta(depth+1, 1)` now live in the ADR
`BanditSchedulingPolicy`'s `warmstart` option, not an in-CM flag.)

### ADR bridge (adr/) — agent-layer scheduling

`src/campaign/adr/` lets a `radical.adr` **Policy** make the campaign's adaptive
scheduling decisions instead of the in-CM bandits. The CM keeps owning
scheduling, execution lifecycle, and resources; a `CampaignOperator` runs the
ADR Run→Observe→Decide→Act loop *alongside* a live CM and only nudges its
levers (priority, batch size, dependent triggers) — the ADR "sacred boundary".

- **adr/view.py**: `CampaignView` — the only CM-coupled code; turns `cm.state`
  into an observation dict and exposes `set_priority` / `set_batch_size` /
  `trigger` levers. Policies/operator depend on `CampaignViewProtocol`, so they
  unit-test against a fake view (no live CM, no engine, no LLM key).
- **adr/operator.py**: `CampaignOperator` (`@observe`/`@act`/`@goals`) +
  `run_supervised(cm, op)` to drive it alongside `cm.wait()`.
- **adr/policies.py**: three interchangeable policies for A/B comparison —
  `DownstreamFirstPolicy` (deterministic rule the bandit had to learn),
  `BanditSchedulingPolicy` (the in-CM `SchedulingBandit` wrapped as a Policy,
  same Thompson-sampling + BP-reward signal), and `LLMSchedulingPolicy`
  (OpenAI-compatible via `instructor`, deps imported lazily). Select with
  `make_scheduling_policy(op, kind="rule"|"bandit"|"llm", …)`; `kind="llm"`
  composes `Policy(primary=LLM, fallback=rule)`.

Install: `pip install -e ".[adr]"` (LLM policy also needs `".[llm]"`). Design
rationale and migration path: `docs/adr_adaptive_decisions.md`.

---

## Key File Organization

### Campaign Manager Core

- **campaign_manager.py**: Main class; constructor, config loading, group registration
- **base_workflow.py**: User-defined workflow base class
- **types.py**: `_WorkflowInfo`, `ResourcePool`, `WorkflowStats`, `CampaignState` data structures
- **scheduler.py**: SchedulerMixin — two-pass scheduling logic
- **executor.py**: ExecutorMixin — replica launch/completion/GPU assignment
- **monitor_mixin.py**: MonitorMixin — periodic health checks
- **gpu.py**: `detect_gpus()` (CUDA/nvidia-smi probe) and `find_gpus()` (Dragon node enumeration) — both degrade gracefully without Dragon/CUDA
- **sync_wrapper.py**: `CampaignManager` — synchronous wrapper around AsyncCampaignManager (thin thread-based bridge)

### Optional Features

- **backpressure.py**: `BackpressureNegotiator` — hysteresis state machine
- **sharder.py**: `Sharder` — buffering and batch dispatch with priority ranking
- **bandit.py**: `Bandit`, `SchedulingBandit` — Thompson-sampling (now consumed by the ADR `BanditSchedulingPolicy`, not the CM scheduler)
- **triage.py**: `Triage`, `TriageDecision` — per-candidate RUN/DISCARD/ADVANCE gate
- **surrogate.py**: `Surrogate` (`Null`/`Random`/`Correlated`), `RecallTracker` — cheap score predictor
- **budget_controller.py**: `BudgetController` — burn-ratio feedback on Triage cutoffs
- **replanning.py**: `ReplanningController` — drift-triggered replanning
- **candidate_log.py**: `CandidateLog`, `CandidateHistory` — tracks upstream results
- **monitor.py**: `Monitor`, `DriftEvent` — drift detection logic
- **profiles.py**: `ProfileWeights`, `PROFILES` — candidate ranking profiles
- **metrics.py**: `CampaignMetrics` — in-process event recording (timing, BP transitions, etc.)
- **plan/schema.py**: `CampaignPlan`, `StageSpec`, `EdgeSpec`, `SurrogateSpec`, ... — typed plan
- **plan/loader.py**: `load_plan()`, `plan_to_workflows_dict()` — structured + legacy config

### Utilities

- **src/utils/logger.py**: Colored structured logging with metrics recording
- **src/utils/workflow.py**: `_expand_env()`, `load_config()`, `find_gpus()`, `make_policies()`
- **src/inference/**: Multi-GPU inference service framework (ESM2 embeddings, HTTP server/client)

### Examples & Workflows

- **workflows/run_campaign/esm2_ddsim_campaign/**: Real multi-workflow campaign (DDMd sim + ESM2 inference)
  - `run_campaing.py`: main entry point
  - `ddmd_workflow.py`: wraps DeepDriveSim DDMd pipeline
  - `inference_workflow.py`: ESM2 client workflow
  - `config.yaml`: multi-stage campaign config
  
- **workflows/run_campaign/dreamer_campaign/**: Emulation campaign using radical.dreamer
  - `dreamer_workflow.py`: simulates task execution in-process
  - `config.yaml`: plan-based campaign (cm-prototype schema)
  - `benchmark.py`: runner with profiling

- **workflows/esm2_inference/**: Standalone ESM2 service
  - `run_esm2_infern.py`: launches inference server with worker pools per GPU

---

## YAML Config Features

### Environment Variable Expansion

All config files support `${VAR}` and `$VAR` shell-style references, expanded at load time by `_expand_env()` in `src/utils/workflow.py`:

```yaml
service_python: "${VE_HOME}/esm2/bin/python"
outdir: "${SPHERICAL_DIR}/workflows/sgdes/output"
```

Unset variables are preserved as literal strings (fail fast with clear `FileNotFoundError`).

### Config File Merging

When a workflow entry has `config_file`, that YAML is loaded and merged (scheduling params in the main config take precedence):

```yaml
workflows:
  inference:
    config_file: inference_specific.yaml  # loaded and merged
    replicas: 4                           # overrides any value in the file
```

---

## Testing

### Test Organization

- **tests/test_campaign_manager.py**: Core CM logic (scheduling, execution, hooks)
- **tests/test_inference_service.py**: Multi-GPU inference orchestration
- **tests/test_server.py**: aiohttp server endpoints
- **tests/test_client.py**: HTTP client interface
- **tests/test_sgdes_workflow.py**: SGDES protein engineering workflow
- **tests/test_logger.py**: Structured logging utilities
- **tests/test_utils.py**: Config loading and GPU helpers

### Test Markers

```bash
# Run only fast tests (skip slow)
pytest -m "not slow"

# Run only integration tests
pytest -m integration

# Run only GPU tests (if available)
pytest -m gpu
```

### Async Tests

All async tests use `anyio` (not `pytest-asyncio` directly). The standard pattern used throughout the test suite:

```python
import pytest
pytestmark = pytest.mark.anyio

@pytest.fixture
def anyio_backend():
    return "asyncio"

async def test_something():
    ...
```

`asyncio_mode = "auto"` in `pyproject.toml` applies to `pytest-asyncio`; the tests themselves rely on `anyio` with the `anyio_backend` fixture pinning execution to asyncio.

---

## Key Design Patterns

### Workflow Authoring Pattern

```python
from src.campaign import BaseWorkflow

class MyWorkflow(BaseWorkflow):
    workflow_id = "my_wf"

    async def run(self, replica_id: str) -> None:
        # Do work
        result = await compute(self.asyncflow, self.config)
        
        # Signal downstream groups
        await self._signal_done()  # broadcast to all dependents
        # OR
        await self._trigger_dependent("specific_group", replicas=1)  # explicit

    async def on_replica_done(self, replica_id, cm, final_state):
        if final_state == "done":
            # cleanup
            pass
```

### Campaign Runner Pattern

```python
from src.campaign import AsyncCampaignManager

WORKFLOW_REGISTRY = {
    "workflow1": Workflow1,
    "workflow2": Workflow2,
}

# Option 1: from config
cm = AsyncCampaignManager.from_config(config, WORKFLOW_REGISTRY)

# Option 2: manual registration
cm = AsyncCampaignManager(engine="concurrent", total_cpus=128, total_gpus=4)
cm.register_workflow("wf1", Workflow1, replicas=4, ...)
cm.register_workflow("wf2", Workflow2, dependencies=["wf1"], ...)

# Run
await cm.start()
await cm.wait()
await cm.close()

# Caller is responsible for asyncflow lifecycle
asyncflow = await WorkflowEngine.create(backend)
# ... start telemetry if needed
cm = AsyncCampaignManager.from_config(config, registry, asyncflow=asyncflow)
# ... run campaign
await telemetry.stop()
await asyncflow.shutdown()
```

### GPU Assignment

When `required_gpus > 0`:
1. CM pops GPU IDs from a global free list (FIFO)
2. Injects `assigned_gpu_ids` and `group_gpu_ids` into replica config
3. Builds Dragon `Policy(HOST_NAME, gpu_affinity=[...])` and injects as `self.policies[0]`
4. Returns IDs to free list when replica finishes

---

## Important Implementation Notes

### No-op Signals in Unit Tests

Both `_signal_done()` and `_trigger_dependent()` are no-ops when `_cm is None`, making workflows safe to unit test without a CM:

```python
# Unit test — no CM injected
wf = MyWorkflow(config={...})
await wf.run("test_0")  # signals are no-ops
```

### Two Signalling Methods Are Mutually Exclusive per Workflow

- If a workflow calls `_signal_done()`, downstream groups are determined entirely by config `dependencies`
- If a workflow calls `_trigger_dependent()`, it explicitly decides which group gets replicas
- Mixing both on the same workflow is allowed but unconventional — `_signal_done()` is simpler for data-driven fan-out

### Scheduler Re-runs on Every State Change

Every replica completion or signal call triggers `_schedule()`, which:
1. Acquires the lock
2. Runs `_schedule_locked()` (two-pass greedy with dependency + resource checks)
3. Fires resulting replica tasks outside the lock

This keeps scheduling immediate and fair across groups.

### Campaign Completion Logic

The CM completes when:
- All groups with `replicas > 0` are finished
- All sharder buffers are empty
- Groups that were never triggered (dependent groups with 0 finished replicas) are excluded from the check

This allows purely dependent groups to remain inactive without stalling the campaign.

---

## Performance Tuning

### Concurrency Caps

- `concurrency_cap`: sliding-window concurrency cap per group
- `concurrency_floor`: guaranteed concurrent slots (priority-ordered across groups in Pass 1)
- If a group has slots but cannot be satisfied by resources, a WARNING is logged

### Backpressure Tuning

High `backpressure_high` + low `backpressure_low` gap = frequent oscillation.  Recommend:
- `high_water ≈ 1.5 × downstream_total`
- `low_water ≈ 0.5 × downstream_total`

### Sharder Target Size

- Too small (e.g., 1): no batching benefit, frequent dispatch overhead
- Too large: causes queue buildup and backpressure throttling
- Recommend: 5–20% of downstream group's total replicas, tuned via A/B testing

### Monitor Interval

- Too small (< 10s): log spam, overhead
- Too large (> 120s): miss transient drifts
- Recommend: 20–60s for typical campaigns; shorter (5–15s) for debugging

---

## Deployment

### Local Testing (Concurrent Backend)

```bash
python run_campaing.py --config config.yaml
```

Uses `radical.asyncflow` ConcurrentExecutionBackend (pure asyncio, no MPI).

### HPC Deployment (Dragon Backend)

```bash
dragon -m workflows/run_campaign/esm2_ddsim_campaign/run_campaing.py \
  --config workflows/run_campaign/esm2_ddsim_campaign/config.yaml
```

Sets `engine: dragon` in config; CM detects and uses DragonExecutionBackendV3.

### Telemetry Visualization

```bash
bash workflows/plot_telemetry.sh \
  workflows/sgdes/telemetry_output/out.jsonl \
  --out-dir plots/sgdes
```

Plots asyncflow native JSONL telemetry to workflow dashboard PNG.

### Campaign Timeline Visualization

```bash
python workflows/run_campaign/plot_cm_timeline.py \
  slurm-17715157.out \
  --config workflows/run_campaign/config.yaml \
  --out replica_timeline.png
```

Parses SLURM log; produces Gantt chart (replicas + resource utilization) + config summary table.

