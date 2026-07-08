# Campaign Manager

RADICAL asyncflow-native orchestrator for multi-workflow HPC campaigns. Runs concurrent instances of heterogeneous workflows inside a single `asyncio` event loop, with priority-based scheduling, sliding-window concurrency caps, resource-pool gating, and data-driven dependency signalling.

Workflow groups form an arbitrary directed acyclic graph (DAG) wired entirely through config — linear chains, fan-out, fan-in joins, and diamonds all work without changing workflow code.

---

## Installation

Requires **Python ≥ 3.10**.

```bash
# Core (concurrent backend — local testing, no HPC runtime needed)
pip install -e .

# With Dragon/RADICAL HPC backend
pip install -e ".[dragon]"

# With ADR adaptive scheduling layer
pip install -e ".[adr]"

# With LLM policy (OpenAI-compatible; also needs adr)
pip install -e ".[adr,llm]"

# Full dev setup
pip install -e ".[dragon,adr,dev]"
```

---

## Quick Start

### 1. Run an example campaign

```bash
# Dreamer emulation campaign (no HPC runtime needed — runs locally)
python workflows/dreamer_campaign/run_campaign.py \
    --config workflows/dreamer_campaign/config.yaml

# ESM2/DDSim campaign (requires Dragon + GPU nodes)
dragon -m workflows/esm2_ddsim_campaign/run_campaing.py \
    --config workflows/esm2_ddsim_campaign/config.yaml
```

### 2. Benchmark a campaign across feature configurations

```bash
python workflows/dreamer_campaign/benchmark.py \
    --config workflows/dreamer_campaign/config.yaml \
    --runs 5 --policies none rule bandit --out benchmark_results.json
```

### 3. Visualize results

```bash
python workflows/plotting/plot_dep_timeline.py slurm-XXXXXX.out \
    --config workflows/dreamer_campaign/config.yaml

# Per-campaign benchmark plots (run from the campaign directory):
python workflows/dreamer_campaign/plot_benchmark.py --results benchmark_results.json
python workflows/dummy_campaign/plot_benchmark.py   --results benchmark_results.json
```

---

## Designing a Campaign

A campaign is a DAG of **steps**, each mapped to a **workflow group** — a pool of instances that all run the same workflow class. Dependencies between groups control when each step becomes eligible to start.

**Define a separate step when:**
- The computation has different resource requirements from its neighbor (e.g. CPU screening → GPU refinement)
- You want a gate between them — drop low-value results before spending expensive compute
- You need independent concurrency caps or scheduling priorities

**Domain logic lives in the workflow, not the CM.** The CM has no concept of what a "good result" means. All scoring, gating, and downstream routing lives in `on_replica_done()`:

```python
async def on_replica_done(self, replica_id, cm, final_state):
    if final_state != "done":
        return
    score = compute_score(replica_id, self.config)
    if score > self.config["threshold"]:
        await self._trigger_dependent("next_stage", replicas=1)
```

`campaign_target: N` sets an early-stop threshold on a group; the campaign ends once that group has N finished instances. Set to 0 to run until all instances are exhausted.

### Scheduling policy

The `cm.adr.policy` key selects how the CM adjusts group priorities at runtime. If the optimal priority ordering is fixed and known, `none` (static config priorities) is sufficient. Otherwise `rule` is a good default — it keeps the DAG flowing by boosting groups with pending work. `bandit` learns the best ordering from observed throughput when the bottleneck shifts across runs. `llm` hands decisions to an OpenAI-compatible model when they require external context or a human-readable rationale, with automatic fallback to `rule` on error.

---

## Building a Campaign

### Step 1 — Write a workflow class

All user workflows subclass `BaseWorkflow`. Define **either** entry point — not both:
- `async def run()` — awaited directly in the event loop; use for async-native code
- `def start()` — dispatched via `asyncio.to_thread()`; use for blocking or synchronous code (subprocess calls, HPC job submission, etc.)

```python
from src.campaign import BaseWorkflow

class SimWorkflow(BaseWorkflow):
    workflow_id = "sim"   # used as prefix for instance IDs

    async def run(self, replica_id: str) -> None:
        # self.config     — dict from the group's YAML section (CM keys stripped)
        # self.asyncflow  — shared radical.asyncflow WorkflowEngine
        # self.policies   — Dragon Policy objects for assigned GPUs (empty on concurrent)

        result = await do_simulation(self.asyncflow, self.config)

        # Notify the CM that this instance produced output.
        # The CM routes +1 instance to every downstream group listed in config.
        await self._signal_done()

    async def on_replica_done(self, replica_id, cm, final_state):
        # Optional: runs after run() returns/raises. Can be async or sync.
        if final_state == "done":
            await cleanup()
```

**Injected attributes** (set by the CM before `run()` is called):

| Attribute | Type | Description |
|-----------|------|-------------|
| `self.config` | `dict` | per-group config (CM scheduling keys stripped) |
| `self._cm` | `AsyncCampaignManager` | running CM (`None` in unit tests) |
| `self._group_name` | `str` | name of this group in the CM |
| `self.asyncflow` | `WorkflowEngine` | shared radical.asyncflow engine |
| `self.policies` | `list[Policy]` | Dragon GPU affinity policies (empty on concurrent) |
| `self.engine_dragon` | backend | Dragon backend handle (`None` on concurrent) |

When `required_gpus > 0` the CM also injects into `self.config`:
- `assigned_gpu_ids` — GPU IDs assigned to this instance
- `group_gpu_ids` — all GPUs held by the group right now

**Signalling downstream groups:**

```python
# Broadcast: CM routes +1 instance to every group listing this group in dependencies.
await self._signal_done()

# Explicit: queue a specific number of instances for a named group.
await self._trigger_dependent("analysis", replicas=1)
```

Both are no-ops when `_cm is None`, making workflows safe to unit-test without a running CM.

---

### Step 2 — Write a config YAML

```yaml
# ── Resources ────────────────────────────────────────────────────────────────
resources:
  total_cpus: 128    # 0 = unlimited
  total_gpus: 4      # 0 = unlimited

# ── Execution backend ─────────────────────────────────────────────────────────
engine: concurrent   # "concurrent" (local asyncio) or "dragon" (HPC)

# ── Workflow registry — maps group names to Python classes ────────────────────
workflow_registry:
  sim:      mypackage.SimWorkflow
  analysis: mypackage.AnalysisWorkflow

# ── Workflow groups ───────────────────────────────────────────────────────────
#
# Independent group (replicas: N) — starts immediately on cm.start().
# Dependent group  (no replicas)  — starts at 0; upstream signals add instances.
#
workflows:
  sim:
    replicas:          8     # independent: starts immediately
    concurrency_floor: 2     # minimum guaranteed concurrent instances (Pass 1)
    concurrency_cap:   4     # maximum concurrent instances (Pass 2)
    priority:          10    # higher priority scheduled first
    required_cpus:     4
    required_gpus:     1

  analysis:
    # No "replicas:" → starts at 0; sim's _signal_done() adds instances at runtime.
    dependencies: [sim]
    concurrency_floor: 1
    concurrency_cap:   4
    priority:          8
    required_cpus:     4
    required_gpus:     1
```

Any key not consumed by the CM (everything except the scheduling fields listed below) is forwarded unchanged as `self.config` in the workflow:

```
replicas  dependencies  dependency_threshold  priority
concurrency_floor  concurrency_cap  required_cpus  required_gpus
```

A group may also include `config_file: path/to/extra.yaml` to merge an external YAML; scheduling keys in the main config take precedence.

---

### Step 3 — Wire and run

```python
import asyncio
from src.campaign import AsyncCampaignManager
from src.utils.workflow import load_config

config = load_config("config.yaml")

REGISTRY = {
    "sim":      SimWorkflow,
    "analysis": AnalysisWorkflow,
}

async def main():
    cm = AsyncCampaignManager.from_config(config, REGISTRY)
    await cm.start()   # schedule all groups with replicas > 0
    await cm.wait()    # block until every triggered group finishes
    await cm.close()   # release CM resources (does NOT shut down asyncflow)

asyncio.run(main())
```

**With pre-built asyncflow engine** (recommended when you need telemetry):

```python
from radical.asyncflow import WorkflowEngine

async def main():
    backend = ...  # ConcurrentExecutionBackend or DragonExecutionBackendV3
    asyncflow = await WorkflowEngine.create(backend)
    telemetry = await asyncflow.start_telemetry(output_dir="telemetry-results")

    cm = AsyncCampaignManager.from_config(config, REGISTRY, asyncflow=asyncflow)
    await cm.start()
    await cm.wait()
    await cm.close()

    await telemetry.stop()
    await asyncflow.shutdown()
```

**Synchronous wrapper** (no async context required):

```python
from src.campaign import CampaignManager   # thin thread-backed sync wrapper

cm = CampaignManager.from_config(config, REGISTRY)
cm.start()
cm.wait()
cm.close()
```

---

## DAG Topologies

Groups and their `dependencies` form a directed acyclic graph. Any shape works:

| Topology | Config | Signal method |
|----------|--------|---------------|
| **Chain** `a → b → c` | each group lists its single upstream | `_signal_done()` |
| **Fan-out** `a → {b, c, d}` | `b`, `c`, `d` each list `a` | `_signal_done()` routes +1 to all |
| **Fan-in / join** `{a, b} → c` | `c: dependencies: [a, b]` | both `a` and `b` call `_signal_done()`; `c` starts only when all upstreams are ready |
| **Diamond** `a → {b, c} → d` | `d: dependencies: [b, c]` | `a` calls `_signal_done()`; `d` waits for both `b` and `c` |

Use `_trigger_dependent()` instead of `_signal_done()` when the workflow needs to decide at runtime which group to trigger and how many instances to spawn — for example, when only a subset of results should proceed, or when the count depends on output:

```python
# Trigger one downstream instance per result that passes a threshold.
for result in results:
    if result.passes_gate:
        await self._trigger_dependent("downstream", replicas=1)
```

`_signal_done()` is simpler and sufficient when the DAG structure alone determines routing — it always triggers exactly one instance in every group that lists this group under `dependencies`.

---

## Scheduling

The CM runs a **two-pass greedy scheduler** on every state change (instance start, finish, or signal):

1. **Pass 1** — guarantee `concurrency_floor` slots for all eligible groups, highest `priority` first. Set `concurrency_floor: 0` to skip this pass for a group.
2. **Pass 2** — fill remaining capacity up to `concurrency_cap`, highest `priority` first.

Both passes gate on `ResourcePool.can_fit()`. A group is **eligible** when every dependency is **ready** — either an upstream called `_signal_done()`, or `dep.finished_replicas >= dependency_threshold` (default 1, count-based fallback).

---

## Optional Features

Enable per-campaign via `cm.features` flags in config:

```yaml
cm:
  features:
    backpressure: true   # hysteresis queue-depth controller per edge
    sharder:      true   # buffered, priority-ranked batch dispatch
    monitor:      true   # periodic health checks + drift alerts
  monitor_interval_s: 30
```

| Feature                  | File                   | What it does |
|--------------------------|------------------------|--------------|
| `BackpressureNegotiator` | `backpressure.py`      | Per-edge HOLD → THROTTLE → WIDEN hysteresis; throttles dispatch when a downstream queue floods |
| `Sharder`                | `sharder.py`           | Buffers upstream trigger signals and batch-dispatches downstream, ranked by surrogate score; `stratify: soft\|strict\|off` |
| `Monitor` / `DriftEvent` | `monitor.py`           | Periodic health table, stall detection, budget-burn and pass-through drift alerts  |
| `Triage`                 | `triage.py`            | Per-candidate RUN / DISCARD / ADVANCE gate using a surrogate model |
| `Surrogate`              | `surrogate.py`         | Cheap score predictor (`Null`/`Random`/`Correlated`) + `RecallTracker` |
| `BudgetController`       | `budget_controller.py` | Proportional feedback loop that nudges Triage cutoffs to keep spend on plan |
| `ReplanningController`   | `replanning.py`        | Reacts to drift events and requests a replan |

Per-group keys for optional features (forwarded to the relevant component):

```yaml
workflows:
  downstream:
    # Backpressure thresholds
    backpressure_high: 200
    backpressure_low:  100

    # Sharder
    sharding:
      target_size:  100
      min_size:     10
      max_size:     200
      stratify:     soft    # soft | strict | off
      dispatch_cap: 500

    # BudgetController
    downstream_input_target: 200   # planned throughput (denominator)
    budget_kp:               0.002
    budget_warmup_min:       20
```

---

## ADR Adaptive Scheduling

The ADR (Autonomous Decision Runtime) layer runs an Observe → Decide → Act loop *alongside* the live CM and nudges group priorities each tick. The CM still owns scheduling, execution, and resources; the ADR layer only observes and advises.

```yaml
cm:
  adr:
    policy: rule       # none | rule | bandit | llm
    tick_s: 2.0
    # LLM policy (requires pip install -e ".[adr,llm]")
    model:           openai/gpt-4o-mini
    base_url:        https://openrouter.ai/api/v1
    llm_api_key_env: OPENROUTER_API_KEY
    system_prompt_file: prompts/scheduling_system_prompt.txt   # optional
```

| Policy   | Behaviour |
|----------|-----------|
| `none`   | Static `group.priority` from config; no dynamic adjustment |
| `rule`   | `DownstreamFirstPolicy` — terminal stages get highest priority each tick |
| `bandit` | `BanditSchedulingPolicy` — Thompson-sampling; learns from backpressure reward |
| `llm`    | `LLMSchedulingPolicy` — OpenAI-compatible endpoint; falls back to `rule` on error |

Requires `pip install -e ".[adr]"` (`llm` policy also needs `".[llm]"`).

To drive the ADR loop alongside an existing CM in code:

```python
from src.campaign.adr.operator import CampaignOperator, run_supervised
from src.campaign.adr.policies import make_scheduling_policy

op = CampaignOperator(cm)
make_scheduling_policy(op, kind="rule")   # or "bandit" / "llm"
await run_supervised(cm, op)              # replaces await cm.wait()
```

---

## Telemetry and Visualization

```yaml
# In config.yaml
telemetry:
  collect_telemetry: true
  telemetry_dir: telemetry-results
  resource_poll_interval: 0.5
```

Campaign timelines and benchmark comparisons live in two places:

- **`workflows/plotting/`** — generic timeline tool usable with any campaign ([README](workflows/plotting/README.md))
- **`workflows/<campaign>/plot_benchmark.py`** — campaign-specific benchmark plots

```bash
# Gantt chart with dependency arrows (short campaigns)
python workflows/plotting/plot_dep_timeline.py slurm-XXXXXX.out

# Gantt chart with simulation stats (long/Dreamer campaigns)
python workflows/plotting/plot_timeline.py slurm-XXXXXX.out

# ADR policy comparison — run from inside the campaign directory
cd workflows/dreamer_campaign
python plot_benchmark.py --results benchmark_results.json
```

---

## Campaign Examples

| Campaign             | Location                         | Description |
|----------------------|----------------------------------|-------------|
| Dummy minimization   | `workflows/dummy_campaign/`      | Minimal two-stage search → refine example; no external dependencies — good starting point for new campaigns |
| Dreamer emulation    | `workflows/dreamer_campaign/`    | Multi-stage emulation campaign using `radical.dreamer`; runs locally, no HPC required |
| ESM2 / DDSim         | `workflows/esm2_ddsim_campaign/` | Real campaign: DDMd MD simulation + ESM2 protein embedding inference on GPU nodes via Dragon |

---

## Module Layout

```
src/campaign/
├── campaign_manager.py   # AsyncCampaignManager — core orchestrator
├── base_workflow.py      # BaseWorkflow — user workflow base class
├── types.py              # _WorkflowInfo, ResourcePool, WorkflowStats, CampaignState
├── scheduler.py          # SchedulerMixin — two-pass greedy scheduling
├── executor.py           # ExecutorMixin — instance launch/completion/GPU assignment
├── monitor_mixin.py      # MonitorMixin — periodic health checks
├── gpu.py                # detect_gpus(), find_gpus()
├── sync_wrapper.py       # CampaignManager — synchronous wrapper
│
│   # ── Optional features ──
├── backpressure.py       # BackpressureNegotiator
├── sharder.py            # Sharder, ShardingSpec
├── bandit.py             # SchedulingBandit (used by ADR BanditSchedulingPolicy)
├── triage.py             # Triage — RUN / DISCARD / ADVANCE gate
├── surrogate.py          # Surrogate models + RecallTracker
├── budget_controller.py  # BudgetController
├── replanning.py         # ReplanningController
├── candidate_log.py      # CandidateLog, CandidateHistory
├── monitor.py            # Monitor, DriftEvent
├── profiles.py           # ProfileWeights — candidate ranking profiles
├── metrics.py            # CampaignMetrics — in-process event recording
│
├── plan/
│   ├── schema.py         # CampaignPlan, StageSpec, EdgeSpec, SurrogateSpec, ...
│   └── loader.py         # load_plan() — auto-detects flat vs structured config
│
└── adr/                  # ADR adaptive scheduling layer
    ├── view.py           # CampaignView — observation dict + levers
    ├── operator.py       # CampaignOperator + run_supervised()
    ├── policies.py       # DownstreamFirst / Bandit / LLM policies
    ├── telemetry.py      # TelemetrySubscriber
    └── recorder.py       # PolicyRecorder — per-cycle decision JSONL

src/utils/
├── workflow.py           # load_config(), _expand_env(), find_gpus(), make_policies(),
│                         #   ensure_dir(), export_metrics(), detect_device_type(), ...
└── logger.py             # Colored structured logging

workflows/
├── dreamer_campaign/     # Dreamer emulation campaign; plot_benchmark.py + plot_timeline.py
├── dummy_campaign/       # Minimal two-stage example; plot_benchmark.py
├── esm2_ddsim_campaign/  # ESM2 + DDSim campaign (Dragon/GPU)
└── plotting/             # Timeline tools: plot_dep_timeline.py (deps) + plot_timeline.py (long)
```

---

## API Reference

### `AsyncCampaignManager`

| Method                                                              | Description |
|---------------------------------------------------------------------|-------------|
| `from_config(config, registry, asyncflow=None, engine_dragon=None)` | Build from YAML config dict + `{name: cls}` registry |
| `register_workflow(name, cls, replicas, ...)`                       | Register a workflow group manually |
| `start()`                                                           | Schedule all groups with `replicas > 0`; creates asyncflow engine if not pre-built |
| `wait(timeout=None)`                                                | Async-block until all triggered groups finish; returns `True` on success |
| `close()`                                                           | Release CM resources (does NOT shut down asyncflow) |
| `add_replicas(group_name, n)`                                       | Dynamically extend a group's instance count at runtime |
| `status()`                                                          | Snapshot dict: all group states + `"resources"` key |
| `stats()`                                                           | Per-group `WorkflowStats(replicas_started, replicas_finished)` |

`register_workflow` parameters:

| Parameter           | Default | Meaning |
|---------------------|---------|---------|
| `replicas`          | `1`     | Total instances to run (0 for dependent groups) |
| `concurrency_floor` | `0`     | Guaranteed concurrent minimum |
| `concurrency_cap`   | `0`     | Sliding-window cap (0 → equals `replicas`) |
| `priority`          | `0`     | Scheduling priority (higher = first) |
| `required_cpus`     | `0`     | CPU cores reserved per running instance |
| `required_gpus`     | `0`     | GPU slots reserved per running instance |
| `dep_threshold`     | `1`     | Finished-instance count fallback for dependency readiness |

### `CampaignManager` (sync wrapper)

Thin synchronous wrapper around `AsyncCampaignManager`. Runs a dedicated event loop in a background thread. Same `from_config` / `register_workflow` / `start` / `wait` / `close` / `status` / `stats` API.

### `ResourcePool`

```python
pool = ResourcePool(total_cpus=128, total_gpus=4)
pool.can_fit(cpus=4, gpus=1)   # True / False
pool.allocate(cpus=4, gpus=1)
pool.release(cpus=4, gpus=1)
pool.usage_str()               # "cpus=20/128  gpus=1/4"
```

Setting a total to `0` disables tracking for that type (unlimited).

---

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Skip slow / integration tests
pytest -m "not slow"

# Single file or test
pytest tests/test_campaign_manager.py::TestClass::test_method
```

Async tests use `anyio` with the `anyio_backend` fixture pinned to asyncio. See `tests/` for examples.
