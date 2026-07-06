# Campaign Manager

RADICAL asyncflow-native orchestrator for multi-workflow HPC campaigns.  Runs
concurrent replicas of heterogeneous workflows inside a single `asyncio` event
loop backed by `radical.asyncflow`, with priority-based scheduling,
sliding-window concurrency caps, resource-pool gating, and data-driven
dependency signalling.

The CM is **topology-agnostic**: workflow groups form an arbitrary directed
acyclic graph (DAG) wired entirely through config. It supports linear chains,
fan-out (one group feeding many), fan-in / joins (one group waiting on several
upstreams), and diamonds — not just linear cascades. A *cascade* (the
SPHERICAL "dreamer" antigen-discovery pipeline `s1→s2→s3→s4→s5`) is simply one
common DAG shape; nothing in the scheduler or dependency model assumes
linearity.

---

## Module layout

```
src/campaign/
├── campaign_manager.py   # AsyncCampaignManager — constructor, config loading,
│                         #   group registration, feature wiring
├── base_workflow.py      # BaseWorkflow — user workflow base class
├── types.py              # _WorkflowInfo, ResourcePool, WorkflowStats, CampaignState
├── scheduler.py          # SchedulerMixin — two-pass greedy scheduling
├── executor.py           # ExecutorMixin — replica launch/completion/GPU assignment
├── monitor_mixin.py      # MonitorMixin — periodic health checks
├── gpu.py                # detect_gpus(), find_gpus(), make_policies()
├── sync_wrapper.py       # CampaignManager — synchronous wrapper
│
│   # ── Optional features (enabled via cm.features flags) ──
├── backpressure.py       # BackpressureNegotiator — hysteresis flow control
├── sharder.py            # Sharder, ShardingSpec — batched, ranked dispatch
├── bandit.py             # SchedulingBandit — Thompson-sampling (used by ADR, not the scheduler)
├── triage.py             # Triage — RUN / DISCARD / ADVANCE per-candidate gate
├── surrogate.py          # Surrogate models (Null/Random/Correlated) + RecallTracker
├── budget_controller.py  # BudgetController — burn-ratio feedback on score cutoffs
├── replanning.py         # ReplanningController — drift-triggered replanning
├── candidate_log.py      # CandidateLog, CandidateHistory — upstream result tracking
├── monitor.py            # Monitor, DriftEvent — drift detection
├── profiles.py           # ProfileWeights — candidate ranking profiles
├── metrics.py            # CampaignMetrics — in-process event recording
│
├── plan/                 # Structured campaign-plan schema + loader
│   ├── schema.py         #   CampaignPlan, StageSpec, EdgeSpec, SurrogateSpec, ...
│   └── loader.py         #   load_plan() — structured + legacy config support
│
├── adr/                  # ADR agent layer (enabled via cm.adr) — drives scheduling priority
│   ├── view.py           #   CampaignView — observation + levers (only CM-coupled code)
│   ├── operator.py       #   CampaignOperator + run_supervised()
│   ├── policies.py       #   DownstreamFirst / Bandit / LLM scheduling policies
│   ├── telemetry.py      #   TelemetrySubscriber — folds asyncflow telemetry into observations
│   └── recorder.py       #   PolicyRecorder — per-cycle decision JSONL
└── __init__.py           # re-exports the public API
```

---

## Core concepts

### BaseWorkflow

All user workflows subclass `BaseWorkflow`.

```python
class BaseWorkflow:
    workflow_id: str = "base"   # unique prefix for replica IDs

    def __init__(self, config, _cm, _group_name, asyncflow, policies, engine_dragon): ...

    async def run(self, replica_id: str): ...                          # entry point (override run OR start)
    async def on_replica_done(self, replica_id, cm, final_state): ... # optional hook
    async def _signal_done(self): ...           # broadcast signal to all dependent groups
    async def _trigger_dependent(self, name, replicas=1): ...  # explicit activation of a named group
```

The CM injects six objects at construction time:

| Injected attribute | Type | Purpose |
|--------------------|------|---------|
| `self.config` | `dict` | per-group config section (CM scheduling keys stripped) |
| `self._cm` | `AsyncCampaignManager` | reference to the running CM (`None` in unit tests without a CM) |
| `self._group_name` | `str` | name of this replica's group (used by `_signal_done`) |
| `self.asyncflow` | `WorkflowEngine` | shared `radical.asyncflow` engine |
| `self.policies` | `list[Policy]` | one Dragon `Policy` per assigned GPU (empty on concurrent backend) |
| `self.engine_dragon` | backend handle | Dragon backend; `None` on concurrent |

When GPUs are assigned, the CM also injects two extra keys into `config`:

| Config key | Value |
|------------|-------|
| `assigned_gpu_ids` | list of GPU IDs assigned to this replica |
| `group_gpu_ids` | all GPU IDs held by the group right now (useful for multi-GPU service init) |

### Workflow entry point

Define **either** `run()` or `start()` — not both.  The CM detects which one
is overridden at `register_workflow` time and raises `ValueError` if both or
neither are defined.

### Workflow groups

A **group** is a named pool of replicas of the same workflow class.  Each group
has:

| Field | Meaning |
|-------|---------|
| `replicas` | total replicas to complete (omit / set to 0 for dependent groups) |
| `concurrency_cap` | sliding-window concurrency cap (defaults to `replicas` if 0) |
| `concurrency_floor` | minimum guaranteed concurrent slots (Pass 1 of scheduler) |
| `priority` | higher → scheduled first |
| `required_cpus` | CPU cores reserved from the pool while a replica runs |
| `required_gpus` | GPU slots reserved from the pool while a replica runs |
| `dependencies` | upstream groups; used to route `_signal_done()` and gate scheduling |
| `dependency_threshold` | count-based fallback: N finished replicas in a dep group counts as "ready" (default 1) |

### ResourcePool

The CM maintains a single `ResourcePool` tracking CPU cores and GPU slots.
Setting a total to `0` disables tracking for that type (unlimited).

```
ResourcePool(total_cpus=128, total_gpus=4)
  available_cpus=108  available_gpus=3   ← after some allocations
```

| Method | Description |
|--------|-------------|
| `can_fit(cpus, gpus)` | `True` when the requested amounts are currently available |
| `allocate(cpus, gpus)` | Decrement available counts (on replica start) |
| `release(cpus, gpus)` | Increment available counts (on replica finish) |
| `usage_str()` | `"cpus=20/128  gpus=1/4"` (used/total, tracked types only) |
| `available_str()` | `"cpus=108/128  gpus=3/4"` |
| `as_dict()` | Full snapshot included in `cm.status()["resources"]` |

### GPU assignment

When `required_gpus > 0`, the CM pops GPU IDs from a global free list (FIFO)
and injects them as `assigned_gpu_ids` and `group_gpu_ids` into the replica's
`config`.  A Dragon `Policy(HOST_NAME, gpu_affinity=[...])` is also built and
injected as `self.policies[0]`.  IDs are returned to the free list when the
replica finishes.

---

## Dependency model — arbitrary DAGs

Groups and their `dependencies` edges form a directed acyclic graph. The
scheduler treats every group independently, so any DAG shape works:

| Topology | How to express it | Behaviour |
|----------|-------------------|-----------|
| **Chain** (`a→b→c`) | each group lists its single upstream in `dependencies` | classic cascade |
| **Fan-out** (`a→{b,c,d}`) | `b`, `c`, `d` each list `a` | `a`'s `_signal_done()` routes +1 replica to **all** of them |
| **Fan-in / join** (`{a,b}→c`) | `c: dependencies: [a, b]` | `c` becomes eligible only when **all** upstreams are ready (AND semantics) |
| **Diamond** (`a→{b,c}→d`) | `d: dependencies: [b, c]` | combines fan-out + join; each edge gated independently |

The join (AND) semantics live in `_deps_satisfied_locked` (scheduler.py): a
group is eligible only when *every* entry in its `dependencies` is ready, so a
join stage never starts on a partial set of inputs. Dependency-chain *depth*
(used for depth-ordered scheduling priors) is computed as `1 + max(depth of
deps)`, which is correct for diamonds and joins, not just chains.

> The optional ADR goal metric (`CampaignView`) infers a single "terminal"
> stage as the deepest leaf for its hit-count goal. For a DAG with **multiple**
> terminal outputs, pass `CampaignView(cm, terminal="...")` explicitly to pick
> which leaf the goal tracks (or leave the goal off — it only drives early-stop).

### Two group modes

A workflow group is **independent** or **dependent**, controlled entirely by
the config — no workflow code changes required to switch between modes.

**Independent** — `replicas: N` present; group starts immediately on `cm.start()`.

**Dependent** — `replicas` omitted (defaults to 0); group stays inactive until an
upstream replica signals the CM.  Each signal adds more replicas to the queue;
signals can repeat throughout the lifetime of the upstream run.

### Two signalling methods

#### `_signal_done()` — broadcast, topology-driven

```python
await self._signal_done()
```

Called from `run()` to indicate that this iteration has produced output.
The CM auto-routes the signal to **every group** that lists the caller's group
in its `dependencies` config field, adding +1 replica to each.  The caller
does not need to know downstream group names — the pipeline topology lives
entirely in the config.

Use this for **data-driven fan-out**: one upstream replica fires once per
result, and the CM decides which downstream groups get a new replica based on
the config graph.

```
md ──signal_done()──► CM routes ──► miniapps (+1 replica per signal)
```

#### `_trigger_dependent(name, replicas=N)` — explicit, named

```python
await self._trigger_dependent("downstream_group", replicas=1)
```

Called from `run()` or `on_replica_done()` when the upstream workflow
decides—based on its own logic—to start a specific number of downstream
replicas.  Each call is additive: calling it again queues more replicas.
If the group was already marked done, it is re-opened for scheduling.

Use this when the **calling workflow knows** the target name and controls
exactly how many replicas to spawn per event (e.g. one inference result
triggers exactly one downstream job).

```
inference ──_trigger_dependent("dummy", replicas=1)──► dummy (+1 per result)
```

### Scheduler re-runs on every signal

Every call to `_signal_done()` or `_trigger_dependent()` increments the
target group's `replicas` counter and immediately re-runs the two-pass
scheduler.  If resources are available the new replica starts at once;
otherwise it queues until resources free up.

### Campaign completion

Groups registered with `replicas=0` (dependent groups that were never
triggered) are **excluded** from the all-done check.  The campaign completes
when all groups that were actually triggered have finished, plus all
independent groups are done.

---

## Scheduling model

The CM runs a **two-pass greedy scheduler** on every state change (replica
start, replica finish, `signal_done`, `trigger_dependent`):

1. **Pass 1** — guarantee `concurrency_floor` concurrent slots for all eligible
   groups, highest priority first.
2. **Pass 2** — fill remaining capacity up to `concurrency_cap`, highest priority
   first.

Each pass also gates on `ResourcePool.can_fit()`: a group that has slots under
`concurrency_cap` but cannot be satisfied by the current resource pool is skipped
and a WARNING is emitted.

A group is **eligible** when every dependency group is **ready**:

- **Workflow-driven** (preferred): a dependency group called `_signal_done()`
  at any point during execution (`group.ready = True`).
- **Count-based fallback**: `dep.finished_replicas >= dep_threshold` (default 1).

---

## Authoring a workflow

### Independent workflow

```python
from src.campaign import BaseWorkflow

class SimWorkflow(BaseWorkflow):
    workflow_id = "sim"

    async def run(self, replica_id: str) -> None:
        # self.config  — dict forwarded from YAML workflow section
        # self.asyncflow — shared WorkflowEngine
        # self.policies  — Dragon Policy list (empty on concurrent backend)
        result = await do_simulation(self.asyncflow, self.config)

        # Signal the CM every time a result is ready.
        # CM auto-routes +1 replica to every group in config's dependencies.
        await self._signal_done()
```

### Dependent workflow (topology-driven via _signal_done)

No changes needed in the dependent workflow itself — it just runs normally.
The CM starts it when an upstream `_signal_done()` fires.

```yaml
# config.yaml
workflows:
  sim:
    replicas: 4        # independent: starts immediately
    ...

  analysis:
    dependencies: [sim] # dependent: starts at replicas=0; sim's _signal_done() adds replicas
    ...                  # no "replicas:" key — the count comes from signals at runtime
```

### Dependent workflow (explicit via _trigger_dependent)

Use when this workflow decides the count and the target name based on its
execution logic (e.g. a quality filter on results).

```python
class InferenceWorkflow(BaseWorkflow):
    workflow_id = "inference"

    async def run(self, replica_id: str) -> None:
        results = await run_inference(self.asyncflow, self.config)
        for r in results:
            if r.quality > THRESHOLD:
                # Explicitly queue 1 more replica of the downstream group.
                await self._trigger_dependent("downstream", replicas=1)

    async def on_replica_done(self, replica_id, cm, final_state):
        # on_replica_done fires after run() returns; useful for teardown
        # that should happen once per replica (e.g. releasing shared services).
        ...
```

Rules:
- Define **either** `run()` or `start()` — not both.
- Both `_signal_done()` and `_trigger_dependent()` are no-ops when no CM was
  injected (safe to call in unit tests).
- `on_replica_done` may be `async def` or `def`; the CM handles both.
- Do **not** call `asyncflow.shutdown()` from within a replica — the engine is
  owned by the caller and shut down after `cm.close()`.

---

## Runner pattern

```python
from src.campaign import AsyncCampaignManager

WORKFLOW_REGISTRY = {"sim": SimWorkflow, "analysis": AnalysisWorkflow}

cm = AsyncCampaignManager.from_config(config, WORKFLOW_REGISTRY)
await cm.start()    # schedules all groups with replicas > 0
await cm.wait()     # blocks until every triggered group is done
await cm.close()    # releases CM resources (does NOT shut down asyncflow)
# caller shuts down asyncflow separately, after telemetry is stopped
```

### Pre-built engine (recommended for telemetry)

When the caller builds the asyncflow engine itself (to start telemetry before
the CM runs), pass it at construction time:

```python
asyncflow = await WorkflowEngine.create(backend)
telemetry = await asyncflow.start_telemetry(...)

cm = AsyncCampaignManager.from_config(config, WORKFLOW_REGISTRY, asyncflow=asyncflow)
await cm.start()
await cm.wait()
await cm.close()

await telemetry.stop()
await asyncflow.shutdown()
```

---

## Configuration

```yaml
# ── Cluster resource budget ──────────────────────────────────────────────────
resources:
  total_cpus: 128
  total_gpus: 4

# ── Execution backend ────────────────────────────────────────────────────────
engine: dragon    # "dragon" or "concurrent" (falls back to concurrent if Dragon unavailable)

# ── Telemetry (optional; needs the opentelemetry SDK) ─────────────────────────
telemetry:
  collect_telemetry: true
  telemetry_dir: "telemetry-results"
  resource_poll_interval: 0.5   # seconds between ResourceUpdate events (HPC)

# ── Campaign Manager runtime + ADR agent layer ───────────────────────────────
cm:
  # Optional feature flags (each wires an adaptive component into the scheduler):
  features:
    backpressure: false   # per-edge hysteresis queue-depth controller
    sharder:      false   # buffered, priority-ranked batch dispatch
    monitor:      false   # periodic health checks + drift alerts
  monitor_interval_s: 30

  # ADR agent layer — drives cross-stage scheduling priority each tick.
  # Omit (or policy: none) to use static group.priority only.
  adr:
    policy: rule        # none | rule | bandit | llm   (override with --policy)
    tick_s: 2.0         # operator decision cadence (seconds)
    # LLM policy (policy: llm): any OpenAI-compatible endpoint via instructor
    model:            openai/gpt-4o-mini
    base_url:         https://openrouter.ai/api/v1
    llm_api_key_env:  OPENROUTER_API_KEY
    # system_prompt_file: prompts/scheduling_system_prompt.txt   # user-tweakable

# ── Workflow registry — maps group names to "module.ClassName" ────────────────
workflow_registry:
  md:        my_workflows.MDWorkflow
  miniapps:  my_workflows.MiniAppsWorkflow
  inference: my_workflows.InferenceWorkflow
  dummy:     my_workflows.DummyWorkflow

# ── Workflow groups ──────────────────────────────────────────────────────────
#
# Two modes — controlled by whether 'replicas' is present:
#
#   Independent (replicas: N):
#     Group starts immediately on cm.start().
#
#   Dependent (no replicas / replicas: 0):
#     Group starts at 0 replicas; stays inactive until an upstream replica
#     calls _signal_done() or _trigger_dependent().  Each call is additive —
#     the upstream workflow decides when and how many replicas to add based on
#     its own execution logic.  Calls can repeat across the lifetime of one
#     upstream replica (e.g. once per iteration, once per result).
#
# To switch a dependent group to independent: add 'replicas: N' and remove
# 'dependencies'.  No workflow code needs to change.

workflows:
  md:
    replicas:      2          # independent: starts immediately
    concurrency_floor:  1
    concurrency_cap:  2
    priority:      10
    required_cpus: 4
    required_gpus: 1
    # Each iteration calls _signal_done() → CM routes +1 replica to miniapps.

  miniapps:
    priority:      8
    concurrency_floor:  1
    concurrency_cap:  2
    required_cpus: 4
    required_gpus: 1
    dependencies:  [md]       # dependent: no replicas key → starts at 0
                              # md's _signal_done() adds replicas at runtime

  inference:
    replicas:      8          # independent
    concurrency_floor:  1
    concurrency_cap:  4
    priority:      6
    required_cpus: 4
    required_gpus: 1
    # on_replica_done calls _trigger_dependent("dummy", replicas=1) per result.

  dummy:
    priority:      5
    concurrency_floor:  2
    concurrency_cap:  4
    required_cpus: 4
    required_gpus: 0
    dependencies:  [inference] # dependent: inference triggers via _trigger_dependent

  aggregate:                   # fan-in / JOIN: waits for BOTH branches
    priority:      4
    concurrency_cap:  1
    required_cpus: 2
    dependencies:  [miniapps, dummy]   # eligible only once miniapps AND dummy are ready
```

The example above is itself a small DAG, not a single chain: two independent
branches (`md→miniapps` and `inference→dummy`) that a final `aggregate` group
**joins**. Swap the edges in `dependencies` to express any other DAG shape — no
workflow code changes.

Per-group keys consumed by the CM and stripped before forwarding the rest to
`workflow.config`:

```
replicas  dependencies  dependency_threshold  priority
concurrency_floor  concurrency_cap  required_cpus  required_gpus
```

Any other per-group keys (plus the injected `assigned_gpu_ids` / `group_gpu_ids`)
are passed through untouched as `self.config`. A group may also use
`config_file: "${VAR}/path.yaml"` to merge an external per-workflow YAML
(`${VAR}` expanded at load time); scheduling keys in the main config win.

> This is the **flat** config shape. The CM also accepts a typed **structured
> plan** (`stages:` + `edges:`) resolved by `load_plan()` — see the *Structured
> plan schema* section below. Both produce the same registration form.

---

## API reference

### `AsyncCampaignManager`

| Method | Description |
|--------|-------------|
| `from_config(config, registry, asyncflow=None, engine_dragon=None)` | Build from YAML config dict + `{name: cls}` registry |
| `register_workflow(name, cls, ...)` | Register a workflow group |
| `start()` | Schedule all groups with `replicas > 0`; creates the shared asyncflow engine if not pre-built |
| `wait(timeout=None)` | Async-block until all triggered groups complete; returns `True` on success |
| `close()` | Release CM resources (does NOT shut down asyncflow) |
| `signal_done(group_name)` | Called by `_signal_done()`; adds +1 replica to every group that lists `group_name` in `dependencies` |
| `trigger_dependent(name, replicas, config=None)` | Called by `_trigger_dependent()`; adds `replicas` to the named group and re-opens it if done |
| `add_replicas(group_name, n)` | Dynamically extend a group up to its `configured_replicas` cap |
| `status()` | Snapshot dict of all group states + `"resources"` key |
| `stats()` | Per-group `WorkflowStats(replicas_started, replicas_finished)` |

`register_workflow` key parameters:

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `replicas` | `1` | Total replicas (0 for dependent groups) |
| `concurrency_floor` | `0` | Guaranteed concurrent minimum |
| `concurrency_cap` | `0` | Sliding-window cap (0 → equals `replicas`) |
| `priority` | `0` | Scheduling priority (higher = first) |
| `required_cpus` | `0` | CPU cores reserved per running replica |
| `required_gpus` | `0` | GPU slots reserved per running replica |
| `dep_threshold` | `1` | Finished-replica count fallback for dependency readiness |

### `CampaignManager` (sync wrapper)

Thin synchronous wrapper around `AsyncCampaignManager`.  Runs a dedicated
event loop in a background thread so callers without an async context can use
plain blocking calls.  Same `from_config` / `register_workflow` / `start` /
`wait` / `close` / `status` / `stats` API.

### `BaseWorkflow`

| Attribute / method | Description |
|--------------------|-------------|
| `workflow_id` | class-level string; used as replica ID prefix |
| `config` | dict forwarded from the group's config section (CM keys stripped) |
| `_cm` | reference to the running `AsyncCampaignManager` (`None` if no CM) |
| `_group_name` | name of this group in the CM (used by `_signal_done`) |
| `asyncflow` | shared `WorkflowEngine` |
| `policies` | list of Dragon `Policy` objects for assigned GPUs (empty on concurrent) |
| `engine_dragon` | Dragon backend handle (`None` on concurrent) |
| `_signal_done()` | broadcast signal to CM; adds +1 replica to all downstream groups; no-op without a CM |
| `_trigger_dependent(name, replicas)` | explicitly queue N replicas of a named group; no-op without a CM |
| `on_replica_done(replica_id, cm, state)` | post-replica hook; override as needed |

### Optional feature components

Enabled per-campaign via `cm.features` flags (see CLAUDE.md and the
Configuration section) and wired into the scheduler/executor by the CM.

| Component | File | Role |
|-----------|------|------|
| `Sharder` / `ShardingSpec` | `sharder.py` | Buffer upstream triggers and batch-dispatch downstream, ranked by priority score (stratify `off`/`soft`/`strict`). |
| `BackpressureNegotiator` | `backpressure.py` | Per-edge hysteresis state machine (HOLD → THROTTLE → WIDEN) that throttles dispatch when a downstream queue floods. |
| `Surrogate` | `surrogate.py` | Cheap predictor of a candidate's downstream score (`Null`/`Random`/`Correlated`), plus `RecallTracker`. Used by Triage. |
| `Triage` | `triage.py` | Per-candidate gate: `RUN`, `DISCARD` (low score), or `ADVANCE` (skip compute on confident leads), using the surrogate prediction. |
| `BudgetController` | `budget_controller.py` | Proportional feedback loop on `burn_ratio` vs the plan budget; nudges Triage score cutoffs within plan-set bounds to keep spend on plan. |
| `ReplanningController` | `replanning.py` | Reacts to drift events (e.g. `BUDGET_LOCKED`) emitted by the Monitor and requests a replan. |
| `Monitor` / `DriftEvent` | `monitor.py` | Periodic health checks + drift detection (budget burn, pass-through ratio, surrogate recall). |
| `CandidateLog` / `CandidateHistory` | `candidate_log.py` | Tracks upstream results so the Sharder can rank candidates. |
| `ProfileWeights` | `profiles.py` | Named ranking profiles (score, uncertainty, age, diversity weights). |
| `CampaignMetrics` | `metrics.py` | In-process event recording (timing, BP transitions, scheduling/budget events). |

### ADR agent layer (`adr/`)

The **ADR (Autonomous Decision Runtime) bridge** is the adaptive scheduling
layer, enabled via `cm.adr` (not `cm.features`). A `radical.adr` `Operator`
runs an Observe → Decide → Act loop *alongside* the live CM and nudges its
levers — chiefly cross-stage **`group.priority`** (which the two-pass scheduler
orders by) and, optionally, sharder batch sizes. The CM keeps owning
scheduling, execution, and resources; the ADR layer only observes and advises
(the "sacred boundary"). Select the decision policy with `cm.adr.policy` (or
`--policy {none|rule|bandit|llm}`).

| Component | File | Role |
|-----------|------|------|
| `CampaignView` | `adr/view.py` | The only CM-coupled code: turns `cm.state` into an observation dict (per-stage `running`/`pending`/`starved`/`bp_state`, …) and exposes `set_priority` / `set_batch_size` / `trigger` levers. |
| `CampaignOperator` | `adr/operator.py` | The `radical.adr` Operator (`@observe`/`@act`/`@goals`); `run_supervised(cm, op)` drives it alongside `cm.wait()`. |
| `DownstreamFirstPolicy` (`rule`) | `adr/policies.py` | Deterministic depth-ordered priorities each cycle — the strong default baseline. |
| `BanditSchedulingPolicy` (`bandit`) | `adr/policies.py` | Wraps the Thompson-sampling `SchedulingBandit` (`bandit.py`) as an ADR policy; learns stage value from a backpressure-derived reward. |
| `LLMSchedulingPolicy` (`llm`) | `adr/policies.py` | LLM-driven (OpenAI-compatible via `instructor`); reasons over the full observation. Composed as `Policy(primary=LLM, fallback=rule)`. Prompt is config-tweakable (`cm.adr.system_prompt[_file]`). |
| `TelemetrySubscriber` | `adr/telemetry.py` | Folds live asyncflow telemetry (GPU/CPU/mem util, task latency, fail rate) into the observation on real HPC runs; no-op when telemetry is off. |
| `PolicyRecorder` | `adr/recorder.py` | ADR observer that logs each decision cycle to JSONL for `plot_policy_comparison.py`. |

> `SchedulingBandit` (`bandit.py`) is **not** wired into the CM scheduler — the
> scheduler orders eligible groups purely by `group.priority`. The bandit is
> consumed only by the ADR `BanditSchedulingPolicy` above. Install the layer
> with `pip install -e ".[adr]"` (the `llm` policy also needs `".[llm]"`).

### Structured plan schema (`plan/`)

The CM accepts two config shapes, resolved by `load_plan()` in `plan/loader.py`:

- **Legacy flat** — the `workflows:` dict documented in the Configuration section.
- **Structured** — a typed `CampaignPlan` of `StageSpec` + `EdgeSpec` objects
  (`plan/schema.py`), with `SurrogateSpec`, `BackpressureEdge`, `RetryPolicy`,
  `PilotSpec`, and `ReplanThresholds`. Per-stage fields include
  `campaign_target` (early-stop trigger), `downstream_input_target`
  (BudgetController denominator), `budget_kp`, and `budget_warmup_min`.

`load_plan(source)` auto-detects the shape; `plan_to_workflows_dict(plan)`
flattens a structured plan back to the registration form the CM consumes.
