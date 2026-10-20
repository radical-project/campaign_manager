# Campaign Developer Guide

*How to write a campaign: controllers, workflows, budgets, drivers, and YAML configs.*

---

## Table of contents

1. [Minimal example](#minimal-example)
2. [Defining workflows](#defining-workflows)
3. [Writing a controller](#writing-a-controller)
4. [Budget and concurrency](#budget-and-concurrency)
5. [Putting it together — CampaignSpec](#campaignspec)
6. [Reporting scores from a workflow](#reporting-scores-from-a-workflow)
7. [Scheduling policies](#scheduling-policies)
8. [Driver interface](#driver-interface)
9. [Config file reference (YAML)](#config-file-reference)
10. [Data types reference](#data-types-reference)

---

## Minimal example

```python
import asyncio
from aucam import (
    CampaignManager, CampaignSpec, CampaignBudget,
    WorkflowConfig, CampaignController, WorkflowReporter,
)

async def my_simulation(reporter: WorkflowReporter) -> None:
    import random
    for step in range(20):
        await asyncio.sleep(0.05)
        score = random.random()
        await reporter.emit(score=score, metrics={"step": step})

class StopAt(CampaignController):
    def __init__(self, n: int):
        super().__init__(
            launch=["sim"],          # keep requesting "sim" until goal
            goal_type="sim",
            goal_n=n,
        )

spec = CampaignSpec(
    name="my_campaign",
    budget=CampaignBudget(max_concurrent_workflows=4, max_concurrency_per_workflow=2),
    controller=StopAt(n=10),
    workflows=[WorkflowConfig(name="sim", block=my_simulation)],
)

cm = CampaignManager()
cm.submit_campaign(spec)
asyncio.run(asyncio.wait_for(cm.run(), timeout=30.0))
```

---

## Defining workflows

A `WorkflowConfig` declares one workflow type.  Every campaign has at least one.  Each run of a workflow type is one workflow execution.

```python
WorkflowConfig(
    name="sim_worker",            # unique within this campaign
    block=my_async_fn,            # block workflow: Python async function  ─── OR ───
    launch_cmd=["python","wf.py"],# subprocess workflow: external executable

    # Expected cost for one full execution — drives budget accounting.
    # Keys: wall_hours (required for proration), cpu_hours, gpu_hours.
    declared_spending={"wall_hours": 0.5, "cpu_hours": 0.5, "gpu_hours": 0.05},

    supports_pause=False,         # True → PAUSE/RESUME signals are delivered
    launch_on_start=True,         # auto-enqueue one instance at campaign start
    max_wall_time_s=300.0,        # auto-cancel if wall time exceeds this (Option A)
    stale_timeout_s=60.0,         # auto-cancel if no emit received after first emit
    env={"MY_VAR": "value"},      # extra env vars injected at launch
    on_completion=callback,       # called with WorkflowResult after each run
)
```

---

### Block workflow — Python async function

The workflow is a Python `async def` that receives a `WorkflowReporter` and calls `reporter.emit()` mid-run to stream scores back to the CM.

```python
async def my_worker(reporter: WorkflowReporter, flow=None) -> None:
    for step in range(n_steps):
        result = run_step(step)
        await reporter.emit(score=result.energy, metrics={"step": step})
```

Every workflow runs as one AsyncFlow block. Declare the `flow` parameter to receive the shared engine and submit compute steps as `@flow` tasks; they then run on the allocation, are cancelled with the workflow, and are charged to it. A `launch_cmd` workflow is a block containing one executable task.

**How `reporter.emit()` reaches the CM** depends on the configured `data_backend`:

| Backend | Where the block runs | Transport |
|---|---|---|
| `LocalDictBackend` | Same process as CM (asyncio task) | In-process dict — no network |
| `RadexDragonBackend` | Dragon `function_task` on a worker node | radex `DragonClient` over Dragon DDict |
| `RhapsodyRedisBackend` | Any node with Redis reachable | `redis-py` over TCP |

The block code is the same in all cases — it just calls `reporter.emit()`.  The CM constructs and injects the correct reporter implementation based on the backend.

---

### Subprocess workflow — external executable

The workflow is an external process launched by `launch_cmd`.  It has no injected reporter object; instead the CM sets environment variables the process can use to reach the backend:

| Variable | Value |
|---|---|
| `CM_WORKFLOW_ID` | Unique ID for this workflow execution |
| `CM_CAMPAIGN_ID` | Name of the campaign |
| `CM_CTX_{KEY}` | One per `LaunchRequest.context` entry (key upper-cased, value as string) |
| *`WorkflowConfig.env`* | Each extra variable as given |
| `CM_DATA_ENDPOINT` | Backend connection string: `host:port` for Redis, serialized DDict descriptor for Dragon.  Not set when `LocalDictBackend` is active. |
| `CM_DATA_BACKEND` | Backend type: `redis` or `dragon`.  Not set when `LocalDictBackend` is active. |

The program can be written in any language. It reports through the campaign's DataBackend, which must be Redis or Dragon (with `LocalDictBackend` a command-line workflow runs but cannot report). Three ways:

**1. The `aucam-report` command** (installed with AuCaM; `python -m src.aucam_client` is the same) — works from shell, C++, Fortran, anything that can run a command:

```bash
aucam-report emit --score 0.82 --metric cpu_hours=1.5 --metric stage=fold --iteration 3
sig=$(aucam-report signal)                 # prints PAUSE or RESUME, or nothing
[ "$sig" = PAUSE ] && aucam-report wait-resume
```

Values are parsed as JSON when possible (`0.82`, `true`, `{"a": 1}`) and kept as text otherwise; `--metrics '{"k": 1}'` passes a whole object. Exit status 2 means the program is not running as a CM workflow (or a value is invalid), 1 a backend error.

**2. The Redis protocol directly**, from any language with a Redis client:

| Key | Value |
|---|---|
| `<CM_WORKFLOW_ID>` | the latest report, a UTF-8 JSON object `{"wf_id", "score", "metrics", "iteration", "emitted_at"}` (`emitted_at` = Unix time; drives stale-reporter detection) — `SET` it on every report |
| `<CM_WORKFLOW_ID>:signal` | `PAUSE` or `RESUME` when CM sends a signal — `GET` it, then `DEL` it. CANCEL is never sent: CM stops the task through AsyncFlow |

**3. Python:** `from src.aucam_client import Reporter` (`emit()`, `poll_signal()`, `wait_for_resume()`). The package imports only `redis` / `radex`, so a compute node does not need the rest of AuCaM. `src.aucam.subprocess_client.SubprocessReporter` is the same class under its old name.

Score and metrics must be JSON values (numbers, strings, lists, dicts). A Python program that wants richer objects is better written as a block workflow.

> These variables are set only for `launch_cmd` workflows. CM runs the program through an inline wrapper (`bash -c 'export …; exec launch_cmd'`), so no file has to be visible on the node where it runs; the program's exit code becomes `WorkflowResult.returncode` (128 + N if it was killed by signal N; 0 = completed, −1 = cancelled). Cancellation is carried out by the execution backend. Block workflows run inside CM's process and get their inputs as arguments instead: the live `reporter`, `flow=` (the shared engine) and `context=` (the `LaunchRequest.context` dict) when the function declares them. CM does not modify its own process environment.

---

### `on_completion` callback

Called by the CM once after each workflow finishes (after `controller.on_complete`).  Use it for side effects such as writing results to a file.

```python
def my_callback(result: WorkflowResult) -> None:
    save_result(result.final_metrics)
```

**Do not call `controller.on_complete()` inside this callback** — the CM already calls it.  Do not `await` inside it.

---

## Writing a controller

Subclass `CampaignController`.  The three most important methods:

### `decide(obs)` — called every policy cycle

```python
def decide(self, obs: dict) -> PolicyResult:
    ...
```

`obs` keys:

| Key | Type | Description |
|-----|------|-------------|
| `queue_snapshot` | `QueueSnapshot` | queued counts by priority and type |
| `budget` | `dict` | this campaign's ledger entry: `limits`, `spent`, `in_flight` (estimate of running workflows), `global_spent` |
| `running` | `dict[str, int]` | `{wf_type: count}` currently active |
| `live_reports` | `dict[str, WorkflowReport]` | latest emit per active wf_id |
| `wf_types` | `dict[str, str]` | `{wf_id: wf_type}` mapping |
| `telemetry` | `TelemetrySnapshot` | node resources, wall times, per-wf consumption |
| `policy_cycles` | `int` | number of cycles elapsed |
| `spent` | `dict[str, float]` | cumulative spending for this campaign so far |

Return a `PolicyResult`:

```python
PolicyResult(
    decisions={wf_id: CampaignDecision.CANCEL},   # per-workflow signals
    launch_requests=[LaunchRequest.make("sim_worker")],
    terminate=False,                               # True → shutdown immediately
    cancel_queued={"sim_worker": 5},               # drop N queued items of this type
)
```

`CampaignDecision` values: `CANCEL`, `PAUSE`, `RESUME`, `NOOP`.

### `on_complete(result)` — called after each workflow finishes

```python
def on_complete(self, result: WorkflowResult) -> None:
    super().on_complete(result)           # keeps base counters accurate
    if result.returncode == 0:
        score = result.final_metrics.get("energy")
        if score is not None:
            self._best = min(self._best, score)
```

### `is_goal_reached()` — return True to stop

```python
def is_goal_reached(self) -> bool:
    return self.completed.get("analysis_worker", 0) >= self._goal_n
```

### `get_summary()` — scientific aggregates for the driver

```python
def get_summary(self) -> dict:
    return {
        "best_score": self._best,
        "n_converged": self.completed.get("analysis_worker", 0),
    }
```

The return value is stored in `CampaignOutcome.summary` and passed to the driver.

### Declarative rules shorthand

For straightforward policies you don't need to override `decide()` — pass rules to the constructor:

```python
ctrl = CampaignController(
    launch=["sim_worker", "analysis_worker"],   # keep one request queued per type
    goal_type="analysis_worker",
    goal_n=50,

    rules=[
        LaunchRule("sim_worker"),               # same as launch= shorthand
        CancelRule(
            workflow_type="sim_worker",
            when=lambda r: r.score is not None and r.score < 0.3,
        ),
    ],
)
```

`LaunchRule` adds one request when the queue has none queued for that type — it keeps exactly one pending request per type at all times, never more.
`CancelRule` cancels any running workflow of that type whose latest report fails the predicate.

### Terminate immediately

`_apply_rules()` (the default `decide()` implementation) sets `terminate=True` as soon as `is_goal_reached()` returns True.  This terminates the **current campaign** only — other campaigns running concurrently under the same `CampaignManager` are unaffected.

**Do not add a "wait for queue to drain" condition.**  `LaunchRule` keeps one pending request per workflow type in the queue at all times, so the queue is never empty while work is in flight.  `_shutdown()` clears the queue and cancels in-flight workflows gracefully within `shutdown_timeout_s`.

---

## Budget and concurrency

```python
CampaignBudget(
    max_concurrent_workflows=4,       # hard cap: no more than N running at once
    max_concurrency_per_workflow=2,   # per-type cap

    # Lifetime spending limit (resource-hours).  The scheduler stops launching
    # once actual spending plus the declared_spending of running workflows
    # reaches it; the campaign ends when its running workflows finish.
    # There is no "resources in use at once" budget — capacity is shared via
    # the allocation (RHAPSODY) and the workflow-count caps above.
    spending={"cpu_hours": 1.0, "gpu_hours": 0.25},
)
```

`spending` is optional (`{}` = unlimited); only `cpu_hours` and `gpu_hours` are charged (`wall_hours` in `declared_spending` is used for prorating).  If a campaign has a `spending` limit but a workflow has no `declared_spending`, the CM warns at startup — that workflow's contribution to the spending total will be zero and it won't count toward the campaign's lifetime cap.

### How spending is computed

Spending for a completed workflow is settled in priority order:

1. **Workflow-reported** — the workflow emitted `cpu_hours` or `gpu_hours` directly via `reporter.emit(metrics={"gpu_hours": 0.04})`.  Most accurate for multi-stage workflows where GPU use is a fraction of wall time.

2. **Prorate** — `declared_spending.X_hours × (actual_wall_h / declared_wall_h)`.  Scales linearly with how long the workflow actually ran relative to its declared wall time.

3. **Zero** — no `declared_spending` set; campaign warning is emitted at startup.

**Option A** (`max_wall_time_s` set): the CM auto-cancels the workflow at the wall time limit, so `actual_wall_h ≤ declared_wall_h` always — the workflow can only underspend its reservation.

**Option B** (no `max_wall_time_s`): the workflow can run past `declared_wall_h`; the prorate ratio exceeds 1.0 and actual spending exceeds declared.  This is intentional — opt in by omitting `max_wall_time_s`.

### Budget reservation model

While a workflow is running, its full `declared_spending` is held as a reservation against the campaign spending limit.  The live check is:

```
completed_spent + sum(declared_spending for each active workflow) ≥ limit → no new launches
```

On completion, the actual prorated cost replaces the reservation.  This is the same conservative model SLURM uses for job scheduling.

### Observability vs enforcement

Actual utilisation is collected into `WorkflowResult.resource_profile` for observability and is not used for budget enforcement.  It requires asyncflow telemetry to be enabled (`GlobalConfig.telemetry.enabled: true`).  `TaskStarted` events do not carry a node assignment, so **the workflow must emit its node as a metric** (e.g. `metrics={"node_id": socket.gethostname()}`) for the CM to correlate `ResourceUpdate` samples to the right workflow.  Without it, `resource_profile` will be empty.  Use `TelemetrySnapshot.node_resources` in `decide()` to inspect live utilisation per node.

Any other spending key (e.g. `llm_tokens`) is not tracked automatically — the workflow must emit it in `reporter.metrics` and the controller must accumulate and enforce it manually.

---

## CampaignSpec

Full specification for one campaign:

```python
CampaignSpec(
    name="fold_sweep",
    budget=CampaignBudget(...),
    controller=MyController(),
    workflows=[
        WorkflowConfig(name="sim_worker", ...),
        WorkflowConfig(name="analysis_worker", ...),
    ],

    # DAG edges: analysis_worker only starts after sim_worker succeeds
    dependencies=[
        WorkflowDependency(
            source="sim_worker",
            target="analysis_worker",
            condition=lambda r: r.returncode == 0,   # default
        ),
    ],

    policy_interval_s=5.0,          # max seconds between policy cycles
    max_queue_depth=100,             # soft warning only — never drops
    shutdown_timeout_s=30.0,        # grace period for in-flight work at shutdown
    data_backend=RadexDragonBackend(descriptor),  # None → LocalDictBackend
    checkpoint_dir="/scratch/ckpts",   # None → no checkpointing
    scheduling_policy=BanditSchedulingPolicy(),   # None → BanditSchedulingPolicy()
)
```

---

## Reporting scores from a workflow

Inside a block workflow, the `reporter` is injected automatically:

```python
async def my_worker(reporter: WorkflowReporter) -> None:
    for step in range(n_steps):
        result = run_step(step)

        await reporter.emit(
            score=result.energy,          # primary scalar surfaced to controller
            metrics={
                "step": step,
                "loss": result.loss,
                "node_id": socket.gethostname(),    # enables wf_node tracking in telemetry
                "cpu_seconds": result.cpu_s,        # surfaced in running_consumption
            },
            iteration=step,
        )

    # At the end of the workflow, emit actual spending for Path 1 accounting.
    # This takes priority over the prorate fallback and is the right approach
    # for multi-stage workflows where GPU use is a fraction of total wall time.
    await reporter.emit(
        metrics={
            "gpu_hours": total_gpu_seconds / 3600.0,
            "cpu_hours": total_cpu_seconds / 3600.0,
        },
    )

        # PAUSE/RESUME support (only if WorkflowConfig.supports_pause=True)
        decision = await reporter.poll()
        if decision == CampaignDecision.PAUSE:
            save_checkpoint()
            await reporter.wait_for_resume()
```

`emit()` is a no-op if the backend is unavailable — workflows must not crash on reporter failure.

---

## Scheduling policies

| Class | Description |
|-------|-------------|
| `BanditSchedulingPolicy` | UCB1 bandit — default; learns which workflow types give better outcomes |
| `FIFOSchedulingPolicy` | Pure queue order |
| `ADRSchedulingPolicy` | ADR engine hook (stub, falls back to FIFO) |
| `LLMSchedulingPolicy` | LLM-guided ranking (stub, falls back to FIFO) |

A policy instance belongs to one campaign: give each `CampaignSpec` its own instance (or leave `scheduling_policy=None` for a fresh `BanditSchedulingPolicy`). CM rejects a spec whose policy instance is already used by another campaign, so campaigns never share learned scheduling state.

---

## Driver interface

### Legacy API (all campaigns submitted before run)

```python
cm = CampaignManager(global_budget=GlobalBudget(max_concurrent_campaigns=2))
cm.submit_campaign(spec_a)
cm.submit_campaign(spec_b)
asyncio.run(cm.run())
```

### Dynamic driver API (campaigns submitted at runtime)

```python
async def my_driver(cm: CampaignManager) -> None:
    try:
        for spec in generate_specs():
            result = await cm.submit(spec)
            if result.accepted:
                outcome = await result.wait()       # blocks until campaign finishes
                print(f"{outcome.campaign_name}: goal={outcome.goal_reached}")
                print(f"  spent={outcome.spent}")
                print(f"  summary={outcome.summary}")   # from controller.get_summary()
            else:
                print(f"Rejected: {result.reason}")
    finally:
        cm.driver_done()              # signal: this driver has no more submissions

async def main():
    cm = CampaignManager(
        global_budget=GlobalBudget(spending={"cpu_hours": 2.0})
    )
    cm.expect_driver()                # MUST call before create_task
    asyncio.create_task(my_driver(cm))
    await cm.run()
```

**`cm.expect_driver()` must be called before `asyncio.create_task()`.**  Without it, `cm.run()` auto-closes the queue before the driver task has a chance to submit anything.

**`cm.driver_done()` must be called when the driver has no more submissions** — use a `finally` block so it fires even on exception.  When all registered drivers have called `driver_done()`, the CM closes the submission queue and `run()` exits once all running campaigns finish.

The driver does not need to `await result.wait()` before submitting the next campaign.  Omitting the `await` submits campaigns concurrently — the CM runs them in parallel (subject to global concurrency limits) and the driver can collect outcomes later or ignore them entirely.

### Global budget admission check

Before a campaign starts, the manager checks:

```
already_spent (actual, from completed campaigns)
+ committed   (declared limits of all admitted campaigns — running + queued-to-start)
+ incoming    (new campaign's declared limit)
> cap  →  reject
```

A campaign with no declared `spending` (`spending={}`) always passes the spending check.  It may still consume budget; see the [architecture overview](architecture-overview.md#what-happens-at-budget-exhaustion) for the implications.

### `CampaignOutcome` fields

```python
outcome.campaign_name: str
outcome.completed:   dict[str, int]    # wf_type → successful count
outcome.cancelled:   dict[str, int]
outcome.failed:      dict[str, int]
outcome.elapsed_s:   float
outcome.goal_reached: bool
outcome.spent:       dict[str, float]  # actual resource consumption
outcome.summary:     dict              # controller.get_summary()
```

---

## Config file reference

The config-driven runner pattern (`run_campaign.py` in the dummy campaign) uses two YAML files.  CLI flags override YAML values.

### GlobalConfig YAML

Controls the `CampaignManager` and driver loop.

```yaml
engine: dragon            # dragon | concurrent (default: concurrent)
                          # selects the execution backend (how workflow tasks are launched)

data_backend: auto        # auto | dragon | redis | local (default: auto)
                          # auto → matches engine (dragon→dragon, concurrent→local)
                          # dragon: RadexDragonBackend via radex DragonClient over DDict
                          # redis:  RhapsodyRedisBackend via redis-py (requires redis-server)
                          # local:  LocalDictBackend — in-process, no external services

redis_server_path: ~      # path to redis-server binary; required when data_backend=redis
                          # redis-server is NOT pip-installable — build from source or install
                          # via OS package manager (e.g. conda install -c conda-forge redis)

asyncflow_blocks: true    # dispatch block workflows through asyncflow

verbose: false

scheduling:
  policy: bandit          # bandit | fifo | adr | llm

global_budget:
  max_concurrent_campaigns: ~          # null = unlimited
  max_total_concurrent_workflows: ~
  spending:
    cpu_hours: 0.120      # global lifetime cap across all campaigns
    gpu_hours: ~          # null = unlimited

telemetry:
  enabled: true
  dir: ~                  # set via --telemetry-dir CLI flag

driver:
  enabled: true
  num_campaigns: 3        # driver submits this many campaigns
  max_concurrent: 2       # run at most N campaigns in parallel
  delay_s: 5.0            # seconds between campaign submissions
```

### CampaignConfig YAML

Template used by the driver to build each `CampaignSpec`.

```yaml
controller:
  goal_analyses: 8        # successful analysis_worker completions = goal
  score_floor: 0.4        # CancelRule threshold

budget:
  max_concurrent: 3
  max_concurrency_per_workflow: 2
  spending:
    cpu_hours: 0.042      # declared per-campaign limit for admission check
    gpu_hours: ~

# Declared spending per workflow arm.
# Keys: wall_hours (required for proration), cpu_hours, gpu_hours.
# Budget is reserved at launch using these values and settled (prorated) at completion.
# Option A: set max_wall_time_s → actual ≤ declared (workflow auto-cancelled at limit).
# Option B: omit max_wall_time_s → overrun allowed; actual may exceed declared.
workflows:
  fast_spending:
    wall_hours: 0.5       # expected wall time; used as proration denominator
    cpu_hours:  0.5
    gpu_hours:  0.05      # GPU used for training only (~few min out of 30)
  slow_spending:          # omit to auto-derive as fast_spending × slow_step_multiplier
    wall_hours: 1.5
    cpu_hours:  1.5
    gpu_hours:  0.15

policy_interval_s: 0.5
shutdown_timeout_s: 10.0
checkpoint_dir: ~         # null = no checkpointing
```

### CLI overrides

| Flag | Overrides |
|------|-----------|
| `--global-config PATH` | load GlobalConfig YAML |
| `--campaign-config PATH` | load CampaignConfig template |
| `--telemetry-dir PATH` | `telemetry.dir` |
| `--num-campaigns N` | `driver.num_campaigns` |
| `--spending-cpu-hours A,B,C` | per-campaign `spending.cpu_hours`; comma-separated list, one value per campaign; prefer setting this in YAML |

---

## Data types reference

```python
from aucam import (
    # Enums
    ScoreDirection,       # HIGHER | LOWER
    WorkflowStatus,       # PENDING | RUNNING | PAUSED | COMPLETED | FAILED | CANCELLED
    CampaignDecision,     # CANCEL | PAUSE | RESUME | NOOP
    LaunchPriority,       # CRITICAL | HIGH | NORMAL | LOW
    SchedulingDecision,   # LAUNCH | HOLD | DROP

    # Workflow
    WorkflowResult, WorkflowReport, WorkflowReporter,

    # Campaign spec
    CampaignSpec, CampaignBudget, WorkflowConfig,
    WorkflowDependency, LaunchRequest, QueueSnapshot,

    # Controller
    CampaignController, PolicyResult, Goal, CancelRule, LaunchRule,

    # Scheduling
    SchedulingPolicy, SchedulingContext,
    BanditSchedulingPolicy, FIFOSchedulingPolicy,
    ADRSchedulingPolicy, LLMSchedulingPolicy, DefaultSchedulingPolicy,

    # Manager / driver
    CampaignManager, GlobalBudget, SubmitResult, CampaignOutcome,

    # Telemetry
    TelemetrySnapshot, NodeResources,

    # Backends
    DataBackend, LocalDictBackend,
    RadexDragonBackend,       # HPC: radex DragonClient over Dragon DDict
    DragonDictBackend,        # alias for RadexDragonBackend (backwards compat)
    RhapsodyRedisBackend,     # Redis: redis-py over a Rhapsody-managed redis-server

    # Budget / checkpointing / profiling
    BudgetAllocator, CampaignCheckpoint,
    checkpoint_path, read_checkpoint, write_checkpoint,
    BlockProfiler, TaskProfiler,
)
```

**`TelemetrySnapshot`**

| Field | Type | Description |
|-------|------|-------------|
| `running_wall_s` | `dict[str, float]` | wall-time seconds per active wf_id |
| `node_resources` | `dict[str, NodeResources]` | CPU/GPU/memory per node |
| `spent` | `dict[str, float]` | cumulative completed spending this campaign |
| `running_consumption` | `dict[str, dict[str, float]]` | live resource use per wf_id |
| `wf_node` | `dict[str, str]` | wf_id → node key |

**`QueueSnapshot`**

| Field | Type | Description |
|-------|------|-------------|
| `total_queued` | `int` | items in queue |
| `by_priority` | `dict[str, int]` | counts by `LaunchPriority` name |
| `by_type` | `dict[str, int]` | counts by workflow type |
| `oldest_wait_s` | `float` | age of the oldest item |
