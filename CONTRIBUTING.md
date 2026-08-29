# Campaign Developer Guide

This guide covers everything you need to build a campaign: constructing and
driving the `AsyncCampaignManager`, writing workflow classes, wiring inter-stage
routing, sharing data between replicas, and adding a scheduling operator for
adaptive control.

---

## `AsyncCampaignManager` — lifecycle API

### Construction

```python
from src.campaign import AsyncCampaignManager as CampaignManager

cm = CampaignManager(engine=asyncflow)
```

`engine` is the workflow execution engine (required). The CM is designed to
work with any compatible engine; `radical.asyncflow.WorkflowEngine` is the
reference implementation used in this repository, but the interface is not
radical-specific — any engine that exposes the same task-submission and
lifecycle API can be substituted.

Optional kwargs set resource limits for the shared pool:

| Kwarg             | Default | Description                                                              |
|-------------------|---------|--------------------------------------------------------------------------|
| `engine`          | —       | Workflow engine instance (required)                                      |
| `engine_dragon`   | `None`  | Dragon backend handle; `None` for the concurrent backend                 |
| `total_cpus`      | `0`     | Total CPUs available to the resource pool (0 = untracked)                |
| `total_gpus`      | `0`     | Total GPUs available to the resource pool (0 = untracked)                |
| `total_memory_gb` | `0.0`   | Total memory available (0 = untracked)                                   |
| `debug`           | `False` | Enable verbose scheduler logging                                         |

### `register_workflow(name, workflow_class, *, replicas, ...)`

Registers a workflow group before the campaign starts. See the full parameter
reference at the end of this document. Must be called before `start()`.

### `await cm.start()`

Starts the campaign scheduler. No replicas run until this is called. Safe to
call once; subsequent calls are no-ops.

### `await cm.wait(timeout=None)`

Blocks until all currently registered replicas complete. Returns as soon as the
internal completion event fires.

> **Multi-phase campaigns**: `wait()` fires after the *current* set of replicas
> drains, not after all future dynamically triggered replicas finish. In phased
> or nested campaigns the CM's internal completion flag is set at that point; any
> new replicas triggered after `wait()` returns must clear it first or the
> scheduler will block. When driving the CM through a scheduling operator,
> `view.reactivate()` handles this — see the [Scheduling Operator](#scheduling-operator) section.

### `await cm.stop()`

Requests a soft stop: no new replicas are started; replicas already in flight
are allowed to finish. Use when a goal is reached mid-campaign.

### `await cm.close()`

Cancels all in-flight replica tasks and releases asyncflow resources. Always
call in a `finally` block after `wait()` or `stop()`:

```python
try:
    await cm.start()
    await cm.wait()
finally:
    await cm.close()
    await asyncflow.shutdown()
```

### `cm.status() -> dict`

Returns a point-in-time snapshot of group state:

```python
gs = cm.status()["groups"]
finished = gs["analysis"]["replicas_finished"]
```

### `cm.metrics() -> CampaignMetrics`

Returns the `CampaignMetrics` object accumulating replica events, shard events,
and decision traces for the current run. Call `cm.metrics().to_dict()` to get a
JSON-serialisable snapshot for plotting or archival:

```python
import json
json.dump(cm.metrics().to_dict(), open("campaign.json", "w"), indent=2)
```

### Typical usage pattern

```python
backend   = await ConcurrentExecutionBackend()
asyncflow = await WorkflowEngine.create(backend)

cm = CampaignManager(engine=asyncflow)
cm.register_workflow("sim",      SimWorkflow,      replicas=50)
cm.register_workflow("analysis", AnalysisWorkflow, replicas=0)

try:
    await cm.start()
    await cm.wait()
finally:
    await cm.close()
    await asyncflow.shutdown()

print(cm.status())
cm.metrics().to_dict()   # save or plot
```

---

## Minimum viable workflow

```python
from src.campaign import BaseWorkflow

class MyWorkflow(BaseWorkflow):
    workflow_id = "my_wf"          # required: unique across all registered classes

    async def run(self, replica_id: str) -> None:
        # Your compute here. Define as async to await directly, or sync to
        # offload via asyncio.to_thread — the executor detects which at runtime.
        await do_the_work(replica_id)
```

Register it:

```python
cm.register_workflow(
    "my_stage",
    MyWorkflow,
    replicas=10,
    config={"param": "value"},
)
```

That's it. Everything else is optional.

---

## `workflow_id` — set a unique class attribute

`BaseWorkflow.workflow_id` defaults to `"base"`. The CM warns if two classes share the same ID because it affects asyncflow task names, Gantt chart labels, and metric keys. Always declare a unique short string:

```python
class SimWorkflow(BaseWorkflow):
    workflow_id = "sim"

class AnalysisWorkflow(BaseWorkflow):
    workflow_id = "analysis"
```

---

## Entry points: `run` vs `start`

Exactly one entry point must be defined — either `run` or `start`. Both
accept sync and async definitions equally: the executor checks
`asyncio.iscoroutinefunction` at dispatch time and either `await`s the method
directly or offloads it via `asyncio.to_thread`. The name you choose does not
affect how the method is dispatched.

The only difference between the two names is how the CM detects them:

| Method                    | How it is detected                                                                    |
|---------------------------|---------------------------------------------------------------------------------------|
| `run(self, replica_id)`   | Present when the class overrides the `BaseWorkflow.run` stub                          |
| `start(self, replica_id)` | Present when the class defines `start` below `BaseWorkflow` in the MRO, so `threading.Thread.start` is never mistaken for a user entry point |

Both receive a single `replica_id: str` of the form `"<group_name>_<idx>"`.

---

## Hook reference

Hooks are instance methods the executor calls after a replica finishes. All are optional — the base implementations are no-ops.

```
Entry point (run / start)
        │
        ▼ [if final_state == "failed"]
on_replica_failed(replica_id, cm)  → bool
        │
        │  returns True  → executor skips auto-retry
        │  returns False → executor retries up to max_retries times
        ▼ [on final outcome only — not intermediate retries]
on_replica_done(replica_id, cm, final_state)
        │
        ▼
_on_completion(replica_id, cm, final_state)  → routing spec or None
```

### `on_replica_failed(replica_id, cm) -> bool`

Called before any retry logic when a replica ends with `final_state="failed"`.

- Return `True` to signal that your hook handled the failure (custom retry, alert, etc.). The executor will **not** attempt its own automatic retry.
- Return `False` (the default) to let the executor retry up to `max_retries` times.

```python
def on_replica_failed(self, replica_id: str, cm) -> bool:
    log.warning("replica %s failed — flagging for manual inspection", replica_id)
    return False  # let the executor retry
```

### `on_replica_done(replica_id, cm, final_state)`

Called once for the **final outcome** of each replica — after the last retry attempt, or immediately after a successful run. Skipped for intermediate retry attempts.

Use this for **side effects**: saving scores, writing results, logging, triggering downstream data.

Can be `async` or sync. The executor detects which and handles accordingly.

```python
async def on_replica_done(self, replica_id: str, cm, final_state: str) -> None:
    if final_state != "done":
        return
    score = self._load_result(replica_id)
    MyWorkflow._scores.setdefault(self._group_name, []).append(score)
    await self._trigger_dependent("analysis", replicas=1, score=score)
```

### `_on_completion(replica_id, cm, final_state) -> routing spec or None`

Called after `on_replica_done`. Used for **declarative routing** — returning a routing spec that tells the CM what group(s) to activate next.

If it returns `None` (the default), the CM falls back to config-based routing: any downstream groups declared as dependents of this group are triggered once enough upstream replicas have finished (see `dependencies` and `dep_threshold` in the parameter reference at the end of this document).

If it returns a non-None value, the CM triggers the named groups directly and ignores the `dependencies` list for this replica.

Supported return shapes:

```python
return None                                        # fall back to config deps
return "analysis"                                  # trigger 1 replica
return ["analysis", "report"]                      # trigger 1 replica each
return {"name": "analysis", "replicas": 2}         # trigger with kwargs
return [{"name": "a"}, {"name": "b", "replicas": 3}]
```

---

## `on_replica_done` vs `_on_completion` — which to use

Both fire per replica at the same point in time. The distinction is **purpose**:

| Hook              | Purpose                                           | Can trigger downstreams?        |
|-------------------|---------------------------------------------------|---------------------------------|
| `on_replica_done` | Side effects: score saving, logging, data writing | Yes (via `_trigger_dependent`)  |
| `_on_completion`  | DAG routing: declare what runs next               | Yes (via return value)          |

**Rule of thumb:** If the downstream group is always the same regardless of what happened, use `_on_completion` — its return value is the declarative DAG edge and the routing logic is clear from a single read. If you need to conditionally trigger different groups based on the score or final_state, use `on_replica_done` with `_trigger_dependent` calls inside it.

Mixing both is fine. `on_replica_done` fires first; `_on_completion` fires second. The CM merges any triggers from both.

Example from `DDMdWrapperWorkflow` — uses only `on_replica_done` because the DAG edge is conditional on success:

```python
async def on_replica_done(self, replica_id: str, cm, final_state: str) -> None:
    if final_state != "done":
        return
    await self._trigger_dependent("miniapps", replicas=1)
```

---

## Inter-replica data: ClassVar-FIFO pattern

When one stage needs to pass data to another, use a `ClassVar` list or dict as a shared FIFO queue. asyncio's cooperative scheduling makes `list.append` / `list.pop(0)` safe without locks — only one coroutine runs at a time.

Any Python object can be stored: scalars, dicts, NumPy arrays, file paths, or arbitrary structures. The constraints are RAM and lifetime — ClassVar persists for the process lifetime, so unconsumed entries accumulate. For large payloads (e.g. trajectory arrays) store them on disk and put only the file path in the queue.

Key the dict by group name when one workflow class serves multiple groups (e.g. `ddsim_a` and `ddsim_b` both use `DdSimWorkflow`).

```python
from typing import ClassVar

class SimWorkflow(BaseWorkflow):
    workflow_id = "sim"

    # Shared across all instances — keyed by group name.
    _scores: ClassVar[dict[str, list[float]]] = {}

    @classmethod
    def reset_state(cls) -> None:
        cls._scores = {}

    async def on_replica_done(self, replica_id: str, cm, final_state: str) -> None:
        if final_state != "done":
            return
        score = self._compute_score()
        SimWorkflow._scores.setdefault(self._group_name, []).append(score)
        await self._trigger_dependent("analysis", replicas=1)


class AnalysisWorkflow(BaseWorkflow):
    workflow_id = "analysis"

    async def on_replica_done(self, replica_id: str, cm, final_state: str) -> None:
        if final_state != "done":
            return
        # Pop the next score from any available group's queue (FIFO).
        for group, bucket in SimWorkflow._scores.items():
            if bucket:
                score = bucket.pop(0)
                self._process(score, source=group)
                break
```

Call `SimWorkflow.reset_state()` at the top of each benchmark run to avoid cross-run state pollution.

---

## Triggering downstream stages

Three methods are available inside workflow hooks (all are CM wrappers on `self`):

### `_trigger_dependent(name, replicas=1, **kwargs)`

Adds `replicas` new replicas of group `name` to the scheduler queue. Call from `on_replica_done` after producing data the downstream stage needs.

```python
await self._trigger_dependent("analysis", replicas=1, candidate_id=replica_id, score=score)
```

### `_trigger_batch(name, candidates)`

Adds multiple candidates to the sharder buffer in a single scheduler cycle, enabling meaningful priority ranking before dispatch. Each dict must contain `candidate_id`.

```python
await self._trigger_batch("analysis", [
    {"candidate_id": "sim_0", "score": 0.82},
    {"candidate_id": "sim_1", "score": 0.61},
])
```

### `_signal_done()`

Marks this group as done and immediately queues one replica in every group
that lists it in its `dependencies` config — without waiting for
`dep_threshold` to be reached. Use this when the group will stop triggering
downstream work before all its replicas finish, so dependents are not
blocked waiting for a threshold that will never be crossed.

```python
await self._signal_done()
```

---

## Scheduling Operator

### Subclassing `CampaignOperator`

```python
from src.campaign.adr import CampaignOperator
from radical.adr import goals, observe
from radical.adr.goals import Goal

class MyCampaignOperator(CampaignOperator):
    n_target: int = 20

    def __init__(self, view, engine=None, *, n_target=20, **kwargs):
        super().__init__(view, engine=engine, **kwargs)
        self.n_target = int(n_target)
        self._validate_stopping_condition()   # REQUIRED — see below

    @goals
    def criteria(self):
        return Goal(
            name="done",
            metric="n_hits",
            threshold=self.n_target - 0.5,
            direction="maximize",
        )

    @observe
    def extract(self, snapshot) -> dict:
        obs = super().extract(snapshot)          # always call super first
        obs["my_metric"] = snapshot.get(...)
        return obs
```

### `_validate_stopping_condition()` — call it at the END of `__init__`

**Forgetting this call means your campaign will run forever without any error.** The validator inspects the `@goals` return value to verify at least one stopping condition is declared. It must be called *after* all instance attributes are set (e.g. `self.n_target`) because `@goals` reads them.

```python
def __init__(self, view, engine=None, *, n_target=20, **kwargs):
    super().__init__(view, engine=engine, **kwargs)
    self.n_target = int(n_target)          # set state first
    self._validate_stopping_condition()    # validate last
```

If `@goals` returns an empty list and `max_cycles` is not set, `_validate_stopping_condition` raises `ValueError` immediately — you get a loud error at construction time instead of a silent runaway campaign.

### Base `@observe` fields

The base `CampaignOperator.extract()` populates:

- `obs["n_hits"]` — total finished replicas across all terminal stages
- `obs["cycle"]` — current operator cycle number
- `obs["stages"]` — per-group dict with keys: `running`, `pending`, `finished`, `n_failed`, `score_p50`, `bp_state`

Always call `super().extract(snapshot)` and extend from its return value.

### `save_extra` / `load_extra` — checkpoint custom state

The base `save_checkpoint` / `load_checkpoint` (inherited from `radical.adr`)
persists the operator's built-in scheduling state — cycle count, policy
weights, goal progress. It knows nothing about attributes you add in your
subclass (e.g. `self.best_score`).

Override `save_extra` / `load_extra` to persist that custom state alongside
the base checkpoint. `save_checkpoint_full` writes the base checkpoint and
then serialises `save_extra()`'s return value to a `<checkpoint>.extra.json`
sidecar file. `load_checkpoint_full` restores the base checkpoint and then
calls `load_extra()` with that sidecar's contents.

```python
def save_extra(self) -> dict:
    return {"best_score": self.best_score}

def load_extra(self, data: dict) -> None:
    self.best_score = data.get("best_score", float("inf"))
```

Always use `save_checkpoint_full` / `load_checkpoint_full` (not the bare
`save_checkpoint` / `load_checkpoint`) whenever your operator has custom state
— the bare variants will silently drop anything `save_extra` returns.

---

## Common pitfalls

### Forgetting `workflow_id`

The default `"base"` triggers a CM warning. Multiple classes with the same ID share metric labels and asyncflow task prefixes.

### `on_replica_done` spelling

The executor looks up the hook by name at runtime. A typo like `on_replica_done_` or `on_replica_finished` is **silently ignored** — the hook never fires. Check the spelling carefully.

### Calling `_validate_stopping_condition()` too early

If called before `self.n_target` is set, the `@goals` function sees `self.n_target` as the class default, not the value passed in. Always call it as the last line of `__init__`.

### Triggering from `_on_completion` AND `on_replica_done`

Both hooks can trigger downstream groups, and their effects are additive. If `on_replica_done` calls `_trigger_dependent("analysis")` and `_on_completion` also returns `"analysis"`, two replicas of `analysis` will be queued. Pick one approach per edge.

### `ClassVar` reset across benchmark runs

`ClassVar` state persists for the lifetime of the Python process. Call `MyWorkflow.reset_state()` at the top of each benchmark run (before `await cm.start()`) to avoid cross-run score contamination. See `DdSimWorkflow.reset_state()` for the pattern.

### Replicas and `dep_threshold`

By default, a dependent group starts after `dep_threshold=1` replica of each dependency finishes. If you need true waterfall (all replicas of stage A before any replica of stage B), set `dep_threshold` high enough or use `_signal_done()` / config `dep_threshold=replicas`.

---

## `register_workflow` parameter reference

| Parameter            | Default | Description                                               |
|----------------------|---------|-----------------------------------------------------------|
| `name`               | —       | Group name used in hooks, metrics, and logs               |
| `workflow_class`     | —       | `BaseWorkflow` subclass                                   |
| `replicas`           | `1`     | Total replicas to run                                     |
| `dependencies`       | `[]`    | Group names that must be ready first                      |
| `dep_threshold`      | `1`     | Finished-replica count that satisfies each dependency     |
| `concurrency_floor`  | `0`     | Minimum concurrent replicas the scheduler guarantees      |
| `concurrency_cap`    | `0`     | Hard ceiling on concurrent replicas (0 = unlimited)       |
| `priority`           | `0`     | Scheduler priority — higher runs first                    |
| `required_cpus`      | `0`     | CPUs reserved per replica (0 = untracked)                 |
| `required_gpus`      | `0`     | GPUs reserved per replica (0 = untracked)                 |
| `required_memory_gb` | `0.0`   | Memory reserved per replica (0 = untracked)               |
| `config`             | `None`  | Dict passed as `self.config` to the workflow instance     |
| `max_retries`        | `0`     | Automatic retries on failure (0 = no retry)               |
