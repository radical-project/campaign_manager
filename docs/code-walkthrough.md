# Code Walkthrough

*Component relationships, class hierarchies, and concurrency model for contributors and backend developers.*

---

## Module map

```
src/aucam/
├── __init__.py       Public API surface — all exported symbols
├── manager.py        CampaignManager — multi-campaign orchestration + global budget
├── campaign.py       _CampaignInstance — full per-campaign lifecycle (scheduler + policy loops)
├── controller.py     CampaignController ABC, PolicyResult, Goal, LaunchRule, CancelRule
├── types.py          Enums, WorkflowResult, TelemetrySnapshot, GlobalBudget, CampaignOutcome
├── spec.py           WorkflowConfig, CampaignSpec, CampaignBudget, WorkflowDependency
├── scheduler.py      SchedulingPolicy ABC + all implementations
├── queue.py          WorkflowQueue — min-heap with priority + aging
├── budget.py         BudgetAllocator — concurrency accounting
├── data_backend.py   DataBackend protocol + Local/Dragon/Redis implementations
├── reporter.py       WorkflowReporter ABC + LocalDictReporter
├── checkpoint.py     CampaignCheckpoint, atomic JSON write, recovery
└── profiler.py       BlockProfiler, TaskProfiler
```

---

## Class hierarchy

### `CampaignController` (abstract base)

```
CampaignController  (controller.py)
├── __init__(launch, goal_type, goal_n, rules)
│   Sets up self._rules — list of LaunchRule/CancelRule
├── observe(...)     → dict (obs)            [not usually overridden]
├── decide(obs)      → PolicyResult          [override for custom policy]
│   └── default: _apply_rules(self._rules, obs)
│       ├── _apply_rules checks is_goal_reached() → terminate=True
│       ├── evaluates each CancelRule: cancel running wf if predicate(report) is True
│       └── evaluates each LaunchRule: add LaunchRequest if nothing queued for that type
├── on_complete(result: WorkflowResult)      [ALWAYS call super() to keep counters]
│   updates: self.completed / self.cancelled / self.failed (dicts[wf_type→int])
├── is_goal_reached() → bool                 [override to define stopping condition]
│   default: completed[goal_type] >= goal_n
└── get_summary()   → dict                   [override to expose scientific results]
    default: {}                              called once at completion; stored in CampaignOutcome
```

**Key subclass contract**: when overriding `decide()`, you must return a `PolicyResult` (never modify `obs` in place).  When overriding `on_complete()`, always call `super().on_complete(result)` first or the `completed`/`cancelled`/`failed` counters won't increment.

`LaunchRule` and `CancelRule` are data classes (not subclasses) consumed by `_apply_rules`.

### `SchedulingPolicy` (abstract base)

```
SchedulingPolicy  (scheduler.py)
└── select(candidates: list[LaunchRequest], ctx: SchedulingContext) → list[LaunchRequest]
    Returns the subset to launch this cycle (respecting concurrency caps in ctx).

    Implementations:
    ├── FIFOSchedulingPolicy     — pass candidates through in queue order
    ├── BanditSchedulingPolicy   — UCB1 multi-armed bandit; arms keyed by (wf_type, context_key)
    │   ├── record_outcome(wf_type, context_key, reward)  called by _on_done
    │   └── _ucb1_score(arm)     exploration-exploitation balance
    ├── ADRSchedulingPolicy      — rule-override hook + ADR engine (stub → falls back to FIFO)
    ├── LLMSchedulingPolicy      — LLM-guided ranking (model-agnostic; stub → FIFO)
    └── DefaultSchedulingPolicy  — alias for FIFOSchedulingPolicy
```

`SchedulingContext` is a dataclass carrying `running_by_type`, `max_concurrency`, `max_concurrency_per_type`, `telemetry`, `history`, `policy_cycles`, and `campaign_name`.

### `DataBackend` (protocol)

```
DataBackend  (data_backend.py, Protocol)
├── write_report(wf_id, report: WorkflowReport)
├── get_report(wf_id)                  → WorkflowReport | None
├── delete_report(wf_id)
├── write_signal(wf_id, decision: CampaignDecision)
└── get_signal(wf_id)                  → CampaignDecision | None

Implementations:
├── LocalDictBackend      in-process dict; used in tests and local dev
│   └── sync_get_report(wf_id)  duck-typed extra method for _on_done callback
├── DragonDictBackend     Dragon DDict; cross-node on HPC
└── RhapsodyRedisBackend  Redis via Rhapsody; requires rhapsody-py
```

The protocol is structural (no ABC inheritance) — backends just need to implement the five methods.

### `WorkflowReporter` (abstract base)

```
WorkflowReporter  (reporter.py)
├── emit(score, metrics, iteration)     workflow → CM channel
├── poll()                              → CampaignDecision | None  (CM → workflow signal)
└── wait_for_resume()                   50 ms yield loop until RESUME signal

Implementations:
└── LocalDictReporter    wraps LocalDictBackend; used for block workflows
```

For subprocess workflows, `SubprocessReporter` (in `src/aucam/subprocess_client.py`) reads `CM_WORKFLOW_ID`, `CM_DATA_ENDPOINT`, and `CM_DATA_BACKEND` from the environment and writes directly to the backend — the Rhapsody-managed Redis instance for the `redis` backend, or the Dragon DDict for the `dragon` backend.

---

## `CampaignManager` (manager.py)

Single instance per CM process (the head process running `run_campaign.py`).  Key state:

```python
_submission_queue: asyncio.Queue[CampaignSpec | None]   # None = sentinel (close)
_active_specs:     dict[str, CampaignSpec]              # name → spec (running campaigns)
_reserved_specs:   dict[str, CampaignSpec]              # name → spec (admitted, not yet started)
_global_spent:     dict[str, float]                     # resource key → actual spent
_has_driver:       bool                                 # True if expect_driver() was called
```

### Admission check (`_admission_check`)

Called inside `submit()` while holding no lock (asyncio is single-threaded).  Computes:

```python
all_committed = {**self._reserved_specs, **self._active_specs}
committed = sum(s.budget.spending.get(key, 0.0) for s in all_committed.values())
incoming  = spec.budget.spending.get(key, 0.0)
if already_spent + committed + incoming > limit:
    return rejection_reason
```

`_reserved_specs` closes the admit-to-start race: `submit()` adds the spec here immediately after a passing admission check; `_start_campaign()` moves it to `_active_specs` when the run loop picks it up.  Both dicts are unioned in every subsequent `_admission_check`, so the declared spending of an admitted-but-not-yet-started campaign counts against the global cap and prevents a concurrent `submit()` from double-admitting against the same headroom.

### Run loop (`run`)

```python
async def run(self):
    if not self._has_driver:
        self._submission_queue.put_nowait(None)   # no driver → auto-close
    async with asyncio.TaskGroup() as tg:
        tg.create_task(self._dispatch_loop())
        # _dispatch_loop dequeues CampaignSpecs and spawns _CampaignInstance tasks
```

`_dispatch_loop` dequeues from `_submission_queue`, calls `_start_campaign(spec)` for each, spawns an asyncio task per campaign, and exits when `None` is dequeued.  The `TaskGroup` keeps `run()` alive until all campaign tasks finish.

---

## `_CampaignInstance` (campaign.py)

The core orchestration class.  One instance per running campaign.  Owns:

```python
_queue:        WorkflowQueue                # priority-heap of LaunchRequests
_budget:       BudgetAllocator              # concurrency borrow/return
_scheduler:    SchedulingPolicy
_controller:   CampaignController
_active:       dict[str, _WorkflowInst]    # wf_id → running instance
_done:         bool
_spent:        dict[str, float]            # lifetime accumulator
```

### Two concurrent asyncio tasks per campaign

```
_scheduler_loop                      _policy_loop
─────────────────────────────        ───────────────────────────────────
while not _done:                     while not _done:
  req = await _queue.get()             try:
  if _budget.fits(req):                  await asyncio.wait_for(
    launch(req)                              _completion_event.wait(),
  else:                                      timeout=policy_interval_s)
    _queue.put_back(req)               except TimeoutError:
    await _slot_event.wait()               pass
                                       _completion_event.clear()
                                       await _run_policy_cycle()
```

`_slot_event` is set whenever a workflow completes (freeing a budget slot).
`_completion_event` is set by the synchronous `_on_done` callback.

The two tasks share `_queue`, `_budget`, and `_active` — all accessed from the same event loop, so there is **no locking**.

### Policy cycle (`_run_policy_cycle`)

```python
1. _collect_live_reports()   → reports: dict[wf_id, WorkflowReport]
2. controller.observe(...)   → TelemetrySnapshot
3. controller.decide(obs)    → PolicyResult
4. _apply_auto_cancels(pr)   → stale/wall-time overrides merged into pr.decisions
5. _execute_decisions(pr)
   ├── CANCEL  → inst.future.cancel()
   ├── PAUSE   → data_backend.write_signal(wf_id, PAUSE)  if supports_pause
   ├── RESUME  → data_backend.write_signal(wf_id, RESUME)
   └── launch_requests → _queue.put(req) for each
6. if pr.terminate:
   └── _shutdown()    # sets _done, clears queue, cancels in-flight
7. if checkpoint_dir:
   └── asyncio.to_thread(write_checkpoint, ...)   # atomic file write, off event loop
```

### Shutdown sequence (`_shutdown`)

```python
async def _shutdown(self, reason):
    self._done = True           # scheduler loop exits on next iteration
    self._slot_event.set()      # unblocks scheduler if it's waiting
    self._queue.clear()         # purge all queued LaunchRequests immediately
    for inst in _active.values():
        inst.future.cancel()    # signal all in-flight tasks
    await asyncio.wait(         # collect all futures; give them shutdown_timeout_s
        [f for f in _active...],
        timeout=shutdown_timeout_s
    )
```

Setting `_done=True` is the primary guard: the scheduler loop exits on `while not self._done` and `_drain_queue` returns immediately if `_done`.  The queue is then cleared before cancelling futures as a belt-and-suspenders measure — discarding any requests that LaunchRules may have pre-filled so they don't linger in memory.

---

## `WorkflowQueue` (queue.py)

Min-heap keyed by `(-priority.value, age_adjusted_time)`.  Higher priority and older requests sort first.

```python
class WorkflowQueue:
    _heap: list[tuple]      # (sort_key, seq, LaunchRequest)

    def put(self, req: LaunchRequest) -> None
    def get(self) -> LaunchRequest         # raises Empty
    def clear() -> int                    # purges all items; returns count
    def snapshot() -> QueueSnapshot
```

Age promotion: items waiting longer than `age_threshold_s` have their effective priority bumped by 1 level (LOW → NORMAL → HIGH).  This prevents starvation when the queue mixes priorities.

---

## `BudgetAllocator` (budget.py)

Tracks concurrency (borrowed/returned per launch/finish).  Lifetime spending is computed separately in `_CampaignInstance._compute_workflow_spent()` and accumulated in `_spent`.

```python
class BudgetAllocator:
    def fits(campaign_id, resources) -> bool   # headroom check against reserved pool
    def charge(campaign_id, resources)         # called on workflow launch
    def release_workflow(campaign_id, resources)  # called on finish
```

Spending is not tracked inside `BudgetAllocator` — it lives in `_CampaignInstance._spent` and is checked via `_spending_limit_reached()` before each `_drain_queue()` call.

The allocator only keeps the books.  **It never cancels workflows** — only the controller can do that via `PolicyResult.decisions`.

---

## Data flow end-to-end

```
Driver
  │  cm.submit(spec)
  ▼
CampaignManager._admission_check()
  │  reserve declared budget in _reserved_specs
  │  enqueue spec in _submission_queue
  ▼
CampaignManager._dispatch_loop()
  │  dequeue spec → _start_campaign(spec)
  │  move _reserved_specs → _active_specs
  ▼
_CampaignInstance  (asyncio task)
  │
  ├── _scheduler_loop                       ├── _policy_loop
  │   │                                     │   │
  │   │ dequeue LaunchRequest               │   │ collect WorkflowReport per wf_id
  │   │ BudgetAllocator.fits()              │   │   (DataBackend.get_report)
  │   │   → launch asyncio.Task             │   │ controller.observe() → TelemetrySnapshot
  │   │       │                             │   │ controller.decide(obs) → PolicyResult
  │   │       │ block workflow(reporter)    │   │ _execute_decisions
  │   │       │   reporter.emit() ──────────┼───┤   CANCEL/PAUSE/RESUME signals
  │   │       │   reporter.poll() ◄─────────┼───┤   new LaunchRequests → queue
  │   │       │                             │   │ if terminate → _shutdown()
  │   │       │ subprocess workflow         │   │
  │   │       │   CM_DATA_ENDPOINT env      │   │
  │   │       │                             │   │
  │   │       └── future.done() callback    │   │
  │   │               _on_done (sync):      │   │
  │   │               BudgetAllocator       │   │
  │   │                 .release_workflow() │   │
  │   │               _compute_workflow_spent()  │
  │   │                 → _spent[key] += actual  │
  │   │               controller            │   │
  │   │                 .on_complete()      │   │
  │   │               _completion_event.set()   │
  │   │               _slot_event.set()     │   │
  │                                         │
  └─────────────────────────────────────────┘
  │
  │ campaign finishes
  ▼
CampaignManager._resolve_future()
  │  moves _active_specs → done
  │  _global_spent updated with actual
  │  CampaignOutcome built:
  │    summary = ctrl.get_summary()
  │    spent   = inst._spent
  ▼
SubmitResult.wait() resolves with CampaignOutcome
  │
  ▼
Driver receives outcome → submits next campaign
```

---

## Checkpointing

`write_checkpoint` is dispatched via `asyncio.to_thread()` (never blocks the event loop):

```python
path = tempfile.mkstemp(dir=checkpoint_dir, suffix=".tmp")
json.dump(checkpoint.to_dict(), f)
os.replace(tmp_path, final_path)   # POSIX rename(2) — atomic
```

On recovery (`_CampaignInstance.__init__` if checkpoint exists):

1. `read_checkpoint()` deserializes JSON to `CampaignCheckpoint`
2. Counters and `_spent` are restored
3. Queued `LaunchRequest`s are re-pushed at HIGH priority with `recovered_from` context
4. Previously active workflows are re-enqueued (they can't be re-attached to their old futures)

The spec (controller object, block callables) is NOT in the checkpoint — the driver must re-supply it.

---

## Threading model

The CM head process is **single-threaded asyncio**.  Workflow execution is dispatched to the Rhapsody backend via asyncflow — block workflows run inside asyncflow's execution environment (Dragon function tasks or concurrent tasks), not on the CM event loop.

Within the CM itself, blocking file I/O (checkpoint writes, `LocalDictBackend` reads/writes, Redis calls) is wrapped in `asyncio.to_thread()` so it never stalls the event loop.

---

## Extension points

| What to extend | Where | How |
|----------------|-------|-----|
| Custom policy | `CampaignController` subclass | Override `decide()`, `is_goal_reached()`, `get_summary()` |
| Custom scheduling | `SchedulingPolicy` subclass | Implement `select(candidates, ctx)` |
| New data backend | Implement `DataBackend` protocol | Five methods: write/get/delete report, write/get signal |
| Custom metrics | `WorkflowConfig.metrics_cls` | Pydantic model; fields appear in `WorkflowReport.metrics` |
| Spending declaration | `WorkflowConfig.declared_spending` | Keys `wall_hours`, `cpu_hours`, `gpu_hours`; drives proration and pool sizing |
| Pre/post hooks | `WorkflowConfig.on_completion` | Sync callback after each `WorkflowResult` |
