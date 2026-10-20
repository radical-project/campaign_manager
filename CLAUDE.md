# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## AuCaM design

The design spec is `aucam-design/aucam-design.md` (architecture diagram: `aucam-design/aucam-architecture.png`; a Mermaid version is inline in the spec). Read it before working on CM code.

- Treat the spec as the source of truth. If code or a request disagrees with it, point out the conflict instead of silently diverging.
- Items marked **Planned** in the spec are not implemented yet; don't assume they exist.
- Planned features and known implementation gaps (not in the spec) are in `docs/known-gaps.md`; read it before working on checkpointing, Tier 2 reporting, accounting, cancellation or campaign exit, and update it when you implement or fix an entry.
- Use its terminology: CampaignManager (CM), driver API (`submit()` → `SubmitResult` → `CampaignOutcome`), GlobalBudget, BudgetAllocator (spending ledger), CampaignBudget (`spending` limit + workflow-count caps), `declared_spending`, CampaignController (observe → decide → act, goals, CancelRule / LaunchRule), PolicyResult, WorkflowQueue, Scheduler, SchedulingPolicy, WorkflowReport / WorkflowReporter, DataBackend, Tier 1 (block) / Tier 2 (launch_cmd) workflows.
- Two levels: manager level (one CM for all campaigns: admission against the global budget, spending ledger, shared AsyncFlow engine) and campaign level (one controller, queue and scheduler per campaign).
- Budgets are spending only (resource-hours). There is no "resources in use at once" budget: workflows are heterogeneous task sets placed by RHAPSODY. Capacity is limited by the allocation and the workflow-count caps.
- The BudgetAllocator is the single spending ledger (per campaign and global). Only the CampaignController decides to cancel workflows.

## Quick Start

### Installation

```bash
# Core (asyncio-only, no HPC runtime)
pip install -e .

# With Dragon/RADICAL HPC backend  (requires Python ≥ 3.11 — dragonhpc constraint)
pip install -e ".[dragon]"

# With ADR adaptive-scheduling layer (radical.adr not on PyPI — install editable first)
pip install -e ".[adr]"

# With LLM scheduling policy
pip install -e ".[llm]"

# Full dev setup (recommended)
pip install -e ".[dev]"
```

Requires **Python ≥ 3.11** (dragonhpc constraint; applies to all usage).

### Common Commands

```bash
# Run all tests
pytest

# Run tests with coverage
pytest --cov=src --cov-report=html

# Run a single test file or test
pytest tests/test_aucam_unit.py
pytest tests/test_checkpointing.py::test_recovery_restores_queue_and_completes

# Lint and format check
ruff check .
ruff format --check .

# Auto-format and fix
ruff check . --fix && ruff format .
```

---

## Architecture Overview

AuCaM (**Au**tonomous **Ca**mpaign **M**anager) is an `asyncio`-native policy engine for concurrent HPC scientific workflows. The central abstraction is the **campaign**: a named policy loop that dispatches, monitors, and adaptively manages a population of workflow instances.

```
CampaignManager
└── _CampaignInstance  (one per submitted campaign)
    ├── _scheduler_loop  — dequeues LaunchRequests, allocates budget, launches workflows
    ├── _policy_loop     — event-driven: wakes on completion event or policy_interval_s timeout
    │   └── _run_policy_cycle
    │       ├── _collect_live_reports()   → WorkflowReport per active workflow
    │       ├── controller.observe()      → TelemetrySnapshot
    │       ├── controller.decide()       → PolicyResult (decisions, terminate, launch_requests)
    │       ├── _apply_auto_cancels()     → stale / wall-time overrides injected into decisions
    │       └── _execute_decisions()      → CANCEL / PAUSE / RESUME / LAUNCH
    └── _on_done callback — sync; increments counters, sets _completion_event
```

### Core loop: Observe → Decide → Act

Every `policy_interval_s` (or immediately on workflow completion via `_completion_event`):

1. **Observe** — collect live `WorkflowReport`s from `DataBackend`; build `TelemetrySnapshot`
2. **Decide** — `CampaignController.decide(obs)` returns `PolicyResult`
3. **Act** — apply `PolicyResult.decisions`; enforce stale/wall-time auto-cancels; write checkpoint

### Bidirectional data channel

```
Workflow ──emit()──► DataBackend ──get_report()──► _CampaignInstance
Workflow ◄──poll()── DataBackend ◄──write_signal()── _CampaignInstance
```

- `WorkflowReporter.emit(score, metrics, iteration)` — workflow → CM report channel
- `WorkflowReporter.poll()` → `CampaignDecision | None` — CM → workflow signal channel
- `WorkflowReporter.wait_for_resume()` — 50 ms yield loop until CM sends RESUME

Three backends: `LocalDictBackend` (in-process, used in tests), `DragonDictBackend` (Dragon DDict, HPC), `RhapsodyRedisBackend` (Redis via Rhapsody, distributed).

---

## Key Concepts

### `CampaignSpec`

```python
CampaignSpec(
    name="fold_sweep",
    budget=CampaignBudget(max_concurrent_workflows=4, max_concurrency_per_workflow=2),
    controller=MyController(),
    workflows=[WorkflowConfig(...)],
    policy_interval_s=5.0,             # max sleep between policy cycles
    shutdown_timeout_s=30.0,
    max_queue_depth=100,               # soft warning above this; never hard-drops
    checkpoint_dir="/scratch/ckpts",   # None = no checkpointing
)
```

### `WorkflowConfig`

```python
WorkflowConfig(
    name="folding_worker",
    block=async_fn,                    # Python async function (Tier 1)  — OR —
    launch_cmd=["python", "wf.py"],    # subprocess command (Tier 2)
    declared_spending={"wall_hours": 1/3600, "gpu_hours": 2/3600},  # for budget accounting
    supports_pause=True,               # False (default) → PAUSE decisions are skipped with WARNING
    launch_on_start=True,              # auto-enqueue one instance at campaign start
    max_wall_time_s=300.0,             # auto-cancel if running longer than this
    stale_timeout_s=60.0,              # auto-cancel if reporter silent > this (after ≥1 emit)
    env={"TORCH_DEVICE": "cuda"},      # launch_cmd only: exported to the program
    on_completion=callback,            # called with WorkflowResult after each run
)
```

### `CampaignController`

```python
class MyController(CampaignController):
    def decide(self, obs: dict) -> PolicyResult:
        # obs keys: running, queue_snapshot, live_reports, telemetry, wf_types
        return PolicyResult(
            terminate=self.done,
            decisions={wf_id: CampaignDecision.CANCEL},   # dict[str, CampaignDecision]
            launch_requests=[LaunchRequest.make("folding_worker")],
        )
```

### `WorkflowReporter` inside block functions

```python
async def my_worker(reporter: WorkflowReporter) -> None:
    for step in range(n_steps):
        result = run_step(step)
        await reporter.emit(score=result.energy, metrics={"step": step, "loss": result.loss})
        # honour PAUSE/RESUME signals
        if await reporter.poll() == CampaignDecision.PAUSE:
            await reporter.wait_for_resume()
```

The reporter is injected automatically by `_CampaignInstance` when launching a Tier 1 block. `CM_DATA_ENDPOINT` is injected into subprocess env for Tier 2 (non-None when using Dragon/Redis backends).

### Checkpointing and recovery

When `CampaignSpec.checkpoint_dir` is set:
- After each policy cycle: `_build_checkpoint()` runs in the event loop, then `write_checkpoint` is dispatched to a thread pool via `run_in_executor` (I/O off the event loop).
- At shutdown: a final synchronous `write_checkpoint` is called before the SUMMARY log.
- At `run()` start: if `<checkpoint_dir>/<campaign_name>.json` exists, counters and `_spent` are restored, queued requests are re-pushed, and active workflows are re-enqueued at HIGH priority with `recovered_from` context in the request.
- **Atomic writes**: each `write_checkpoint` call uses `tempfile.mkstemp` for a unique `.tmp` file, then `os.replace()` (POSIX `rename(2)`). Concurrent policy-cycle and shutdown writes never race on the same temp file.

### Observability

After shutdown, `_CampaignInstance` emits a SUMMARY log:

```
[CM] Campaign fold_sweep SUMMARY — exit=controller_terminate run=12 ok=10
  cancelled=2 (stale=1 walltime=1 policy=0) failed=0 cycles=24 spent={'gpu_hours': 1.3}
```

`WorkflowResult.final_metrics` holds the last `emit()` payload for Tier 1 workflows. It is populated in the synchronous `_on_done` callback via `LocalDictBackend.sync_get_report(wf_id)` (duck-typed via `hasattr`) before the async `delete_report` fires. Remote backends fall back to `_inst.last_report` cached by the previous policy cycle.

Policy loop wakeup is **event-driven**: `_policy_loop` uses `asyncio.wait_for(_completion_event.wait(), timeout=policy_interval_s)`. A campaign with `policy_interval_s=60` reacts within milliseconds of the last workflow finishing.

`max_queue_depth` is a **soft limit** — exceeding it logs a WARNING but always enqueues. Hard drops were removed; throttling belongs in the controller.

---

## Key File Organization

### `src/aucam/`

| File | Responsibility | Lines |
|---|---|---|
| `campaign.py` | `_CampaignInstance` — full lifecycle | 1299 |
| `manager.py` | `CampaignManager` — multi-campaign submission + run loop | 462 |
| `data_backend.py` | `DataBackend` protocol + `LocalDict`/`Dragon`/`Redis` backends | 402 |
| `scheduler.py` | All scheduling policies (Bandit/FIFO/ADR/LLM) + `SchedulingContext` | 346 |
| `controller.py` | `CampaignController` ABC, `PolicyResult`, `Goal`, `CancelRule`, `LaunchRule` | 227 |
| `types.py` | Enums, `WorkflowResult`, `LaunchRequest`, `TelemetrySnapshot`, `GlobalBudget`, `CampaignOutcome` | 233 |
| `checkpoint.py` | `CampaignCheckpoint` dataclass, atomic write, recovery deserialization | 168 |
| `profiler.py` | `BlockProfiler`, `TaskProfiler` — per-block resource tracking | 147 |
| `spec.py` | `WorkflowConfig`, `CampaignSpec`, `CampaignBudget`, `WorkflowDependency` | 129 |
| `queue.py` | `WorkflowQueue` — min-heap with priority + aging | 124 |
| `subprocess_client.py` | Backward-compatible alias of `src.aucam_client.Reporter` | 24 |
| `../aucam_client/` | Language-neutral reporting client and `aucam-report` command for command-line workflows (JSON over Redis, or Dragon) | — |
| `budget.py` | `BudgetAllocator` — spending ledger: admission, in-flight estimates, actual spending, global total | 230 |
| `reporter.py` | `WorkflowReporter` ABC + `LocalDictReporter` — emit/poll/wait_for_resume | 103 |
| `__init__.py` | Public API surface — all exported symbols | 82 |

### `tests/`

| File | What it covers |
|---|---|
| `test_aucam_unit.py` | `BudgetAllocator` (spending ledger), `WorkflowQueue` unit tests |
| `test_spending_ledger.py` | Global spending admission, outcome spending, ledger close |
| `test_aucam_dummy.py` | Basic campaign run + budget-limited concurrency |
| `test_aucam_multi.py` | Multiple concurrent campaigns |
| `test_aucam_reporter.py` | Score-based controller cancellation |
| `test_budget_control.py` | Three-tier budget control |
| `test_stale_and_walltime.py` | `stale_timeout_s` and `max_wall_time_s` auto-cancel |
| `test_workflow_config_fields.py` | `supports_pause` and field defaults |
| `test_pause_resume.py` | PAUSE/RESUME signal lifecycle |
| `test_observability.py` | `final_metrics`, SUMMARY log, soft queue limit, event-driven wakeup |
| `test_checkpointing.py` | JSON roundtrip, atomic write, file creation, counters, recovery |
| `test_resource_profiling.py` | `BlockProfiler`, `_correlate_from_jsonl`, `_compute_workflow_spent` paths |

---

## Testing

All tests are synchronous at the top level (`asyncio.run(asyncio.wait_for(..., timeout=N))`). No `anyio` or `pytest-asyncio` marks are used in the AuCaM test suite.

```bash
pytest                               # full suite (54 tests, 1 skipped)
pytest tests/test_checkpointing.py   # checkpointing + recovery
pytest -m "not slow"
```

---

## Important Implementation Notes

### Dragon backend on Lustre/Delta HPC

Cold-page-fault stalls on Lustre freeze the asyncio event loop. Use `asyncio.to_thread()` for any blocking file I/O inside workflows. Pre-warming the file into page cache alone is insufficient — confirmed fix on Delta HPC.

`dragon -m` fails on Delta (FQDN vs short NodeName mismatch in SLURM).  Multi-node launch:
```bash
(cd $NET_DIR && dragon-network-config --output-to-yaml --no-stdout)
dragon -w ssh --network-config $NET_DIR/slurm.yaml -t tcp run_campaign.py ...
```

Python `logging` → stderr (`.err` file); `print()` → stdout (`.out` file).  CM budget/spending logs are at `INFO` level → `.err`.

### `expect_driver()` must be called before `asyncio.create_task`

`cm.run()` checks `_has_driver` synchronously before yielding to the event loop.  If you use driver mode, call `cm.expect_driver()` before `asyncio.create_task(_driver(...))` so the CM doesn't auto-close the queue before the driver task runs.

### `_apply_rules` terminates on `is_goal_reached()` immediately

Do not wait for the queue to drain. `_shutdown()` clears the queue and cancels in-flight workflows gracefully.

`LaunchRule` only enqueues a new request when `q.by_type[wf_type] == 0` — i.e. nothing is already queued for that type. This prevents unbounded queue growth when completions stall. One pending request per type is sufficient: the scheduler picks it up when a slot opens, and the next policy cycle enqueues a replacement.

`_shutdown()` does three things atomically:
1. Sets `_done = True` (scheduler loop exits; `_drain_queue` becomes a no-op)
2. Calls `self._queue.clear()` — purges all queued `LaunchRequest`s immediately
3. Cancels all in-flight workflow futures and waits up to `shutdown_timeout_s`

The queue is purged first so there is no window where the scheduler could dequeue and launch new workflows between `_done` being set and in-flight cancellation completing.

### Global budget admission check formula

`already_spent` (`_global_spent`, actual from completed campaigns) + `committed` (declared spending limits of **all admitted campaigns** — both `_reserved_specs` and `_active_specs`) + `incoming` (new campaign's declared limit) > cap → reject.

`_reserved_specs` holds specs admitted by `submit()` but not yet started by `_start_campaign()`.  This closes the admit-to-start race window: rapid back-to-back `submit()` calls can't both pass the check before either campaign is actually running.  `_start_campaign()` moves a spec from `_reserved_specs` to `_active_specs`.

A campaign with no declared spending (`spending={}`) contributes `incoming=0.0` and is never rejected on spending grounds alone.

### `_on_done` is synchronous

`_on_done` is a sync callback registered with `asyncio.Task.add_done_callback`. It cannot `await` anything. Use `sync_get_report()` (LocalDictBackend) or cached `_inst.last_report` (remote backends) to get `final_metrics` without awaiting.

### `CampaignSpec` (and `WorkflowConfig`) carries callables

The checkpoint only stores serializable state — queue items, active metadata, counters, spent. The `CampaignSpec` (controller object, block functions, callbacks) must be provided by the driver on recovery; it is not serialized.

### `supports_pause=False` (the default)

PAUSE decisions from the controller are silently skipped with a WARNING log when `supports_pause=False`. This prevents signal delivery to workflows that haven't implemented `poll()` / `wait_for_resume()`.
