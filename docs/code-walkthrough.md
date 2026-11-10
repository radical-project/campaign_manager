# Code walkthrough

A map of the code for contributors: what each module holds and the main paths
through it. Concepts and design decisions are in the [architecture overview](architecture-overview.md),
planned work in [known gaps](known-gaps.md), and budget internals in
[budgets and spending](budget.md) §8–9.

---

## Module map

| Module | Holds |
|---|---|
| `src/aucam/manager.py` | `CampaignManager`: `run(spec)` (engine set-up, one campaign, its outcome), `stop()`, signal handling |
| `src/aucam/campaign.py` | `_CampaignInstance`: everything that runs one campaign — launch, policy cycle, scheduler, accounting, dependencies, checkpoint, shutdown |
| `src/aucam/controller.py` | `CampaignController` base class, `CancelRule`, `LaunchRule`, `Goal`, `PolicyResult` |
| `src/aucam/scheduler.py` | `SchedulingPolicy` (FIFO base), `FIFOSchedulingPolicy`, `BanditSchedulingPolicy` (UCB1), `ADRSchedulingPolicy` / `LLMSchedulingPolicy` (stubs), `SchedulingContext` |
| `src/aucam/queue.py` | `WorkflowQueue`: priority heap with aging |
| `src/aucam/budget.py` | `BudgetAllocator`: the campaign's spending ledger |
| `src/aucam/spec.py` | `CampaignSpec`, `CampaignBudget`, `WorkflowConfig`, `WorkflowDependency` |
| `src/aucam/types.py` | `WorkflowResult`, `LaunchRequest`, `CampaignOutcome`, snapshots and enums |
| `src/aucam/reporter.py` | `WorkflowReport`, `WorkflowReporter` (interface), `LocalDictReporter` |
| `src/aucam/data_backend.py` | `DataBackend` protocol; `LocalDictBackend`, `RadexDragonBackend`, `RhapsodyRedisBackend` and their reporters |
| `src/aucam/checkpoint.py` | `CampaignCheckpoint` and its JSON read/write |
| `src/aucam/profiler.py` | `BlockProfiler`, `TaskProfiler`: optional CPU/memory sampling inside workflows |
| `src/aucam_client/` | `Reporter` and the `aucam-report` command for command-line workflows (any language) |
| `campaigns/` | Example campaigns: `demo_campaign` (API walkthrough, all capabilities), `hpc_campaign` (production HPC example), `ddsim_aucam` (DeepDriveSim integration) |

---

## Main paths

### Running a campaign — `CampaignManager.run(spec)`

```
run(spec)
├── WorkflowEngine.create(backend)   a local backend if none was given; telemetry if enabled
├── _CampaignInstance(spec, BudgetAllocator(), flow).run()   as a task; SIGTERM/SIGINT → stop()
├── _outcome()                       CampaignOutcome from the instance, controller and ledger
└── flow.shutdown()                  the backend too, unless it was passed in
```

`stop()` cancels the campaign task; the instance shuts down gracefully and the
outcome has `exit_reason="signal"`. One `run()` at a time; the manager can run
again after it returns.

### One campaign — `_CampaignInstance.run()`

```
run()
├── restore from checkpoint (if any), else enqueue the initial requests
├── _scheduler_loop()   ─┐ two asyncio tasks
├── _policy_loop()      ─┘
└── _shutdown(reason)
```

- **`_policy_loop()`** wakes every `policy_interval_s` or when a workflow
  finishes and calls `_run_policy_cycle()`:
  `_collect_live_reports()` → `_collect_telemetry()` → `controller.observe()`
  → `controller.decide()` → `_apply_auto_cancels()` (stale reporter, wall time)
  → apply decisions (CANCEL / PAUSE / RESUME, `cancel_queued`, new requests,
  goal changes) → write checkpoint → end if `terminate` (`controller_terminate`)
  or all goals met (`goals_satisfied`).
- **`_scheduler_loop()`** calls `_drain_queue()` whenever a slot frees up or a
  request arrives (at least every 2 s): stop if the spending limit is reached;
  otherwise launch CRITICAL requests, then the requests the `SchedulingPolicy`
  selects, within the workflow-count limits. It also ends the campaign on
  `spending_limit` (nothing running) and `campaign_idle` (`idle_exit_s`).

### Launching a workflow — `_launch()`

```
_launch(request)
├── _learned_cost() → estimate → BudgetAllocator.set_in_flight()
└── _make_future()
    ├── Python block:  _run_as_block(block)            reporter / flow= / context= injected
    └── launch_cmd:    _run_as_block(_cmd_block)       one executable task: _wrap_cmd() → bash -c 'export …; exec cmd'
```

Every workflow is one AsyncFlow block. `_run_as_block()` maps the block's uid
to the workflow (`_wf_tags`) so task telemetry events can be charged to it
(`_on_task_event()`). The future's done-callback `_on_done` (inside
`_launch()`):

1. maps the outcome to a `returncode` (0 / −1 cancelled / exit code);
2. charges spending (`_compute_workflow_spent()`, unless charged per task) and
   updates the learned cost (`_update_type_cost()`, successful runs only);
3. builds the `WorkflowResult` and passes it, in this order, to
   `SchedulingPolicy.on_outcome()`, `controller.on_complete()`, the workflow's
   `on_completion` hook and `_check_dag()` (dependencies).

### Dependencies — `enqueue()` and `_check_dag()`

`enqueue()` holds requests for a dependency target in `_dep_pending` until the
target is unlocked (`_dep_satisfied`). `_check_dag()` runs on every finished
workflow: if a dependency's condition passes, it unlocks the target, releases
held requests, or adds one new request.
These are dependencies between workflow types at campaign level, with
conditions on results. Dependencies between tasks inside one workflow use
AsyncFlow's own (pass one task's future to another).

### Reporting — `DataBackend`

A Python workflow gets `backend.make_reporter(wf_id)` and calls `emit()`;
command-line workflows use `aucam_client` with Redis or Dragon. Each policy
cycle `_collect_live_reports()` reads `get_report()` for every running workflow;
PAUSE / RESUME go out with `write_signal()`.

---

## Extension points

| To add | Implement | Pass it via |
|---|---|---|
| Decision logic | subclass `CampaignController` (`observe`, `decide`, `on_complete`, `get_summary`, `goals`) | `CampaignSpec.controller` |
| Scheduling policy | subclass `SchedulingPolicy` (`select`, optionally `on_outcome`) | `CampaignSpec.scheduling_policy` |
| Data channel | the `DataBackend` protocol and a `WorkflowReporter` | `CampaignSpec.data_backend` |

## Tests

`tests/` covers each area; start with `test_design_alignment.py` (design rules),
`test_new_features.py` (budget features, idle exit), `test_launch_cmd.py` and
`test_aucam_client.py` (command-line workflows), `test_spending_ledger.py` and
`test_task_level_accounting.py` (accounting).

## Figures

The figures in `docs/figures/` are generated, so they can be redrawn when the
code changes (the scripts need matplotlib: `pip install matplotlib`):

| Figure | Script |
|---|---|
| `aucam-architecture.png` | `python docs/figures/src/architecture.py` |
| `aucam_budget_burn.png`, `aucam_bandit_budget.png`, `aucam_run_limit.png` | `python campaigns/hpc_campaign/figures/doc_figures.py` (real runs, ~1 min) |
| `aucam_impact.png` | `campaigns/hpc_campaign/figures/run_impact.py --json-out …`, then `figures/gen_impact_fig.py` |
