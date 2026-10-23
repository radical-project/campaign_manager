# Code walkthrough

A map of the code for contributors: what each module holds and the main paths
through it. Concepts and design decisions are in the [architecture overview](architecture-overview.md),
planned work in [known gaps](known-gaps.md), and budget internals in
[budgets and spending](budget.md) §8–9.

---

## Module map

| Module | Holds |
|---|---|
| `src/aucam/manager.py` | `CampaignManager`: admission (`submit()`, `_admit()`), driver bookkeeping (`expect_driver()`, `driver_done()`), the main `run()` loop, auto-close, signal handling |
| `src/aucam/campaign.py` | `_CampaignInstance`: everything that runs one campaign — launch, policy cycle, scheduler, accounting, dependencies, checkpoint, shutdown |
| `src/aucam/controller.py` | `CampaignController` base class, `CancelRule`, `LaunchRule`, `Goal`, `PolicyResult` |
| `src/aucam/scheduler.py` | `SchedulingPolicy` (FIFO base), `FIFOSchedulingPolicy`, `BanditSchedulingPolicy` (UCB1), `ADRSchedulingPolicy` / `LLMSchedulingPolicy` (stubs), `SchedulingContext` |
| `src/aucam/queue.py` | `WorkflowQueue`: priority heap with aging |
| `src/aucam/budget.py` | `BudgetAllocator`: the spending ledger for all campaigns |
| `src/aucam/spec.py` | `CampaignSpec`, `CampaignBudget`, `WorkflowConfig`, `WorkflowDependency` |
| `src/aucam/types.py` | `WorkflowResult`, `LaunchRequest`, `GlobalBudget`, `CampaignOutcome`, `SubmitResult`, snapshots and enums |
| `src/aucam/reporter.py` | `WorkflowReport`, `WorkflowReporter` (interface), `LocalDictReporter` |
| `src/aucam/data_backend.py` | `DataBackend` protocol; `LocalDictBackend`, `RadexDragonBackend`, `RhapsodyRedisBackend` and their reporters |
| `src/aucam/checkpoint.py` | `CampaignCheckpoint` and its JSON read/write |
| `src/aucam/profiler.py` | `BlockProfiler`, `TaskProfiler`: optional CPU/memory sampling inside workflows |
| `src/aucam_client/` | `Reporter` and the `aucam-report` command for command-line workflows (any language) |
| `campaigns/` | Example campaigns: `demo_campaign` (quick start, capability demo), `ddsim_aucam` (HPC example) |

---

## Main paths

### Admission — `CampaignManager`

```
submit(spec) / submit_campaign(spec)
└── _admit(spec)
    ├── _policy_check()          scheduling-policy instance not used by another campaign
    ├── _admission_check()       running campaigns, total concurrent workflows (GlobalBudget)
    └── BudgetAllocator.admit()  spent + committed limits + new limit ≤ global spending
```

Rejected → `SubmitResult(accepted=False, reason=…)`. Accepted → the spec goes
to the submission queue; `run()` starts it with `_start_campaign()`, which
creates a `_CampaignInstance` and a task for its `run()`. When the task ends,
`_resolve_future()` closes the campaign in the ledger and resolves the
driver's `CampaignOutcome`.

`run()` creates the shared AsyncFlow engine (and telemetry, if enabled), then
waits on campaign tasks, the submission queue and a shutdown event until all
drivers are done and all campaigns have finished.

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
code changes:

| Figure | Script |
|---|---|
| `aucam-architecture.png` | `python docs/figures/src/architecture.py` |
| `aucam_budget_burn.png`, `aucam_bandit_budget.png`, `aucam_global_budget.png` | `python campaigns/demo_campaign/doc_figures.py` (real runs, ~1 min) |
| `aucam_impact.png` | `campaigns/demo_campaign/run_impact.py --json-out …`, then `gen_impact_fig.py` |
