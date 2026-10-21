# AuCaM ↔ radical.asyncflow integration contract

Audience: agents working on `campaign_manager` (AuCaM) or `radical.asyncflow`.
Purpose: what AuCaM requires from asyncflow, where each side implements it, how
to test it, and what happens when it is missing. Keep this file in sync when
either side changes.

## Status

| Item | asyncflow side | AuCaM side |
|---|---|---|
| R1. Requested resources on task telemetry | Implemented, **not released** (uncommitted, on top of asyncflow 0.6.0 / `175cf76`) | Implemented (`src/aucam/campaign.py`) |
| R2. Exit code on task futures | Implemented, **not released** (same change set) | Implemented (`src/aucam/campaign.py`) |

AuCaM depends on `radical.asyncflow>=0.3.0` (`pyproject.toml`). Released
asyncflow (≤ 0.6.0) has neither R1 nor R2; AuCaM degrades gracefully (see
"Fallback"). When asyncflow releases them, raise the minimum version in
`pyproject.toml` and remove gap 3 from `docs/known-gaps.md`.

## R1. Requested resources on task telemetry events

### Contract (asyncflow provides)

- Events `TaskCreated`, `TaskStarted`, `TaskCompleted`, `TaskFailed`,
  `TaskCanceled` carry `event.attributes["resources"] = {"ranks": float,
  "cpus": float, "gpus": float}`.
- Only tasks carry it; blocks never do (prevents double counting).
- Events also carry (pre-existing) `event.duration_seconds` on end events and
  `event.attributes["asyncflow.workflow_id"]`: the uid of the enclosing block,
  which every task registered inside the block inherits.
- `resources` is computed once per task from
  `task_backend_specific_kwargs` (the task_description) by
  `radical.asyncflow.workflow_manager.requested_resources(desc) -> Optional[dict]`.
  Returns `None` when `task_description` contains none of the backend resource
  keys (`ranks`, `cores_per_rank`, `gpus_per_rank`, `process_templates`);
  events for those tasks carry no `resources` attribute and are filtered by
  AuCaM.  Rules when a key is present, first match wins:
  - `ranks`: `ranks` → sum of `entry[0]` in `process_templates` (Dragon) → 1
  - `cpus`: `cpus` → `ranks * cores_per_rank` (default 1)
  - `gpus`: `gpus` → `ranks * gpus_per_rank` (default 0)
  - Uninterpretable values fall back to the defaults.

### asyncflow implementation

File `src/radical/asyncflow/workflow_manager.py`:
- `requested_resources()` — module-level helper (new).
- In component registration: `comp_desc["_tel_resources"] =
  requested_resources(...)` for `comp_type == TASK`, else `None`.
- `TaskCreated` emission: adds `"resources"` to attributes when set.
- Task state-update path (`tel_attrs` for TaskStarted/Completed/Failed/Canceled):
  adds `tel_attrs["resources"]` when set.
- Tests: `tests/unit/test_task_resources_telemetry.py`.

### AuCaM consumer

File `src/aucam/campaign.py`, class `_CampaignInstance`:
- Subscription: `telemetry.subscribe(self._on_task_event)` (only when
  `CampaignManager(enable_telemetry=True)`).
- Tag mapping: `_run_as_block()` reads the block's tag from
  `flow._workflow_id_ctx` inside the block body and stores
  `self._wf_tags[tag] = wf_id`; `_launch()` also maps `wf_id → wf_id`.
- `_on_task_event(event)`:
  1. Ignore events without `resources` or without a known tag.
  2. `TaskCreated` with resources → add wf to `self._task_level` (**lazy,
     one-way**: the workflow stays eligible for Path 1/2/3 until the first
     specced `TaskCreated` arrives; once set, R1 is locked for the lifetime
     of that workflow instance).
  3. On `TaskCompleted|TaskFailed|TaskCanceled` (`_TASK_END_EVENTS`): charge
     `cpu_hours = cpus * duration_s/3600`, `gpu_hours = gpus * duration_s/3600`
     via `BudgetAllocator.record()`; accumulate in `self._task_usage[wf_id]`.
  4. Shrink the running workflow's in-flight estimate:
     `set_in_flight(wf_id, declared_spending - charged)` (floored at 0).
- Unspecced tasks (no `resources` attribute) are filtered at step 1 and treated
  as free overhead — not charged, but they do not prevent R1 from activating if
  another task in the same workflow is specced.
- At workflow end (`_on_done`): if `wf_id in self._task_level`, the
  completion-time charge is skipped (already fully charged per task); Path 1/2/3
  is also skipped.

### Invariants

- Charge = requested resources × task duration; AuCaM never needs placement
  (node/GPU ids) — RHAPSODY decides placement.
- Each task is charged once (end event only); blocks are never charged.
- Spending is live: ledger `spent` grows during a workflow; `spent + in_flight`
  stays an upper-bound estimate used by `limit_reached()` and admission.
- Command-line workflows: their single executable task gets
  `WorkflowConfig.task_description` as the resource request, so they are charged
  the same way as Python workflows.

### Tests

- AuCaM: `tests/test_task_level_accounting.py` (module skipped when
  `workflow_manager.requested_resources` is absent).

## R2. Exit code on task futures

### Contract (asyncflow provides)

- When a task reaches a terminal state (any state except RUNNING), its future
  gets attribute `exit_code` = backend task record's `exit_code` (`None` if the
  backend reports none). Set before the success/failure handlers run.
- Reason: the exception set on failure carries only stderr; callers could not
  distinguish e.g. exit 2 from 137.

### asyncflow implementation

- `src/radical/asyncflow/workflow_manager.py`, task state-update path:
  `if state != RUNNING: task_fut.exit_code = task_dct.get("exit_code")`.
- Tests: `tests/unit/test_task_exit_code.py`.

### AuCaM consumer

File `src/aucam/campaign.py`:
- Command-line workflow = block with one executable task; the inline wrapper
  (`_wrap_cmd`: `bash -c 'export ...; exec <launch_cmd>'`) makes the program's
  exit code the task's.
- `_cmd_block()`: in `finally`, `code = getattr(task, "exit_code", None)`;
  if not None, `self._exit_codes[wf_id] = code`.
- `_on_done()` maps to `WorkflowResult.returncode`:
  - `0` completed; `-1` cancelled by AuCaM (cancellation wins);
  - failure: stored code if non-zero; negative (signal N) → `128 + N`
    (so `-1` keeps meaning "cancelled"); missing or 0 → `1`.
- Consumers of `returncode`: `CampaignController.on_complete()`,
  `WorkflowDependency.condition`, `WorkflowConfig.on_completion` hooks,
  `SchedulingPolicy.on_outcome()` (bandit reward).

### Tests

- AuCaM: `tests/test_launch_cmd.py::test_exit_code_reaches_workflow_result`
  (skipped via `_asyncflow_reports_exit_code()` when futures lack `exit_code`).

## Fallback (asyncflow without R1/R2)

- No R1: workflows are charged once, at completion, from the first available of:
  (1) `cpu_hours`/`gpu_hours` in the last WorkflowReport metrics;
  (2) `declared_spending` × (actual wall time / declared `wall_hours`);
  (3) node telemetry. The in-flight estimate stays the full `declared_spending`
  until the workflow ends. (`_compute_workflow_spent()` in `campaign.py`.)
- No R2: a failed command-line workflow gets `returncode = 1`.
- Tracked as gap 3 in `docs/known-gaps.md`.

## How to test both repos together

Copy each checkout without `.git`/`__pycache__`, then:

```
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=<asyncflow>/src python -m pytest -q -p no:cacheprovider tests/unit          # in asyncflow: 125 passed
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=<asyncflow>/src:<campaign_manager> python -m pytest -q -p no:cacheprovider  # in campaign_manager: 78 passed, 1 skipped
```

The one skip is unrelated (`tests/test_resource_profiling.py`: real JSONL
not present). If `test_task_level_accounting.py` or the exit-code test in
`test_launch_cmd.py` is skipped, the asyncflow on `PYTHONPATH` lacks R1/R2.

Verified on 2026-10-20 against asyncflow 0.6.0 (`175cf76`) + R1/R2 changes.

## Related planned work

Per-task live charges from R1 feed future features: spending rate estimation,
learned per-type costs, the launch fit check, `CostCancelRule` and cost-aware
UCB1.  Budget dimensions that asyncflow does not measure (e.g. `llm_tokens`)
come from workflow reports, not from R1.

See `docs/budget.md` for the complete spending and burn-down reference.
