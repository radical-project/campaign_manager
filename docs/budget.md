# AuCaM — Budget, Spending, and Burn-Down

This document is the single reference for everything related to resource budgets
in AuCaM: how limits are declared, how spending is measured, how the ledger
works, what the scheduler and controller see, and what is logged.

Cross-references: `architecture-overview.md` §"Budget accounting",
`asyncflow_integration.md` §R1, `campaign-developer-guide.md` §"Budget and
concurrency".

---

## 1. What AuCaM tracks

AuCaM budgets **resource-hours**: the total compute consumed by a campaign
across its lifetime.  Each dimension is independently limited — `cpu_hours`,
`gpu_hours`, or any custom dimension such as `llm_tokens`.

A limit of `gpu_hours: 10.0` means the campaign may consume at most 10
GPU-hours in total.  Two workflows running in parallel, each using 1 GPU for
1 hour, count as 2 GPU-hours against that limit.

There are two independent budget levels:

| Level | Scope | Checked when |
|---|---|---|
| **Per-campaign** | One campaign's lifetime spending | Before each new workflow is launched |
| **Global** | All campaigns in one CM session | When a new campaign is submitted |

---

## 2. Configuration

### Per-campaign spending limit

```python
CampaignBudget(
    max_concurrent_workflows=4,
    max_concurrency_per_workflow=2,
    spending={"cpu_hours": 1.0, "gpu_hours": 0.25},   # optional
)
```

`spending` is optional; omitting it means unlimited.  Any dimension key is
accepted (`llm_tokens`, etc.).

### Workflow declared spending

```python
WorkflowConfig(
    name="sim",
    block=my_workflow,
    declared_spending={"wall_hours": 0.5, "cpu_hours": 0.5, "gpu_hours": 0.05},
)
```

`declared_spending` is optional (defaults to `{}`).  A workflow without it
runs and completes normally, but AuCaM cannot reserve budget before launch and
may charge it zero.  CM logs a warning at startup when a spending limit is set
but a workflow has no `declared_spending`.

At workflow completion, AuCaM charges spending using the first available source,
in priority order:

1. **Task telemetry** — if the workflow uses asyncflow tasks with explicit
   resource specs, each task is charged `cpus × duration` and `gpus × duration`
   as it completes.  Requires `cpu_hours` / `gpu_hours` in `declared_spending`
   for the pre-launch reservation.
2. **Workflow-reported** — the workflow calls `reporter.emit(cpu_hours=X,
   gpu_hours=Y)` and AuCaM uses those values directly.  Requires matching keys
   in `declared_spending` for the reservation.
3. **Proration** — neither of the above is available.  AuCaM estimates spending
   as `declared × (actual_wall / wall_hours)`.  Requires both the resource
   dimensions (`cpu_hours`, etc.) and `wall_hours` in `declared_spending`.
4. **Zero** — no `declared_spending`, no telemetry, no reports.  Workflow is
   charged nothing; the spending limit is not enforced for it.

### Global spending cap

Caps the total spending across all campaigns in a CM session — the sum of
actual spending from completed campaigns plus the declared limits of all
currently running ones.

```python
CampaignManager(
    global_budget=GlobalBudget(
        spending={"gpu_hours": 50.0},
        max_concurrent_campaigns=4,
        max_rejections=10,      # auto-close after N consecutive rejections
    )
)
```

---

## 3. Accounting paths — how spending is measured

For each completed workflow, exactly one path is chosen, in priority order:

### Option 1 — Task-level (AsyncFlow telemetry events)

A local patch to asyncflow propagates each task's requested resources (`cpus`,
`gpus`) into its telemetry events.  AuCaM charges `cpus × duration / 3600` and
`gpus × duration / 3600` as each task finishes.  More accurate analytics based
on actual resource consumption (rather than requested) is under development.

**Path selection is lazy and one-way.**  When `requested_resources()` returns
`None` (task description contains none of `ranks`, `cores_per_rank`,
`gpus_per_rank`, `process_templates`), the event carries no `resources`
attribute and is filtered — the workflow stays eligible for Options 2/3/4.
The first `TaskCreated` event *with* resources locks the workflow into
Option 1 for the rest of its lifetime (per-task events give fine-grained
accuracy that cannot be combined with workflow-level estimates).  From that
point, unspecced tasks in the same workflow are treated as free overhead (not
charged); ordering does not matter — an earlier unspecced task does not
prevent a later specced task from triggering Option 1.

The first `TaskCreated` event with resources marks the workflow `_task_level`.
From that point the budget reservation held against the spending limit
(`in_flight`) shrinks after each task completes, rather than staying fixed
at `declared_spending` until workflow end:

```
in_flight[wf_id] = max(0, declared_spending - cumulative_charged)
```

At workflow completion (`_on_done`), the completion-time charge is **skipped**
for task-level workflows — they are already fully charged per task.

**Tag mapping** is the critical piece that routes events to the correct campaign:
- `_launch()` seeds `_wf_tags[wf_id] = wf_id` (self-mapping for direct-task
  workflows).
- `_run_as_block()` uses the `_fut_box` closure: `_block()` reads
  `fut.block["uid"]` at call time (guaranteed set by asyncflow before
  `execute_block` calls `_block`) and stores `_wf_tags[block_uid] = wf_id`.
- `_on_task_event()` propagates **inner blocks**: when a `TaskCreated` event
  has no resources and `task_id.startswith("block.")`, and the parent tag is
  already in `_wf_tags`, the child block uid is registered too.  This chains
  through arbitrarily nested blocks (e.g. ddsim `miniapps_fast` has 2 levels).

### Option 2 — Workflow-reported

The workflow calls `reporter.emit(metrics={"cpu_hours": X, "gpu_hours": Y})`.
CM reads the last emitted report at completion and uses those values directly.
Takes priority over proration.  Also picks up custom dimensions declared in
`declared_spending` (e.g. `llm_tokens`).

**Live in-flight** — each policy cycle, `_collect_telemetry()` reads the last
report and updates the ledger:
```
in_flight[wf_id][dim] = max(0, declared[dim] - reported[dim])
```

### Option 3 — Proration

```
spending[dim] = declared_spending[dim] × (actual_wall_s / declared_wall_s)
```

Used when neither Option 1 nor Option 2 values are available.

- **Option A** (`max_wall_time_s` set): ratio ≤ 1 always (workflow auto-cancelled
  at wall limit); can only underspend.
- **Option B** (no `max_wall_time_s`): ratio may exceed 1 if the workflow runs
  past its declared wall time; actual spending may exceed declared.

**Live in-flight** — each policy cycle, `_collect_telemetry()` shrinks the
reservation proportionally to elapsed wall time:
```
remaining_frac = max(0, 1 - elapsed_h / declared_wall_h)
in_flight[wf_id][dim] = declared[dim] × remaining_frac
```
Falls back to this when Option 2 has no report yet.

### Option 4 — Zero

No `declared_spending` set.  Workflow contributes nothing to the ledger.
CM warns once at campaign startup.

### Identifying the path in logs

Every workflow completion emits a `[SPEND] wf_done` line at INFO level:

```
[SPEND] wf_done  wf=<wf_id>  method=<method>  source=<source>  cpu_h=X  gpu_h=Y  wall_s=Z  rc=N
```

| `method` | `source` | Meaning |
|---|---|---|
| `exact` | `task_telemetry` | Option 1 — charged per individual task via asyncflow events |
| `exact` | `wf_reported` | Option 2 — workflow emitted `cpu_hours`/`gpu_hours` via `reporter.emit()` |
| `estimated` | `wall_proration` | Option 3 — `declared × (actual_wall / declared_wall)` |
| `none` | `no_declared_spending` | Option 4 — no data available; nothing charged |

---

## 4. The ledger — `BudgetAllocator` (`src/aucam/budget.py`)

Single shared object; all public methods hold a lock.

```
per campaign:
  limits      — CampaignBudget.spending (set at admission)
  spent       — actual cumulative spending of finished workflows
  in_flight   — estimated remaining cost of running workflows; shrinks every
                policy cycle for all accounting options (not only Option 1)

global:
  global_spent — sum of spent across all closed campaigns
  committed    — sum of limits across all open campaigns (for admission check)
```

Key methods:

| Method | Called by | When |
|---|---|---|
| `admit(campaign, limits, global_limits)` | `CampaignManager.submit_campaign()` | Before campaign starts |
| `set_in_flight(campaign, wf_id, estimate)` | `_launch()`, `_on_task_event()` (R1), `_collect_telemetry()` (non-R1) | On launch, per task (R1), per policy cycle (non-R1) |
| `record(campaign, usage)` | `_on_done()` (Options 2/3), `_on_task_event()` (Option 1) | On charge |
| `clear_in_flight(campaign, wf_id)` | `_on_done()` | Workflow ends |
| `limit_reached(campaign)` | `_spending_limit_reached()` in scheduler | Before each launch batch |
| `campaign_view(campaign)` | `_CampaignInstance.observe()` | Each policy cycle |
| `spending_fraction(campaign)` | `BanditSchedulingPolicy.select()`, `[SPEND] task_charged` log | Per task + per cycle |
| `close(campaign)` | `CampaignManager` | Campaign ends |

### `limit_reached()` semantics

Conservative: `spent + sum(in_flight) >= limit` in any dimension.  This counts
the full declared in-flight estimate even if the workflow will likely underspend
(R1 shrinks `in_flight` incrementally, but it starts at `declared_spending`).

---

## 5. Spending limit enforcement

`_spending_limit_reached()` calls `limit_reached()` at the start of each
`_drain_queue()` invocation.  When a limit is reached:

- No new workflows are launched (`[SCHEDULER] spending limit reached` at INFO).
- Already-running workflows continue to completion.
- The campaign exits with `reason="spending_limit"` once the queue is empty and
  nothing is running.

The limit is checked campaign-wide (any dimension), not per workflow type.  If
you have a budget for `cpu_hours` and two workflow types, a cpu-heavy type can
exhaust the budget before the gpu-heavy type has had its turn.  See B2 in
per-request fit checking (not yet implemented).

---

## 6. Burn observation — what the controller sees

Each policy cycle, `campaign_view()` is passed to the controller as
`obs["budget"]`:

```python
{
    "limits":       {"cpu_hours": 1.0, "gpu_hours": 0.25},
    "spent":        {"cpu_hours": 0.34, "gpu_hours": 0.028},
    "in_flight":    {"cpu_hours": 0.10},
    "global_spent": {"gpu_hours": 0.12},
    "burn": {
        "cpu_hours": {"remaining": 0.56, "fraction_left": 0.56},
        "gpu_hours": {"remaining": 0.222, "fraction_left": 0.888},
    },
}
```

`burn[dim].remaining = limit - spent - in_flight` (floored at 0).
`burn[dim].fraction_left = remaining / limit`.

Still missing from `burn`: `rate_per_hour`, `projected`, `hours_to_limit`,
`by_type` — not yet implemented.

---

## 7. Bandit integration

`BanditSchedulingPolicy.select()` reads `spending_fraction(campaign)` —
the maximum over all limited dimensions of `(spent + in_flight) / limit`,
clamped to [0, 1] — and scales the exploration weight:

```
c_eff = exploration × √(1 − spending_fraction)
```

As the budget is consumed, `c_eff → 0` and the bandit switches from exploration
to pure exploitation.  The `[BANDIT]` log line emitted each cycle shows
`budget_rem=%`, `c_eff`, and each arm's mean score.

---

## 8. Learned cost estimates (B3)

After `min_samples` (default 3) successful completions of a workflow type,
`_learned_cost()` returns the per-type mean actual spending instead of
`declared_spending` for the in-flight estimate set at launch time.  The mean
is updated in `_update_type_cost()` on each `_on_done`.  Learned stats are
stored in the campaign checkpoint and survive a restart.

---

## 9. `WorkflowResult.spent`

Every `WorkflowResult` carries the actual charged spending of that workflow:

```python
result.spent  # e.g. {"cpu_hours": 0.0349, "gpu_hours": 0.0028}
```

Populated from `_task_usage[wf_id]` (Option 1) or from the proration/reported
contribution.  Written to `results_<label>.jsonl` by `_ResultsWriter`.

---

## 10. Log reference

All budget-related log lines (prefix `[SPEND]` or `[BANDIT]`):

| Line | Level | When |
|---|---|---|
| `[SPEND] task_started  wf=… task=… cpus=N gpus=M` | INFO | `TaskStarted` with resources |
| `[SPEND] task_charged  wf=… task=… cpu_h=X gpu_h=Y wf_cpu_h=X wf_gpu_h=Y budget_used=N%` | INFO | `TaskCompleted/Failed` |
| `[SPEND] wf_done  wf=… method=<method> source=<source> cpu_h=X gpu_h=Y wall_s=Z rc=N` | INFO | Workflow completion |
| `[BLOCK-TAG] wf=… block_uid=…` | DEBUG | Outer block uid registered (`_fut_box`) |
| `[BLOCK-TAG] inner block wf=… parent=… child=…` | DEBUG | Inner block uid propagated |
| `[SPEND-DBG] <event> filtered: res=F tag=F` | DEBUG | Event has no resources or no tag (unspecced task) |
| `[SCHEDULER-<name>] spending limit reached: <dim>: completed=X + in-flight=Y >= Z` | INFO | Limit hit |
| `[CM] Campaign <name> SUMMARY — exit=spending_limit … spent={…}` | INFO | Campaign ends |
| `[BANDIT] budget_rem=X% c_eff=Y arm[type]=Z` | DEBUG | Per policy cycle |

---

## 11. Pending work (summary)

| ID | Feature | Status |
|---|---|---|
| B1 | `rate_per_hour`, `projected`, `hours_to_limit` in burn observation | Not implemented |
| B2 | Skip requests that don't fit budget instead of blocking all launches | Not implemented |
| B4 | `CostCancelRule` — cancel workflow that spends without score improvement | Not implemented |
| B5 | Cost-aware UCB1 — reward weighted by cost, C scaled by `fraction_left` | Not implemented |
