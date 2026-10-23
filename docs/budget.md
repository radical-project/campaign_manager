# AuCaM budgets and spending

How AuCaM limits what campaigns spend, how it measures spending, and what it
does with the numbers. Sections 1–7 are for campaign authors; sections 8–9
are implementation notes for developers.

---

## 1. Concepts

- **Campaign** — a set of workflows working toward one goal, steered by a
  controller (its decision logic).
- **Workflow** — one unit of work that AuCaM launches and cancels, such as a
  simulation run. It runs as an AsyncFlow block and may submit many **tasks**
  (individual programs or functions) to the execution backend.
- **Budget dimension** — what is counted, e.g. `cpu_hours` or `gpu_hours`
  (resource-hours: 2 GPUs for 1 hour = 2 GPU-hours), or any other quantity a
  workflow reports, such as `llm_tokens`. Each dimension is limited separately.
- **Spending limit** — the most a campaign may spend over its lifetime, per
  dimension.
- **Estimate** — how much one run of a workflow type is expected to cost. It
  starts as the workflow's `declared_spending` and is replaced by the average
  actual cost once a few runs have finished (§6).
- **Spent** — what AuCaM has actually charged so far.
- **In-flight** — what running workflows are still expected to cost and is not
  in *spent* yet. Counting it lets AuCaM stop launching *before* running
  workflows push a campaign over its limit.
- **Ledger** — the single place where all of the above is kept, for every
  campaign (`BudgetAllocator`, §8).

There are two levels of control:

| Level | Limits | Checked |
|---|---|---|
| Campaign | `CampaignBudget.spending` | Before launching workflows (§4) |
| Global (all campaigns in one AuCaM run) | `GlobalBudget.spending` | When a campaign is submitted (§4) |

---

## 2. Configuration

```python
# Campaign limit (optional; no `spending` = no limit)
CampaignBudget(
    max_concurrent_workflows=4,
    max_concurrency_per_workflow=2,
    spending={"cpu_hours": 1.0, "gpu_hours": 0.25},
)

# Expected cost of one run of a workflow type (optional)
WorkflowConfig(
    name="sim",
    block=my_workflow,
    declared_spending={"wall_hours": 0.5, "cpu_hours": 0.5, "gpu_hours": 0.05},
)

# Global limit across all campaigns (optional)
CampaignManager(
    global_budget=GlobalBudget(
        spending={"gpu_hours": 50.0},
        max_concurrent_campaigns=4,
        max_rejections=10,  # stop accepting campaigns after more than 10 budget rejections
    )
)
```

`declared_spending` has two jobs:

- it is the initial **estimate**, which AuCaM holds as in-flight while the
  workflow runs;
- its `wall_hours` (expected run time) lets AuCaM **prorate** the cost when
  nothing better is measured (§3, method C). `wall_hours` itself is never
  charged.

A workflow without `declared_spending` still runs, but AuCaM reserves nothing
for it in advance and may charge it nothing. AuCaM logs a warning at start-up
when a campaign has a spending limit and a workflow type has no
`declared_spending`.

---

## 3. How spending is measured

AuCaM uses the most accurate source available for each workflow:

| Method | Source | When it is charged | `[SPEND] wf_done` shows |
|---|---|---|---|
| **A. Per task** | AsyncFlow telemetry: resources each task requested × how long it ran | As each task finishes | `method=exact source=task_telemetry` |
| **B. Reported** | Values the workflow reports itself | When the workflow ends | `method=exact source=wf_reported` |
| **C. Prorated** | `declared_spending` × actual / declared run time | When the workflow ends | `method=estimated source=wall_proration` |
| **D. None** | — | Never | `method=none source=no_declared_spending` |

### A. Per task

With telemetry on (`CampaignManager(enable_telemetry=True)`), AsyncFlow
publishes a telemetry event each time a task is created, starts and ends. Each
event identifies the workflow the task belongs to, the resources the task
requested, and (when the task ends) how long it ran. AuCaM subscribes to these
events. When a task ends, AuCaM charges the workflow `cpus × hours` and
`gpus × hours`, where `cpus` and `gpus` are what the task **requested** (not
what it actually used) and `hours` is how long it ran.

- A task counts only if its `task_description` states the resources it
  requests. On Dragon, that is `process_template` (one process) or
  `process_templates` (an MPI job: a list of `(ranks, template)`). Each rank
  counts as 1 CPU and no GPUs; add `cores_per_rank` or `gpus_per_rank` to
  charge more (Dragon ignores these two keys; only AuCaM reads them). For
  example:

  ```python
  task_description = {
      "process_templates": [(4, {})],
      "gpus_per_rank": 1,
  }  # 4 CPUs and 4 GPUs × run time
  ```

  A Dragon function task without a template is not charged per task; AuCaM
  uses methods B–D for it.
- The first such task switches the workflow to method A for the rest of its
  run, and AuCaM no longer charges it by methods B–D. Per-task charges are
  more accurate, and adding a workflow-level estimate on top would count those
  tasks twice. Tasks of that workflow without resource keys state no
  resources, so they are not charged; give them resource keys if their cost
  matters.
- Tasks inside nested blocks are charged to the workflow that contains them.
- Only `cpu_hours` and `gpu_hours` are charged this way. Other dimensions that
  a method-A workflow reports (e.g. `llm_tokens`) are **not** charged — a known
  gap (§7, B0).
- Method A needs an AsyncFlow version whose telemetry events carry the
  task's requested resources (not yet released). Without it, or with
  telemetry off, AuCaM uses methods B–D.

While the workflow runs: in-flight = estimate − charged so far (never below 0).

### B. Reported

The workflow reports how much it has spent so far, as metrics in its regular
reports. The values are running totals since the workflow started, not amounts
per report:

```python
await reporter.emit(score=s, metrics={"cpu_hours": 1.2, "loss": 0.03, "step": 40})
```

Metrics that count as spending are `cpu_hours`, `gpu_hours`, and any other
name that also appears in the workflow's `declared_spending` (e.g.
`llm_tokens`). Other keys in the same `metrics`, such as `loss` or `step`
above, are not charged; the controller still receives them as usual. When the
workflow ends, AuCaM charges the totals from its last report.

While the workflow runs: in-flight = the larger of the estimate and the latest
reported value. It never drops below the estimate, because nothing is in
*spent* yet.

### C. Prorated

For each dimension in `declared_spending` that the workflow did not report:

```
charge = declared_spending[dim] × actual_wall_hours / declared_spending["wall_hours"]
```

A workflow that finishes early is charged less than declared. One that runs
longer is charged more, unless `WorkflowConfig.max_wall_time_s` cancels it
first.

While the workflow runs: in-flight = estimate × max(1, elapsed / declared
wall time), so it grows once the workflow overruns.

Methods B and C combine per dimension: a reported dimension uses B, the others
use C.

### D. None

A dimension that is neither reported nor declared (with `wall_hours`) is not
charged.

---

## 4. How limits are enforced

### Campaign limit

Before launching, AuCaM checks every limited dimension:

```
spent + in_flight >= limit   →   launch nothing more
```

AuCaM does not stop running workflows when the limit is reached. It keeps the
campaign within its limit by counting each running workflow's expected cost in
advance (in-flight) and starting no new workflow once spent + in-flight reaches
the limit. The campaign can end slightly above its limit only when running
workflows cost more than expected (in the figure below, some ran longer than
declared).

When the limit is reached:

- no new workflows are started; running workflows continue to the end;
- the campaign ends with exit reason `spending_limit` once none is running;
- the log shows `[SCHEDULER-<campaign>] spending limit reached: <dim>: completed=… + in-flight=… >= …`.

A campaign that needs a hard limit can enforce one in its own controller: each
policy cycle `obs["budget"]` shows what is spent, in flight and remaining
(§5), and the controller can cancel running workflows.

![Campaign spending limit](figures/aucam_budget_burn.png)

The check covers the whole campaign, not each workflow type: one expensive
type can use up the budget before others run. Planned: skip only the requests
whose estimated cost doesn't fit, so a cheaper workflow type can still start
(B2 in §7 below and in [known gaps](known-gaps.md)).

A campaign that is idle below its limit ends separately, after
`CampaignSpec.idle_exit_s` (see the developer guide, "How a campaign ends").

### Global limit

When a campaign is submitted, AuCaM checks each dimension of
`GlobalBudget.spending`:

```
spent by finished campaigns + limits of running campaigns + new campaign's limit <= global limit
```

If it does not fit, the campaign is rejected with a reason. A running
campaign's whole limit counts until it ends; then its actual spending replaces
it. A campaign that sets no limit for a dimension adds nothing for that
dimension, so the global limit cannot hold back its spending there.

`GlobalBudget` has four more optional settings, all off unless set:

- `max_concurrent_campaigns` and `max_total_concurrent_workflows`: a campaign
  that would exceed either is rejected;
- `n_campaigns`: AuCaM stops accepting campaigns after this many have finished;
- `max_rejections`: AuCaM stops accepting campaigns after more than this many
  budget rejections.

![Global spending limit](figures/aucam_global_budget.png)

*A and B reserve their limits (24 of 32 CPU·s), so C is rejected. When A
finishes, only its actual spending (6) stays counted, so D fits.*

---

## 5. What decision logic sees

### Controller

Every policy cycle (the controller's periodic decision step), the
controller's observation includes `obs["budget"]` for its own campaign:

```python
{
    "limits": {"cpu_hours": 1.0, "gpu_hours": 0.25},
    "spent": {"cpu_hours": 0.34, "gpu_hours": 0.028},
    "in_flight": {"cpu_hours": 0.10},
    "global_spent": {"gpu_hours": 0.12},  # finished campaigns, all of AuCaM
    "burn": {
        "cpu_hours": {"remaining": 0.56, "fraction_left": 0.56},
        "gpu_hours": {"remaining": 0.222, "fraction_left": 0.888},
    },
}
```

`remaining = max(0, limit − spent − in_flight)`, `fraction_left = remaining / limit`.
A controller can use these, for example, to stop launching expensive types or
to end the campaign early.

### Scheduler (bandit)

The default scheduling policy (a UCB1 bandit, which balances trying workflow
types it knows little about against favoring types that complete) explores
less as the budget is used up:

```
used        = max over limited dimensions of (spent + in_flight) / limit   (0–1)
exploration = default_exploration × √(1 − used)
```

With no budget used it explores normally; with the budget used up it only
picks the types that have done best so far.

![Bandit and budget](figures/aucam_bandit_budget.png)

---

## 6. Learned cost estimates

After 3 successful runs of a workflow type, AuCaM uses their average actual
cost as the estimate for new runs of that type, instead of
`declared_spending`. Only successful runs count, since cancelled and failed
runs are partial.

Limits of the current version: the number of runs (3) is fixed, and the
averages are not saved in checkpoints, so after a restart estimates start
again from `declared_spending`.

---

## 7. Results, logs and status

### Per-workflow spending

`WorkflowResult.spent` holds what the workflow was charged, e.g.
`{"cpu_hours": 0.035, "gpu_hours": 0.003}`. The controller's `on_complete()`
and on-completion hooks receive it. (For method A, task charges that arrive
after the workflow has ended go into the ledger but not into `spent`.)

### Log lines

| Line | Level | When |
|---|---|---|
| `[SPEND] wf_done  wf=… method=… source=… cpu_h=… gpu_h=… wall_s=… rc=…` | INFO | A workflow ends (method: §3) |
| `[SPEND] campaign=…  spent_cpu_h=… spent_gpu_h=… in_flight_cpu_h=… in_flight_gpu_h=… budget_used=…%` | INFO | Each policy cycle, once anything is spent or in flight |
| `[SPEND] task_started  wf=… task=… cpus=… gpus=…` | INFO | A method-A task starts |
| `[SPEND] task_charged  wf=… task=… cpu_h=… gpu_h=… wf_cpu_h=… wf_gpu_h=… budget_used=…%` | INFO | A method-A task is charged |
| `[SCHEDULER-…] spending limit reached: …` | INFO | Campaign limit reached (once) |
| `[BANDIT] …  budget_rem=…%  c_eff=…  arms={type: μ=…}` | INFO | The bandit chooses among queued requests |
| `[BANDIT] …  → <type>  ucb=…  c_eff=…` | INFO | The bandit picks a type it has seen before |
| `[CM] Campaign … SUMMARY — exit=… spent={…}` | INFO | A campaign ends |
| `[BLOCK-TAG] …`, `[SPEND-DBG] … filtered` | DEBUG | Method-A event routing (§9) |

### Not implemented yet

| ID | Feature | Status |
|---|---|---|
| B0 | Any budget dimension | Partial: limits, in-flight and methods B/C accept any dimension; method A charges only CPU/GPU hours; `[SPEND]` logs show only CPU/GPU |
| B1 | Burn-down in the observation | Partial: `remaining`, `fraction_left`; missing spending rate, projected spending, time to limit, cost per type |
| B2 | Launch only requests that fit the remaining budget | Not started |
| B3 | Learned cost estimates | Done (§6); count not configurable, not checkpointed |
| B4 | `CostCancelRule`: cancel workflows that spend without improving | Not started |
| B5 | Cost-aware bandit | Partial: exploration shrinks with the budget (§5); reward not yet cost-weighted |

---

## 8. Implementation: the ledger (`src/aucam/budget.py`)

`BudgetAllocator` is shared by `CampaignManager` and all campaigns; every
public method takes one lock.

| Kept per campaign | Meaning |
|---|---|
| `limits` | `CampaignBudget.spending`, set at admission |
| `spent` | Actual charges |
| `in_flight[wf_id]` | Expected remaining cost of each running workflow (§3) |

| Kept globally | Meaning |
|---|---|
| `global_spent` | `spent` of closed campaigns |
| committed | Sum of `limits` of open campaigns |

| Method | Called from | Purpose |
|---|---|---|
| `admit()` | `CampaignManager` (submit) | Global check, open the campaign |
| `set_in_flight()` | `_launch()`; `_on_task_event()` (A); `_collect_telemetry()` (B–D) | Set a workflow's in-flight |
| `record()` | `_on_task_event()` (A); `_on_done()` (B–D) | Add charges to `spent` |
| `clear_in_flight()` | `_on_done()` | Workflow ended |
| `limit_reached()` | `_spending_limit_reached()` | Campaign limit check (§4) |
| `spending_fraction()` | `_drain_queue()` → `SchedulingContext.budget_remaining_fraction`; `[SPEND]` logs | "used" in §5 |
| `campaign_view()` | Policy cycle | `obs["budget"]` (§5) |
| `close()` | `CampaignManager` | Move `spent` into `global_spent` |

Per-workflow state in `_CampaignInstance` (`src/aucam/campaign.py`):
`_estimates` (launch estimate), `_task_level` (workflows on method A),
`_task_usage` (their charges), `_type_cost_stats` (learned averages, §6).

## 9. Implementation: routing task events (method A)

AsyncFlow tags each task event with `attributes["asyncflow.workflow_id"]`, the
uid of the enclosing block, and adds `attributes["resources"]` for tasks that
requested resources (`requested_resources()` in asyncflow's
`workflow_manager.py`). AuCaM maps tags to workflows in `_wf_tags`:

- `_launch()` maps the workflow id to itself;
- `_run_as_block()` maps the workflow's block uid (read from the block's own
  future through `_fut_box` when the block body starts);
- `_on_task_event()` maps each nested block (a `TaskCreated` event whose
  `task_id` starts with `block.` and whose parent tag is already mapped), so
  any depth of nesting works.

`_on_task_event()` marks a workflow method A on its first event with
resources, charges `TaskCompleted` / `TaskFailed` / `TaskCanceled` events, and
updates in-flight. `_on_done()` then skips the completion-time charge for that
workflow.
