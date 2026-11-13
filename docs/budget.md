# AuCaM budgets and spending

How AuCaM limits what a campaign spends, how it measures spending, and what it
does with the numbers. Sections 1–7 are for campaign authors; sections 8–9
are implementation notes for developers.

---

## 1. Concepts

Campaign, workflow and controller are defined in the
[architecture overview](architecture-overview.md). A workflow runs as an
AsyncFlow block and may submit many **tasks** to the execution backend.

- **Budget dimension** — what is counted, e.g. `cpu_hours` or `gpu_hours`
  (resource-hours: 2 GPUs for 1 hour = 2 GPU-hours), or any other quantity a
  workflow reports, such as `llm_tokens`. Each dimension is limited separately.
  The tokens an AI scheduling policy or controller uses are charged only if
  the campaign limits `llm_tokens` (developer guide §8, "AI policies").
- **Spending limit** — the most a campaign may spend over its lifetime, per
  dimension (`CampaignBudget.spending`).
- **Workflow limit** — the most one workflow may spend: its type's
  `spending_limit` at first, then its learned cost + 25 % (§4, §6).
- **Spent** — what AuCaM has actually charged so far.
- **In-flight** — what running workflows are held at and is not in *spent*
  yet: their limits.
- **Ledger** — the single place where all of the above is kept
  (`BudgetAllocator`, §8).

---

## 2. Configuration

```python
# Campaign limit (optional; no `spending` = no limit)
CampaignBudget(
    max_concurrent_workflows=4,
    max_concurrency_per_workflow=2,
    capacity={"cpus": 64, "gpus": 4},  # the allocation's resources (required)
    spending={"cpu_hours": 1.0, "gpu_hours": 0.25},
)

# Each workflow type (both required)
WorkflowConfig(
    name="sim",
    block=my_workflow,
    spending_limit={"cpu_hours": 1.0},  # starting limit per workflow (§4); no wall_hours
    resources={"cpus": 8, "gpus": 1},  # what one workflow holds at once
    max_wall_time_s=3600,  # the run-time limit
    learn=True,  # False: every workflow keeps this limit (§4)
)

# A different limit for one workflow (optional; overrides the type's)
LaunchRequest.make("sim", spending_limit={"cpu_hours": 2.0})
```

A command-line workflow can leave out `resources`: AuCaM takes them from its
`task_description` (`ranks` × `cores_per_rank` CPUs, `ranks` ×
`gpus_per_rank` GPUs, or Dragon's `process_template(s)`).

---

## 3. How spending is measured

AuCaM uses the most accurate source available for each workflow:

| Method | Source | When it is charged | `[SPEND] wf_done` shows |
|---|---|---|---|
| **A. Per task** | AsyncFlow telemetry: resources each task requested × how long it ran | As each task finishes | `method=exact source=task_telemetry` |
| **B. Reported** | Values the workflow reports itself | When the workflow ends | `method=exact source=wf_reported` |
| **C. Resources × time** | The CPUs / GPUs it was given × how long it ran | When the workflow ends | `method=estimated source=resources_x_time` |
| **D. None** | — | Never | `method=none source=not_measured` |

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
- The first such task switches the workflow to method A until it ends, and
  AuCaM no longer charges it by methods B–D. Per-task charges are
  more accurate, and adding a workflow-level estimate on top would count those
  tasks twice. Tasks of that workflow without resource keys state no
  resources, so they are not charged; give them resource keys if their cost
  matters.
- Tasks inside nested blocks are charged to the workflow that contains them.
- Only `cpu_hours` and `gpu_hours` are charged this way. Other dimensions that
  a method-A workflow reports (e.g. `llm_tokens`) are charged from its last
  report when it ends, as in method B.
- Method A needs an AsyncFlow version whose telemetry events carry the
  task's requested resources (not yet released). Without it, or with
  telemetry off, AuCaM uses methods B–D.

While the workflow runs: in-flight = its limit − charged so far (never below 0).

### B. Reported

The workflow reports how much it has spent so far, as metrics in its regular
reports. The values are running totals since the workflow started, not amounts
per report:

```python
await reporter.emit(score=s, metrics={"cpu_hours": 1.2, "loss": 0.03, "step": 40})
```

Metrics that count as spending are `cpu_hours`, `gpu_hours`, and any other
name that also appears in the workflow's own `spending_limit` or the
campaign's limit (e.g. `llm_tokens`). Other keys in the same `metrics`, such as `loss` or `step`
above, are not charged; the controller still receives them as usual. When the
workflow ends, AuCaM charges the totals from its last report.

While the workflow runs: in-flight = the larger of its limit and the latest
reported value. It never drops below the limit, because nothing is in
*spent* yet.

### C. Resources × time

For `cpu_hours` and `gpu_hours`, when the workflow did not report them:

```
charge = resources["cpus"] × actual_wall_hours     (gpu_hours: resources["gpus"] × …)
```

A workflow is charged for what it holds, as an HPC allocation charges a job,
whether or not it uses all of it. Methods B and C combine per dimension: a
reported dimension uses B, `cpu_hours` / `gpu_hours` otherwise use C.

### D. None

Any other dimension (e.g. `llm_tokens`) that the workflow does not report is
not charged.

---

## 4. How limits are enforced

AuCaM keeps a campaign within its spending limit at three points: before a
workflow starts, while it runs, and as a safety net for the whole campaign.
`wall_hours` is not a spending dimension; use `max_wall_time_s` to limit run
time.

### Before a workflow starts

When a request is about to start, the scheduling policy gives the workflow its
own spending limit and resources: the controller's amount if it gave one,
else its type's starting value (`spending_limit`), then its learned cost
+ 25 %, and 25 % more after a workflow of the
type is cancelled at its limit. The full rule, and how to replace it:
[developer guide](campaign-developer-guide.md) §8, "How much each workflow
gets". Then AuCaM checks every limited dimension:

```
spent + in_flight + the workflow's limit <= campaign limit   →   launch it
```

A request that does not fit waits while other workflows run (a cheaper one,
even of lower priority, may start instead; CRITICAL requests are checked too),
and is dropped once nothing else runs. AuCaM never gives a workflow less than
its amount to make it fit. The log shows `[SCHEDULER-<campaign>] WAIT <type>:
… stays queued` once per request.

### While it runs

The workflow's limit is:

- **Held against the campaign.** Its in-flight amount is its limit, in every
  dimension the limit names, so the running workflows together are never held
  at more than what is left.
- **Passed to the workflow.** A Python workflow with a `context` parameter
  finds it in `context["spending_limit"]`; a command-line workflow gets one
  environment variable per dimension, e.g. `CM_SPENDING_LIMIT_CPU_HOURS=1.0`.
  The workflow can use it to size its own work.
- **Enforced.** Every policy cycle, AuCaM measures each workflow's spending so
  far, with the best source available, in the same order it charges (§3):
  per-task charges (finished tasks plus the elapsed share of running ones),
  else the running totals the workflow reports, else the CPUs / GPUs it holds
  × elapsed time. A dimension with none of these is not checked.
  When spending so far exceeds the limit in any dimension, AuCaM cancels the
  workflow — gracefully if its type has a `cancel_grace_s` — and logs
  `[WORKFLOW-…] spending limit exceeded <dim>=… > … — auto-cancel`. The
  workflow is charged what it used; its `WorkflowResult` has
  `over_budget=True` and the limit in `spending_limit`;
  `CampaignOutcome.budget_cancels` counts such workflows.

The check runs once per policy cycle, so a workflow can overshoot its limit by
what it spends in one `policy_interval_s` (more if it reports rarely, plus its
grace period), and the campaign its limit by as much. AuCaM's check is the
safety net: a workflow that must stay within its limit should check it itself
and stop before a step that would pass it, as all the example campaigns do.
To tighten it, lower `policy_interval_s`; a separate, faster check is not done
because it would read every running workflow's report each time.

![Per-workflow spending limit](figures/aucam_run_limit.png)

*Two workflow types, each with a 3 CPU·s limit per workflow. `steady` reads
its limit and sizes its work to fit; `runaway` ignores it, and AuCaM cancels
every `runaway` workflow as it passes the limit (the tick on each bar). Generated by
`campaigns/hpc_campaign/figures/doc_figures.py --only runlimit`.*

Ready-made configurations show this: `campaigns/demo_campaign/run_campaign.py
--overspend`, `campaigns/hpc_campaign/campaign_configs/campaign_overuse.yaml`
and `campaigns/ddsim_aucam/campaign_configs/campaign_overuse.yaml`; for
resources (a workflow that uses more CPUs than it was given),
`run_campaign.py --overcpu` and `campaign_overuse_cpus.yaml`.

### Safety net: the campaign limit

A workflow's limit covers only the dimensions it names. If the campaign
limits another one — e.g. `llm_tokens`, or `gpu_hours` for a type whose limit
names only `cpu_hours` — nothing is held for it. Every policy cycle AuCaM therefore also adds up actual
spending: what has been charged plus what each running workflow has used so
far. Once that goes past the limit, it cancels the running workflows
(gracefully, if their type has a `cancel_grace_s`), charges what they used,
and ends the campaign with `spending_limit`. The log shows
`[SCHEDULER-<campaign>] campaign spending limit exceeded by actual spending:
<dim>=… > … — cancelling N running workflow(s)`. This is
`CampaignBudget.on_limit="cancel"` (the default); with `"finish"`, running
workflows finish and the campaign can end above its limit.

The campaign also ends with `spending_limit` when nothing is running and
either spent + in-flight has reached the limit (`[SCHEDULER-<campaign>]
spending limit reached: …`) or no queued request fits what is left.

![Campaign spending limit](figures/aucam_budget_burn.png)

*Workflows of 2 CPUs that want 0.7–2.5 s of work, up to 3 at once, against a
24 CPU·s limit. A: each workflow gets a limit — 2.5 CPU·s at first, 25 % more
after each cancel at it, then its type's learned cost + 25 % — held against the
campaign; it ends at 23.0 CPU·s. B: the workflows' limits name only GPU-hours,
so nothing is held for CPU-hours; when actual spending passes 24 CPU·s, AuCaM
cancels the running ones (○); it ends at 24.4.
Generated by `campaigns/hpc_campaign/figures/doc_figures.py --only budget`.*

The check covers the whole campaign, not each workflow type: one expensive
type can still use up the budget before others run, as long as each of its
workflows fits.

---

## 5. What decision logic sees

### Controller

Every policy cycle (the controller's periodic decision step), the
controller's observation includes `obs["budget"]`:

```python
{
    "limits": {"cpu_hours": 1.0, "gpu_hours": 0.25},
    "spent": {"cpu_hours": 0.34, "gpu_hours": 0.028},
    "in_flight": {"cpu_hours": 0.10},
    "capacity": {"cpus": {"total": 64, "in_use": 16, "free": 48}},
    "burn": {
        "cpu_hours": {
            "remaining": 0.56,
            "fraction_left": 0.56,
            "rate_per_hour": 0.9,
            "projected": 0.62,
            "hours_to_limit": 0.62,
        },
        "gpu_hours": {...},
    },
    "by_type": {
        "sim": {
            "completed": 5,
            "cancelled": 2,
            "failed": 0,
            "spent": {"cpu_hours": 0.30},
            "avg_cost": {"cpu_hours": 0.043},
            "cost_per_completed": {"cpu_hours": 0.06},
            "mean_score": 0.81,
        },
    },
}
```

Per limited dimension in `burn`:

| Key | Meaning |
|---|---|
| `remaining` | `max(0, limit − spent − in_flight)` |
| `fraction_left` | `remaining / limit` |
| `rate_per_hour` | Spending charged per hour over the last (up to 20) policy cycles; 0 in the first cycle |
| `projected` | spent + in-flight + the estimated cost of the queued requests (each at its own limit) |
| `hours_to_limit` | `remaining / rate_per_hour`; `None` while the rate is 0 |

`by_type` covers every finished workflow of each type: `completed` /
`cancelled` / `failed` counts, total `spent`, `avg_cost` (spent ÷ finished
workflows), `cost_per_completed` (spent, cancelled and failed workflows
included, ÷ completed workflows; empty until one completes) and `mean_score`
(mean final score of completed workflows).

A controller can use these, for example, to stop launching a type whose cost
per completed workflow is too high, to stop when `projected` exceeds the
limit, or to end the campaign early. `obs["learned_cost"]` and
`obs["allocations"]` show what AuCaM has learned per type and what each
finished workflow got and achieved (developer guide §8).

`obs["wf_spend"]` gives each running workflow's spending so far. The built-in
`CostCancelRule` uses it to cancel a workflow that has spent a set amount since
its score last improved (developer guide, "Built-in rules").

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

With `BanditSchedulingPolicy(cost_aware=True)` the reward also accounts for
cost: reward × 1 / (1 + `cost_weight` × relative cost). A workflow's cost is
the share of the campaign's budget left that it spent, summed over the limited
dimensions — so 2 GPU-hours out of 10 left weigh more than 3 CPU-hours out of
1000 — and its relative cost is that ÷ the average over the campaign's finished
workflows. A type that completes as often as another but costs twice the
average earns a third of the reward instead of a half. Without a campaign
limit, each dimension is compared with its own average instead.

![Bandit and budget](figures/aucam_bandit_budget.png)

---

## 6. Learned costs

After 3 workflows of a type have completed (`CampaignSpec.min_cost_samples`),
new workflows of that type get their average actual cost + 25 % as their limit,
instead of the starting `spending_limit`, and their average peak CPUs / GPUs in
use + 25 % as their resources. Only completed workflows count, since cancelled and
failed ones are partial. A type with `learn=False` keeps its starting
amount; its averages are still shown.

The averages, and the per-type totals in `obs["budget"]["by_type"]`, are saved
in the checkpoint: a restarted campaign keeps using what it learned.

---

## 7. Results, logs and status

### Per-workflow spending

`WorkflowResult.spent` holds what the workflow was charged, e.g.
`{"cpu_hours": 0.035, "gpu_hours": 0.003}`; `spending_limit` the workflow's own
limit, if any; `over_budget` whether AuCaM cancelled it for exceeding that
limit. The controller's `on_complete()`
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
| `[WORKFLOW-…] spending limit exceeded <dim>=… > … — auto-cancel` | WARNING | A workflow spent past its own limit (§4) |
| `[WORKFLOW-…] resources exceeded <key>=… > … — auto-cancel` | WARNING | A workflow used more CPUs/GPUs at once than it was given |
| `[SCHEDULER-…] campaign spending limit exceeded by actual spending: …` | INFO | `on_limit="cancel"`: running workflows are cancelled (§4) |
| `[WORKFLOW-…] CANCEL signal sent; grace …s` | INFO | A graceful cancel starts |
| `[WORKFLOW-…] did not exit within its grace period — cancelling` | WARNING | A graceful cancel ends in a forced one |
| `[SCHEDULER-…] WAIT <type>: …; stays queued` | INFO | A request does not fit the remaining budget or the free CPUs / GPUs yet (§4); logged once per request |
| `[SCHEDULER-…] DROP <type>: …` | INFO | A request was dropped: it does not fit with nothing else running, or needs more than the capacity |
| `[BANDIT] …  budget_rem=…%  c_eff=…  arms={type: μ=…}` | INFO | The bandit chooses among queued requests |
| `[BANDIT] …  → <type>  ucb=…  c_eff=…` | INFO | The bandit picks a type it has seen before |
| `[CM] Campaign … SUMMARY — exit=… spent={…}` | INFO | A campaign ends |
| `[BLOCK-TAG] …`, `[SPEND-DBG] … filtered` | DEBUG | Method-A event routing (§9) |

---

## 8. Implementation: the ledger (`src/aucam/budget.py`)

`CampaignManager.run()` creates one `BudgetAllocator` per run and passes it
to the campaign; every public method takes one lock.

| Kept | Meaning |
|---|---|
| `limits` | `CampaignBudget.spending`, set when the campaign starts |
| `spent` | Actual charges |
| `in_flight[wf_id]` | Expected remaining cost of each running workflow (§3) |

| Method | Called from | Purpose |
|---|---|---|
| `open()` | `_CampaignInstance` start | Set the limits |
| `set_in_flight()` | `_launch()`; `_on_task_event()` (A); `_collect_telemetry()` (B–D) | Set a workflow's in-flight |
| `record()` | `_on_task_event()` (A); `_on_done()` (B–D) | Add charges to `spent` |
| `clear_in_flight()` | `_on_done()` | Workflow ended |
| `limit_reached()` | `_spending_limit_reached()` | Campaign limit check (§4) |
| `shortfall()` | `_drain_queue()` | Does a request's cost fit what is left (§4) |
| `spending_fraction()` | `_drain_queue()` → `SchedulingContext.budget_remaining_fraction`; `[SPEND]` logs | "used" in §5 |
| `campaign_view()` | Policy cycle | `obs["budget"]` (§5) |
| `close()` | `CampaignManager.run()` | Return `spent` for `CampaignOutcome.spent` |

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
