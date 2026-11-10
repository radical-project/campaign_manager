# Known gaps and planned work

What is not implemented yet, for anyone working on the code: the parts of the
target design that the code does not do yet (with how they should work), and
other known problems. The [architecture overview](architecture-overview.md)
describes the current design and the reasons for it. Remove an entry when it is
done.

## Status

| ID | Item | Status |
|---|---|---|
| B0 | Any budget dimension (e.g. `llm_tokens`) | Partial |
| B1 | Burn-down in the controller's observation | Partial |
| B2 | Launch only requests that fit the remaining budget | Partial |
| B3 | Learned cost estimates | Done, two follow-ups |
| B4 | `CostCancelRule` | Not started |
| B6 | Stop at the campaign spending limit (uses P1) | Not started |
| U1 | Change a running run's allocation | Not started (decided: not for now) |
| U2 | Value-based sizing and cost-aware bandit; learned resource sizes | Partial |
| U3 | Example campaigns use their resource allocation | Not started |
| U4 | Graceful cancel for per-run limits (uses P1) | Not started |
| U5 | Per-run enforcement lags by one check | Open |
| R1 | Restart: rejoin running workflows, keep goals | Not started |
| R2 | Restart: dependency state | Partial |
| P1 | Graceful cancel through the DataBackend; PAUSE timeout | Not started |
| C1 | Controller fallback when `decide()` fails or is slow | Not started |
| H1 | On-completion hooks can enqueue requests | Not started |
| A1 | Per-dimension prices | Not started |
| S1 | ADR- and LLM-based scheduling policies | Stubs (fall back to FIFO) |
| L1 | Launch fewer workflows when the backend is backlogged | Not started |
| W1 | Concurrency cap per workflow type | Not started |
| X1 | More accurate task data in AsyncFlow telemetry | Open (external) |
| X2 | RHAPSODY concurrent backend does not kill cancelled programs | Open (external) |

---

## Budget burn-down (B0–B6)

Goal: the budget becomes a decision input — AuCaM forecasts the campaign's
spending, launches only what fits, cancels workflows that spend without
improving, and favours cost-effective workflow types as the budget runs down.
Current behaviour: [budgets and spending](budget.md). When an item lands,
update `budget.md` and the developer guide.

- **B0 — any budget dimension.** AsyncFlow telemetry measures only CPU and
  GPU time, so other dimensions (e.g. `llm_tokens`) come from the workflow's
  own reports: when the workflow ends, AuCaM charges the totals from its last
  report for every dimension named in `declared_spending`. Missing: when a
  workflow's CPU and GPU hours are charged from telemetry (method A in
  `budget.md`), that end-of-workflow charge is skipped entirely, so its other
  dimensions are never charged and a limit on them is not enforced. Fix: for
  such workflows, still charge the reported totals of the dimensions other
  than CPU and GPU hours. `[SPEND]` logs also show only CPU/GPU.
- **B1 — burn-down in the observation.** `campaign_view()` has
  `burn[dim] = {remaining, fraction_left}`. Missing: `rate_per_hour` (smoothed
  over recent policy cycles; needs spending timestamps in the ledger),
  `projected` (spent + in-flight + estimated cost of queued requests),
  `hours_to_limit`, and `burn["by_type"]` (completions, average cost, cost per
  successful run, mean final score).
- **B2 — launch only what fits.** Today a request starts whenever spent +
  in-flight is still below the limit, whatever the request costs, so the last
  launch can overshoot the limit, and once the limit is reached even a cheap
  request that would fit is not started. Change: in `_drain_queue()` /
  `_take()`, start a request only if spent + in-flight + its estimated cost
  (`_learned_cost()`) ≤ limit in every dimension, CRITICAL included; a request
  that does not fit stays queued and a cheaper one may start. The spending exit becomes "no
  queued request fits and nothing is running" (`_spending_limit_reached()`,
  `_scheduler_loop()`). A request with a spending limit
  (`WorkflowConfig.spending_limit` or `LaunchRequest.spending_limit`) can use
  that limit as its cost here, since the run can never spend more.
  Done so far: `SchedulingPolicy.size()` caps each run's spending limit at the
  remaining budget and drops a request whose minimum (`LaunchRequest.bounds`)
  does not fit; a request is still started whenever spent + in-flight is below
  the limit, before sizing.
- **B3 — learned costs, follow-ups.** Implemented (`_learned_cost()`,
  `_update_type_cost()`, `_estimates`). Missing: make the threshold (3 runs)
  a `CampaignSpec.min_cost_samples` setting; save `_type_cost_stats` in the
  checkpoint.
- **B4 — cost-based cancel rule.** A new built-in rule in `controller.py`,
  `CostCancelRule(workflow_type, max_without_improvement={"gpu_hours": 5.0},
  direction="maximize")`, applied by `_apply_rules()`: cancel a running
  workflow that has spent the given amount since its score last improved.
  Needs per running workflow: spending so far (per-task charges are in
  `_task_usage`), best score, spending at the best score — tracked on
  `_WorkflowInstance` and passed in the observation (e.g. `obs["wf_spend"]`).
- **B6 — stop at the campaign spending limit.** Today, when spent +
  in-flight reaches the limit, AuCaM stops launching but lets running
  workflows finish, so a campaign can end above its limit (e.g. up to
  27.6 CPU·s against a 24 CPU·s limit in the slides' use case). The limit
  should hold: when it is reached, cancel the running workflows gracefully
  (P1), charge what they used, and end with `spending_limit`.
  Make it the default, with an option on `CampaignBudget` (e.g.
  `on_limit="finish"`) for campaigns that prefer to let running work
  complete. Today a custom controller can do this in `decide()`.

## Per-run allocations (U1–U5)

Each run gets an allocation when it starts — a spending limit and the CPUs and
GPUs it may use at once (`Allocation`) — sized by `SchedulingPolicy.size()`
from the controller's intent (`LaunchRequest.spending_limit` / `resources` /
`bounds`), passed to the workflow and enforced by AuCaM. Current behaviour:
[budgets and spending](budget.md) §4 and the developer guide, "How much each
run gets".

- **U1 — change a running run's allocation.** An allocation is fixed at
  launch. Plan: a controller decision, e.g.
  `PolicyResult.extend_allocations={wf_id: {"cpu_hours": 2.0}}`, that raises
  (or lowers) a running run's limit, with the in-flight amount updated in the
  ledger and the new limit sent to the workflow through the DataBackend.
  Enables "start small, give more to the promising" (successive halving).
- **U2 — smarter sizing, cost-aware bandit.** The default `size()` uses the
  type's learned cost plus a margin, or fair share, and the bandit already
  explores less as the budget is used (exploration × `√(1 − spending_fraction)`
  in `BanditSchedulingPolicy.select()`). Missing: value-based decisions — a
  bandit reward of success × 1 / (1 + k × cost), with cost = Σ over dimensions
  of the run's spending / the campaign limit (`WorkflowResult.spent` is in
  `on_outcome()`; the limits are not), `cost_aware=False` to keep today's
  completion-only reward, and more budget in `size()` for types with a better
  result per unit spent; and
  learned resource sizes — a resource range always gets its maximum, since
  AuCaM does not learn how many CPUs/GPUs a type actually uses (it could, from
  `cpus_in_use` reports or task telemetry).
- **U3 — example campaigns and resources.** The example campaigns pass and
  respect spending limits but not resources: their workflows should read
  `context["resources"]` / `CM_RESOURCES_<KEY>`, size their tasks to them, and
  report `cpus_in_use` / `gpus_in_use`, with an overuse configuration like the
  spending ones (`campaign_overuse.yaml`, `--overspend`).
- **U4 — graceful per-run cancel.** A run over its spending limit or resources
  is cancelled at once. Plan: cancel it gracefully (P1).
- **U5 — enforcement lag.** Spending and resources in use are checked once
  per policy cycle, from the workflow's last report or its tasks, so a run can
  go past its limit by what it uses in one `policy_interval_s` — more for a
  workflow that reports rarely (the DeepDriveSim MiniApps worker reports once
  per prediction cycle). Resources in use come from task telemetry only with
  X1; otherwise only from what the workflow reports, and a workflow that
  reports nothing is not checked.

## Other planned features

- **R1 — restart.** Rejoin workflows still running after a restart instead of
  restarting them; re-send PAUSE to paused workflows (`paused_wf_ids` in the
  checkpoint is always empty); save run-time goal changes and return the
  final goals in `CampaignOutcome`.
- **R2 — dependency state after restart.** `dep_satisfied` is saved and
  restored before queued requests are re-pushed. Missing: `_dep_triggered`
  (source instances that already triggered) and `_dep_pending` (held
  requests); after a restart a completion can be counted again and held
  requests are lost. Fix: add both to `CampaignCheckpoint` (with defaults for
  old checkpoints) and restore them like `dep_satisfied`.
- **P1 — graceful cancel; PAUSE timeout.** One mechanism for every cancel
  that can wait a moment: send CANCEL through the DataBackend so the workflow
  can save its state and exit, and cancel it for real only if it is still
  running after a grace period. Used at shutdown, at the campaign spending
  limit (B6) and for runs over their own limits (U4). Also: cancel a workflow
  that does not acknowledge PAUSE within a timeout.
- **C1 — controller fallback.** `controller.decide()` is not guarded: an
  exception in it ends the campaign's policy loop. Add a try/except and a
  rule-based fallback, also when `decide()` takes too long.
- **H1 — hooks.** Let `WorkflowConfig.on_completion` hooks enqueue requests.
- **A1 — accounting.** Per-dimension prices for comparing dimensions.
- **S1 — scheduling.** `ADRSchedulingPolicy` and `LLMSchedulingPolicy` are
  stubs that fall back to FIFO.
- **L1 — load-aware launching.** Today the controller sees AsyncFlow task
  counts (`obs["tasks"]`: waiting, submitted, running) and each policy-cycle log
  line shows them, but nothing acts on them. Plan: an opt-in setting for the
  built-in controller, e.g. `CampaignController(max_pending_tasks=20)`, that
  keeps a target number of workflows in flight (running + queued): above the
  threshold of submitted tasks it halves the target and drops surplus queued
  requests (`PolicyResult.cancel_queued`); below it, it raises the target by
  one. `max_concurrent_workflows` stays the cap. Decide after looking at real
  task counts from Delta runs. Limits: only AsyncFlow tasks are counted, so a
  command-line workflow is one task; a running workflow's own burst of tasks
  is not slowed, only new launches.
- **W1 — concurrency cap per workflow type.** `CampaignBudget.max_concurrency_per_workflow`
  is one number applied to every workflow type, so a long, expensive
  simulation and a short analysis get the same cap. Plan: an optional cap on
  each type, e.g. `WorkflowConfig(name="precise_sim", ..., max_running=2)`;
  `max_concurrency_per_workflow` becomes the default for types that set none,
  and `max_concurrent_workflows` stays the cap for the whole campaign. The
  scheduler and `SchedulingContext.max_concurrency_per_type` need the
  per-type values.

## Known problems

- **X1 — radical.asyncflow telemetry.** AuCaM needs AsyncFlow's task events
  to carry more accurate data: each task's requested resources or actual
  spending (for per-task charging), and the program's exit code (for
  `returncode`). Today this comes from a local, temporary change to
  radical.asyncflow, which is being reworked. With a released version,
  workflows are charged only when they finish, a failed command-line workflow
  gets `returncode` 1, the CPUs and GPUs a run uses at once can only come from
  its own reports (U5), and the tests that need this data skip.
- **X2 — RHAPSODY concurrent backend.** Checked with rhapsody-py 0.5.0:
  `ConcurrentExecutionBackend.cancel_task()` cancels the asyncio task waiting
  on the subprocess but never kills the subprocess, so a cancelled `sleep`
  keeps running, even after `shutdown()`. On this backend (the default for
  local runs), cancelling a command-line workflow marks it cancelled while its
  program keeps running.
  Needs a fix in RHAPSODY: terminate the process (group) on cancel.
