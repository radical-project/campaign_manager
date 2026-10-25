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
| B2 | Launch only requests that fit the remaining budget | Not started |
| B3 | Learned cost estimates | Done, two follow-ups |
| B4 | `CostCancelRule` | Not started |
| B5 | Cost-aware bandit | Partial |
| R1 | Restart: rejoin running workflows, keep goals and learned costs | Not started |
| R2 | Restart: dependency state | Partial |
| P1 | PAUSE timeout; CANCEL through the DataBackend at shutdown | Not started |
| C1 | Controller fallback when `decide()` fails or is slow | Not started |
| H1 | On-completion hooks can enqueue requests | Not started |
| A1 | Per-dimension prices; usage that belongs to no campaign | Not started |
| A2 | Campaigns without a spending limit are not bounded by the global limit | Open |
| S1 | ADR- and LLM-based scheduling policies | Stubs (fall back to FIFO) |
| D1 | Remote driver submission | Not started |
| L1 | Launch fewer workflows when the backend is backlogged | Not started |
| X1 | More accurate task data in AsyncFlow telemetry | Open (external) |
| X2 | RHAPSODY concurrent backend does not kill cancelled programs | Open (external) |

---

## Budget burn-down (B0–B5)

Goal: the budget becomes a decision input — AuCaM forecasts each campaign's
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
  `_scheduler_loop()`).
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
- **B5 — cost-aware bandit.** Done: exploration scaled by
  `√(1 − spending_fraction)` (`BanditSchedulingPolicy.select()`). Missing:
  default reward = success × 1 / (1 + k × cost), with cost = Σ over dimensions
  of workflow spending / campaign limit (`WorkflowResult.spent` is available
  in `on_outcome()`; the limits are not); `cost_aware=False` to keep today's
  completion-only reward.

## Other planned features

- **R1 — restart.** Rejoin workflows still running after a restart instead of
  restarting them; re-send PAUSE to paused workflows (`paused_wf_ids` in the
  checkpoint is always empty); save run-time goal changes and return the
  final goals in `CampaignOutcome`; save learned costs (B3).
- **R2 — dependency state after restart.** `dep_satisfied` is saved and
  restored before queued requests are re-pushed. Missing: `_dep_triggered`
  (source instances that already triggered) and `_dep_pending` (held
  requests); after a restart a completion can be counted again and held
  requests are lost. Fix: add both to `CampaignCheckpoint` (with defaults for
  old checkpoints) and restore them like `dep_satisfied`.
- **P1 — PAUSE and shutdown.** Cancel a workflow that does not acknowledge
  PAUSE within a timeout. At shutdown, send CANCEL through the DataBackend
  first so workflows can exit cleanly; cancel only those still running after
  the timeout.
- **C1 — controller fallback.** `controller.decide()` is not guarded: an
  exception in it ends the campaign's policy loop. Add a try/except and a
  rule-based fallback, also when `decide()` takes too long.
- **H1 — hooks.** Let `WorkflowConfig.on_completion` hooks enqueue requests on
  their own campaign.
- **A1 — accounting.** Per-dimension prices for comparing dimensions; record
  usage that belongs to no campaign.
- **S1 — scheduling.** `ADRSchedulingPolicy` and `LLMSchedulingPolicy` are
  stubs that fall back to FIFO.
- **D1 — drivers.** A remote submission interface, so drivers can run outside
  CM's process.
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

## Known problems

- **A2 — campaigns without a limit.** A campaign that sets no spending limit
  for a dimension commits 0 for it at admission, so the global limit cannot
  hold back its spending in that dimension ([budgets and spending](budget.md)
  §4). Options: (1) reject a campaign without a spending limit when a global
  limit is set; or (2) also check the global limit before every launch,
  counting actual spending plus running workflows across all campaigns. (2)
  lets campaigns run without a limit, but they could then use budget that other
  campaigns have reserved.
- **X1 — radical.asyncflow telemetry.** AuCaM needs AsyncFlow's task events
  to carry more accurate data: each task's requested resources or actual
  spending (for per-task charging), and the program's exit code (for
  `returncode`). Today this comes from a local, temporary change to
  radical.asyncflow, which is being reworked. With a released version,
  workflows are charged only when they finish, a failed command-line workflow
  gets `returncode` 1, and the tests that need this data skip.
- **X2 — RHAPSODY concurrent backend.** Checked with rhapsody-py 0.5.0:
  `ConcurrentExecutionBackend.cancel_task()` cancels the asyncio task waiting
  on the subprocess but never kills the subprocess, so a cancelled `sleep`
  keeps running, even after `shutdown()`. On this backend (the default for
  local runs), cancelling a command-line workflow marks it cancelled while its
  program keeps running.
  Needs a fix in RHAPSODY: terminate the process (group) on cancel.
