# Known gaps and planned work

What is not implemented yet, for anyone working on the code: the parts of the
target design that the code does not do yet (with how they should work), and
other known problems. The [architecture overview](architecture-overview.md)
describes the current design and the reasons for it. Remove an entry when it is
done.

## Status

Work in priority order; within a level, top to bottom. "Needs" names an item
that must land first.

- **1 — first:** problems that can break a campaign or let it overspend, and
  items that unblock others.
- **2 — the vision:** AI steering a campaign and the budget as a decision input.
- **3 — robustness:** long campaigns on HPC.
- **4 — later:** when a real campaign needs it.
- **External:** raise with the AsyncFlow / RHAPSODY teams; not scheduled here.

| Priority | ID | Item | Status | Needs |
|---|---|---|---|---|
| 4 | U1 | Change a running workflow's allocation | Not started (decided: not for now) | |
| 4 | U5 | Per-workflow enforcement lags by one check | Decided: workflows check their own limit | |
| 4 | U7 | Elastic workflows: start with less when the budget is short | Not started | |
| 4 | L1 | Launch fewer workflows when the backend is backlogged | Not started | task counts from Delta runs |
| 4 | H1 | On-completion hooks can enqueue requests | Not started | |
| 4 | A1 | Per-dimension prices | Not started | |
| External | X1 | More accurate task data in AsyncFlow telemetry | Open | |
| External | X2 | RHAPSODY concurrent backend does not kill cancelled programs | Open | |
| External | X3 | Reattach to running tasks after a restart | Open | |

---

## Per-workflow allocations (U1, U5, U7)

Each workflow gets an allocation when it starts — a spending limit and the CPUs and
GPUs it may use at once (`Allocation`) — sized by `SchedulingPolicy.size()`
from the controller's amount (`LaunchRequest.spending_limit` / `resources`),
the type's starting values and what AuCaM learned, passed to the workflow and enforced by AuCaM. Current behaviour:
[budgets and spending](budget.md) §4 and the developer guide, "How much each workflow gets".

- **U1 — change a running workflow's allocation.** An allocation is fixed at
  launch. Plan: a controller decision, e.g.
  `PolicyResult.extend_allocations={wf_id: {"cpu_hours": 2.0}}`, that raises
  (or lowers) a running workflow's limit, with the in-flight amount updated in the
  ledger and the new limit sent to the workflow through the DataBackend.
  Enables "start small, give more to the promising" (successive halving).
  Unlike U7, which sizes a workflow at launch, U1 changes it after launch;
  both need workflows that read their limit and adapt.
- **U5 — enforcement lag.** Spending and resources in use are checked once
  per policy cycle, from the workflow's last report or its tasks, so a workflow can
  go past its limit by what it uses in one `policy_interval_s` — more for a
  workflow that reports rarely (the DeepDriveSim MiniApps worker reports once
  per prediction cycle). Resources in use come from task telemetry only with
  X1; otherwise only from what the workflow reports, and a workflow that
  reports nothing is not checked. A graceful cancel adds up to the type's
  `cancel_grace_s` of spending. The same lag applies to the campaign limit
  with `on_limit="cancel"`. Decided: keep AuCaM's per-cycle check as the
  safety net and document that a workflow should check its own limit (it
  receives it) and stop itself, as the example campaigns do (budget.md §4,
  developer guide §9). Revisit — checking on every report, or a faster check
  loop — if a real campaign needs a tighter bound.
- **U7 — elastic workflows.** AuCaM gives a workflow its full amount or
  nothing: if the amount does not fit the remaining budget or the free
  capacity, the request waits, or is dropped once nothing else runs. Some
  workflows can do useful work with less — an LLM agent with a token budget,
  adaptive sampling, an optimizer that stops when its limit is reached — and
  for them a smaller amount near the end of a campaign is better than none.
  Plan: a type-level minimum, e.g. `WorkflowConfig(elastic={"cpu_hours": 0.5})`,
  below which the workflow is not worth starting. The upper amount is what
  `size()` gives today; if it does not fit, the workflow gets what is left,
  down to that minimum, and sizes its work to the amount it receives. A
  controller's explicit amount is used as given and never reduced. U7 decides
  the amount at launch; U1 changes it while the workflow runs, and the two
  combine: start small when the budget is short, give more later. An earlier
  version had ranges (`bounds`) for this; they were removed because no
  campaign needed them and they made sizing harder to follow. Revisit when a real campaign has such a workflow.

## Other planned features

- **H1 — hooks.** Let `WorkflowConfig.on_completion` hooks enqueue requests.
- **A1 — accounting.** Per-dimension prices for comparing dimensions.
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

- **X1 — radical.asyncflow telemetry.** AuCaM needs AsyncFlow's task events
  to carry more accurate data: each task's requested resources or actual
  spending (for per-task charging), and the program's exit code (for
  `returncode`). Still missing in radical.asyncflow 0.7.0 (checked
  2026-11-11); today it comes from a local, temporary change to
  radical.asyncflow, which is being reworked. With a released version,
  workflows are charged only when they finish, a failed command-line workflow
  gets `returncode` 1, the CPUs and GPUs a workflow uses at once can only come from
  its own reports (U5), and the tests that need this data skip.
- **X2 — RHAPSODY concurrent backend.** Still open in rhapsody-py 0.7.0
  (checked 2026-11-11, first seen in 0.5.0):
  `ConcurrentExecutionBackend.cancel_task()` cancels the asyncio task waiting
  on the subprocess but never kills the subprocess, so a cancelled `sleep`
  keeps running, even after `shutdown()`. On this backend (the default for
  local runs), cancelling a command-line workflow marks it cancelled while its
  program keeps running. A program that checks for CANCEL
  (`aucam-report signal`) and has a grace period (`cancel_grace_s`) stops
  itself; one that does not keeps running.
  Needs a fix in RHAPSODY: terminate the process (group) on cancel.
- **X3 — reattach after a restart.** A restarted AuCaM adopts command-line
  workflows that report through Redis or Dragon by following their reports
  (R1), but has no handle to their tasks: it cannot force-cancel them, charge
  them per task or get an exit code except from their `done` report. Python
  workflows run inside AuCaM and always die with it. Needs asyncflow /
  RHAPSODY to look up a running task by id (e.g. on a Dragon runtime that
  outlives AuCaM). Related: AuCaM saves no "paused" state — a workflow that was
  paused is adopted or re-run as running.
