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
| 3 | S1 | ADR- and LLM-based scheduling policies | Stubs (fall back to FIFO) | |
| 3 | R1 | Restart: rejoin running workflows, keep goals | Not started | |
| 3 | U5 | Per-workflow enforcement lags by one check | Open | |
| 4 | U1 | Change a running workflow's allocation | Not started (decided: not for now) | |
| 4 | L1 | Launch fewer workflows when the backend is backlogged | Not started | task counts from Delta runs |
| 4 | H1 | On-completion hooks can enqueue requests | Not started | |
| 4 | A1 | Per-dimension prices | Not started | |
| External | X1 | More accurate task data in AsyncFlow telemetry | Open | |
| External | X2 | RHAPSODY concurrent backend does not kill cancelled programs | Open | |

---

## Per-workflow allocations (U1, U5)

Each workflow gets an allocation when it starts — a spending limit and the CPUs and
GPUs it may use at once (`Allocation`) — sized by `SchedulingPolicy.size()`
from the controller's intent (`LaunchRequest.spending_limit` / `resources` /
`bounds`), passed to the workflow and enforced by AuCaM. Current behaviour:
[budgets and spending](budget.md) §4 and the developer guide, "How much each workflow gets".

- **U1 — change a running workflow's allocation.** An allocation is fixed at
  launch. Plan: a controller decision, e.g.
  `PolicyResult.extend_allocations={wf_id: {"cpu_hours": 2.0}}`, that raises
  (or lowers) a running workflow's limit, with the in-flight amount updated in the
  ledger and the new limit sent to the workflow through the DataBackend.
  Enables "start small, give more to the promising" (successive halving).
- **U5 — enforcement lag.** Spending and resources in use are checked once
  per policy cycle, from the workflow's last report or its tasks, so a workflow can
  go past its limit by what it uses in one `policy_interval_s` — more for a
  workflow that reports rarely (the DeepDriveSim MiniApps worker reports once
  per prediction cycle). Resources in use come from task telemetry only with
  X1; otherwise only from what the workflow reports, and a workflow that
  reports nothing is not checked. A graceful cancel adds up to the type's
  `cancel_grace_s` of spending. The same lag applies to the campaign limit
  with `on_limit="cancel"`. Possible fix: a workflow checks its own limit
  (it receives it) and stops itself, as the example campaigns do.

## Other planned features

- **R1 — restart.** Rejoin workflows still running after a restart instead of
  restarting them; re-send PAUSE to paused workflows (`paused_wf_ids` in the
  checkpoint is always empty); save run-time goal changes and return the
  final goals in `CampaignOutcome`.
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
