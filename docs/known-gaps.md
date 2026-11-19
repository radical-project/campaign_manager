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
- **External:** needs a change in AsyncFlow / RHAPSODY; not scheduled here.

| Priority | ID | Item | Status | Needs |
|---|---|---|---|---|
| 3 | R3 | A paused workflow is restarted as a running one | Not started | |
| 4 | U1 | Change a running workflow's allocation | Not started (decided: not for now) | |
| 4 | U7 | Elastic workflows: start with less when the budget is short | Not started | |
| External | X1 | AsyncFlow task events carry no resources and no exit code | Filed upstream; open in 0.7.0 | |
| External | X3 | Recover in-flight work after a restart | Future design — not filed; see below | |

---

## Per-workflow allocations (U1, U7)

Each workflow gets an allocation when it starts — a spending limit and the CPUs and
GPUs it may use at once (`Allocation`) — sized by `SchedulingPolicy.size()`
from the controller's amount (`LaunchRequest.spending_limit` / `resources`),
the type's starting values and what AuCaM learned, passed to the workflow and enforced by AuCaM. Current behaviour:
[budgets and spending](budget.md) §4 and the developer guide, "How much each workflow gets".

- **U1 — change a running workflow's allocation.** An allocation is fixed at
  launch. Plan: a controller decision, e.g.
  `PolicyResult.extend_allocations={wf_id: {"cpu_hours": 2.0}}`, that raises
  (or lowers) a running workflow's limit, with the in-flight amount updated in the
  ledger and the new limit sent to the workflow as a message
  (`PolicyResult.messages` / `reporter.messages()`, which exist now).
  Enables "start small, give more to the promising" (successive halving).
  Unlike U7, which sizes a workflow at launch, U1 changes it after launch;
  both need workflows that read their limit and adapt.
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

## Future design: recovering in-flight work after a restart (X3)

Not raised with the AsyncFlow / RHAPSODY teams: the API alone would not be
enough to use it. What else it would take, and why it is not urgent, is
recorded here so it is not re-derived.

**The problem.** The `WorkflowEngine`, its `components` registry (uid →
description + future) and the backend's own task table are objects in AuCaM's
process, so they go with it. A restarted AuCaM gets a new engine with an empty
registry, and nothing can refill it: a task is registered only when it is
submitted. So when the execution runtime outlives AuCaM — it was started
outside AuCaM's launch — its tasks keep running with nothing able to find them,
wait for them or cancel them.

**Take ORBIT as the example.** The tasks run on the endpoint node, under a
rhapsody session that lives there; AuCaM is one of its clients. So when AuCaM
dies, the endpoint, that session and the tasks all carry on — and the restarted
AuCaM arrives as a new client with a new session, unable to collect their
results or stop them while the allocation goes on paying for them. Closing that
takes three things, in order:

1. *rhapsody:* let a new client rejoin an existing session by its id, and
   **query** the state of its tasks. Subscribing is not enough: a task that
   finished while nobody was attached emitted a notification no one heard, so a
   subscriber waits for an event that has already passed. ORBIT has the id
   (`OrbitExecutionBackend._session_sid`) and a `sid` join for dispatcher
   sessions already; what it does not have is a way back into the session a
   dead client left behind.
2. *asyncflow:* `await flow.attach_task(uid)`, to turn what that query returns
   into an ordinary awaitable task — and a clear error on backends that cannot
   (concurrent, dask).
3. *AuCaM:* checkpoint one opaque token per backend, naming the endpoint and
   session to rejoin. Look the tasks up by our own tag rather than by uid: the
   checkpoint holds `wf_id`s and no AsyncFlow uids, and AuCaM already sets
   AsyncFlow's `workflow_id` to the `wf_id` before submitting.

**Why it is not urgent.** Even with all three, a Python workflow does not come
back: it is a coroutine that died with AuCaM, so it has to resume from durable
state of its own first — for IMPRESS-A, its provenance — and that part is
nobody's to fix but ours. It is also where the hours are: a workflow that can
resume keeps everything it had absorbed and loses only the tasks that were in
flight. Command-line workflows already have the equivalent of reattachment: a
restarted AuCaM adopts one that reports through Redis or Dragon and follows it
to its `done` report.

## Known problems

External entries carry their own status line: the project, and the upstream
issue. The version the gap was last seen in is in the Status column above.

- **X1 — AsyncFlow task events carry no resources and no exit code.**
  *radical.asyncflow · [issue #135](https://github.com/radical-cybertools/radical.asyncflow/issues/135)*

  So AuCaM charges a workflow only when it finishes, a failed command-line
  workflow gets `returncode` 1, the CPUs and GPUs a workflow holds can come
  only from its own reports, and the tests that need per-task data skip.
  Per-task charging works today only against a local, unreleased change to
  radical.asyncflow.
- **R3 — a paused workflow is restarted as a running one.** AuCaM does not
  record which workflows it paused: PAUSE lives in the signal the workflow
  read, and nothing on this side remembers it. After a restart an adopted or
  re-launched workflow is treated as running. `CampaignCheckpoint` has a
  `paused_wf_ids` field that looks like the fix, but `_build_checkpoint` always
  writes it empty ("populated by caller when needed") and nothing reads it
  back.
