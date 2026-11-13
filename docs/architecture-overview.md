# AuCaM architecture overview

*What AuCaM does, how its parts fit together and why.*

---

## The problem

Adaptive HPC science means stopping poor workflows early, launching more of what
works and staying within the allocation. Today most projects handle this with
their own scripts, and a person still watches the workflows and decides what to do
next. AuCaM takes over that work: a campaign describes what success looks like
and how to react to results, and AuCaM launches the workflows, watches their
progress, cancels the ones that fall behind and ends the campaign when it is
done. It then returns the campaign's outcome to the caller — a person, a
script or an agent — which can plan the next campaign.

---

## Concepts

| Term | Meaning |
|---|---|
| **Campaign** | One coordinated scientific effort toward a goal, e.g. "run folding simulations until 50 high-confidence structures are found". It has workflow types, decision logic and a budget. |
| **Workflow** | One unit of work that AuCaM launches and cancels: a simulation, an analysis, a scoring pass. Many run in parallel, up to the campaign's limit on workflows running at once. |
| **Caller** | Whoever runs the campaign — a person, a script or an agent. It calls `CampaignManager.run(spec)` and gets the campaign's outcome when it ends, then can run the next campaign. |
| **Controller** | A campaign's decision logic: which workflows to launch, which to cancel, when the goal is reached. |
| **Policy cycle** | The controller's periodic step: observe the campaign, decide, act. It runs every `policy_interval_s` and whenever a workflow finishes. |
| **Budget** | Spending limits (CPU-hours, GPU-hours or any other dimension, such as LLM tokens) for the campaign, plus limits on how many workflows run at once. |

A workflow can be a **Python function** or a **command-line program** written
in any language. AuCaM runs either kind as a single AsyncFlow *block*, so both
are launched, cancelled and charged the same way. A Python workflow submits its
compute steps as AsyncFlow tasks, and RHAPSODY places them on the allocation; a
command-line program runs as a single task. Cancelling a workflow cancels all of
its tasks, and what those tasks cost is charged to the workflow.

---

## Components

![AuCaM architecture](figures/aucam-architecture.png)

- **Campaign Manager** — sets up execution, runs one campaign and returns
  its outcome:

  ```python
  cm = CampaignManager()
  outcome = await cm.run(spec)  # returns when the campaign ends
  ```

  A second `run()` while one is running raises an error. *Why:* the caller
  decides what to run next from each outcome; a scheduling policy passed to
  several specs keeps what it learned.
- **Controller** — the campaign's decision logic: which workflows to launch
  and cancel, and when the campaign ends. Built in, with no code:
  - **Launch** (`LaunchRule`) — keeps a request of each type in its `launch`
    list queued, so new workflows start as slots free up.
  - **Cancel** (`CancelRule`, `CostCancelRule`) — cancels a workflow whose
    latest report meets a condition, such as a score below a threshold, or
    that keeps spending without improving.
  - **Goal** — ends the campaign after N successful workflows of a type
    (`goal_type`, `goal_n`), or when all its goals (target values of metrics)
    are met.
  - **ADR** (`ADRController`) — an ADR policy makes these decisions instead.
  - **Fallback** — if custom or ADR decision logic fails or is too slow
    (`decide_timeout_s`), the built-in rules decide that cycle.

  A campaign can extend or replace this with its own logic, including one
  that calls an LLM. AuCaM itself cancels a workflow only when it stops
  reporting, runs past its wall-time limit, or goes past its own limits.
  *Why:* what counts as a poor result depends on the science.
- **Queue** — waiting launch requests, ordered by priority (CRITICAL, HIGH,
  NORMAL, LOW) and arrival; NORMAL requests that wait too long become HIGH.
- **Scheduler** — starts a queued request when there is a free slot: the
  workflow-count limits, the free CPUs / GPUs and the remaining budget all
  allow it. CRITICAL requests go first; a scheduling policy picks among the
  rest and decides how much each workflow gets (see "Budgets and resources").
  Built in:
  - **Bandit** (default) — a UCB1 (Upper Confidence Bound) bandit over
    workflow types: it balances *exploring* (trying types it knows little
    about) with *exploiting* (launching the types that complete most often),
    and explores less as the budget runs down.
  - **FIFO** — queue order.
  - **LLM** and **ADR** — a language model or an ADR policy ranks the
    workflow types; AuCaM asks it in the background, so launches never wait
    for it, and uses the bandit until it answers.
  - **Load-aware launching** — with telemetry, how many workflows may run
    follows the allocation: fewer when tasks back up in the backend, more
    while the nodes have room, up to `max_concurrent_workflows`.

  A campaign can replace the policy, or just its sizing, with its own. AuCaM
  chooses which workflows start; each running workflow's tasks go straight to
  Dragon / RHAPSODY, which places them. *Why:* work handed to the backend
  runs in the backend's order and AuCaM can only cancel it; work held in
  AuCaM's queue can still be re-ranked, re-sized or dropped.
- **Budget ledger** (`BudgetAllocator`) — the campaign's limits, what it has
  spent, and what its running workflows are held at.
- **Data backend** — the channel through which workflows report to AuCaM (a
  score and metrics). An in-process store for local runs; Redis or Dragon for
  multi-node runs.
- **Execution** — an AsyncFlow engine on a RHAPSODY backend; AuCaM launches
  and cancels workflows through it.

**Also built in:**

- **Dependencies** — a workflow type can wait for another to succeed.
- **Checkpoint and restart** — a restarted campaign continues where it
  stopped: it adopts command-line workflows that are still running and
  charges the work lost with the others.
- **The outcome** — counts, spending, whether the goal was reached and the
  controller's summary go back to the caller, which can plan the next
  campaign (for example, drop workflow types that never succeeded, or adjust
  parameters and limits).

The [capability demo](cm-advantages.md) shows cancellation, the bandit and
accounting on one campaign.

---

## Example: one policy cycle

1. Three simulations are running. A policy cycle starts, because
   `policy_interval_s` has passed or a workflow has just finished.
2. AuCaM collects each simulation's latest report, the queue and the
   budget, and passes them to the controller.
3. The built-in rules decide: `CancelRule` sees one simulation scoring 0.1,
   below the 0.4 threshold, and cancels it; `LaunchRule` sees no simulation
   queued and adds a request.
4. When the cancelled simulation has ended and its spending is recorded, the
   scheduler starts the queued request, since a workflow, its CPUs and its
   budget are now free.

A custom controller could decide anything else in step 3, such as launching
several workflows at once or changing goals.

---

## Budgets and resources

A campaign states the CPUs / GPUs of its allocation (`capacity`), how many
workflows may run at once (in total, and per type), and optionally a spending
limit per dimension (CPU-hours, GPU-hours, LLM tokens, …).

- **Accounting** — AuCaM charges each workflow what it costs: from the
  resources its tasks requested and how long they ran, from what the
  workflow reports, or else the CPUs / GPUs it was given × its run time.
- **Per-workflow allocation** — each workflow gets a spending limit and the
  CPUs / GPUs it may use at once when it starts: the controller's amount if
  it gives one, else its type's starting value at first, then what earlier
  workflows of the type used + 25 %, and 25 % more after one is cancelled at
  its limit. It starts only if these fit what is left of the campaign's
  limit and capacity, receives them, and is cancelled if it goes past them.
  *Why:* AuCaM cannot know which nodes a workflow's tasks hold; the
  workflow, which does, is told its share and held to it.
- **Safety net** — a workflow's limit covers only the dimensions it names
  (e.g. not `llm_tokens`). If actual spending still goes past the campaign's
  limit, AuCaM cancels the running workflows and ends the campaign
  (`on_limit="cancel"`, the default), or lets them finish (`"finish"`).
- **Graceful cancel** — for a workflow type with a grace period
  (`cancel_grace_s`), AuCaM first sends the workflow CANCEL so it can save
  its state and exit, and cancels it for real only if it is still running
  when the grace period ends.

See [budgets and spending](budget.md) for how spending is measured and
limited.

---

## How a campaign ends

| Exit reason | When |
|---|---|
| `goals_satisfied` | The goal is reached: all declared goals (target values for metrics, e.g. "best energy ≤ −8", checked by AuCaM every cycle) are met, or the controller's own goal is (e.g. the built-in "N successful workflows"). |
| `controller_terminate` | The controller ended the campaign before its goal was reached, e.g. because the goal looks unreachable. |
| `spending_limit` | The spending limit is reached and no workflows are running, or actual spending went past it and AuCaM cancelled the running workflows |
| `campaign_idle` | Nothing queued or running for a while (`idle_exit_s`). On by default — *why:* a campaign without goals would otherwise wait forever and hold the allocation; set `idle_exit_s=None` for a controller that waits on purpose. |
| `signal` | AuCaM receives SIGTERM or SIGINT, e.g. before a SLURM job's time limit, or the caller calls `cm.stop()` |

In each case the queue is cleared, running workflows are cancelled (with
their grace period), a final checkpoint is written (if enabled), and `run()`
returns a `CampaignOutcome`: the exit reason, workflow counts per type, time,
spending, whether the goal was reached, and the controller's summary (e.g.
the best score).

---

## Further reading

- [Demo campaign guide](demo-campaign-guide.md) — run a campaign in minutes.
- [Capability demo](cm-advantages.md) — cancellation, learning and accounting
  on one campaign.
- [Campaign developer guide](campaign-developer-guide.md) — write workflows,
  controllers and the code that runs campaigns.
- [Budgets and spending](budget.md) — how spending is measured, limited and
  reported.
- [Code walkthrough](code-walkthrough.md) — module map for contributors.
- [Known gaps](known-gaps.md) — what is planned but not built yet.
