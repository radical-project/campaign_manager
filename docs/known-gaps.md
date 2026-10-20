# Known gaps and planned work

What is not implemented yet. The design documents describe what CM does today,
plus the budget burn-down design (B0–B5 below), which is designed but not coded
yet; the detailed design also still has a few "Planned:" notes in place. This
file is the list for anyone working on the code. Remove an entry when it is done.

## Planned features (agreed, not implemented)

- **Restart:** rejoin workflows that are still running after a restart instead
  of restarting them; re-send PAUSE to paused workflows (`paused_wf_ids` in the
  checkpoint is always empty today); store run-time goal changes in the
  checkpoint and return the final goals to the driver (`CampaignOutcome`).
- **PAUSE / shutdown:** cancel a workflow that does not acknowledge PAUSE within
  a timeout; at shutdown, send CANCEL through the DataBackend first so workflows
  can exit cleanly, and cancel only those still running after the timeout.
- **Campaign exit:** end a campaign when its queue is empty and no workflows
  are running (today such a campaign keeps waiting until CM is stopped). If added, count a campaign as
  idle only after a full policy cycle with no new launch requests.
- **Controller:** a rule-based fallback when `decide()` fails or takes too long.
- **Hooks:** let on-completion hooks enqueue requests on their own campaign.
- **Accounting:** per-dimension prices for comparing dimensions, and recording
  usage that belongs to no campaign (token accounting: see B0 below).
- **Scheduling:** ADR- and LLM-based scheduling policies
  (`ADRSchedulingPolicy`, `LLMSchedulingPolicy` are stubs that fall back to FIFO).
- **Drivers:** a remote submission interface, so drivers can run outside CM's
  process (today `cm.submit()` is called in the same Python process).

## Planned: budget burn-down as a decision input (B0–B5)

Agreed design for capability 4 ("budget burn-down as a decision input"),
already described in both design docs (Budgets and accounting / Burn-down,
Scheduling, Campaign Controller). Today
CM only tracks spending and passes it to the controller (`campaign_view()`:
limits, spent, in_flight, global_spent); the built-in rules and the scheduler
do not use it. B0 is a prerequisite; B1 feeds B2–B5. Each item needs tests, and
the design docs (Budgets and accounting, Scheduling, Campaign Controller) and
`docs/campaign-developer-guide.md` must be updated when it lands.

### B0. Any budget dimension (e.g. LLM tokens)

- **Today:** limits and `record()` accept any key, but `_SPEND_KEYS =
  ("cpu_hours", "gpu_hours")` in `src/aucam/budget.py` limits in-flight
  estimates to those two, and the reported-spending fallback in
  `_compute_workflow_spent()` (`src/aucam/campaign.py`) reads only
  `cpu_hours` / `gpu_hours` from the final report. A token limit is therefore
  checked only after workflows finish, and only if something records tokens.
- **Change:** use every dimension in the campaign's limits for in-flight
  estimates (`set_in_flight`), for `limit_reached()`, and for charging what a
  workflow reports in its metrics (e.g. `llm_tokens`). AsyncFlow telemetry
  still supplies cpu/gpu hours; other dimensions come from workflow reports.
  Decide whether to charge reported non-hour dimensions incrementally (latest
  cumulative value in live reports) or only at completion.

### B1. Burn-down in the observation

- Add a `burn` entry to `BudgetAllocator.campaign_view()` (the controller's
  `budget` observation). Per dimension:
  - `remaining` = limit − spent − in_flight; `fraction_left` = remaining / limit;
  - `rate_per_hour`: smoothed spending rate (e.g. EWMA over recent policy
    cycles, from spent + in_flight deltas);
  - `projected` = spent + in_flight + estimated cost of queued requests (B3);
  - `hours_to_limit` = remaining / rate_per_hour (None when rate is 0).
- Per workflow type (`burn["by_type"]`): completions, average actual cost,
  cost per successful workflow, mean final score.
- Needs: spending timestamps per campaign in the ledger; the queue snapshot
  (or the campaign instance) to price queued requests.

### B2. Launch only what fits

- In `_drain_queue()` / `_take()` (`src/aucam/campaign.py`), start a request
  only if spent + in_flight + the request's estimated cost (B3) ≤ limit in
  every dimension. Applies to CRITICAL requests too.
- A request that does not fit stays queued; a cheaper request behind it may
  start.
- Campaign exit (see "Campaign exit" above): the spending condition becomes
  "no queued request fits and no workflows are running", replacing "limit
  reached and no workflows running" (`_spending_limit_reached()`,
  `_scheduler_loop()`).

### B3. Learned cost estimates

- Per workflow type, keep the mean actual spending (per dimension) of
  completed workflows (returncode 0; cancelled/failed runs are partial).
- Use it instead of `WorkflowConfig.declared_spending` once a type has
  `min_cost_samples` completions (default 3, configurable on the campaign),
  for the in-flight estimate (`_launch()` → `set_in_flight`), the fit check
  (B2) and `projected` (B1). Fall back to declared spending before that.
- Store the per-type statistics in the checkpoint so they survive a restart.

### B4. Cost-based cancel rule

- New built-in rule in `src/aucam/controller.py`:
  `CostCancelRule(workflow_type, max_without_improvement={"gpu_hours": 5.0},
  direction="maximize")`, applied by `_apply_rules()`.
- Cancels a running workflow that has spent at least the given amount (any
  dimension) since its score last improved in `direction`.
- Needs per running workflow: spending so far (task-level charges are already
  recorded in `_on_task_event()`), best score, and spending when the best score
  was reached — tracked on `_WorkflowInstance` and passed in the observation
  (e.g. `obs["wf_spend"][wf_id] = {"spent": {...}, "at_best": {...},
  "best_score": x}`).

### B5. Cost-aware UCB1

- `UCB1SchedulingPolicy` (`src/aucam/scheduler.py`): new default reward =
  success × 1 / (1 + k × cost), where cost = Σ over dimensions of
  (workflow spending / campaign limit), so it is unit-free and in [0, 1].
  Needs the campaign limits and the workflow's actual spending in
  `on_outcome()` (add them to `WorkflowResult`, e.g. `WorkflowResult.spent`).
- Scale the exploration weight C by `fraction_left` (B1; minimum over limited
  dimensions): explore early, exploit as the budget runs down.
- `cost_aware=False` restores today's completion-only reward and fixed C.

## Known gaps (not in the design docs)

### 1. Dependency (DAG) state is not restored after a restart

- **What:** the campaign checkpoint (`src/aucam/checkpoint.py`,
  `CampaignCheckpoint`) stores the queue, running workflows, counters and
  spending, but not which `WorkflowDependency` targets were already unlocked
  (`_CampaignInstance._dep_satisfied`) or which source instances already
  triggered (`_dep_triggered`), nor the held requests (`_dep_pending`).
- **Effect:** after a restart from a checkpoint, a dependency target with
  `launch_on_start=False` is locked again. Its restored queued requests are
  held in `_dep_pending` (via `enqueue()`), and nothing runs for that type until
  a source workflow completes again and passes the condition.
- **Where:** `_build_checkpoint()` and `_restore_from_checkpoint()` in
  `src/aucam/campaign.py`; `CampaignCheckpoint` / `_from_json()` in
  `src/aucam/checkpoint.py`.
- **Fix outline:** add `dep_satisfied: list[str]` (and optionally
  `dep_triggered`, `dep_pending`) to `CampaignCheckpoint` with defaults so old
  checkpoints still load; save them in `_build_checkpoint()`; restore them in
  `_restore_from_checkpoint()` *before* re-pushing queued requests.
- **Test:** checkpoint a campaign after a source completed and the target was
  unlocked; restart; the queued target request must launch without another
  source completion.

### 2. Command-line workflows cannot report with the default backend

- **What:** command-line (Tier 2, `launch_cmd`) workflows report through
  `src/aucam_client` (`aucam-report` command or `Reporter`), which needs a
  remote DataBackend (Redis or Dragon). With the default `LocalDictBackend`,
  CM exports no `CM_DATA_ENDPOINT`, and `aucam-report` exits with status 2.
- **Effect:** in local runs (and any run without Redis/Dragon) a command-line
  workflow runs but sends no reports: no live scores or metrics for the
  controller, no stale-reporter detection, no PAUSE/RESUME, empty
  `final_score` / `final_metrics`.
- **Decision so far:** stay with Redis/Dragon for now.
- **Fix outline (if needed later):** a file channel. CM exports
  `CM_REPORT_FILE` and `CM_SIGNAL_FILE` under a run directory on a shared
  filesystem; `aucam-report` writes the JSON report atomically (temp file +
  rename) and reads/clears the signal file; `LocalDictBackend` (or a new
  `FileBackend`) reads the report file in `get_report()` and writes signals.

### 3. External dependencies (not CM features)

- **radical.asyncflow changes are not released.** Task-level accounting needs
  `attributes["resources"]` on task telemetry events (`requested_resources()`
  in `workflow_manager.py`), and real program exit codes need `exit_code` on
  task futures. Both are uncommitted changes in the local radical.asyncflow
  checkout. With a released asyncflow (≤ 0.5.1):
  - workflows are charged at completion (reported hours, or declared spending
    scaled by wall time), not task by task;
  - a failed command-line workflow gets `returncode` 1 instead of its exit code.
  Tests that need them skip (`tests/test_task_level_accounting.py`,
  `tests/test_launch_cmd.py::test_exit_code_reaches_workflow_result`).
- **RHAPSODY `ConcurrentExecutionBackend` does not kill cancelled programs.**
  Its `cancel_task()` cancels the asyncio future but leaves the subprocess
  running (verified: a cancelled `sleep` keeps running after engine shutdown).
  CM's cancellation of command-line workflows (controller CANCEL, wall-time
  and stale-reporter cancels, shutdown) is therefore only effective on
  backends that terminate the process. Needs a fix in RHAPSODY (terminate the
  process / process group on cancel).
