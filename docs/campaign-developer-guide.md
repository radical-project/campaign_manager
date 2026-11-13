# Campaign developer guide

How to write and run a campaign: workflows, decision logic and limits. For the
ideas behind AuCaM read the [architecture overview](architecture-overview.md)
first; to run something right away, see the [demo campaign](demo-campaign-guide.md).

1. [Concepts](#1-concepts)
2. [Minimal example](#2-minimal-example)
3. [Workflows](#3-workflows)
4. [Reporting from a workflow](#4-reporting-from-a-workflow)
5. [The controller](#5-the-controller)
6. [How a campaign ends](#6-how-a-campaign-ends)
7. [Dependencies between workflow types](#7-dependencies-between-workflow-types)
8. [Scheduling](#8-scheduling)
9. [Budgets and limits](#9-budgets-and-limits)
10. [Running campaigns](#10-running-campaigns)
11. [Troubleshooting](#11-troubleshooting)
12. [Reference](#12-reference)

---

## 1. Concepts

- **Campaign** (`CampaignSpec`) — workflow types, a controller, limits and
  settings. AuCaM runs it until it ends (§6).
- **Workflow type** (`WorkflowConfig`) — a kind of work, e.g. `sim_worker`.
  Each launch of it is one **workflow**; AuCaM runs every workflow as one
  AsyncFlow block, its unit of launch, cancellation and accounting.
- **Controller** (`CampaignController`) — decides what to launch and cancel.
  It is called every **policy cycle**: every `policy_interval_s` and whenever
  a workflow finishes.
- **Queue and scheduler** — the controller's launch requests wait in the
  campaign's queue; the scheduler starts them when a slot is free, choosing
  with the campaign's **scheduling policy**.
- **Ledger** — records what the campaign spends; limits are checked against
  it (§9).
- **Running** — `outcome = await CampaignManager().run(spec)` runs the
  campaign and returns its outcome (§10).

---

## 2. Minimal example

```python
import asyncio
import random

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    WorkflowConfig,
    WorkflowReporter,
)


async def my_sim(reporter: WorkflowReporter) -> None:
    for step in range(5):
        await asyncio.sleep(0.05)
        await reporter.emit(score=random.random(), iteration=step)


spec = CampaignSpec(
    name="my_campaign",
    budget=CampaignBudget(
        max_concurrent_workflows=4, max_concurrency_per_workflow=4, capacity={"cpus": 8}
    ),
    controller=CampaignController(launch=["sim"], goal_type="sim", goal_n=10),
    workflows=[
        WorkflowConfig(
            name="sim",
            block=my_sim,
            spending_limit={"cpu_hours": 0.01},  # most one workflow may spend, at first
            resources={"cpus": 1},  # what one workflow holds at once
        )
    ],
    policy_interval_s=0.5,
)


async def main() -> None:
    outcome = await CampaignManager().run(spec)
    print(outcome.exit_reason, outcome.completed, outcome.spent)


asyncio.run(main())
```

The controller keeps one `sim` request queued (`launch=`) and ends the
campaign after 10 successful workflows (`goal_type`, `goal_n`). The log ends with a
line like:

```
[CM] Campaign my_campaign SUMMARY — exit=goals_satisfied run=13 ok=10 cancelled=3 ...
```

(`cancelled` counts the workflows still going when the goal was reached.)

---

## 3. Workflows

```python
WorkflowConfig(
    name="sim_worker",  # unique within the campaign
    block=my_async_fn,  # a Python workflow ── or ──
    launch_cmd=["./sim", "--fast"],  # a command-line program (any language)
    spending_limit={"cpu_hours": 6.0},  # required: starting limit per workflow (§8)
    resources={"cpus": 8},  # required: starting CPUs / GPUs per workflow (§8, §9)
    learn=True,  # False: keep spending_limit / resources fixed (§8)
    task_description={"ranks": 4, "cores_per_rank": 8},  # launch_cmd: resource request
    env={"OMP_NUM_THREADS": "8"},  # launch_cmd: extra environment variables
    launch_on_start=True,  # queue one request when the campaign starts
    max_running=None,  # at most this many at once; None: CampaignBudget's per-type cap (§9)
    max_wall_time_s=3600.0,  # cancel a workflow that takes longer
    stale_timeout_s=120.0,  # cancel a workflow that stops reporting (after its first report)
    supports_pause=False,  # True only if the workflow handles PAUSE/RESUME
    pause_timeout_s=None,  # cancel a workflow that has not picked up PAUSE in time
    cancel_grace_s=None,  # seconds to save and exit after CANCEL (§6); None: the spec's
    on_completion=callback,  # called with the WorkflowResult of every workflow
)
```

Exactly one of `block` and `launch_cmd` must be set. `spending_limit` and
`resources` are required; a command-line workflow can leave out `resources`,
and AuCaM takes them from its `task_description` (here 4 × 8 = 32 CPUs).

### Python workflows

An `async def` that runs inside AuCaM's process as the body of an AsyncFlow
block. AuCaM passes these arguments by name, to any function that declares
them (in any order):

- `reporter` — the `WorkflowReporter` (§4);
- `flow` — the AsyncFlow engine; submit compute steps as `@flow` tasks
  so they run on the allocation, are cancelled with the workflow and are
  charged to it;
- `context` — the `LaunchRequest.context` dict of this launch, plus
  `spending_limit` and `resources`: what AuCaM gave this workflow (§9).

```python
async def my_worker(reporter: WorkflowReporter, flow=None, context=None) -> None:
    @flow.function_task
    async def step(i): ...

    for i in range(context.get("n_steps", 10)):
        result = await step(i)
        await reporter.emit(score=result, iteration=i)
```

### Command-line workflows

`launch_cmd` runs as one executable task inside the workflow's block, with
`task_description` as its resource request (RADICAL-Pilot: `ranks`,
`cores_per_rank`, `gpus_per_rank`; Dragon: `process_template(s)`). AuCaM
starts it through `bash -c 'export …; exec <launch_cmd>'`, so no file has to
exist on the compute node. The program's environment has:

| Variable | Value |
|---|---|
| `CM_WORKFLOW_ID` | ID of this workflow |
| `CM_CAMPAIGN_ID` | Campaign name |
| `CM_CTX_<KEY>` | One per `LaunchRequest.context` entry |
| `CM_SPENDING_LIMIT_<DIM>`, `CM_RESOURCES_<KEY>` | The workflow's spending limit and CPUs / GPUs (§9) |
| `CM_DATA_ENDPOINT`, `CM_DATA_BACKEND` | How to reach the data backend (Redis or Dragon only) |
| *`WorkflowConfig.env`* | As given |

Its exit code becomes `WorkflowResult.returncode` (§12).

### `on_completion`

A plain function called once per finished workflow, after
`controller.on_complete()` — for side effects such as saving results. Don't
call `controller.on_complete()` from it, and don't `await` in it. To start
follow-on work, use a dependency (§7) or the controller.

---

## 4. Reporting from a workflow

A report has a **score** (any value: a number, a dict, …) and optional
**metrics** (a dict). Each report replaces the previous one; the controller
sees the latest one every policy cycle, and the last one ends up in
`WorkflowResult.final_score` / `final_metrics`. A workflow that never reports
still runs; AuCaM then only tracks its lifecycle.

### From Python

```python
async def my_worker(reporter: WorkflowReporter) -> None:
    for step in range(n_steps):
        sig = await reporter.poll()
        if sig == CampaignDecision.CANCEL:  # only with cancel_grace_s > 0
            save_state()
            return
        if sig == CampaignDecision.PAUSE:  # only with supports_pause=True
            save_state()
            if await reporter.wait_for_resume() == CampaignDecision.CANCEL:
                return

        result = run_step(step)
        await reporter.emit(
            score=result.energy,
            metrics={
                "loss": result.loss,
                "node_id": socket.gethostname(),  # lets AuCaM match node telemetry
                "cpu_hours": cpu_s_so_far / 3600,
            },  # own spending, cumulative (§9)
            iteration=step,
        )
```

Keep the score and all metrics in every report — the last report is the one
that counts. `emit()` raises if the backend fails.

Metric keys AuCaM itself uses: `cpu_hours`, `gpu_hours` and any other budget
dimension (spending, §9); `cpu_seconds` / `gpu_seconds` (live use estimate);
`node_id`; `resource_usage` (from `BlockProfiler` / `TaskProfiler`).

### From a command-line program

Needs the Redis or Dragon data backend (with the default in-process backend
the program runs but cannot report). Three options:

1. **The `aucam-report` command** (installed with AuCaM; also
   `python -m src.aucam_client`):

   ```bash
   aucam-report emit --score 0.82 --metric cpu_hours=1.5 --iteration 3
   sig=$(aucam-report signal)                 # PAUSE, RESUME, CANCEL or nothing
   [ "$sig" = CANCEL ] && { save_state; exit 0; }
   [ "$sig" = PAUSE ] && [ "$(aucam-report wait-resume)" = CANCEL ] && { save_state; exit 0; }
   # ... at the very end:
   aucam-report done --rc $?                  # finished, with this exit code
   ```

   The final `done` report lets a restarted AuCaM adopt the program if AuCaM
   was restarted while it ran (§6); Python: `Reporter().done(rc)`.

   Values are parsed as JSON when possible; `--metrics '{"k": 1}'` passes an
   object. Exit status 2: not running as an AuCaM workflow (or bad value);
   1: backend error.
2. **Redis directly**, from any language: `SET <CM_WORKFLOW_ID>` to a JSON
   object `{"wf_id", "score", "metrics", "iteration", "emitted_at"}`
   (`emitted_at` = Unix time, used for stale detection); read signals with
   `GET` then `DEL` of `<CM_WORKFLOW_ID>:signal`.
3. **Python:** `from src.aucam_client import Reporter` (`emit()`,
   `poll_signal()`, `wait_for_resume()`); it needs only `redis` / `radex`.

### Data backends

Set with `CampaignSpec.data_backend`:

| Backend | Use |
|---|---|
| `LocalDictBackend` (default) | Local runs and tests; in-process |
| `RhapsodyRedisBackend` | Redis server; `pip install -e ".[redis]"` |
| `RadexDragonBackend` | Dragon distributed dictionary, multi-node HPC |

---

## 5. The controller

### Built-in rules — no code

```python
CampaignController(
    launch=["sim_worker", "analysis_worker"],  # keep one request of each queued
    goal_type="analysis_worker",
    goal_n=50,  # end after 50 successful analyses
    rules=[
        CancelRule(
            workflow_type="sim_worker",
            when=lambda r: r.score is not None and r.score < 0.3,  # r: latest WorkflowReport
        )
    ],
)
```

`LaunchRule(type)` (what `launch=` creates) adds a request only when none of
that type is queued. `CancelRule` cancels running workflows of its type whose
latest report matches `when`.

`CostCancelRule` cancels a workflow that keeps spending without getting better:

```python
CostCancelRule(
    "sim_worker",
    max_without_improvement={"gpu_hours": 5.0},  # since its score last improved
    direction="maximize",  # or "minimize"; min_delta= sets the smallest improvement
)
```

A `sim_worker` that has spent 5 GPU-hours since its best score (or since it
started, if it has not reported a score) is cancelled. Its spending so far comes
from `obs["wf_spend"]` (below): per-task charges, the totals it reports, or the
GPUs / CPUs it holds × elapsed time.

### Your own logic

Subclass `CampaignController` and override what you need:

| Method | Called | Default |
|---|---|---|
| `observe(queue_snapshot, budget, running, **kw)` → `dict` | Each policy cycle | Passes the inputs through |
| `decide(obs)` → `PolicyResult` | Each policy cycle, after `observe` | Applies the rules |
| `on_complete(result)` | Each finished workflow | Counts `completed` / `cancelled` / `failed` per type — call `super()` |
| `progress(result)` → `float` | Each finished workflow | How much it moved the campaign toward its goal: 1.0 for a successful `goal_type` workflow, else 0; override for a richer measure or partial credit (§8, "Progress and credit") |
| `is_goal_reached()` → `bool` | By the default `decide()` | `goal_n` completions of `goal_type` |
| `goals()` → `list[Goal]` | At start | `[]` |
| `decide_async(obs)` → `PolicyResult` | Instead of `decide`, if overridden | For a decide that awaits a model or ADR; same timeout and fallback as `decide` |
| `get_summary()` → `dict` | When the campaign ends | `{}`; returned in `CampaignOutcome.summary` |
| `state()` → JSON-serializable / `restore(state)` | Every checkpoint / on restart | Saves and restores the `completed` / `cancelled` / `failed` counters; extend to keep your own state (e.g. the best score) across a restart |

The observation has these keys (plus any your `observe()` adds):

| Key | Content |
|---|---|
| `queue_snapshot` | `QueueSnapshot`: queued counts by priority and type |
| `budget` | This campaign's ledger entry: `limits`, `spent`, `in_flight`, `burn`, `by_type`, `capacity` (§9) |
| `running` | `{workflow_type: number running}` |
| `live_reports` | `{wf_id: WorkflowReport}` — latest report of each running workflow |
| `wf_types` | `{wf_id: workflow_type}` |
| `wf_spend` | `{wf_id: {dim: spending so far}}` — CPU/GPU hours, the campaign's limited dimensions and the workflow's own; a dimension with no source is absent |
| `allocations` | The allocation history: one dict per finished workflow — `workflow_type`, `spending_limit`, `resources`, `wall_time_s`, `spent`, `outcome`, `source`, `progress`, `credit`, `pending` (§8) |
| `learned_cost` | `{workflow_type: {dim: mean spending of its successful workflows}}`, for types with 3 or more |
| `telemetry` | `TelemetrySnapshot`: run times, node readings, spending, live use estimates |
| `tasks` | AsyncFlow task counts for this campaign: `waiting` (not yet handed to the backend), `submitted` (waiting in the backend), `running`. `None` without telemetry (`enable_telemetry=True`). |

`decide()` returns:

```python
PolicyResult(
    decisions={wf_id: CampaignDecision.CANCEL},  # CANCEL, PAUSE, RESUME, NOOP
    launch_requests=[LaunchRequest.make("sim_worker", context={"seed": 7})],
    cancel_queued={"sim_worker": 2},  # drop up to 2 queued requests of a type
    goals=[Goal("best", metric="best_energy", threshold=-8.0, direction="minimize")],
    remove_goals=["old_goal"],
    terminate=False,  # True ends the campaign
)
```

When scaling a type down, cancel running workflows **and** drop queued
requests (`cancel_queued`), or the freed slots are refilled from the queue.

If `decide()` raises or returns something other than a `PolicyResult`, the
campaign keeps going: AuCaM uses the built-in rules for that cycle (the
controller's `launch`, `LaunchRule`, `CancelRule` and goal, as the base
`decide()` applies them). Set `decide_timeout_s` in the spec to also cover a
`decide()` that is too slow, for example one that waits for an LLM: it then runs
in a thread, and cycles where it has not returned in time use the built-in rules
until it does. Each policy-cycle log line says which decide was used —
`decide=controller`, or `decide=fallback(error|timeout|busy)` — and
`CampaignOutcome.controller_errors` counts the cycles that failed. The same
applies to `observe()`: if it raises, the base `observe()` is used. A
controller whose decision is async (it awaits a model or ADR) overrides
`decide_async(obs)` instead; AuCaM awaits it with the same timeout and
fallback, without a thread. `ADRController` (§8, "AI policies") is one.

Example — a controller that tracks the best score and stops on a target:

```python
class BestScore(CampaignController):
    def __init__(self) -> None:
        super().__init__(launch=["sim"])
        self.best = None

    def on_complete(self, result: WorkflowResult) -> None:
        super().on_complete(result)
        if result.returncode == 0 and result.final_score is not None:
            self.best = (
                result.final_score if self.best is None else max(self.best, result.final_score)
            )

    def decide(self, obs: dict) -> PolicyResult:
        result = super().decide(obs)  # keep the launch rule
        result.terminate = self.best is not None and self.best > 0.95
        return result

    def get_summary(self) -> dict:
        return {"best": self.best}
```

---

## 6. How a campaign ends

| Exit reason | When |
|---|---|
| `goals_satisfied` | The goal is reached: all `Goal`s are satisfied (each names a key of the observation, a threshold and `"minimize"` / `"maximize"`), or `decide()` returned `terminate=True` and the controller's `is_goal_reached()` is True (the built-in `goal_type` / `goal_n`, or your own) |
| `controller_terminate` | `decide()` returned `terminate=True` with the goal not reached |
| `spending_limit` | The campaign's spending limit is reached and nothing is running, or actual spending went past it and the running workflows were cancelled (`on_limit="cancel"`, §9) |
| `campaign_idle` | Idle for `idle_exit_s`: queue empty, nothing running or paused, no requests held by dependencies |
| `signal` | AuCaM received SIGTERM or SIGINT, or `cm.stop()` was called |

Then AuCaM clears the queue, cancels running workflows (gracefully where
their type has a grace period, see below; waiting up to `shutdown_timeout_s`
in all), writes a final checkpoint and `run()` returns the outcome.

**Restarting a campaign.** With `checkpoint_dir` set, AuCaM saves the
campaign's state after every policy cycle. Started again with the same spec,
it continues from it: the queue (with each request's own limits), requests
held by dependencies, counters, spending, learned costs and raised limits,
the allocation history, goals (with the
controller's changes) and the `state()` of the scheduling policy (the
bandit's statistics) and of the controller. Workflows that were running:

- a command-line program reporting through Redis or Dragon may still be
  running: AuCaM **adopts** it under its old id, follows its reports, and
  finishes it on its `done` report (`aucam-report done --rc N`). If it sends
  no report within `stale_timeout_s` (default 60 s), it died with AuCaM:
  AuCaM charges what it last reported and runs it again. An adopted program
  can be cancelled only through CANCEL (AuCaM has no handle to its task);
- any other workflow died with AuCaM: AuCaM charges what it had spent by the
  last checkpoint and runs it again from the start.

**Graceful cancel.** Every cancel — by the controller, a rule, a limit, or at
shutdown — is graceful for a workflow type with `cancel_grace_s > 0`
(`WorkflowConfig`, or `CampaignSpec.cancel_grace_s` for all types; default 0):
AuCaM sends the workflow CANCEL through the data backend, and cancels it for
real only if it is still running when the grace period ends. The workflow
should check `reporter.poll()` (or `aucam-report signal`) and, on CANCEL, save
what it needs and return. Either way it counts as cancelled (`returncode`
−1); `WorkflowResult.cancel` is `"graceful"` if it exited on its own and
`"forced"` otherwise. Give a grace period only to workflows that check for
CANCEL: the others keep running, and keep their spending held against the
campaign, until it ends. A command-line workflow needs the Redis or Dragon
backend to receive CANCEL.

`CampaignSpec.idle_exit_s`:

- `"auto"` (default): `max(5 s, policy_interval_s)` — `decide()` always gets
  at least one policy cycle to add work;
- seconds (≥ 0): a longer window, e.g. for a controller that adds work in bursts;
- `None`: never end an idle campaign, e.g. for a controller that waits for
  external data. It then ends only by the other reasons.

---

## 7. Dependencies between workflow types

```python
CampaignSpec(
    ...,
    workflows=[
        WorkflowConfig(name="sim_worker", block=sim, ...),
        WorkflowConfig(name="analysis_worker", block=analysis, launch_on_start=False, ...),
    ],
    dependencies=[
        WorkflowDependency(
            source="sim_worker",
            target="analysis_worker",
            condition=lambda r: r.returncode == 0,  # the default
        )
    ],
)
```

The target must have `launch_on_start=False`; otherwise it starts right away.
Requests for the target wait outside the queue until a `sim_worker` workflow
finishes and passes the condition. The first such workflow releases them (or adds
one request); each later passing workflow adds one more. For anything more complex
(all of several sources, a count, a score), launch from `decide()`.

---

## 8. Scheduling

When a slot frees up, CRITICAL requests start first; the scheduling policy
chooses among the rest (HIGH, NORMAL, LOW; NORMAL requests that wait longer
than `age_threshold_s` become HIGH).

| Policy | Behaviour |
|---|---|
| `BanditSchedulingPolicy` (default) | UCB1 over workflow types. Reward 1 for a completed workflow, 0 for cancelled or failed (`reward_fn=` to change). Explores less as the campaign's budget is used up. With `cost_aware=True`, the reward is divided by 1 + `cost_weight` × the workflow's cost relative to the campaign's average, cost being the share of the budget left it spent, so cheaper types that succeed as often are preferred, and spending in a scarce dimension counts more ([budgets and spending](budget.md) §5). |
| `FIFOSchedulingPolicy` | Queue order |
| `LLMSchedulingPolicy`, `ADRSchedulingPolicy` | A language model or an ADR policy ranks the workflow types; see "AI policies" below |

Pass a policy instance as `scheduling_policy=`, or `None` for a new bandit.
An instance passed to the specs of several campaigns keeps what it learned. With `checkpoint_dir`, what it learned is also saved in the checkpoint: a policy returns it from `state()` (JSON-serializable) and loads it in `restore(state)`; the bandit does both.

![Bandit and budget](figures/aucam_bandit_budget.png)

### AI policies

`select()` and `size()` run every time a slot frees up, so they must be fast.
A policy that asks a language model or ADR does so in `refresh(ctx, queued,
obs)`: AuCaM runs it in the background once per policy cycle (and when a
workflow type it has not seen is queued), cancels it after
`refresh_timeout_s` (default 60 s), and `select()` / `size()` use its latest
result. The policy-cycle log line shows `refresh=ok|busy|timeout|error`.

The built-in AI policies subclass `RankingSchedulingPolicy`: `refresh()`
produces a ranking of workflow types, `{"rank": ["b", "a"], "value": {"b":
1.5}}`. While it is at most `max_age_cycles` (default 3) cycles old, queued
requests start in ranked order, within the free slots and caps, and `size()`
multiplies a type's learned-cost amount by its `value`. Before the first
ranking, or after errors, the `fallback` policy decides (default: a bandit).

- **`LLMSchedulingPolicy(model=..., endpoint=...)`** sends a model a short
  JSON summary of the campaign — queued and running types, caps, budget,
  per-type results, learned costs (`render()`, which you can override) — and
  reads the ranking from its reply. `endpoint` is any OpenAI-compatible API,
  e.g. Ollama's `http://localhost:11434/v1` (default: `$AUCAM_LLM_ENDPOINT` or
  `$OPENAI_BASE_URL`; key: `api_key=` or `$AUCAM_LLM_API_KEY` /
  `$OPENAI_API_KEY`). `client=` replaces the built-in HTTP client, e.g. with a
  stub in tests.
- **`ADRController(policy)`** makes the campaign's decisions with a radical.adr
  `Policy` (`pip install -e ../radical.adr`): every cycle AuCaM awaits
  `policy.decide(obs)` and applies the Decision — spawn → launch request
  (`task_name` is the workflow type, `task_kwargs` its context), cancel →
  CANCEL, goals, stop → end the campaign. Its `launch` / `rules` only decide
  in cycles where the ADR policy fails or times out.
- **`ADRSchedulingPolicy(controller=adr_controller)`** ranks from the
  Decision's `directives`, so one ADR call per cycle serves both. Without an
  `ADRController`, `ADRSchedulingPolicy(policy=adr_policy)` calls its own.

```python
adr_ctl = ADRController(MyADRPolicy(), goal_type="analysis", goal_n=50)
spec = CampaignSpec(
    ...,
    controller=adr_ctl,
    scheduling_policy=ADRSchedulingPolicy(controller=adr_ctl),
    decide_timeout_s=30.0,
)
```

**Model usage.** A policy or controller reports what its model calls used
with `take_usage()` — `LLMSchedulingPolicy` reports `{"llm_tokens": …}`.
AuCaM adds it up in `CampaignOutcome.model_usage` and logs `[SPEND] model
usage`; it charges it to the campaign's ledger only if the campaign limits
that dimension (`CampaignBudget(spending={"llm_tokens": 2e6})`) or the object
sets `charge_usage=True`.

### How much each workflow gets

When a request is about to start, the policy's `size(request, ctx)` decides
its allocation: a spending limit and the resources it may use at once.

- **The inputs:**
  - the type's starting values: `WorkflowConfig.spending_limit` / `resources`
    (used as given for the first workflows of the type);
  - the controller's amount for one launch: `LaunchRequest.spending_limit` /
    `resources` (always wins);
  - what AuCaM has learned about the type: its learned cost and peak resource
    use (`obs["learned_cost"]` shows the costs to a controller), and amounts
    raised after cancels at the limit.
- **The scheduling policy decides the amount.** When the scheduler is about to
  start a request, it calls the policy's `size()`, which every built-in policy
  inherits from `SchedulingPolicy`. By default `size()` gives, per dimension,
  the first that applies:
  1. the controller's amount for this workflow;
  2. the type's learned cost + 25% (`size_margin`), once 3 workflows of the
     type have finished (`CampaignSpec.min_cost_samples`); for resources, its
     peak use + 25% rounded up (the most CPUs / GPUs its successful workflows
     used at once, from task telemetry or `cpus_in_use` / `gpus_in_use`
     reports);
  3. after a workflow of the type was cancelled at its own limit, that limit
     + 25%, until learned values take over;
  4. the type's starting value, as given.

  A type with `learn=False` skips steps 2 and 3: each of its workflows gets the
  same amount (`ctx.learn` is False, so a learner overriding `size()` can keep it).

  Then it checks the remaining campaign budget: if the amount does not fit,
  `size()` returns nothing: the request waits while other workflows run, and is dropped
  (`CampaignOutcome.unfit_dropped`) once nothing else runs.
- **Give more to better types** by overriding the policy's `value(workflow_type,
  ctx)`: `size()` multiplies the learned-cost amount by it (default 1.0), then
  checks it against the remaining budget. What
  "better" means is up to the campaign; `ctx.by_type` has each type's cost per
  completed workflow and mean score.
- **Override `size()`** in your own `SchedulingPolicy` to decide differently;
  `ctx` (`SizingContext`) has the request's and the type's amounts, learned
  costs and resource use, remaining budget, queue length, per-type results
  (`by_type`) and the allocation history (`allocations`, below).

#### Progress and credit

To learn how much to give each workflow so the campaign reaches its goal
sooner, a learner needs to know what each allocation achieved. AuCaM records
every finished workflow as an `AllocationRecord`: what it got
(`spending_limit`, `resources`), its run time and spending, how it ended
(`completed`, `cancelled_at_limit`, `cancelled`, `failed`), and its progress:

- **`progress`** — the controller's `progress(result)`: how much the workflow
  moved the campaign toward its goal. By default 1.0 for a successful workflow
  of `goal_type`, else 0. Override it for a richer measure, or to give an
  intermediate step partial credit.
- **`credit`** — most workflows are steps toward a result, not results: a sim
  counts only through the analysis it triggers. So when a workflow makes
  progress, every workflow that led to it gets the same amount as credit. The
  chain is `LaunchRequest.context["source"]`: dependency launches set it
  (including requests a dependency held), and a controller sets it for its own
  launches, e.g. `LaunchRequest.make("refine", context={"source": wf_id})`.
- **`pending`** — credit arrives late: a record is pending while workflows it
  led to are still queued or running.

The history reaches a learner as `ctx.allocations` in `size()` and a
controller as `obs["allocations"]`, and is saved in the checkpoint.

A campaign's own sizing rule overrides `size()` and uses it. The demo's
`AllocationLearner` (`campaigns/demo_campaign/scheduler.py`,
`run_campaign.py --policy learn`) is a small example: for each sim type it
scales AuCaM's default amount by ×0.5, ×1 or ×1.5, tries each a few times,
and then keeps the factor whose workflows brought the most progress (their
own plus credit) per hour of run time, skipping records still pending.

**Writing your own sizing rule.**

- **The aim** is to reach the campaign's goal in the least time; the budget is
  a constraint, not something to minimize. More for one workflow can make it
  faster or more likely to succeed, but leaves less for the others.
- **What to measure:** `total_progress` (own progress + credit) per hour of
  run time, or per resource-hour (run time × the CPUs or GPUs it was given),
  so that a workflow holding half the capacity counts twice as much as one
  holding a quarter. Skip records that are still `pending`.
- **Workflows cancelled at their limit** (`cancelled_at_limit`) show the limit
  was too low: their true cost is unknown, only that it is higher. Count them
  as such; don't drop them.
- **Zero credit** is final once a record is no longer pending: what the
  workflow led to failed, was cancelled, or never came. A `progress()` that
  raises or returns `None` counts as 0.
- **Keep what is fixed:** the controller's amount (`ctx.requested`) and types
  with `learn=False` (`ctx.learn`); the default `size()` already does. Return
  `None` if the amount does not fit `ctx.remaining` or `ctx.free`.

---

## 9. Budgets and limits

```python
CampaignBudget(
    max_concurrent_workflows=4,  # running at once in this campaign
    max_concurrency_per_workflow=2,  # running at once per workflow type
    capacity={"cpus": 256, "gpus": 8},  # the allocation's resources (required)
    spending={"gpu_hours": 10.0},  # lifetime limit per dimension (optional)
    # Load-aware launching, with telemetry (defaults shown):
    target_utilization=0.85,  # one more workflow per cycle while nodes are below this
    max_waiting_tasks=None,  # fewer above this many waiting tasks; None: 2 × max_concurrent
    overcommit=1.0,  # with room, give out up to this × capacity
)
```

`capacity` is the total CPUs / GPUs of the allocation the campaign runs on. A
workflow starts only if the resources it is given fit what is free (capacity
minus what the running workflows were given); one that does not fit waits, and
one that needs more than the whole capacity is dropped. A resource the capacity
does not name counts as 0. The controller sees it in
`obs["budget"]["capacity"]`: `{key: {"total", "in_use", "free"}}`.

**Load-aware launching.** With telemetry (`CampaignManager(enable_telemetry=True)`
on a RHAPSODY / Dragon backend), how many workflows may run follows the
allocation, between 1 and `max_concurrent_workflows`. Every policy cycle:

- more than `max_waiting_tasks` tasks wait (not yet handed to the backend, or
  queued in it): a quarter fewer workflows may run (at least one fewer);
- the nodes' CPU use (and GPU use, if `capacity` has GPUs; smoothed over
  cycles) is below `target_utilization` and at most half that many tasks wait:
  one more may run, and the capacity counts `overcommit` times;
- otherwise the count stays.

It starts at `max_concurrent_workflows`, so set that to the most the
allocation should ever run. The log shows `[LOAD-…] workflows allowed 4 → 3`
and `allowed=3/4` in the policy-cycle line; the controller sees
`obs["load"]` (`utilization`, `waiting_tasks`, `allowed`, `max`, `room`).
Without telemetry, or with `target_utilization=None`, the limits stay fixed.

A workflow type can set its own cap, `WorkflowConfig(..., max_running=1)`, for
example to run one long, expensive simulation at a time next to several short
analyses; `max_concurrency_per_workflow` applies to the types that set none.
A custom scheduling policy reads the caps with `ctx.cap(workflow_type)`.

- **Spending.** Each workflow is charged per task (with telemetry), from the
  totals it reports (cumulative `cpu_hours`, `gpu_hours`, …), or as the CPUs /
  GPUs it was given × its run time ([budgets and spending](budget.md) §3). It gets its own spending limit when it starts (§8), starts only if that
  fits what is left of the campaign's limit, receives it
  (`context["spending_limit"]`, `CM_SPENDING_LIMIT_<DIM>`) and is cancelled if
  it spends more (`over_budget=True`); AuCaM checks once per policy cycle, so
  a workflow should stop itself before passing its limit (see the demo's
  `make_sim()`). The campaign-limit safety net and `on_limit`:
  [budgets and spending](budget.md) §4.
- **Resources per workflow.** `WorkflowConfig.resources={"cpus": 8, "gpus": 1}`
  (or `LaunchRequest.resources`) is what one workflow may use at once. The workflow
  gets it (`context["resources"]`, `CM_RESOURCES_<KEY>`) and must size its tasks
  to it; AuCaM cancels a workflow that uses more — measured from its running tasks
  with telemetry, or from the `cpus_in_use` / `gpus_in_use` it reports
  (`over_resources=True` in the result). The example campaigns do this; see
  `--overcpu` in the demo and `campaign_overuse_cpus.yaml` in `hpc_campaign`.
- **In the observation:** `obs["budget"]` — limits, spending, burn-down,
  per-type results and capacity ([budgets and spending](budget.md) §5).

---

## 10. Running campaigns

`CampaignManager.run(spec)` runs one campaign and returns its
`CampaignOutcome` when it ends. Your code — a script, an agent, a notebook —
reads the outcome and decides what to run next:

```python
async def main() -> None:
    cm = CampaignManager()
    spec = first_spec()
    while spec is not None:
        outcome = await cm.run(spec)
        print(outcome.exit_reason, outcome.goal_reached, outcome.spent, outcome.summary)
        spec = next_spec(outcome)  # e.g. drop types that never completed
```

- One `run()` at a time; a second call while one is running raises
  `RuntimeError`.
- `cm.stop()` ends the running campaign gracefully, as SIGTERM does
  (`exit_reason="signal"`).
- `CampaignManager(backend=...)` reuses a backend you started (e.g. Dragon on
  HPC) across campaigns and leaves it running; without one, each campaign starts and
  stops a local backend.

`CampaignOutcome`: `campaign_name`, `exit_reason`, `completed` / `cancelled` /
`failed` (per type), `elapsed_s`, `goal_reached`, `spent`, `summary`,
`budget_cancels` / `resource_cancels` (workflows cancelled for exceeding their own
spending limit or resources), `unfit_dropped` (requests that did not fit the
remaining budget), `controller_errors` (policy cycles where `decide()` or
`observe()` failed or timed out, §5), `model_usage` (tokens and other model use
reported by the policy and controller, §8). `goals`: the goals when it ended,
including the controller's changes.

### Config-driven runners

`campaigns/hpc_campaign/run_campaign.py` and `campaigns/ddsim_aucam/` build
specs from YAML files (one for the manager, one for the campaign) with
command-line overrides. Their keys are specific to each example; see the
[demo campaign guide](demo-campaign-guide.md) and the files'
`campaign_configs/`.

---

## 11. Troubleshooting

| Symptom | Cause |
|---|---|
| The workflow never gets a reporter | The function must have a parameter named `reporter` |
| A command-line workflow's reports never arrive | It needs `RhapsodyRedisBackend` or `RadexDragonBackend` |
| A dependency target starts immediately | Set `launch_on_start=False` on it |
| The campaign ends with `campaign_idle` too early | Raise `idle_exit_s` or set it to `None` |
| PAUSE has no effect | `supports_pause=True` is needed, and the workflow must `poll()` |
| A cancel takes `cancel_grace_s` seconds | The workflow does not check for CANCEL; set its `cancel_grace_s` to 0 |
| `llm_tokens` (or another dimension) stays 0 | Only `cpu_hours` / `gpu_hours` are charged without reports; the workflow must report it; see [budget.md](budget.md) §3 |

Planned features and known problems: [known gaps](known-gaps.md).

---

## 12. Reference

### `CampaignSpec`

| Field | Default | Meaning |
|---|---|---|
| `name`, `budget`, `controller`, `workflows` | — | Required |
| `dependencies` | `[]` | `WorkflowDependency` list (§7) |
| `policy_interval_s` | 60.0 | Seconds between policy cycles (also runs on every completion) |
| `shutdown_timeout_s` | 120.0 | Grace period for running workflows at shutdown |
| `decide_timeout_s` | `None` | Longest `decide()` may take; slower cycles use the built-in rules (§5) |
| `cancel_grace_s` | 0.0 | Default grace period for a graceful cancel (§6) |
| `min_cost_samples` | 3 | Successful workflows of a type before its learned cost and resource use are used (§8) |
| `idle_exit_s` | `"auto"` | §6 |
| `max_queue_depth` | 50 | Above this, a warning is logged (requests are still queued) |
| `age_threshold_s` | 300.0 | NORMAL requests waiting longer become HIGH |
| `data_backend` | `None` → `LocalDictBackend` | §4 |
| `checkpoint_dir` | `None` | Save state after every policy cycle; restore on restart (§6, "Restarting a campaign") |
| `scheduling_policy` | `None` → new `BanditSchedulingPolicy` | §8 |

### `WorkflowResult`

| Field | Meaning |
|---|---|
| `workflow_id`, `workflow_type` | Which workflow |
| `returncode` | 0 completed; −1 cancelled; otherwise failed: the program's exit code (128 + N if killed by signal N), or 1 |
| `wall_time_s` | Run time |
| `final_score`, `final_metrics` | From the last report |
| `spent` | What the workflow was charged |
| `spending_limit`, `over_budget` | The workflow's own spending limit (empty: none), and whether AuCaM cancelled it for exceeding it |
| `resources`, `over_resources` | What the workflow could use at once, and whether AuCaM cancelled it for using more |
| `cancel` | For a cancelled workflow: `"graceful"` (it exited after CANCEL) or `"forced"`; otherwise `None` |
| `resource_profile` | Measured CPU/memory use, when available |

### Imports

Everything is in `src.aucam`: `CampaignManager`, `CampaignSpec`,
`CampaignBudget`, `WorkflowConfig`, `WorkflowDependency`,
`CampaignController`, `CancelRule`, `CostCancelRule`, `LaunchRule`, `Goal`, `PolicyResult`,
`CampaignDecision`, `LaunchRequest`, `LaunchPriority`, `WorkflowReporter`,
`WorkflowReport`, `WorkflowResult`, `CampaignOutcome`,
`BanditSchedulingPolicy`, `FIFOSchedulingPolicy`, `SchedulingPolicy`,
`SchedulingContext`, `SizingContext`, `Allocation`, `AllocationRecord`,
`RankingSchedulingPolicy`, `LLMSchedulingPolicy`, `ADRSchedulingPolicy`,
`ADRController`, `LocalDictBackend`, `RhapsodyRedisBackend`,
`RadexDragonBackend`, `TelemetrySnapshot`, `QueueSnapshot`, `BlockProfiler`,
`TaskProfiler`, checkpoint helpers. The command-line reporting client is
`src.aucam_client`.

### Changes from earlier versions

- Python ≥ 3.11, `radical.asyncflow` and `rhapsody-py` ≥ 0.7.0.
- `CampaignBudget.capacity` is required.
- `WorkflowConfig.spending_limit` / `resources` are starting values, not fixed
  amounts: after `min_cost_samples` successful workflows, each new one gets the
  learned amount + 25 %. Set `learn=False` to keep them fixed.
- `WorkflowConfig.spending_limit` and `resources` are required (a
  command-line workflow's `resources` can come from its `task_description`).
  If the starting limit is too low, the first workflows are cancelled at it,
  each next one getting 25 % more.
- `declared_spending` is removed. A workflow that reports no spending is
  charged the CPUs / GPUs it was given × its run time; in-flight is each
  workflow's limit. To keep an expected run time, use `max_wall_time_s`.
- `WorkflowConfig.bounds`, `LaunchRequest.bounds`, `fair_share` and
  `WorkflowResult.capped` are removed (known gaps, U7).
