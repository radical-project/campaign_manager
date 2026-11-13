# Demo campaign

One self-contained file, `campaigns/demo_campaign/run_campaign.py`, that uses
every part of the AuCaM API. It runs anywhere in a few seconds: no HPC, no
Dragon.

---

## What it runs

| Workflow | Behaviour |
|---|---|
| `fast_sim` | 6 steps, converges ~70 % of the time. Kept queued by a `LaunchRule`. |
| `precise_sim` | 10 steps, converges ~90 % of the time. Queued by `decide()`, at HIGH priority once a good score has been seen. |
| `analysis` | Runs after every successful sim (`WorkflowDependency`). |

Each workflow gets a spending limit and the CPUs it may use (`fast_sim` 1,
`precise_sim` 2). It reports what it has spent and the CPUs it uses, and stops
before passing its limit. AuCaM cancels a workflow that spends more or uses
more CPUs. Sims that score below 0.3 are cancelled (`CancelRule`).

The campaign ends after 4 analyses (`--goal-analyses`) or when a sim reaches a
score of 0.99, whichever comes first.

---

## Run it

```bash
pip install -e .
cd campaigns/demo_campaign
python run_campaign.py
```

The log shows launches, cancels and dependency triggers:

```
[SCHEDULER-demo] LAUNCH fast_sim id=demo.fast_sim.… requester=cm:init spending_limit={'cpu_hours': 0.0018} resources={'cpus': 1.0}
[CTRL] CancelRule(fast_sim): cancelling demo.fast_sim.… score=0.238
[WORKFLOW-demo.fast_sim.…] CANCELLED wall=0.2s rc=-1 cancel=graceful
[CM] DAG fast_sim→analysis satisfied — created 1 request(s)
```

| Option | What changes |
|---|---|
| `--policy science` | The bandit's reward is the final score, not completion |
| `--policy bandit_tight` | Less exploration |
| `--policy custom` | A `SchedulingPolicy` subclass that prefers the type that last succeeded |
| `--policy fifo` | Queue order, no learning |
| `--policy learn` | Also learns how much each sim gets (below) |
| `--overspend` | `precise_sim` runs 3× longer and ignores its limit |
| `--overcpu` | `precise_sim` uses twice the CPUs it is given |
| `-v` | Every launch and policy cycle |

With `--overspend`, AuCaM cancels `precise_sim` once its reported spending
passes its limit. The sims have a 2 s grace period (`cancel_grace_s=2.0`), so
AuCaM first sends CANCEL and the workflow stops itself at its next step:

```
[WORKFLOW-demo.precise_sim.…] spending limit exceeded cpu_hours=0.0039 > 0.00375 — auto-cancel
[WORKFLOW-demo.precise_sim.…] CANCEL signal sent; grace 2s
[precise] CANCEL received at step 14 — stopping
[WORKFLOW-demo.precise_sim.…] CANCELLED wall=0.7s rc=-1 cancel=graceful
[CM] Campaign demo SUMMARY — … (stale=0 walltime=0 budget=2 resources=0 policy=…) …
```

With `--overcpu`, AuCaM cancels it for using more CPUs than it was given. After
each such cancel the next `precise_sim` gets 25 % more CPUs, and still uses
twice that:

```
[WORKFLOW-demo.precise_sim.…] resources exceeded cpus=4 > 2 — auto-cancel
[WORKFLOW-demo.precise_sim.…] resources exceeded cpus=6 > 3 — auto-cancel
[CM] Campaign demo SUMMARY — … (stale=0 walltime=0 budget=0 resources=5 policy=…) …
```

---

## How it is built

### Workflows

Each workflow is an `async def`. AuCaM passes the arguments it declares:
`reporter` and `context`, which holds the workflow's spending limit and CPUs.
One factory, `make_sim()`, builds both sims; each step costs a fixed amount
(`FAST_STEP_CPU_H`, `PRECISE_STEP_CPU_H`):

```python
async def sim(reporter: WorkflowReporter, context: dict) -> None:
    limit = context["spending_limit"]["cpu_hours"]  # given by AuCaM
    cpus = context["resources"]["cpus"]
    for step in range(steps):
        if await reporter.poll() == CampaignDecision.CANCEL:  # graceful cancel
            return
        if spent + step_cpu_h > limit:  # stop before passing the limit
            break
        await asyncio.sleep(STEP_S)  # one step of work
        spent += step_cpu_h
        score += ...
        await reporter.emit(
            score=score,
            metrics={"step": step, "cpu_hours": spent, "cpus_in_use": cpus},
            iteration=step,
        )
```

AuCaM reads the latest report every policy cycle: the score for `CancelRule`,
`cpu_hours` and `cpus_in_use` to check the workflow's limits.

### Spending and resources

```python
WorkflowConfig(
    name="fast_sim",
    block=fast_sim,
    cancel_grace_s=2.0,  # it stops on CANCEL; AuCaM waits up to 2 s
    # Starting limit: 9 steps (1.5x expected). Once 3 fast_sims have finished,
    # each new one gets their average spending + 25 %.
    spending_limit={"cpu_hours": 9 * FAST_STEP_CPU_H},
    # CPUs it may use at once.  A workflow that reported no spending would be
    # charged these × its run time; the sims report their own.
    resources={"cpus": 1},
)
```

`precise_sim` starts at 12.5 steps (0.00375 CPU-hours, 1.25× expected), with
2 CPUs. A controller can give one workflow a different amount:
`LaunchRequest.make("precise_sim", spending_limit={"cpu_hours": 0.006})`.

With `--policy learn`, `AllocationLearner` (`scheduler.py`) overrides the
policy's `size()`: it tries ×0.5, ×1 and ×1.5 of each sim's default limit and
keeps the factor with the most progress per hour. The log shows
`[LEARN] fast_sim: ×1 → cpu_hours=…`. In this mode an analysis runs only for a
sim whose final score reaches 0.75, so a sim given too little to converge
brings no progress.

How AuCaM sizes each workflow: [developer guide](campaign-developer-guide.md)
§8; how it measures and limits spending: [budgets and spending](budget.md).

### Dependencies and budget

```python
def _dep(r: WorkflowResult) -> bool:  # an analysis after each successful sim
    if r.returncode != 0:
        return False
    return analysis_threshold is None or (r.final_score or 0.0) >= analysis_threshold


WorkflowDependency(source="fast_sim", target="analysis", condition=_dep)
WorkflowDependency(source="precise_sim", target="analysis", condition=_dep)

CampaignBudget(
    max_concurrent_workflows=max_concurrent,  # 4 by default
    max_concurrency_per_workflow=2,  # at most 2 of each type at once
    capacity={"cpus": 2 * max_concurrent},  # a sim starts only if its CPUs fit
    spending={"cpu_hours": 0.05},  # the campaign's limit
)
```

`analysis_threshold` is 0.75 with `--policy learn`, else `None`. Each running
workflow holds its own limit against the campaign's, so AuCaM stops launching
before the running workflows could take the campaign past its limit.

### Controller

`ShowcaseController` uses every controller hook:

| Hook | In the demo |
|---|---|
| `__init__` | `rules=[LaunchRule("fast_sim"), CancelRule("fast_sim", …), CancelRule("precise_sim", …)]`, `goal_type="analysis"`, `goal_n` |
| `observe()` | Adds `obs["cancel_rate"]` for `decide()` |
| `decide()` | Applies the rules, keeps one `precise_sim` queued, and drops queued `fast_sim`s when the cancel rate is high (below) |
| `is_goal_reached()` | Also ends the campaign on a score ≥ 0.99 |
| `goals()` | Declares a `Goal` |
| `on_complete()` | Tracks the best sim score; calls `super()` to keep the counters |
| `get_summary()` | Best score, analyses done, cancel rate; returned in `CampaignOutcome.summary` |

```python
def decide(self, obs: dict) -> PolicyResult:
    base = self._apply_rules(self._rules, obs)  # LaunchRule and CancelRules
    extra = []
    if not self.is_goal_reached() and obs["queue_snapshot"].by_type.get("precise_sim", 0) == 0:
        good = self._best is not None and self._best > 0.7
        priority = LaunchPriority.HIGH if good else LaunchPriority.NORMAL
        extra.append(LaunchRequest.make("precise_sim", priority=priority))
    return PolicyResult(
        decisions=base.decisions,
        launch_requests=base.launch_requests + extra,
        terminate=base.terminate,
        # many cancels: drop up to 2 queued fast_sims to free slots for analyses
        cancel_queued={"fast_sim": 2} if obs["cancel_rate"] > 0.6 else None,
    )
```

### Scheduling policies (`scheduler.py`)

`make_policy(name)` builds the policy for `--policy`:

| Name | Policy |
|---|---|
| `bandit` | `BanditSchedulingPolicy()`: reward 1 for a completed workflow, 0 otherwise |
| `science` | `BanditSchedulingPolicy(reward_fn=_score_reward)`: reward = final score |
| `bandit_tight` | `BanditSchedulingPolicy(exploration=0.5)` |
| `custom` | `_RecencyBiasPolicy`: its own `select()` and `on_outcome()` |
| `fifo` | `FIFOSchedulingPolicy()` |
| `learn` | `AllocationLearner`: the bandit, plus its own `size()` |

After the campaign the demo logs the bandit's view of each type,
`policy.arm_stats()`, e.g.
`{'fast_sim': {'n': 4, 'mean': 0.5, …}, 'precise_sim': {'n': 8, 'mean': 0.875, …}}`.

### Running the campaign

```python
outcome = await CampaignManager().run(spec)  # returns when the campaign ends
print(outcome.exit_reason, outcome.spent, outcome.budget_cancels, outcome.summary)
```

---

## Next

- [Campaign developer guide](campaign-developer-guide.md) — every API in full.
- `campaigns/hpc_campaign/` — an HPC example: Dragon backend, SLURM,
  telemetry, configuration files.
- `campaigns/ddsim_aucam/` — DeepDriveSim workflows run by AuCaM.
