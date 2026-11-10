# Demo campaign — API walkthrough

A single self-contained file (`campaigns/demo_campaign/run_campaign.py`) that
exercises every AuCaM API.  Runs anywhere in a few seconds — no HPC, no Dragon.

---

## What the demo runs

Two simulation arms compete; a dependency triggers analysis after each successful sim.

| Workflow | Behaviour |
|---|---|
| `fast_sim` | 6 steps, ~70% convergence. Kept queued via `LaunchRule`. |
| `precise_sim` | 10 steps, ~90% convergence. Launched by `decide()` with dynamic priority. |
| `analysis` | Post-processing; fires after every successful sim via `WorkflowDependency`. |

Every workflow has a spending limit. AuCaM passes it to the workflow, the
workflow reports what it has spent and stops before it would pass the limit,
and AuCaM cancels a workflow that spends more (§2, "Spending").

The campaign ends after `goal_analyses` analyses complete **or** a simulation
reaches a perfect score (≥ 0.99) — whichever comes first.

---

## 1. Run it

```bash
pip install -e .
cd campaigns/demo_campaign
python run_campaign.py
```

Finishes in a few seconds. You should see:

```
[SCHEDULER-demo] LAUNCH fast_sim id=demo.fast_sim.… requester=policy_agent
[CTRL] CancelRule(fast_sim): cancelling demo.fast_sim.… score=0.212
[CM] DAG fast_sim→analysis satisfied — created 1 request(s)
```

Try other policies:

```bash
python run_campaign.py --policy science       # reward = final score
python run_campaign.py --policy bandit_tight  # low exploration, faster exploitation
python run_campaign.py --policy custom        # recency-bias subclass of SchedulingPolicy
python run_campaign.py --policy fifo          # no learning; queue order
python run_campaign.py -v                     # verbose: every launch and policy cycle
```

See AuCaM enforce a workflow's spending limit:

```bash
python run_campaign.py --overspend            # precise_sim ignores its limit
```

`precise_sim` now runs 3× longer and ignores its limit, so AuCaM cancels it once
its reported spending passes the limit:

```
[WORKFLOW-demo.precise_sim.…] spending limit exceeded cpu_hours=0.0057 > 0.0045 — auto-cancel
[CM] Campaign demo SUMMARY — … (stale=0 walltime=0 budget=1 resources=0 policy=…) …
spent 0.0169 cpu_hours; workflows cancelled for exceeding their own limit: 1
```

---

## 2. API tour

### Workflows (`run_campaign.py`)

Each workflow is an `async def`. AuCaM passes it the arguments it declares:
`reporter` (a `WorkflowReporter`) and `context` (a dict, here with the spending
limit). The workflow calls `reporter.emit()` every step to send AuCaM a score
and metrics; AuCaM reads them every policy cycle and evaluates `CancelRule`
against the latest score.

```python
async def sim(reporter: WorkflowReporter, context: dict) -> None:
    limit = context["spending_limit"]["cpu_hours"]  # given by AuCaM
    spent = 0.0
    for step in range(steps):
        if spent + step_cpu_h > limit:  # stop before passing the limit
            break
        await asyncio.sleep(0.05)  # one step of work
        spent += step_cpu_h
        score += ...
        await reporter.emit(
            score=score,
            metrics={"step": step, "cpu_hours": spent},  # spending so far
            iteration=step,
        )
```

In the demo each step costs a fixed amount (`FAST_STEP_CPU_H`,
`PRECISE_STEP_CPU_H`); one factory, `make_sim()`, builds both sims.

---

### Spending: the workflow–AuCaM interface (`run_campaign.py → build_spec`)

Spending goes both ways between AuCaM and a workflow:

| Direction | How | In the demo |
|---|---|---|
| AuCaM → workflow | the workflow's spending limit, in `context["spending_limit"]` (a command-line program gets `CM_SPENDING_LIMIT_CPU_HOURS`) | `limit = context["spending_limit"]["cpu_hours"]` |
| workflow → AuCaM | its spending so far, as a running total in every `emit()` (`metrics={"cpu_hours": …}`) | `metrics={"cpu_hours": spent}` |

Each workflow type declares two amounts:

```python
WorkflowConfig(
    name="fast_sim",
    block=fast_sim,
    # Expected: what one workflow usually costs (6 steps) and how long it takes.
    # AuCaM charges it, prorated by actual wall time, if the workflow reports
    # no spending. fast_sim reports cpu_hours, so here it is only a fallback.
    declared_spending={"wall_hours": 6 * STEP_S / H, "cpu_hours": 6 * FAST_STEP_CPU_H},
    # Allowed: the most one workflow may spend (9 steps, 1.5x expected).
    # Passed to the workflow; AuCaM cancels the workflow if it spends more.
    spending_limit={"cpu_hours": 9 * FAST_STEP_CPU_H},
)
```

- **`declared_spending`** is what one workflow is expected to cost, with its
  expected wall time (`wall_hours`). AuCaM uses it when a workflow reports no
  spending: it charges the declared amount prorated by actual wall time (half
  the declared time, half the cost). For a type without a `spending_limit`, it
  is also what AuCaM holds against the campaign's limit while the workflow runs.
- **`spending_limit`** is what one workflow may spend. AuCaM holds it against
  the campaign's limit while the workflow runs, passes it to the workflow, and
  cancels the workflow if its reported spending goes past it. The
  `WorkflowResult` then has `over_budget=True`, and
  `CampaignOutcome.budget_cancels` counts such workflows.

The controller can give one workflow a different limit than its type's:
`LaunchRequest.make("precise_sim", spending_limit={"cpu_hours": 0.006})`.
Each workflow can also be given resources (CPUs, GPUs) to use at once, and the
scheduler can size both from the remaining budget; see the
[developer guide](campaign-developer-guide.md), "How much each workflow gets".

---

### WorkflowDependency (`run_campaign.py → build_spec`)

```python
WorkflowDependency(source="fast_sim", target="analysis")
WorkflowDependency(
    source="precise_sim",
    target="analysis",
    condition=lambda r: r.returncode == 0 and r.final_score > 0.5,  # custom gate
)
```

Default condition is `returncode == 0` (successful completion only).
Override `condition` to gate on score, metrics, or any field of `WorkflowResult`.

---

### CampaignBudget (`run_campaign.py → build_spec`)

```python
CampaignBudget(
    max_concurrent_workflows=4,
    max_concurrency_per_workflow=2,  # per-type cap enforced by the CM
    # Campaign limit: no new workflows once spent + in flight reaches it.
    spending={"cpu_hours": 0.05},
)
```

The campaign limit and each workflow's own limit work together: a workflow
holds its limit against the campaign's while it runs, so the campaign stops
launching before running workflows could take it past its limit.

---

### ShowcaseController — all controller hooks (`run_campaign.py`)

#### `__init__` — rules and goal

```python
super().__init__(
    rules=[
        LaunchRule("fast_sim"),  # explicit form of launch= shorthand
        CancelRule("fast_sim", when=lambda r: r.score is not None and r.score < threshold),
        CancelRule("precise_sim", when=...),
    ],
    goal_type="analysis",
    goal_n=goal_analyses,
)
```

`LaunchRule` keeps exactly one request of the given type queued at all times
(guarded: only enqueues when `queue_snapshot.by_type[type] == 0`).

#### `observe` — augment the snapshot

```python
def observe(self, queue_snapshot, budget, running=None, **kwargs) -> dict:
    obs = super().observe(queue_snapshot, budget, running, **kwargs)
    obs["cancel_rate"] = cancelled / max(1, total)  # custom field for decide()
    return obs
```

Called by the CM before every `decide()`.  Add any derived metric here so
`decide()` doesn't recompute it.

#### `decide` — custom launch logic and full PolicyResult

```python
def decide(self, obs: dict) -> PolicyResult:
    base = self._apply_rules(self._rules, obs)  # applies LaunchRule + CancelRule from __init__
    q = obs["queue_snapshot"]

    # Keep one precise_sim queued; promote it to HIGH when a good score was seen.
    extra = []
    if not self.is_goal_reached() and q.by_type.get("precise_sim", 0) == 0:
        priority = (
            LaunchPriority.HIGH
            if self._best is not None and self._best > 0.7
            else LaunchPriority.NORMAL
        )
        extra.append(LaunchRequest.make("precise_sim", priority=priority))

    # High cancel rate → drop queued fast_sims to free slots for analysis.
    cancel_queued = {"fast_sim": 2} if obs["cancel_rate"] > 0.6 else None

    return PolicyResult(
        decisions=base.decisions,
        launch_requests=base.launch_requests + extra,
        terminate=base.terminate,
        cancel_queued=cancel_queued,
    )
```

`_apply_rules` processes the declarative rules from `__init__` (the `LaunchRule`
that keeps `fast_sim` queued, and the `CancelRule`s).  `decide()` then adds
`precise_sim` imperatively — it always keeps one in the queue and raises its
priority to `HIGH` once a score above 0.7 has been seen.

`cancel_queued` removes queued-but-not-yet-running requests.  When the cancel
rate exceeds 60 %, up to 2 queued `fast_sim` requests are dropped to free
concurrency slots for `analysis` workflows.


#### `is_goal_reached` — custom early-stop

```python
def is_goal_reached(self) -> bool:
    if self._best is not None and self._best >= 0.99:
        return True  # perfect score: stop regardless of analysis count
    return super().is_goal_reached()
```

#### `goals` — declare tracked targets

```python
def goals(self) -> list[Goal]:
    return [Goal(name="quality", metric="score", threshold=0.8, direction="maximize")]
```

#### `on_complete` — accumulate per-result state

```python
def on_complete(self, result: WorkflowResult) -> None:
    super().on_complete(result)  # keeps completed/cancelled/failed counters accurate
    is_sim = result.workflow_type in ("fast_sim", "precise_sim")  # analysis always scores 1.0
    if is_sim and result.returncode == 0 and result.final_score is not None:
        if self._best is None or result.final_score > self._best:
            self._best = result.final_score
```

#### `get_summary` — campaign outcome

```python
def get_summary(self) -> dict:
    return {"best_score": self._best, "analyses_done": ..., "cancel_rate": ...}
```

Accessible directly from `spec.controller.get_summary()` after `cm.run()` returns.

---

### Scheduling policies (`scheduler.py`)

| Name | Class | What changes |
|---|---|---|
| `bandit` | `BanditSchedulingPolicy()` | Default throughput reward (1=success, 0=cancel) |
| `science` | `BanditSchedulingPolicy(reward_fn=_score_reward)` | reward = final score |
| `bandit_tight` | `BanditSchedulingPolicy(exploration=0.5)` | Less exploration, faster exploitation |
| `custom` | `_RecencyBiasPolicy` | `SchedulingPolicy` subclass: `select()` + `on_outcome()` |
| `fifo` | `FIFOSchedulingPolicy()` | Queue order, no learning |

**Custom subclass pattern:**

```python
class _RecencyBiasPolicy(SchedulingPolicy):
    def select(self, candidates, ctx: SchedulingContext) -> list:
        # ctx fields: running_by_type, history, policy_cycles,
        #             budget_remaining_fraction, max_concurrency, …
        ...

    def on_outcome(self, request, result: WorkflowResult) -> None:
        # update local state after each completion
        if result.returncode == 0:
            self._last_success = request.workflow_type
```

**Reading arm beliefs after a campaign:**

```python
if isinstance(policy, BanditSchedulingPolicy):
    logging.info("arm stats: %s", policy.arm_stats())
# → {'fast_sim': {'n': 4, 'mean': 0.500, 'ucb': 1.23}, 'precise_sim': {'n': 8, 'mean': 0.875, 'ucb': 1.05}}
```

---

### CM lifecycle (`run_campaign.py → run`)

```python
outcome = await CampaignManager().run(spec)  # returns when the campaign ends
s = outcome.summary  # the controller's get_summary()
print(outcome.exit_reason, outcome.spent, outcome.budget_cancels)
```

`run()` sets up the AsyncFlow engine, runs the campaign, shuts the engine down
and returns the `CampaignOutcome`.  Call it again with another spec to run the
next campaign.

---

## 3. What to read next

- [Campaign developer guide](campaign-developer-guide.md) — full reference for every API.
- `campaigns/hpc_campaign/` — production example: Dragon backend, SLURM, telemetry,
  config files, and figures for benchmarking.
- `campaigns/ddsim_aucam/` — real DeepDriveSim workflows wired to AuCaM.
