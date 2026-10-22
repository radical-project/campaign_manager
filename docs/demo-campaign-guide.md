# Demo campaign — quick start

Run an AuCaM campaign on your laptop in a few seconds, then adapt it.
Everything is in `campaigns/demo_campaign/`.

---

## What the demo runs

A campaign that searches for good simulation results:

| Workflow type | What it does |
|---|---|
| `sim_worker` | A synthetic simulation (~0.8 s). Its score drifts up or down; about a third of runs go bad. |
| `sim_worker_b` | A shorter simulation (~0.3 s) that goes bad less often. |
| `analysis_worker` | Analyses a simulation's result; starts after a successful simulation (a dependency). |

The campaign's controller (`ScoreController` in `controller.py`) keeps both
simulation types queued, cancels any simulation whose score falls below 0.4,
and ends the campaign after 3 successful analyses. The scheduler (a bandit)
learns which simulation type succeeds more often and launches it more.

---

## 1. Run one campaign

```bash
pip install -e .
cd campaigns/demo_campaign
python run_campaign.py
```

It finishes in about 3 seconds. Look for:

```
[CM] Campaign demo-campaign SUMMARY — exit=controller_terminate run=19 ok=14 cancelled=5 ... spent={'cpu_hours': 0.0027}

  bandit arms (3 learned):
    analysis_worker   n=  4  mean=0.750
    sim_worker        n=  4  mean=0.500
    sim_worker_b      n= 11  mean=0.818
```

- `exit=controller_terminate` — the goal (3 analyses) was reached.
- `cancelled=5` — runs stopped early because their scores fell below 0.4.
- The bandit launched `sim_worker_b` most, because it succeeded most often
  (`mean` is its success rate).
- `spent` — the CPU-hours the campaign was charged.

Add `-v` to see every launch, cancellation and policy cycle.

## 2. Run several campaigns with a driver

```bash
python run_campaign.py --driver --num-campaigns 3 --driver-delay 0
```

A driver submits the campaigns one after another and reads each outcome
before planning the next:

```
[DRIVER] demo-campaign-a: accepted
[DRIVER] demo-campaign-a: science summary — n_scores=3  best=1.000  avg=1.000
[DRIVER] demo-campaign-a → planning demo-campaign-b | prev: goal_reached=True  cancel_rate=36%  analyses=3  elapsed=2.6s
...
```

## 3. See what AuCaM adds

```bash
python run_impact.py        # ~75 s
```

Cancellation, learning, spending and driver feedback on one run, explained in
the [capability demo](cm-advantages.md).

---

## Adapt it

| To change | Edit |
|---|---|
| Goal, cancel threshold | `campaign_configs/campaign.yaml` → `controller.goal_analyses`, `controller.score_floor` |
| Concurrency and spending limits | `campaign.yaml` → `budget.max_concurrent`, `budget.max_concurrency_per_workflow`, `budget.spending` |
| Expected cost per run | `campaign.yaml` → `workflows.<type>.declared_spending` |
| Number of campaigns, global limits, scheduling policy | `campaign_configs/global.yaml` → `num_campaigns`, `global_budget`, `scheduling.policy` |
| The science | `workflows.py` (workflows) and `controller.py` (decision logic) |

`python run_campaign.py --help` lists the command-line overrides.

To write your own campaign from scratch, see the
[campaign developer guide](campaign-developer-guide.md).

## Run on an HPC allocation

`delta_sbatch.sh` is a SLURM script for NCSA Delta (two nodes). It uses the
Dragon backend with `campaign_configs/global_hpc.yaml` and `campaign_hpc.yaml`
(longer runs, a global spending limit):

```bash
sbatch delta_sbatch.sh                       # Dragon backend
ENGINE=concurrent sbatch delta_sbatch.sh     # without Dragon, for debugging
```

Set `SBATCH_ACCOUNT`, and adjust the node count and time limit for your site.

---

## Files

| File | Purpose |
|---|---|
| `run_campaign.py` | Entry point: reads the configs, builds the campaign, runs it (with or without a driver) |
| `controller.py` | `ScoreController`: cancel rule, goal, summary for the driver |
| `workflows.py` | The three workflow types |
| `config.py` | Config dataclasses and YAML loading |
| `scheduler.py` | Builds the scheduling policy named in `global.yaml` |
| `campaign_configs/` | `global.yaml`, `campaign.yaml` (local); `global_hpc.yaml`, `campaign_hpc.yaml` (HPC) |
| `delta_sbatch.sh` | SLURM batch script |
| `run_impact.py`, `gen_impact_fig.py` | Capability demo and its figure |
| `tier2_sim_worker.py` | A command-line workflow, added automatically with the Redis or Dragon backend |
