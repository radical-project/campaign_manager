# IMPRESS AuCaM Campaign

Runs repeated IMPRESS-A protein-binding design campaigns under AuCaM supervision.
Each AuCaM *workflow instance* is one full IMPRESS-A run: plan → score → Pareto
front. AuCaM enforces a per-workflow CPU + GPU budget, tracks live scores, and
decides when to stop.

## Quick start

```bash
# Local dev — mock tools, finishes in seconds, no GPU needed
export WORK_DIR=<your_work_dir>
cd $WORK_DIR/campaign_manager/campaigns/impress_campaign

python run_campaign.py \
    --spec $WORK_DIR/IMPRESS/agent/campaigns/mock-small-molecule-binding.yaml

# Delta HPC
export IMPRESS_SPEC=$WORK_DIR/IMPRESS/agent/campaigns/delta-small-molecule-smoke.yaml
sbatch delta_sbatch.sh
```

---

## Files

| File | Purpose |
|---|---|
| `run_campaign.py` | Entry point — builds the AuCaM `CampaignSpec` and runs it |
| `controller.py` | `ImpressController` — goal tracking, live score logging, budget monitor |
| `config.py` | Dataclasses + YAML loaders for `GlobalConfig` / `CampaignConfig` |
| `campaign_configs/campaign.yaml` | Campaign defaults (goal, budget, spending limits) |
| `campaign_configs/global.yaml` | Engine / telemetry defaults |
| `delta_env_setup.sh` | Create the VE on Delta (run once) |
| `delta_sbatch.sh` | Slurm job script |

---

## Architecture

```
CampaignManager (AuCaM)
│
├── CampaignSpec
│   ├── BanditSchedulingPolicy   — picks which arm to launch next
│   ├── ImpressController        — goal check, score log, budget monitor
│   └── WorkflowConfig "impress" — one IMPRESS-A campaign as an async block
│
└── per policy cycle:
    ├── controller.observe()     — log budget burn, warn at 80%
    ├── controller.is_goal_reached() — stop when N completions done
    └── [SPEND] line             — AuCaM's own budget accounting
```

Each workflow instance wraps `impress_a.integrations.aucam.make_impress_worker()`,
which builds a fresh `CampaignManager` and runs IMPRESS-A's own inner loop.
AuCaM owns the outer lifecycle; IMPRESS-A owns the science.

---

## AuCaM mechanics — traced through the log

### 1. Startup and initial launch

```
[CM] Campaign impress-mock-small-molecule-binding: starting
[BANDIT]  budget_rem=100.0%  c_eff=1.414  arms={'impress': 'μ=nan'}
[SCHEDULER] LAUNCH impress id=...2eb88b94  requester=cm:init
    spending_limit={'cpu_hours': 4.0, 'gpu_hours': 4.0}
    resources={'cpus': 8.0, 'gpus': 1.0}
```

AuCaM fires the first workflow immediately at startup (`requester=cm:init`) without
waiting for the first policy cycle. The bandit shows `μ=nan` — no prior reward data
yet. `spending_limit` and `resources` come from `WorkflowConfig`, which
`impress_a.integrations.aucam.workflow_config()` builds from the IMPRESS offer.

### 2. IMPRESS runs the design chain

```
impress_a.aucam  fitted to the caller's grant {'cpus': 8.0, 'gpus': 1.0}:
                 concurrency=1 replicas=1
impress_a.runtime.executor  submitted r0001: {
    'l0_s0_mock_rfd3_design', 'l0_s1_mock_mpnn_design',
    'l0_s2_mock_packmin', 'l0_s3_mock_fastrelax',
    'l0_s4_mock_filter_shape', 'l0_s5_mock_boltz_predict'
} (trusted=True, estimate={'gpu_hours': 0.006, 'cpu_hours': 0.006})
impress_a.runtime.executor  reaped r0001: 6/6 tasks ok,
    cost={'gpu_hours': 0.006, 'cpu_hours': 0.006}
```

`_fit_resources()` in `impress_a.aucam` sizes IMPRESS concurrency to the GPU grant.
IMPRESS submits `r0001` (cycle 1 of its own inner loop): six staged tools —
RFdiffusion → ProteinMPNN → Rosetta pack/minimize → FastRelax → shape filter →
Boltz predict. Each `task.000NNN is in DONE state` line is one asyncflow task.
After both cycles (r0001, r0002) satisfy all Pareto objectives, IMPRESS stops:

```
impress_a.runtime.executor  stop requested: every spec objective satisfied
impress_a.aucam  campaign mock-small-molecule-binding ended:
    every spec objective satisfied (3 report(s) emitted)
```

### 3. Workflow completion and spend accounting

```
[WORKFLOW-...2eb88b94] COMPLETED wall=0.7s rc=0
[SPEND] wf_done  wf=...2eb88b94
    method=exact  source=wf_reported
    cpu_h=0.0000  gpu_h=0.0000  wall_s=0.7  rc=0
```

`method=exact  source=wf_reported` — AuCaM used the measured figures IMPRESS
reported via `reporter.emit()`. For the mock spec the measured spend is near zero
because no real GPU work ran (the displayed `0.0000` is `4e-6 cpu_h` rounded).

### 4. Controller records the live score

```
[CTRL] impress done  progress=1.000  front_size=1
    best=[complex_plddt=0.931  ligand_iptm=0.902
          shape_complementarity=0.838  total_score=-388.446]
    total=1/20  spent={'cpu_hours': 4e-06, 'gpu_hours': 0.0}
```

`ImpressController.on_complete()` fires here. `progress=1.000` — the full Pareto
front is satisfied. `total=1/20` tracks completions toward `goal_n=20`.

### 5. Budget monitoring and policy cycle

```
[BUDGET] cpu_hours: 0.0000/10.0000 (0.0%)  |  gpu_hours: 0.0000/10.0000 (0.0%)
[POLICY] cycle: active=0 queued=0 decisions=0
    launches=1 terminate=False decide=controller
[SPEND] campaign=...  spent_cpu_h=0.0000 spent_gpu_h=0.0000  budget_used=0.0%
[BANDIT]  budget_rem=100.0%  arms={'impress': 'μ=nan'}
[SCHEDULER] LAUNCH impress id=...fc1a3fec  requester=policy_agent
    spending_limit={'cpu_hours': 4.0, 'gpu_hours': 4.0}
```

`[BUDGET]` is logged by `ImpressController.observe()` each cycle — it shows
per-dimension consumption against the campaign-wide `spending` limits from
`campaign.yaml`. A WARNING fires once when either dimension crosses 80%.

`decide=controller` — `is_goal_reached()` returns `False` (1/20), `terminate=False`
→ `launches=1`. The second run receives the same `spending_limit={4.0, 4.0}`.

### 6. Run 4 — AuCaM scheduler drops before launch (budget_floor)

After three successful runs AuCaM's cost learning kicks in (`min_cost_samples=3`):
the scheduler now sizes from the **learned cost** rather than the starting
`spending_limit`. The mock spec reports near-zero GPU spend, so the learned
`gpu_hours ≈ 0`, and `size()` computes `spending_limit[gpu_hours] = 0 × 1.25 = 0`.
The `budget_floor` check catches this before the workflow starts:

```
[BANDIT]  budget_rem=100.0%  arms={'impress': 'μ=1.000'}
[BANDIT]  → impress  ucb=1.833  c_eff=1.414
[SCHEDULER] DROP impress: spending_limit[gpu_hours]=0 < budget_floor 0.006
```

The scheduler computed `alloc.spending_limit` for run 4 and found that
`gpu_hours=0` — the remaining per-workflow budget rounded to zero. That is below
`budget_floor=0.006` (the cost of one IMPRESS cycle, taken from `offer.cost[d].floor`
at `workflow_config()` time). The request is removed from the queue before any
workflow starts: no IMPRESS process, no tasks, no estimated-cost charge.
`unfit_dropped` increments to 1.

After the drop the queue is empty and nothing can run, so the campaign shuts down:

```
[CM] Campaign impress-mock-small-molecule-binding SUMMARY —
    exit=spending_limit run=3 ok=3 ... unfit_dropped=1
```

### 7. Campaign exits — spending_limit

```
========================================================================
  CAMPAIGN SUMMARY  impress-mock-small-molecule-binding
========================================================================
  exit            : spending_limit
  goal reached    : False
  elapsed         : 6.9s

  workflows run   : 3
    completed     : 3
    cancelled     : 0  (cancel rate 0%)
    failed        : 0
    budget_cancels: 0
    resource_cancels: 0
    unfit_dropped : 1
  controller_errors: 0

  spent:
    cpu_hours           : 0.000012
    gpu_hours           : 0.000000

  impress scores:
    n               : 3
    best_progress   : 1.000
    avg_progress    : 1.000
========================================================================
```

`unfit_dropped=1` — the one DROP at the scheduler level. All three completed
workflows scored `progress=1.000`, so `avg_progress=1.000`.

The campaign-wide `spending` (10.0 cpu_h, 0.000012 cpu_h used) was never the
constraint. The DROP was driven by AuCaM's **cost learning**: after 3 successful
runs (`min_cost_samples=3` by default), AuCaM switches from the starting
`spending_limit` to the type's **learned cost** × 1.25. The mock spec reports
near-zero GPU spend (`wf_reported gpu_h=0.0000`), so the learned `gpu_hours ≈ 0`.
On run 4, `size()` computed `spending_limit[gpu_hours] = 0 × 1.25 = 0`, which is
below `budget_floor[gpu_hours] = 0.006` → DROP.

---

## Budget control — three layers

| Layer | Where | Log signal | What it does |
|---|---|---|---|
| **AuCaM scheduler DROP — campaign spending** | `[SCHEDULER] DROP` | `unfit_dropped` ++ | Refuses to launch if declared allocation exceeds remaining campaign `spending`; fires before any instance is created |
| **AuCaM scheduler DROP — budget_floor** | `[SCHEDULER] DROP` | `unfit_dropped` ++ | Refuses to launch if `spending_limit` to be passed is below the workflow's `budget_floor` (minimum viable spend); also fires before any instance is created |
| **AuCaM per-workflow limit** | `spending_limit` on `WorkflowConfig` | `[SPEND] method=estimated` | Passes remaining allowance as the workflow's own limit; charges estimated cost if the workflow completes with no real work |
| **IMPRESS gate 5** | `_narrow_budget()` → executor | `rejected at budget` | Applies AuCaM's limit as IMPRESS's own budget; refuses a graph before any task runs |

Enforcement fires outermost-first. In this run the `budget_floor` DROP fired (layer 2):
after 3 real runs the per-workflow budget remainder fell to 0, below `budget_floor=0.006`,
and the scheduler dropped run 4 before IMPRESS ever started.

---

## Score flow

```
IMPRESS tool (Boltz) → metrics dict
  → CampaignDigest.best["ligand_iptm"] / ["complex_plddt"]
  → CampaignDigest.progress  (fraction of Pareto front satisfied)
  → _ReporterSink.report()   (impress_a.integrations.aucam)
  → reporter.emit(score=progress, metrics={cpu_hours, gpu_hours, best, ...})
  → WorkflowResult.final_score / .final_metrics
  → ImpressController.on_complete()  → [CTRL] log line
  → BanditSchedulingPolicy reward    → μ updates → UCB arm selection
```

The score AuCaM's bandit sees is `progress` — a scalar in [0, 1]. The rich
per-objective bests (`ligand_iptm`, `complex_plddt`, etc.) travel in `metrics` and
are logged by the controller but not used for arm selection.

---

## Configuration reference

### `campaign_configs/campaign.yaml`

```yaml
controller:
  goal_n: 20            # ambitious goal — budget exhausts before this is reached
  budget_warn_fraction: 0.80  # WARNING fires when this fraction of budget is used

budget:
  max_concurrent: 2     # max parallel workflow instances
  capacity:             # total resources the CM allocates across all workflows
    cpus: 8
    gpus: 2
  spending:             # campaign-wide total; campaign stops when this is exhausted
    cpu_hours: 10.0
    gpu_hours: 10.0

workflows:
  impress:
    spending_limit:     # per-workflow hard limit; passed to IMPRESS as its budget
      cpu_hours: 2.0
      gpu_hours: 0.5
    resources:          # grant given to each workflow instance
      cpus: 4
      gpus: 1
    stale_timeout_s: 3600.0  # cancel if no report received in this window
    score_floor: 0.0    # cancel THIS workflow mid-run if progress < this (0 = off)
```

### Key CLI flags

| Flag | Default | Description |
|---|---|---|
| `--spec` | *(required)* | Path to IMPRESS campaign spec YAML |
| `--goal-n` | 4 | Completions before campaign stops |
| `--max-concurrent` | 2 | Max parallel workflow instances |
| `--spending-cpu-hours` | — | Override campaign-wide CPU budget |
| `--spending-gpu-hours` | — | Override campaign-wide GPU budget |
| `--score-floor` | 0.0 | Cancel an impress workflow mid-run if its `progress` drops below this |
| `--engine` | concurrent | `concurrent` (local) or `dragon` (HPC) |
| `--telemetry-dir` | — | Write AuCaM telemetry JSONL here |
| `--verbose` | false | DEBUG-level logging |

---

## IMPRESS spec — what `--spec` points to

The spec is an IMPRESS-A campaign YAML describing the protein design problem.
Ready-made specs live in `$IMPRESS_DIR/agent/campaigns/`:

| Spec | Use |
|---|---|
| `mock-small-molecule-binding.yaml` | Local dev — mock tools, seconds, no GPU |
| `delta-small-molecule-smoke.yaml` | Quick Delta smoke test — real tools, small run |
| `delta-small-molecule.yaml` | Full Delta run — RFdiffusion + ProteinMPNN + Rosetta + Boltz |

Key fields in the spec:

```yaml
objectives:
  - {name: complex_plddt,  direction: maximize, min: 0.75}
  - {name: ligand_iptm,    direction: maximize, min: 0.4}
  - {name: shape_complementarity, direction: maximize, min: 0.55}
  - {name: total_score,    direction: minimize}

budget:
  gpu_hours: 4.0
  cpu_hours: 4.0

stages: [mock_rfd3_design, mock_mpnn_design, mock_packmin,
         mock_fastrelax, mock_filter_shape, mock_boltz_predict]
```

AuCaM narrows `budget` to its own `spending_limit` before each run via
`_narrow_budget()` (line 166 in the log).
