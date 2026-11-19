# ASD prototype campaign

Autoagent-Steered Diffusion, end to end under AuCaM: IMPRESS-A designs, IMPRESS-A
labels each design, ROME fine-tunes on the labels, and the **controller promotes
the new model only once it has been shown to design better**.

```
          ┌──────────────── AuCaM: one engine, one budget, one controller ─────────────┐
          │                                                                            │
 collect  │  design ─▶ label ─▶ records.jsonl          (design_in_flight at a time)     │
          │                         │                                                  │
 training │                         └─▶ train (ROME) ─▶ candidate checkpoint            │
          │                                                   │                         │
 trial    │  design(candidate) ─▶ label ─▶ mean score ────────┘                         │
          │                                   │                                        │
 promoted │   WorkflowMessage(to="design", name="model", sticky=True) ◀─ cleared the bar │
          └────────────────────────────────────────────────────────────────────────────┘
```

Every launch is the controller's decision. ROME runs with `auto_train=False`, so
a round happens when the controller asks for one and never on ROME's own clock.

## Why promotion waits for a trial

ROME's training manager returns a **checkpoint path and no quality figure**, and
a training metric (validation loss, sequence recovery) is not the campaign's bar
anyway. So a candidate is proved the only honest way: run `trial_n` designs with
it — passed per launch in `context["model"]`, so nothing else switches — label
them the way every other design is labelled, and compare their mean with what
the current model has been scoring. `target_score` in `campaign.yaml` makes that
an absolute bar instead of a relative one.

Promotion is one `WorkflowMessage(..., sticky=True)`: designs still running pick
the model up through `reporter.messages()`, and every design launched afterwards
finds it in `context["messages"]["model"]`.

## Files

| File | What it is |
|---|---|
| `run_campaign.py` | Entry point: builds the AuCaM `CampaignSpec` and runs it |
| `controller.py` | `AsdController` — the collect / training / trial / promote machine |
| `workflows.py` | The three blocks: two IMPRESS-A campaigns and one ROME round |
| `records.py` | `records.jsonl` + `records.consumed`: the label → train handoff |
| `campaign_configs/campaign.yaml` | Budgets, limits, bars, counts |
| `campaign_configs/campaign_smoke.yaml` | The same flow with nothing real: mock chain, fake round, no GPU |
| `delta_env_setup.sh` | Builds the VE on Delta (AuCaM + IMPRESS + ROME); run once |
| `delta_sbatch.sh` | The Slurm job |

Tested by `tests/test_asd_campaign.py` in the repo root: the real controller and
the real training block over stub design and label blocks, so the decision logic
is covered without a GPU, ROME or IMPRESS.

## Running it

Laptop — IMPRESS's mock chain for both halves, a fake training round, no GPU and
no Dragon. `--label-spec` is the mock chain again and `score_key` is `progress`,
because the real label campaign does not exist yet:

```bash
python run_campaign.py \
    --design-spec $WORK_DIR/IMPRESS/agent/campaigns/mock-small-molecule-binding.yaml \
    --label-spec  $WORK_DIR/IMPRESS/agent/campaigns/mock-small-molecule-binding.yaml \
    --config campaign_configs/campaign_smoke.yaml --train-impl fake
```

`--train-impl fake` writes the same shape of checkpoint as ROME's `DummyTrainer`
without importing ROME: `import rome` needs Dragon, so the real path only runs on
an allocation.

Delta — the real chain and a real ROME round:

```bash
export WORK_DIR=<your_work_dir>
bash campaigns/asd_campaign/delta_env_setup.sh          # once: AuCaM + IMPRESS + ROME

export SBATCH_ACCOUNT=<project>-delta-gpu
export ASD_DESIGN_SPEC=$WORK_DIR/IMPRESS/agent/campaigns/delta-small-molecule.yaml
export ASD_LABEL_SPEC=$WORK_DIR/IMPRESS/agent/campaigns/delta-label.yaml
mkdir -p $WORK_DIR/campaign_manager/campaigns/asd_campaign/logs
sbatch campaigns/asd_campaign/delta_sbatch.sh
```

The job runs under `dragon -s` (ROME needs the Dragon runtime) and cleans up
after itself whether it finishes or not. Everything it writes goes to
`$WORK_DIR/asd-runs/$SLURM_JOB_ID` — runs, corpus, checkpoints and AuCaM's own
checkpoint — on the shared filesystem, because the corpus is the handoff between
workflows and the checkpoints are read by later launches. Export `CORPUS` to
point at a directory you keep, and the next job starts from the records this one
produced. `ENGINE=concurrent TRAIN_IMPL=fake CONFIG=campaign_configs/campaign_smoke.yaml`
runs the same control flow on a CPU partition.

ROME is installed with `--no-deps`: the DummyTrainer needs only numpy, asyncflow
and Dragon, while ROME's declared dependencies pull torch, transformers, trl and
peft. Export `ROME_FULL=1` when switching to a real trainer.

## One engine (CON-1)

`run_campaign.py` builds AuCaM's engine over `impress_backends(design_spec)` —
IMPRESS-A's own `compute` and `local` routes — and AuCaM passes that engine to
every workflow as `flow`. So the designs, the labels **and ROME's training round**
all submit to one engine: their tasks sit inside AuCaM's block for their
workflow, are cancelled with it, and are visible to its accounting.

## What is real and what is standing in

| Piece | State |
|---|---|
| Controller, trial, promotion, message plumbing | Real, tested, and exercised end to end on the mock chain |
| `design` | A real IMPRESS-A campaign, one per launch, its own `campaign_id` and run directory |
| `label` | A real IMPRESS-A campaign — but **the label spec does not exist yet**; the smoke run points `--label-spec` at the design chain and takes `score_key: progress` |
| `train` | A real ROME round with `DummyTrainer`: a real checkpoint, written by a real task on AuCaM's engine, from a model that does not exist |
| Promotion changing the science | **Not wired.** `workflows.apply_model()` records the version and leaves the chain alone; it is the one place the real weights parameter goes, once there is a LigandMPNN trainer |
| Corpus | `records.jsonl`. A Dragon `DDict` is the next step — see below |

## Known gaps, in the order they will bite

1. **The label campaign.** Its YAML, and the `label_input_param` that tells it
   which design directory to score (`"<stage>.<key>"`). Until then `label` is the
   design chain run a second time.
2. **A LigandMPNN trainer.** `delta-small-molecule.yaml` designs sequences with
   LigandMPNN; ROME ships a trainer for `dauparas/ProteinMPNN`
   (`examples/impress_r/mpnn.py`), which is the thing to model it on. Then
   `apply_model()` sets the weights parameter and the loop closes for real.
3. **`target_score` does not gate rounds.** It is carried into the train launch
   and is what the controller measures the trial against. Once a trainer reports
   a validation metric, "keep training until it clears the target" belongs in
   `make_train_block`; the per-workflow spending limit is the backstop either
   way, because a score target with no cap is unbounded.
4. **The corpus should be a `DDict`.** ROME is built for one ("records from any
   node are synchronized automatically"), and a file only works while every
   workflow shares a filesystem. The seam is `records.py` — three functions.
5. **Two seams belong in IMPRESS.** `workflows.py` imports three private helpers
   from `impress_a.integrations.aucam` because `make_impress_worker()` cannot yet
   take a per-launch spec patch (each design needs its own `campaign_id`) or extra
   report fields (the controller needs to know which directory a design wrote).
   Both are small additions to that adapter.

## Two traps already paid for

**`learn` is off for all three types.** AuCaM sizes a workflow type from the cost
of its first few runs; a design that then costs slightly more is cancelled at its
own limit, and in a trial that throws away the comparison the promotion depends
on. Turn it on per type once the real cost on Delta is known.

**`budget_floor` is unset on purpose.** AuCaM *drops* a request whose sized limit
would fall below the floor (`unfit_dropped`) rather than raising it to the floor.
Set above the learned cost, it makes every launch silently disappear — which is
exactly what happened the first time this campaign ran.
