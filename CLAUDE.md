# CLAUDE.md

Guidance for Claude Code in this repository.

## Read first

- `docs/architecture-overview.md` — what AuCaM does and the
  **design decisions** behind it. Don't change a decision listed there without
  asking.
- `docs/known-gaps.md` — what is planned or not implemented yet, with how it
  should work. Read it before working on budgets, checkpointing, command-line
  reporting, cancellation or campaign exit; update it when you implement or
  fix an entry.
- `docs/budget.md` — budget and spending behaviour as implemented.
- The design originals are Google Docs owned by the team
  ([AuCaM Design](https://docs.google.com/document/d/16oeg4FGlew5XUsdDedk4V3qdui3LhJqB_qwxlIn4C8Q/edit),
  [Detailed Design](https://docs.google.com/document/d/1aIEElgo-jP7h95SJR1cfMa-pHe4bk-zvnHtNxuryWC8/edit)).
  If code or a request conflicts with the documented design, point out the
  conflict instead of silently diverging.

## Rules

- Budgets are spending only (resource-hours or any other dimension, one ledger:
  `BudgetAllocator`). Don't add a "resources in use at once" budget; capacity is
  limited by the allocation and the workflow-count limits.
- Only the `CampaignController` decides to cancel workflows (plus CM's
  automatic stale-reporter and wall-time cancels).
- One scheduling-policy instance per campaign; no cross-campaign learning.
  Results reach other campaigns only through the driver.
- Every workflow runs as one AsyncFlow block (`launch_cmd` = a block with one
  executable task).
- Terms: CampaignManager (CM), driver API (`submit()` → `SubmitResult` →
  `CampaignOutcome`), GlobalBudget, CampaignBudget, `declared_spending`,
  CampaignController, PolicyResult, WorkflowQueue, SchedulingPolicy,
  WorkflowReporter, DataBackend.

## Traps

- Imports are `from src.aucam import ...`.
- A `WorkflowDependency` target needs `launch_on_start=False`, or it starts
  immediately.
- A Python workflow gets `reporter`, `flow` and `context` only if its
  function declares parameters with exactly these names.
- `_on_done` (in `_launch`) is a synchronous done-callback: it cannot await.
- Command-line workflows can report only with the Redis or Dragon backend.
- The built-in `goal_type`/`goal_n` goal ends a campaign with
  `controller_terminate`; `goals_satisfied` is for `Goal` objects.
- `max_queue_depth` only warns.

## Commands

```bash
pip install -e ".[dev]"      # Python ≥ 3.10; [dragon] needs ≥ 3.11; [redis] for Redis + aucam-report
pytest                       # tests needing unreleased radical.asyncflow features skip (known-gaps X1)
pytest -m "not slow"
ruff check --no-fix . && ruff format --check .   # lint (plain `ruff check .` edits files)
python docs/figures/src/architecture.py              # redraw the architecture figure
python campaigns/demo_campaign/doc_figures.py        # redraw the budget/bandit figures (~1 min)
```

`docs/code-walkthrough.md` has the module map and main code paths.

## Dragon on Delta (NCSA)

- Cold page faults on Lustre can freeze the event loop: use
  `asyncio.to_thread()` for blocking file I/O inside workflows.
- `dragon -m` fails on Delta (FQDN vs short node name). Multi-node launch:
  ```bash
  (cd $NET_DIR && dragon-network-config --output-to-yaml --no-stdout)
  dragon -w ssh --network-config $NET_DIR/slurm.yaml -t tcp run_campaign.py ...
  ```
- Python `logging` goes to stderr (`.err`), `print()` to stdout (`.out`); CM
  budget and spending logs are INFO → `.err`.
