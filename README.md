# AuCaM — Autonomous Campaign Manager

AuCaM runs many scientific campaigns at once under a shared budget. A
**campaign** pursues one scientific goal through many **workflows** (simulation
runs, analyses, …). You describe the workflows, the decision logic and the
limits; AuCaM launches the workflows, watches their results while they run, and
acts on them.

What AuCaM does:

- **Steers each campaign from live results** — a controller (built-in rules,
  or your own code, which may call an LLM or ADR) decides which workflows to launch and which to cancel,
  every policy cycle and whenever a workflow finishes.
- **Learns what to run** — a per-campaign scheduler (a UCB1 bandit) favours
  workflow types that complete and explores less as the budget runs down.
- **Keeps spending within limits** — every run is charged (CPU-hours,
  GPU-hours or any other dimension) in one ledger; campaigns stop launching at
  their limit, and new campaigns are admitted only if they fit the global budget.
- **Runs any workflow** — Python functions or command-line programs in any
  language, on AsyncFlow and RHAPSODY backends, from a laptop to an HPC
  allocation.
- **Closes the loop with drivers** — when a campaign ends, its outcome
  (counts, spending, the controller's summary) goes back to the driver that
  submitted it, which can plan the next campaign.

![AuCaM capability demo](docs/figures/aucam_impact.png)

*One campaign cancels poor runs and learns to avoid them; the driver uses its
outcome to plan a cheaper follow-up campaign (see
[the capability demo](docs/cm-advantages.md)).*

---

## Install

```bash
pip install -e .              # core (Python ≥ 3.10)
pip install -e ".[redis]"     # + Redis DataBackend and the aucam-report command
pip install -e ".[dragon]"    # + Dragon/RADICAL HPC backend (Python ≥ 3.11)
pip install -e ".[adr]"       # + ADR scheduling bridge
pip install -e ".[llm]"       # + LLM scheduling policy
pip install -e ".[plotting]"  # + matplotlib, for the demo figures
pip install -e ".[dev]"       # + pytest, ruff, coverage
```

## Hello world

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
    name="hello",
    budget=CampaignBudget(max_concurrent_workflows=4, max_concurrency_per_workflow=4),
    controller=CampaignController(launch=["sim"], goal_type="sim", goal_n=10),
    workflows=[WorkflowConfig(name="sim", block=my_sim)],
    policy_interval_s=0.5,
)


async def main() -> None:
    cm = CampaignManager()
    cm.expect_driver()  # one driver will submit campaigns

    async def driver() -> None:
        try:
            result = await cm.submit(spec)  # accepted, or rejected with a reason
            outcome = await result.wait()  # CampaignOutcome, when the campaign ends
            print(outcome.completed, outcome.elapsed_s)
        finally:
            cm.driver_done()

    task = asyncio.create_task(driver())
    await cm.run()  # returns when all drivers are done
    await task


asyncio.run(main())
```

---

## Documentation

| Doc | For | What it covers |
|---|---|---|
| [Architecture overview](docs/architecture-overview.md) | Everyone | Concepts, components and design decisions, without code |
| [Demo campaign guide](docs/demo-campaign-guide.md) | New users | Run a demo campaign in a few minutes |
| [Capability demo](docs/cm-advantages.md) | Everyone | What AuCaM adds, shown on one run |
| [Campaign developer guide](docs/campaign-developer-guide.md) | Campaign authors | Workflows, controllers, scheduling, budgets, drivers |
| [Budgets and spending](docs/budget.md) | Campaign authors | How spending is measured, limited and reported |
| [Code walkthrough](docs/code-walkthrough.md) | Contributors | Module map and main code paths |
| [Known gaps](docs/known-gaps.md) | Contributors | What is planned or not implemented yet |

---

## Running tests

```bash
pytest                   # full suite
pytest -m "not slow"     # skip slow integration tests
pytest --cov=src         # with coverage
```
