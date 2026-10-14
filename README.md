# AuCaM — Autonomous Campaign Manager

An `asyncio`-native policy engine for concurrent HPC scientific workflows.  You define what "success" looks like and what to do with intermediate results; AuCaM handles launching, monitoring, cancelling, and stopping — across one campaign or many, under a shared resource budget.

---

## Install

```bash
pip install -e .              # Python ≥ 3.11 — core only
pip install -e ".[dev]"       # + pytest, ruff, coverage
pip install -e ".[dragon]"    # + Dragon/RADICAL HPC backend
pip install -e ".[adr]"       # + ADR adaptive scheduling
pip install -e ".[llm]"       # + LLM scheduling policy
```

## Hello world

```python
import asyncio
from aucam import (
    CampaignManager, CampaignSpec, CampaignBudget,
    WorkflowConfig, CampaignController, WorkflowReporter,
)

async def my_sim(reporter: WorkflowReporter) -> None:
    import random
    for step in range(20):
        await asyncio.sleep(0.05)
        await reporter.emit(score=random.random(), metrics={"step": step})

spec = CampaignSpec(
    name="hello",
    budget=CampaignBudget(max_concurrent_workflows=4),
    controller=CampaignController(
        launch=["sim"], goal_type="sim", goal_n=10,
    ),
    workflows=[WorkflowConfig(name="sim", block=my_sim)],
)

cm = CampaignManager()
cm.submit_campaign(spec)
asyncio.run(asyncio.wait_for(cm.run(), timeout=30.0))
```

---

## Documentation

| Doc | Audience | What it covers |
|-----|----------|----------------|
| [Architecture overview](docs/architecture-overview.md) | Everyone | Key concepts — campaign, workflow, policy cycle — and how the components relate, without requiring programming background |
| [Campaign developer guide](docs/campaign-developer-guide.md) | Scientists / campaign authors | Controllers, workflows, budgets, the driver API, YAML config, and the full type reference |
| [Demo campaign guide](docs/demo-campaign-guide.md) | New users | File layout, the three workflow types, `ScoreController`, how to run locally and on Delta HPC with Dragon |
| [Code walkthrough](docs/code-walkthrough.md) | Contributors / backend developers | Class hierarchies, `CampaignManager` admission check, `_CampaignInstance` dual task loop, data flow end-to-end, extension points |

---

## Running tests

```bash
pytest                           # full suite (42 tests)
pytest --cov=src                 # with coverage
pytest -m "not slow"             # skip slow integration tests
```
