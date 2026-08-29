# Campaign Manager — Core Package

`src/campaign` is the engine layer of the CM: asyncio execution, ADR policy integration,
metrics, checkpointing, and HPC resource management. This document describes seven design
patterns adopted from the SPLASH orchestrator that strengthened the CM's robustness,
observability, and developer experience.

---

## Pattern 1 — Structured decision trace in campaign state

**Files:** `metrics.py` · `src/campaign/adr/operator.py`

### What was built

`DecisionTraceEvent` is a dataclass in `CampaignMetrics` that records one ADR policy cycle:

```python
@dataclass
class DecisionTraceEvent:
    cycle: int
    t: float                          # wall-clock seconds from campaign start
    policy: str                       # e.g. "RuleCorrectionsPolicy"
    actions: list[dict]               # serialised Action objects
    llm_prompt_tokens: int | None     # None for rule-based decisions
    llm_completion_tokens: int | None
```

`CampaignMetrics.record_decision()` appends one event per ADR tick.
`to_dict()` serialises all events into the campaign JSON alongside replica events —
no separate log file to correlate.

### Why it matters

Flat JSONL logs are hard to query after a run and disappear on SLURM log rotation.
Having token costs co-located with the decision they produced makes LLM cost auditing
and post-hoc policy comparison straightforward.

### Future work

- Wire token counts through the LLM policy path so `llm_*_tokens` fields are populated
  on every `LLMSchedulingPolicy` cycle (currently None).
- Add an `add_obs_trace()` variant that snapshots the ADR observation dict each cycle,
  enabling replay of a policy against historical observations without re-running the campaign.

---

## Pattern 2 — Forward-compatible checkpoint loading

**Files:** `src/campaign/adr/operator.py` (`load_checkpoint_full`, `save_extra`, `load_extra`)

### What was built

`CampaignOperator.load_checkpoint_full()` loads a checkpoint JSON and:

1. Checks `schema_version` — raises `ValueError` if the file was written by a newer codebase.
2. Calls `self.load_checkpoint(path)` for the radical.adr base state.
3. Calls `self.load_extra(data)` so subclasses can restore custom fields without touching
   the base implementation.

`save_extra` / `load_extra` default to returning / accepting `{}`, making them
optional for subclasses that need no custom state.

### Why it matters

Adding new fields to the operator state between runs previously caused a `KeyError` on
resume. The `known-fields-only` load pattern silently drops unknown keys, so old
checkpoint files load cleanly into a newer codebase without explicit migration code.

### Future work

- Add schema migration hooks: `migrate_v1_to_v2(data)` so breaking field renames
  (not just additions) can be handled without discarding the checkpoint.
- Expose `schema_version` as a class attribute on `CampaignOperator` so subclasses
  can version their own extra state independently.

---

## Pattern 3 — Retryable-cycle tolerance and abort signalling

**Files:** `src/campaign/adr/supervisor.py` · `src/campaign/adr/operator.py` · `src/campaign/executor.py`

### What was built

Two independent mechanisms:

**ADR-loop abort tolerance** (`supervisor.py`):

```python
from src.campaign.adr import CampaignAbortedError, run_supervised

try:
    await run_supervised(cm, operator, max_failed_ticks=5)
except CampaignAbortedError as exc:
    log.error("Campaign aborted: %s", exc)
```

`run_supervised` wraps the ADR drive loop. It tracks consecutive "all-failed" ticks —
ticks where at least one replica finished and every finishing replica failed. After
`max_failed_ticks` consecutive bad ticks it calls `cm.stop()` and raises
`CampaignAbortedError` (a `RuntimeError` subclass). A single success resets the counter.

`CampaignAbortedError` and `run_supervised` live in `supervisor.py` with no
radical.adr import dependency, so they are testable and importable without the full
ADR stack.

**Flow-level retry with lineage** (`executor.py`, `metrics.py`):

`register_workflow(max_retries=N)` retries failed replicas up to N times. Each retry
is tagged with `retry_of` (original `replica_id`) and `attempt` (integer) in
`record_replica_start` so lineage travels with the campaign JSON.

### Why it matters

Without abort signalling, a misconfigured campaign (wrong Docker image, wrong data path)
runs until the SLURM wall-clock limit — burning node-hours while reporting nothing
useful. `max_failed_ticks` turns a silent burn into a fast, actionable failure.

### Future work

- Add a `on_abort` hook on `CampaignOperator` so subclasses can persist partial results
  or send a notification before the campaign exits.
- Surface the bad-tick counter in `CampaignView.observe()` so an LLM policy can
  react to a rising failure rate before the hard abort fires.

---

## Pattern 4 — Campaign developer guide and registration enforcement

**Files:** `CONTRIBUTING.md` · `campaign_manager.py` · `base_workflow.py`

### What was built

`CONTRIBUTING.md` is a full developer guide covering:

- Hook execution order: `on_replica_failed` → auto-retry → `on_replica_done` → `_on_completion`
- When to use `on_replica_done` vs `_on_completion` (side effects vs DAG routing)
- `ClassVar` FIFO pattern and mandatory `reset_state()` for benchmark isolation
- `workflow_id` uniqueness requirement and the silent-ignore risk on hook name typos
- `_trigger_dependent` / `_trigger_batch` / `_signal_done` API
- `save_extra` / `load_extra` for operator checkpoint custom state
- `register_workflow` parameter reference table

Enforcement baked into `register_workflow`:

- Warns when `workflow_id='base'` (likely forgot to set a unique ID).
- Warns on likely-misspelled `on_replica_*` hook names (e.g. `on_replica_dnoe`)
  by comparing against the known hook set.

### Why it matters

Hook misspellings and missing `reset_state()` calls cause silent, hard-to-diagnose
failures in benchmarks. The warnings surface the mistake at registration time rather
than at the end of a 6-hour SLURM job.

### Future work

- Add missing lifecycle hooks: `on_replica_start`, `on_budget_event`, `on_campaign_stop`.
- Enforce `reset_state()` existence at registration time (currently only documented).
- Add a `cm.validate()` dry-run method that checks the entire workflow graph for
  likely misconfiguration before submitting to SLURM.

---

## Pattern 5 — Gantt timeline from campaign metrics JSON

**Files:** `campaigns/plotting/plot_gantt.py`

### What was built

`plot_gantt.py` — standalone script, no asyncflow dependency:

```bash
python plot_gantt.py campaign.json
python plot_gantt.py bench_results.json --policy flat_rule --run 0
python plot_gantt.py bench_results.json -o gantt.png --title "Benchmark 3"
```

`CampaignMetrics.replica_events` already recorded `(group, t_start, t_end, status)` for
every replica in every campaign. `plot_gantt.py` consumes that field and produces:

- Stage swimlanes (Y) vs wall-clock time from campaign start (X)
- Greedy interval packing: concurrent replicas stacked in rows within each lane
- Failed replicas: hatched bars; still-running replicas: dotted bars
- Handles both direct `metrics.to_dict()` JSON and benchmark results JSON
  (`{policy: [run, ...]}`) with `--policy` / `--run` selectors

### Why it matters

The previous `plot_timeline.py` parsed ANSI log files — fragile and asyncflow-only.
`plot_gantt.py` reads the structured CM JSON that is already written by every campaign
type, making the Gantt chart universal.

### Future work

- Colour-code retried replicas differently from first-attempt replicas using the
  `retry_of` lineage field added in Pattern 3.
- Add an interactive HTML mode (Plotly) so collaborators can zoom in on individual
  replicas without re-running the script.
- Overlay the ADR decision trace (from Pattern 1) as vertical markers so policy
  transitions are visible alongside execution.

---

## Pattern 6 — `asyncio.Semaphore` concurrency ceiling

**Files:** `types.py` · `scheduler.py` · `executor.py` · `campaign_manager.py`

### What was built

A per-group `asyncio.Semaphore` replaces the counter-based `running_count < cap` check
that was previously the only concurrency gate:

- `_WorkflowInfo._semaphore` — created in `register_workflow` when `concurrency_cap > 0`; `None` means unlimited.
- `_WorkflowInfo._queued_count` — tasks created by the scheduler (waiting on semaphore + executing).
- `started_count` — incremented inside `_run_replica` only **after** `semaphore.acquire()`, so `running_count = started_count - finished_replicas` counts only executing replicas.
- The scheduler creates all pending tasks eagerly up to `replicas`; the semaphore gates how many run concurrently.
- Cancellation while waiting on the semaphore decrements `_queued_count` and releases pre-allocated CPU/GPU resources.
- The semaphore is released **before** `_handle_replica_done` in the `finally` block so the next queued task wakes immediately.

Counter invariant: `0 ≤ finished_replicas ≤ started_count ≤ _queued_count ≤ replicas`

### Why it matters

The previous counter approach had a race: `running_count` could be stale between the
scheduler's read and the replica's actual start, allowing bursts above `concurrency_cap`
under high asyncio concurrency. The semaphore is atomic with respect to the event loop.

### Future work

- Expose `_queued_count` through `CampaignView.observe()` as `queued` so ADR policies
  can react to a growing queue depth before it becomes a backpressure event.
- Support dynamic cap changes mid-campaign via a `set_concurrency_cap(group, n)` method
  that resizes the semaphore without stopping the campaign.

---

## Pattern 7 — `ArtifactManifest` checksummed output lineage

**Files:** `artifacts.py` · `metrics.py` · `campaigns/orbit_campaign/orbit_workflow.py`

### What was built

`ArtifactManifest` (`artifacts.py`, exported from `src/campaign`):

```python
@dataclass
class ArtifactManifest:
    artifact_id: str
    version: str
    created_by: str
    parent_ids: list[str]      # lineage: IDs of input manifests
    endpoint_id: str
    path: str
    sha256: str | None         # deferred: None until file checksums are available
    metadata: dict
```

`validate()` raises `ValueError` on missing required fields or a malformed sha256.
`to_dict()` / `from_dict()` are JSON-serialisable; `from_dict` silently drops unknown
keys for forward compatibility.

`CampaignMetrics.record_manifest(manifest)` appends manifests to `manifest_events` in the
campaign JSON, timestamped relative to campaign start.

`OrbitWorkflow` wiring: the search replica's `on_replica_done` creates a manifest
(endpoint + path + score metadata), records it, and passes `candidate_id` downstream
to the refine stage. The refine `on_replica_done` builds a child manifest with
`parent_ids=[search_artifact_id]`, completing the lineage chain.

### Why it matters

Without lineage, it is impossible to trace which search output produced a given refine
result after the campaign finishes — especially relevant for multi-site orbit campaigns
where outputs land on different storage endpoints.

### Future work

- Populate `sha256` once orbit task results carry file checksums natively; `validate()`
  already enforces the format when the field is non-None.
- Add `ArtifactManifest.fetch_content()` that retrieves the file from `endpoint_id`
  using the Globus SDK, enabling post-campaign analysis scripts to pull outputs
  without knowing the storage topology.
- Expose `manifest_events` through a `cm.manifests(group=None)` query API so workflows
  can look up parent manifests at runtime without parsing the raw campaign JSON.
