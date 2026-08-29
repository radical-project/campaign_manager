# Nested-Loop Campaign

Demonstrates the ADR hierarchical operator pattern with **sequential outer
rounds**, each containing an inner `sim → analysis` loop.  Between rounds the
parent reads the inner loop's best score via SNAPSHOT messaging and tightens
the analysis score gate cutoff, so later rounds draw from a progressively
higher-scoring candidate pool.

This is a demonstration-only campaign — all workflows are dummy sleep-based
stubs.  The focus is the orchestration pattern, not the science.

---

## Architecture

```
OuterOperator  (parent, ADR loop)
│
│  round 1 ──────────────────────────────────────────────────────────
│    launch_inner():
│      set_score_cutoff("analysis", 0.0)           ← open gate round 1
│      reactivate()                                ← clear "all done" flag so CM accepts new replicas
│      trigger("sim", 50)
│      baseline = analysis.finished                ← snapshot before sims
│      inner = InnerOperator(baseline_hits=baseline)
│      inner.start()  ──────────────────────────────────────────────
│                      InnerOperator (child, ADR loop)
│                        sim (50 replicas, cap 8)
│                          └── _trigger_dependent("analysis",
│                                score=score, source_stage="sim")
│                                  └─► score gate   ──────────────────
│                                        analysis (triggered, cap 4)
│                        @goals: hits_this_round >= n_per_round - 0.5
│                        @observe: best_score_p50 ──► state.objectives
│                                                     ──► SNAPSHOT ──►
│    watch(): snapshot.objectives["child:InnerOperator:obj:best_score_p50"]
│             state.artifacts["round_1_best"] = running_max
│
│  round 2  (inner DONE, more rounds remain)
│    launch_inner():
│      cutoff = max(floor, round_1_best × shrink)  ← tighter gate
│      set_score_cutoff("analysis", cutoff)
│      reactivate()                                ← same: clear "all done" before next batch
│      trigger("sim", 50)
│      ...
│
│  round N  (inner DONE, all rounds done)  → Decision(stop=True)
```

---

## Files

| File                  | Contents                                                             |
|-----------------------|----------------------------------------------------------------------|
| `nested_workflow.py`  | `SimWorkflow`, `AnalysisWorkflow` — dummy sleep-based workflows      |
| `nested_operator.py`  | `InnerOperator`, `OuterOperator`, `OuterPolicy`                      |
| `run_campaign.py`     | Single-run entry point; wires CM, sharder, score gate, and ADR operator |

---

## Running

```bash
cd campaigns/nested_campaign
python run_campaign.py                          # defaults
python run_campaign.py --n-rounds 4 --n-sim 30 --tick 0.3
```

All parameters have CLI flags:

| Flag               | Default | Meaning                                    |
|--------------------|---------|--------------------------------------------|
| `--n-rounds`       | 3       | Number of outer rounds                     |
| `--n-per-round`    | 10      | Analysis hits required to end each round   |
| `--n-sim`          | 50      | Sim replicas triggered at the start of each round |
| `--sim-sleep`      | 0.05    | Sim replica sleep (seconds)                |
| `--analysis-sleep` | 0.02    | Analysis replica sleep (seconds)           |
| `--shrink`         | 0.85    | Cutoff shrink factor between rounds        |
| `--floor`          | 0.10    | Minimum allowed score cutoff               |
| `--tick`           | 0.5     | ADR tick interval (seconds)                |

---

## ADR concepts demonstrated

### 1. `child.start()` and SNAPSHOT messaging

`InnerOperator` declares `best_score_p50: float = 0.0` as an annotated class
attribute — ADR automatically includes it in the SNAPSHOT sent to the parent
each tick:

```python
# InnerOperator.extract() — called every ADR tick
p50 = analysis.get("score_p50")
if p50 is not None and p50 > self.best_score_p50:
    self.best_score_p50 = p50          # writes into state.objectives

# OuterOperator.watch() — reads from parent's SNAPSHOT inbox
p50_now = snapshot.objectives.get("child:InnerOperator:obj:best_score_p50") or 0.0
self.state.artifacts[f"round_{current_round}_best"] = max(stored, p50_now)
```

The parent saves the running-max into `state.artifacts` (which survives the
per-cycle observation clear) so `launch_inner()` can read the final round score
even if the SNAPSHOT and the TASK_EVENT DONE arrive in different parent cycles.

### 2. Fresh child instance per round

`InnerOperator` is re-constructed inside `launch_inner()` rather than reused:

```python
inner = InnerOperator(
    self.view, engine=self._engine,
    n_per_round=self.n_per_round,
    baseline_hits=baseline,
)
object.__setattr__(inner, "parent", self)   # wire SNAPSHOT pipeline
inner.policy = RuleCorrectionsPolicy(DownstreamFirstPolicy(inner), inner)
fut = inner.start()
```

A fresh instance guarantees `state.objectives` (and therefore `best_score_p50`
in the SNAPSHOT) starts at 0.0 each round rather than carrying stale state
from the previous round.

The CM's `analysis.finished` counter is global — it accumulates across all
rounds and never resets.  `baseline_hits` captures its value at the start of
each round so the inner's stopping metric counts only **new** analyses this
round:

```python
hits_this_round = max(0, total_done - self._baseline_hits)
```

### 3. `set_score_cutoff()` as inter-round information passing

After each round the parent computes a tighter cutoff from the round's best
score and applies it before triggering the next round's sims:

```python
cutoff = round(max(self.cutoff_floor, prev_best * self.shrink_factor), 3)
self.view.set_score_cutoff("analysis", cutoff)
```

Because `SimWorkflow._trigger_dependent()` passes `score=` to the CM's score
gate, sims whose score falls below the cutoff never reach the analysis group.
Later rounds therefore work on a higher-quality subset of candidates.

### 4. `view.reactivate()`

Once all triggered replicas drain between rounds the CM sets its `_all_done`
event.  `reactivate()` clears it before the next `trigger()` call so the
scheduler is not blocked by the stale flag:

```python
await self.view.reactivate()   # clears "all done" flag; safe to call even if CM is still running (flag already clear)
await self.view.trigger("sim", self.n_sim)
```

---

## Possible adjustments

### Convergence-based outer stopping

Replace the fixed `n_rounds` check with a score-improvement threshold:

```python
improvement = round_N_best - round_{N-1}_best
if improvement < min_improvement:
    return Decision(stop=True)
```

Useful when you want the campaign to self-terminate once additional rounds
stop yielding gains, rather than always running exactly N rounds.

### Adaptive inner stopping

Replace the fixed `n_per_round` hit count with a score-stability check inside
`InnerOperator.extract()`.  Track a rolling window of `score_p50` values and
stop when the standard deviation falls below a threshold.

```python
self._p50_window.append(p50)
if len(self._p50_window) >= window and np.std(self._p50_window[-window:]) < stability_thr:
    obs["inner_done"] = self.n_per_round  # trigger @goals threshold
```

### Parallel rounds

Run all N inner loops concurrently using `asyncio.gather` on the futures from
`inner.start()`.  The outer operator would wait for all futures before
computing per-round scores.  This improves throughput but loses the
sequential information-passing benefit: round 2's score gate cutoff can't be set
using round 1's best score because round 1 hasn't finished yet.

```python
futs = [self._launch_inner_round(r) for r in range(self.n_rounds)]
await asyncio.gather(*futs)
```

### Bandit warm-start between rounds

Pass the previous round's bandit arm weights to the next `InnerOperator`
instead of (or in addition to) tightening the score gate cutoff.  The inner
policy would start with informed priors on which sim configurations are
productive rather than the uniform Beta(1,1) prior.

```python
prior_weights = prev_inner.policy.arm_weights()
inner.policy = BanditSchedulingPolicy(warm_start=prior_weights)
```

### Per-round sim count that tracks the cutoff

As the cutoff tightens, fewer sims pass the score gate.  To keep the number of
analysis hits per round stable, increase `n_sim` proportionally to the
expected pass rate:

```python
pass_rate = 1.0 - cutoff          # approximate for uniform score distribution
n_sim_this_round = max(n_sim, int(n_per_round / pass_rate) + margin)
```

### Adaptive cutoff shrink factor

Replace the fixed `shrink_factor=0.85` with one that depends on the spread of
scores seen in the previous round.  A round with a tight score distribution
(low `score_p90 - score_p50`) should shrink more aggressively than a round
with a wide spread.

```python
spread = obs.get("score_p90", 1.0) - obs.get("score_p50", 0.5)
shrink = max(0.5, 1.0 - spread)   # tight spread → aggressive shrink
cutoff = round(max(self.cutoff_floor, prev_best * shrink), 3)
```

### Score-bleed mitigation

After `InnerOperator` reaches `n_per_round` hits it stops, but sims triggered
in that round may still be running and will keep pushing new analyses into the
CM.  These "leaked" analyses are counted against the next round's baseline.
The baseline snapshot in `launch_inner()` already handles this correctly for
counting, but the leaked analyses do dilute the next round's score gate filtering.
Mitigation: call `cm.drain("sim")` before `launch_inner()` to wait for all
in-flight sims to complete before triggering the next round.

```python
await self.view.drain("sim")   # wait for all in-flight sims before next round
await self.view.reactivate()
await self.view.trigger("sim", self.n_sim)
```

### Checkpoint / resume

`OuterOperator` inherits `save_checkpoint_full` / `load_checkpoint_full` from
`CampaignOperator`.  Wiring checkpoint saves (e.g. after each round completes)
and restoring from a checkpoint path via `--resume` would make long runs
recoverable after SLURM preemption — the same pattern used in
`run_campaign_phased.py`.

```python
# save after each round completes
await outer.save_checkpoint_full(f"checkpoint_round_{current_round}.pkl")

# restore on resume
await outer.load_checkpoint_full(args.resume)
```

### Benchmark harness

Wrap `run_campaign.py` in a loop that resets `SimWorkflow.reset_state()` and
`AnalysisWorkflow.reset_state()` between runs, sweeps over `n_rounds` or
`shrink_factor`, and records wall-clock time per configuration — the same
structure as `ddsim_campaign/benchmark.py`.

```python
for shrink in [0.70, 0.80, 0.85, 0.90]:
    SimWorkflow.reset_state()
    AnalysisWorkflow.reset_state()
    t0 = time.monotonic()
    await main(shrink_factor=shrink)
    results.append({"shrink": shrink, "wall_s": time.monotonic() - t0})
```
