# CM Advantages Over Independent Execution

Running campaigns independently means every campaign starts from scratch: it cannot benefit from
what earlier campaigns learned, and there is no mechanism to cut losses on a bad run mid-flight.
`campaigns/demo_campaign/run_impact.py` makes the contrast concrete.  It runs 8 sequential
campaigns that share one bandit scheduler, and three measurable improvements emerge from that
sharing — each visible as a separate panel in the figure below.

---

## Experiment setup

Two workflow types compete for a single execution slot (CPU pool = 4 units, `max_concurrent = 1`):

| Type | CPU units | Wall time | Score | What happens |
|------|-----------|-----------|-------|--------------|
| `fast_good` | 1 | ~1.5 s | 0.75 – 0.95, drifts up | always completes |
| `slow_poor` | 4 | up to 5 s | 0.30 – 0.45, drifts **down** | cancelled at ~3 s when score < 0.30 |

`slow_poor` consumes the entire CPU pool, so while it runs, `fast_good` is blocked in the queue
(backpressure).  The bandit must choose between the two types at every scheduler cycle; with one
slot it can only pick one.

Eight campaigns run sequentially, each stopping after 5 `fast_good` completions.  The
`BanditSchedulingPolicy` (UCB1, `exploration=2.0`, binary reward) is instantiated once before
campaign A and reused through campaign H — its arm beliefs accumulate across all 8 campaigns.

---

## The three panels

![AuCaM Impact Demo](figures/aucam_impact.png)

### Panel 1 — Cancellation saves CPU

Each bar is one cancelled `slow_poor` run.  The stacked bar shows how much CPU was actually
consumed (red) vs how much the cancellation recovered (green).  The dashed line marks the full
5 s × 4 CPU = 20 CPU·s cost of an uncancelled run.

At iteration 2 the score falls below 0.30; the `CancelRule` fires and the workflow is killed at
~3 s wall time.  At 4 CPU units that is **~12 CPU·s consumed** (the red bar) vs 20 CPU·s for a
full run (the dashed line) — **8 CPU·s recovered per cancellation**, 48 CPU·s across all 6
cancelled runs.  Without CM every `slow_poor` would always reach the dashed line.

### Panel 2 — Bandit learns across campaigns

The red bars show how many times the bandit chose `slow_poor` per campaign.  The count falls from
2 in campaign A to 0 in campaigns D, F, and H as the arm statistics accumulate:

| Campaign | slow_poor launches | Cumulative bandit pulls (fast / slow) |
|----------|--------------------|--------------------------------------|
| A | 2 | 5 / 2 |
| B | 1 | 10 / 3 |
| C | 1 | 15 / 4 |
| D | **0** | 20 / 4 |
| E | 1 | 25 / 5 |
| F | **0** | 30 / 5 |
| G | 1 | 35 / 6 |
| H | **0** | 40 / 6 |

The alternating exploit/explore pattern from D onward is the UCB1 guarantee — the bad arm is
periodically re-checked to confirm it has not improved.  The UCB score lines on the right axis
show the convergence: `fast_good`'s UCB stays above `slow_poor`'s for increasing stretches.

Without a shared policy each campaign would start at equal UCB scores and go through the same
initial exploration phase every time — wasting 2–3 `slow_poor` slots per campaign indefinitely.

### Panel 3 — Goal time improves as the bandit exploits

Goal time (time to 5 `fast_good` completions) tracks directly with the bandit's decisions in
Panel 2:

- **Explore campaigns** (A, B, C, E, G): `slow_poor` occupies the pool at 4 CPU, blocking
  `fast_good`.  Backpressure adds 2.5–5 s of dead time.  Goal time: 10.5–13.5 s.
- **Exploit campaigns** (D, F, H): no `slow_poor`, no backpressure.  Only 1-CPU `fast_good`
  runs back-to-back.  Goal time: ~7.5 s (5 × 1.5 s wall time + negligible scheduling overhead).

**28% reduction** in goal time on exploit campaigns (7.5 s vs explore average 11.1 s).

---

## Causal chain

The three panels are not independent observations — Panel 3 is a consequence of Panel 2, which is
a consequence of Panel 1:

```
Panel 1                    Panel 2                       Panel 3
Cancellation          →    Bandit learns slow_poor   →   Goal time drops
limits waste from          is never worth picking         as bandit exploits
each bad run               (reward always 0.0)
```

Cancellation is what makes the bandit's learning clean: because every `slow_poor` run is killed
before it can complete, the reward signal is deterministic — always 0.0 for `slow_poor`,
always 1.0 for `fast_good`.  A noisier reward (e.g. `slow_poor` occasionally completing with a
nonzero score) would slow convergence and delay the goal time improvement in Panel 3.

---

## Running it

```bash
cd /work/nvme/bdyk/mgoliyad1/campaign_manager

# Run the experiment (8 campaigns, ~85 s total)
/work/nvme/bdyk/mgoliyad1/ve/miniapps/bin/python3 \
    campaigns/demo_campaign/run_impact.py \
    --json-out /tmp/impact_run.json

# Regenerate the figure from the JSON output
/work/nvme/bdyk/mgoliyad1/ve/miniapps/bin/python3 -I \
    campaigns/demo_campaign/gen_impact_fig.py \
    --json-in /tmp/impact_run.json \
    --out docs/figures/aucam_impact.png
```

Expected terminal output (abridged):

```
────────────────────────────────────────────────────
  Campaign 1/8: impact-a
────────────────────────────────────────────────────
  elapsed=13.5s  fast_done=5  slow_cancelled=2  cpu_saved=16.0  bp=5.0s
  bandit: fast_good→1.000(n=5)  slow_poor→0.000(n=2)

  ...

  Campaign 4/8: impact-d
────────────────────────────────────────────────────
  elapsed=7.5s  fast_done=5  slow_cancelled=0  cpu_saved=0.0  bp=0.0s
  bandit: fast_good→1.000(n=20)  slow_poor→0.000(n=4)
```

---

## Files

| File | Purpose |
|------|---------|
| `campaigns/demo_campaign/run_impact.py` | Self-contained experiment script |
| `campaigns/demo_campaign/gen_impact_fig.py` | Generates `docs/aucam_impact.png` from JSON output |
| `docs/figures/aucam_impact.png` | Static three-panel figure |
