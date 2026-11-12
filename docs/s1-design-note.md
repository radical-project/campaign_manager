# S1 — AI-driven scheduling: design note (draft for review)

Status: proposal. Nothing here is implemented; decisions marked **Decide** are open.

## 1. Where we are

`ADRSchedulingPolicy` and `LLMSchedulingPolicy` (`src/aucam/scheduler.py`) are
stubs: they log a warning once and behave as FIFO. `pyproject.toml` already has
an `adr` extra for radical.adr (not on PyPI; installed editable).

AuCaM already has two places where decisions are made, and AI can plug into
either:

| Level | Component | Decides | Called |
|---|---|---|---|
| What / when | `CampaignController.decide(obs)` | which workflows to launch, cancel, pause; goals; when to stop | once per policy cycle (seconds to minutes) |
| Which next / how much | `SchedulingPolicy.select(candidates, ctx)` and `size(request, ctx)` | which queued request starts when a slot frees; its limit and resources | every time the queue drains (often, many times per cycle) |

## 2. The main constraint: `select()` and `size()` are synchronous and on the hot path

The scheduler calls `select()` and `size()` inside `_drain_queue()`, whenever a
slot frees up. They are plain functions, not coroutines. An LLM or ADR call
takes seconds and is asynchronous. Calling it there would either block the
event loop (stopping reports, enforcement and every other workflow) or need
`select()` to become async, which changes the interface for every policy.

**Proposal: decide ahead, apply fast.** An AI policy computes its decision once
per policy cycle, in a new optional async hook, and `select()` / `size()` only
read the latest result:

```python
class SchedulingPolicy:
    async def refresh(self, ctx: SchedulingContext, queued: list[LaunchRequest]) -> None:
        """Optional: called once per policy cycle (awaited, with a timeout)."""

class LLMSchedulingPolicy(SchedulingPolicy):
    async def refresh(self, ctx, queued):
        self._ranking = await self._ask_model(ctx, queued)  # e.g. ["precise_sim", "fast_sim"]
        self._ranking_at = time.monotonic()

    def select(self, candidates, ctx):
        if self._ranking is fresh:
            return candidates ordered by self._ranking, within ctx.cap() and free slots
        return self._fallback.select(candidates, ctx)  # e.g. the bandit
```

The campaign awaits `refresh()` in the policy loop with a timeout, the same
way as `decide_timeout_s` for the controller (C1). The built-in policies do
nothing in `refresh()`, so nothing changes for them.

**Decide 1:** OK with an optional async `refresh()` hook (no change to
`select()`), or make `select()` async?

## 3. What each AI policy decides

- **LLM scheduling policy:** a ranking of workflow types (which to start
  first), and optionally a factor per type for sizing, used through the U2
  `value()` hook (more budget for types it judges more promising). AuCaM still
  enforces everything else: free slots, per-type caps (W1), what fits the
  budget (B2), ranges and limits. A wrong or malformed answer can make a poor
  choice but cannot break a limit.
- **ADR:** radical.adr's `Policy.decide(obs) -> Decision` is a whole decision
  step — spawn and cancel tasks, update parameters, goals, stop. That matches
  the **controller** better than the scheduling policy:

  | ADR `Decision` | AuCaM `PolicyResult` |
  |---|---|
  | `SPAWN_TASK` / `SPAWN_BLOCK` (task_name, task_kwargs) | `launch_requests` (workflow type, context) |
  | `CANCEL_TASK` (target_uid) | `decisions[wf_id] = CANCEL` |
  | `goals`, `remove_goals` | `goals`, `remove_goals` |
  | `stop` | `terminate` |
  | `directives` | e.g. `{"rank": [...]}` passed to the scheduling policy |

  So the natural bridge is an `ADRController(policy)` — a `CampaignController`
  whose async decide step builds the observation, awaits the ADR policy, and
  translates its `Decision`. `ADRSchedulingPolicy` would then only apply a
  ranking from `directives`, or be removed.

**Decide 2:** ADR as a controller (`ADRController`), as a scheduling policy, or
both?

This needs the controller's `decide()` to be able to await, too. With C1 it
already runs in a thread when `decide_timeout_s` is set; an async variant
(`async def decide_async(obs)`, awaited with the same timeout) would avoid the
thread for async code like ADR and LLM clients.

## 4. What the model sees

Built from the same data the built-in policies use, kept small:

- queued requests by type and priority; running counts and caps;
- per type (`by_type`): completed / cancelled / failed, cost per completed
  workflow, mean score; learned costs;
- the budget: limits, spent, in flight, remaining, rate, hours to limit;
- the campaign goal, if any.

A `render(ctx, queued) -> str` method that campaigns override (LLMPolicy in
radical.adr already has `render_observation()`). Raw reports and telemetry are
not sent by default (size, and they may contain data the campaign does not
want to send to an external endpoint).

## 5. Failures, time and cost

- **Timeout / error / unparsable answer:** use the fallback policy for that
  cycle (default: `BanditSchedulingPolicy`), log `select=llm` or
  `select=fallback(error|timeout)` like the controller's `decide=…`, and count
  them in the outcome.
- **Stale answer:** if the last ranking is older than N cycles, use the
  fallback.
- **Cost of the calls themselves:** each call spends tokens (and time).
  Proposal: record them in the campaign's ledger as `llm_tokens` (B0 already
  charges any dimension), so a campaign can limit them like any other budget.

**Decide 3:** charge the policy's own LLM tokens to the campaign budget?

## 6. Proposed scope

1. `refresh()` hook and its timeout/fallback in the campaign; tests with a fake
   async policy.
2. `LLMSchedulingPolicy`: OpenAI-compatible client (optional dependency),
   default prompt and `render()`, ranking + optional value factors, fallback.
   Tests with a stubbed client; one demo run against a local model if
   available.
3. `ADRController` bridge (if Decide 2 = controller): observation → ADR
   `Policy.decide()` → `PolicyResult`; example with a small rule-based ADR
   policy, so it runs without an LLM.
4. Docs: developer guide §5/§8, known-gaps.

**Decide 4:** which endpoint/model to test against (local Ollama, or a hosted
API), and whether a demo campaign should use it.
