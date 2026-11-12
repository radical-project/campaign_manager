# Learning how much each workflow gets — design note (draft for review)

Status: implemented (commits 68d4c3d, ca4a7bc, 21ba201, 9ae837c). One change from §4.1: an amount that does not fit the remaining budget is capped only within a range; otherwise the request waits while others run and is dropped once nothing runs.

## 1. Goal

AuCaM gives every workflow an allocation when it starts: a spending limit and
the resources (CPUs, GPUs) it may use at once. The aim is to learn the
allocation that gets the campaign to its **goal in the least time**, without
exceeding its budget. The budget is a constraint, not something to minimize.

How much a workflow gets affects time to goal in three ways:

- its speed — more CPUs / GPUs can make it finish sooner;
- what it can achieve — a larger spending limit lets it run longer (more
  steps), which can make a useful result more likely;
- parallelism — what one workflow gets is not available to others.

## 2. Today

A workflow's spending limit is, first match: the controller's
`LaunchRequest.spending_limit`; the type's fixed `WorkflowConfig.spending_limit`;
a range (`bounds`) sized by the scheduler's `size()` from learned cost + 25 %;
otherwise **no limit**. Resources follow the same order. The amounts are capped
at what is left of the budget. So the scheduler decides only when a range is
given, a type with a fixed amount never learns, and nothing learns what helps
the goal.

AuCaM limits how many workflows run at once by count
(`max_concurrent_workflows`), not by CPUs / GPUs, so it cannot trade "fewer,
larger workflows" against "more, smaller ones".

## 3. Agreed

1. Objective: reach the goal in the least time, within the budget.
2. The **scheduler** decides each workflow's allocation (`size()`, at launch);
   the **controller** provides the goal signal and can override the amount for
   one workflow.
3. Default rule: learned cost + 25 % (`size_margin`); a campaign can replace
   it with its own learner, AI or not.
4. Starting values per workflow type in the spec (`WorkflowConfig`): an
   explicit starting limit is used as given; otherwise `declared_spending` ×
   (1 + `size_margin`).
5. The campaign's total capacity is part of `CampaignBudget` and required in
   every campaign spec.
6. Progress of intermediate workflows comes from credit passed back along the
   chain of workflows that led to a result (4.3).

## 4. Design

### 4.1 Where an allocation comes from (first match)

1. The controller's request for this workflow (`LaunchRequest.spending_limit`
   / `resources` / `bounds`) — always wins.
2. The scheduler's `size()`:
   - default rule: the type's starting value until it has `min_cost_samples`
     finished workflows, then learned cost × (1 + `size_margin`) for
     spending and learned peak use × (1 + `size_margin`) for resources;
   - starting value: the type's `WorkflowConfig.spending_limit` /
     `resources`, used as given; without one, `declared_spending` ×
     (1 + `size_margin`) — the margin covers ordinary variation in cost, so
     most early workflows finish and learning can start (`size_margin=0` for
     a campaign that trusts its estimates);
   - a campaign's own learner: overrides `size()` and uses the history and
     progress below.
3. Always within the request's or type's range, if any, and capped at what is
   left of the budget and of the capacity.

`WorkflowConfig.spending_limit` / `resources` change meaning: from a fixed
amount to the **starting value**. A type that also wants a fixed amount sets a
range with min = max.

**Raising a limit that is too low.** Learned costs come only from successful
workflows, so if the first workflows of a type are cancelled at their limit,
learning never starts. Proposed default: after a workflow is cancelled at its
own limit, the next one of its type gets that limit × (1 + `size_margin`), i.e. 25 % more by default (within its range
and the budget), until one succeeds and learned costs take over.

Agreed: raise by `size_margin` (25 %), like the starting margin.

### 4.2 Progress toward the goal

A workflow's progress is its **direct progress** plus the **credit** passed
back to it from the work it led to.

**Direct progress** — a new controller hook:

```python
def progress(self, result: WorkflowResult) -> float:
    """How much this finished workflow moved the campaign toward its goal."""
```

Default, from the existing goal settings: 1.0 for a successful workflow of
`goal_type`, 0 otherwise. A science controller returns something richer, e.g.
the improvement of the best score, or partial credit for an intermediate step
("a sim that passes the quality threshold counts 0.3"). An exception or None
counts as 0.

**Credit along the chain** — most workflows are intermediate steps: a sim
produces nothing that counts toward the goal; the analysis it triggers does.
When a workflow makes progress, every workflow that led to it gets the same
amount as credit (full credit: each step was needed; the learner compares
workflow types, not steps of one chain).

- The chain: a request records the workflow that caused it in
  `context["source"]`. Dependency launches already do; requests held by a
  dependency get the workflow whose completion released them; a controller
  sets it for its own launches (e.g. a "refine" workflow launched because a
  sim looked promising).
- Credit arrives late: an intermediate workflow's credit is **pending** until
  what it led to has finished; it is 0 if that fails or is cancelled, or if
  nothing comes of it by the end of the campaign. A learner treats pending as
  unknown, not as 0.
- A controller's `progress()` adds to the credit; it does not replace it.

### 4.3 The allocation history (scheduler input)

For each finished workflow AuCaM records: type, the allocation it got
(spending limit, resources), run time, what it spent, how it ended
(completed / cancelled at its limit / cancelled / failed), its source, its
direct progress, and its credit (updated as it arrives; pending until known).

- `SizingContext.history` — what `size()` learns from;
- `obs["allocations"]` — the same for the controller (so an AI controller can
  choose amounts itself);
- saved in the checkpoint, like learned costs.

Workflows cancelled at their own limit matter: they show the limit was too
low (their true cost is unknown, only that it is higher). A learner should
treat them that way, not drop them.

### 4.4 Capacity

`CampaignBudget(capacity={"cpus": 512, "gpus": 16})`: the total resources of
the allocation the campaign runs on, required in every campaign. A workflow
starts only if its resources fit what is free (capacity minus what running
workflows were given), as well as the count limits and the spending check.

Required means every existing campaign and test sets it (the examples: the
CPUs of the machine or allocation they run on). A workflow type without
resources counts as using none of the capacity.

### 4.5 What a learner optimizes

Progress per hour is the natural measure: for each allocation tried, the
progress it produced divided by its run time — or, with capacity, per
"resource-hour" of capacity used, so that a workflow using half the machine
counts twice as much as one using a quarter. The default rule does not use it;
it is what a campaign's learner (or a later, better default) maximizes.

## 5. Compatibility

- Campaigns that set exact `spending_limit` / `resources` today: the first
  workflows of the type get the same amount; later ones get learned amounts.
  To keep a fixed amount, use a range with min = max.
- Every campaign must now set `CampaignBudget.capacity` (a breaking change
  for existing specs).
- The per-workflow limit is then set for every workflow that has a starting
  value or a learned cost, so the campaign stays within its limit through
  step 2 of budget.md §4; the campaign-limit cancel (step 3) remains as the
  safety net for workflows whose spending AuCaM cannot measure.

## 6. Steps

Each step is its own commit, with tests and docs:

1. Starting values, the default rule for every launch and the
   raise-after-cancel rule (4.1); budget.md §4 and its figure.
2. Progress and credit (4.2) and the allocation history (4.3), in
   `SizingContext`, the observation and the checkpoint.
3. Capacity (4.4): required in `CampaignBudget`, launch check, sizing capped
   at free capacity; all specs and examples updated.
4. An example learner in one example campaign (e.g. the demo): tries larger
   and smaller allocations and keeps what gives the most progress per hour.

Agreed: all four steps now.
