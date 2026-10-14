## AuCaM design

The design spec is `docs/design/aucam-design.md` (architecture diagram: `docs/design/aucam-architecture.png`; a Mermaid version is inline in the spec). Read it before working on CM code.

- Treat the spec as the source of truth. If code or a request disagrees with it, point out the conflict instead of silently diverging.
- Use its terminology: CampaignManager (CM), Driver interface, BudgetAllocator, DataBackend (`rhapsody.backends.data`), CampaignController (observe → decide → act, goals), PolicyResult, CampaignBudget, WorkflowQueue, Scheduler, SchedulingPolicy, WorkflowReporter, Tier 1 / Tier 2 workflows.
- Two levels: service level (one CM for all campaigns: driver interface, shared budget, DataBackend, telemetry) and campaign level (one CampaignController, queue and scheduler per campaign).
- The BudgetAllocator only keeps the books; only the CampaignController cancels workflows and releases budget. A campaign's budget is a firm ceiling.
- Lifecycle signals are only CANCEL, PAUSE and RESUME. All workflows run through AsyncFlow (blocks or executable tasks); RHAPSODY is the runtime underneath AsyncFlow.
- Do not name a specific data-backend implementation (e.g. Redis); use DataBackend / `rhapsody.backends.data`.
