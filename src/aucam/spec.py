"""Campaign and workflow specification classes."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, field_validator

from .types import WorkflowResult

if TYPE_CHECKING:
    pass


@dataclass
class WorkflowDependency:
    """Declares that `target` may not launch until `source` completes and `condition` passes."""

    source: str
    target: str
    condition: Callable[[WorkflowResult], bool] = field(
        default_factory=lambda: lambda r: r.returncode == 0
    )


class WorkflowConfig(BaseModel):
    """Configuration for one workflow type within a campaign.

    Every workflow runs as one AsyncFlow block — CM's unit of launch,
    cancellation and accounting.  A command-line workflow (``launch_cmd``) is a
    block containing one executable task.  A Python workflow (``block``) is the
    block body: it should submit its compute steps as ``@flow`` tasks (declare a
    ``flow`` parameter to receive the engine) so they run on the allocation, are
    cancelled with the workflow, and are charged to it.  Example::

          async def my_wf(reporter: WorkflowReporter, flow=None) -> None:
              if flow is not None:
                  @flow.function_task
                  async def _step_a(): ...
                  @flow.function_task
                  async def _step_b(a_result): ...
                  a = _step_a()
                  await _step_b(a)
              else:
                  ...  # pure-asyncio fallback
    """

    name: str
    block: Callable | None = None
    launch_cmd: list[str] | None = None
    # Expected cost for one full execution: wall_hours (expected run time, used
    # only to prorate) plus any budget dimension (cpu_hours, gpu_hours,
    # llm_tokens, ...).  Counts against the campaign's spending limit while the
    # workflow runs (until learned costs replace it) and prorates actual
    # spending when nothing better is measured.  See docs/budget.md.
    declared_spending: dict[str, float] = {}
    # Spending limit for each run of this type, per dimension (e.g.
    # {"cpu_hours": 2.0}).  A LaunchRequest can override it for one run.  While
    # the run is going, the limit is held against the campaign's spending limit;
    # the workflow receives it (context["spending_limit"] for a Python workflow,
    # CM_SPENDING_LIMIT_<DIM> for a command-line one); and AuCaM cancels the run
    # once its spending so far exceeds the limit.  Empty: no per-run limit.
    spending_limit: dict[str, float] = {}
    # Resources each run of this type may use at once, e.g. {"cpus": 8, "gpus": 1}.
    # A LaunchRequest can override it.  The workflow receives it
    # (context["resources"], or CM_RESOURCES_<KEY> for a command-line program)
    # and must size its tasks to it; AuCaM cancels a run that uses more (measured
    # from its running tasks, or reported as cpus_in_use / gpus_in_use).
    resources: dict[str, float] = {}
    # Ranges, key -> (min, max), for a spending dimension or a resource, used for
    # every launch of this type (a LaunchRequest's own bounds override them per
    # key).  The scheduling policy's size() picks the amount: for spending, the
    # type's learned cost x (1 + size_margin) once 3 workflows of the type have
    # finished, the range's max before that; for a resource, the max.  Use it
    # instead of spending_limit / resources for that key: an exact amount is
    # kept as is.  See docs/budget.md, "Sizing each workflow".
    bounds: dict[str, tuple[float, float]] = {}
    # Graceful cancel (P1): seconds this workflow gets to save its state and
    # exit after AuCaM sends it CANCEL (reporter.poll() returns
    # CampaignDecision.CANCEL; a command-line program reads it with
    # aucam-report signal); after that, or with 0, AuCaM cancels it at once.
    # None: CampaignSpec.cancel_grace_s.  Only give a grace period to
    # workflows that check for CANCEL — others keep running (and holding
    # budget) until it ends.
    cancel_grace_s: float | None = None
    # PAUSE timeout (P1): cancel the workflow if it has not picked up a PAUSE
    # signal (reporter.poll()) within this many seconds.  None: no check.
    pause_timeout_s: float | None = None
    # Set True only for workflows that handle PAUSE/RESUME: Python workflows via
    # reporter.poll() + wait_for_resume(); launch_cmd programs via
    # aucam_client.Reporter.poll_signal() (needs a remote DataBackend).
    supports_pause: bool = False
    launch_on_start: bool = True
    max_wall_time_s: float | None = None
    # Auto-cancel if the workflow's reporter goes silent longer than this.
    # Only checked for workflows that have emitted at least one report.
    # Set to at least 3× the workflow's expected report interval.
    stale_timeout_s: float | None = None
    # launch_cmd only: extra environment variables exported by the inline
    # wrapper script.  Python workflows run inside CM's process and get their inputs as
    # arguments (reporter, flow=, context=), not through the environment.
    env: dict[str, str] = {}
    # launch_cmd only: resource request of the executable task, passed to AsyncFlow
    # as its task_description (backend-specific, e.g. {"ranks": 4,
    # "cores_per_rank": 8, "gpus_per_rank": 1}).  With telemetry enabled the
    # task is charged requested resources × duration.
    task_description: dict[str, Any] = {}
    on_completion: Callable[[WorkflowResult], None] | None = None

    model_config = {"arbitrary_types_allowed": True}

    def model_post_init(self, __context: Any) -> None:
        if self.block is None and self.launch_cmd is None:
            raise ValueError(
                f"WorkflowConfig '{self.name}': one of block or launch_cmd is required"
            )
        if self.block is not None and self.launch_cmd is not None:
            raise ValueError(
                f"WorkflowConfig '{self.name}': block and launch_cmd are mutually exclusive"
            )

    @field_validator("bounds")
    @classmethod
    def _check_bounds(cls, v):
        for key, rng in v.items():
            lo, hi = rng
            if lo < 0 or hi < lo:
                raise ValueError(f"bounds[{key!r}] must be (min, max) with 0 <= min <= max")
        return v


@dataclass
class CampaignBudget:
    """Resource limits for one campaign.

    max_concurrent_workflows     — workflows running at once in this campaign.
    max_concurrency_per_workflow — running instances per workflow type.
    spending — lifetime spending limit per dimension
               (e.g. {"gpu_hours": 100.0} = stop launching after 100 GPU·h).
               Tracked in the BudgetAllocator ledger; checked by the scheduler.

    There is no "resources in use at once" budget: a workflow is a set of
    heterogeneous tasks placed by RHAPSODY, so CM cannot know which GPUs or
    nodes it holds.  Capacity is shared through the allocation itself and the
    workflow-count limits above.
    """

    max_concurrent_workflows: int
    max_concurrency_per_workflow: int
    spending: dict[str, float] = field(default_factory=dict)
    # What happens when actual spending (charged + what running workflows have
    # used so far) goes past the spending limit (B6): "cancel" — cancel the
    # running workflows (gracefully, if their type has a cancel_grace_s),
    # charge what they used and end with spending_limit; "finish" — let them
    # finish (the campaign can end above its limit).
    on_limit: str = "cancel"

    def __post_init__(self) -> None:
        if self.on_limit not in ("cancel", "finish"):
            raise ValueError("on_limit must be 'cancel' or 'finish'")


class CampaignSpec(BaseModel):
    """Full specification of a campaign, passed to ``CampaignManager.run()``."""

    name: str
    budget: CampaignBudget
    controller: Any
    workflows: list[WorkflowConfig]
    dependencies: list[WorkflowDependency] = []
    policy_interval_s: float = 60.0
    max_queue_depth: int = 50
    age_threshold_s: float = 300.0
    shutdown_timeout_s: float = 120.0
    # Default WorkflowConfig.cancel_grace_s for workflow types that set none
    # (P1).  0: cancel at once.
    cancel_grace_s: float = 0.0
    # Longest the controller's decide() may take.  None (default): decide() runs
    # in the event loop with no limit.  A number: decide() runs in a thread, and
    # a cycle where it does not finish in time uses the built-in rules instead
    # (CampaignController.decide()), as does a cycle where decide() raises.
    decide_timeout_s: float | None = None
    # Optional live-telemetry channel; defaults to LocalDictBackend when None.
    data_backend: Any | None = None  # DataBackend
    # Directory to write/read checkpoint files.  None = no checkpointing.
    checkpoint_dir: str | None = None
    # Scheduling policy for this campaign.  None = a new BanditSchedulingPolicy().
    # The instance keeps what it learned; reuse it in the next campaign's spec
    # to carry that knowledge over.
    scheduling_policy: Any | None = None
    # End the campaign after it has been idle this many seconds: queue empty,
    # no workflows running or paused, no requests held by dependencies, and at
    # least one policy cycle run.  "auto" (default) = max(5 s,
    # policy_interval_s), so decide() gets at least one cycle to add work;
    # None = never end an idle campaign (it ends by goals, terminate, the
    # spending limit or a signal).
    idle_exit_s: float | Literal["auto"] | None = "auto"

    model_config = {"arbitrary_types_allowed": True}

    @field_validator("idle_exit_s")
    @classmethod
    def _check_idle_exit_s(cls, v):
        if isinstance(v, (int, float)) and not isinstance(v, bool) and v < 0:
            raise ValueError("idle_exit_s must be >= 0, 'auto' or None")
        return v

    def idle_exit_after(self) -> float | None:
        """Seconds of idleness after which the campaign ends (None = never)."""
        if self.idle_exit_s is None:
            return None
        if self.idle_exit_s == "auto":
            return max(5.0, self.policy_interval_s)
        return float(self.idle_exit_s)

    def model_post_init(self, __context: Any) -> None:
        if self.data_backend is None:
            from .data_backend import LocalDictBackend  # noqa: PLC0415

            object.__setattr__(self, "data_backend", LocalDictBackend())
