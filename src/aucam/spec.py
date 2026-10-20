"""Campaign and workflow specification classes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

from pydantic import BaseModel

from .types import WorkflowResult

if TYPE_CHECKING:
    from .data_backend import DataBackend


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
    # Expected cost for one full execution.  Keys: wall_hours, cpu_hours, gpu_hours.
    # Counts against the campaign's spending limit while the workflow runs
    # (in-flight estimate) and prorates actual spending when nothing better
    # is reported.
    declared_spending: dict[str, float] = {}
    # Set True only for workflows that handle PAUSE/RESUME: Python workflows via
    # reporter.poll() + wait_for_resume(); launch_cmd programs via
    # SubprocessReporter.poll_signal() (needs a remote DataBackend).
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


class CampaignSpec(BaseModel):
    """Full specification for one campaign, submitted by a driver."""

    name: str
    budget: CampaignBudget
    controller: Any
    workflows: list[WorkflowConfig]
    dependencies: list[WorkflowDependency] = []
    policy_interval_s: float = 60.0
    max_queue_depth: int = 50
    age_threshold_s: float = 300.0
    shutdown_timeout_s: float = 120.0
    # Optional live-telemetry channel; defaults to LocalDictBackend when None.
    data_backend: Any | None = None  # DataBackend
    # Directory to write/read checkpoint files.  None = no checkpointing.
    checkpoint_dir: str | None = None
    # Deprecated, ignored: every workflow now runs as an AsyncFlow block.
    # Kept so existing specs and configs that set it still validate.
    use_asyncflow_blocks: bool = True
    # Scheduling policy for this campaign.  None = BanditSchedulingPolicy().
    # The instance belongs to this campaign only: CampaignManager rejects a
    # spec whose policy instance is already used by another campaign.
    scheduling_policy: Any | None = None

    model_config = {"arbitrary_types_allowed": True}

    def model_post_init(self, __context: Any) -> None:
        if self.data_backend is None:
            from .data_backend import LocalDictBackend  # noqa: PLC0415
            object.__setattr__(self, "data_backend", LocalDictBackend())
