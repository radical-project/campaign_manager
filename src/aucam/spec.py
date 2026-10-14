"""Campaign and workflow specification classes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

from pydantic import BaseModel

from .types import ScoreDirection, WorkflowResult

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

    Tier 1 asyncflow contract (required when CampaignSpec.use_asyncflow_blocks=True):
      block must accept ``flow=None``.  When flow is provided, the function IS
      the asyncflow.block — it should register its work as inner ``@flow.task``
      nodes and await the terminal task, so CM can track members, cascade
      cancellation, and emit per-task telemetry.  Example::

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
    # BudgetAllocator concurrency resources are derived from this + max_wall_time_s.
    declared_spending: dict[str, float] = {}
    # Tier 1 — typed metrics class for deserialization; None = Tier 2.
    metrics_cls: type | None = None
    # Score semantics surfaced to observe() and any scoring controllers.
    score_directions: dict[str, ScoreDirection] = {}
    # Set True only for workflows that implement reporter.poll() + wait_for_resume().
    supports_pause: bool = False
    launch_on_start: bool = True
    max_wall_time_s: float | None = None
    # Tier 1: auto-cancel if reporter goes silent longer than this.
    # Only checked for workflows that have emitted at least one report.
    # Set to at least 3× the workflow's expected report interval.
    stale_timeout_s: float | None = None
    env: dict[str, str] = {}
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

    budget   — concurrency ceiling per resource dimension
               (e.g. {"gpu": 4.0} = no more than 4 GPUs in use at once).
    spending — lifetime spending limit per dimension
               (e.g. {"gpu_hours": 100.0} = stop after 100 GPU·h consumed).
               Computed from Rhapsody block telemetry; checked by the controller.
    """

    max_concurrent_workflows: int
    max_concurrency_per_workflow: int
    budget: dict[str, float] = field(default_factory=dict)
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
    # When True, Tier 1 blocks are submitted through the shared asyncflow
    # WorkflowEngine (as @flow.block) instead of asyncio.create_task.
    # Enables per-wf telemetry spans and cascading member-task cancellation.
    use_asyncflow_blocks: bool = False
    # Scheduling policy for this campaign.  None = BanditSchedulingPolicy().
    # To share learned beliefs across campaigns, pass the same policy instance
    # to multiple CampaignSpec objects.
    scheduling_policy: Any | None = None

    model_config = {"arbitrary_types_allowed": True}

    def model_post_init(self, __context: Any) -> None:
        if self.data_backend is None:
            from .data_backend import LocalDictBackend  # noqa: PLC0415
            object.__setattr__(self, "data_backend", LocalDictBackend())
