"""Structured campaign plan schema.

Designed as the read-only contract between an upstream Planner (which solves
the global Pareto optimisation) and the downstream CM (which executes within
plan-allowed bands).  The CM may *nudge* a small set of fields within
explicit bounds (surrogate cutoffs, currently); everything else is owned by
the Planner.

Each dataclass validates its own invariants in ``__post_init__``.  When the
project takes on Pydantic, these classes port to BaseModel + field validators
without changing public APIs — the validation logic is the same.

Schema overview
---------------
CampaignPlan
├── resources: dict           # {total_cpus, total_gpus, total_memory_gb}
├── stages:    list[StageSpec]
│   ├── pilot:        PilotSpec        # partition, nodes, walltime_h
│   ├── surrogate:    SurrogateSpec    # cutoffs + nudge bounds (CM-adjustable)
│   ├── retry_policy: RetryPolicy
│   └── (budget_node_hours, threshold_top_fraction, concurrency_floor/cap, ...)
├── edges:     list[EdgeSpec]          # profile + backpressure per edge
├── replan:    ReplanThresholds        # Monitor escalation thresholds
└── features:  dict[str, bool]         # sharder/backpressure/monitor toggles
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# ── Leaf specs ────────────────────────────────────────────────────────────────

@dataclass
class PilotSpec:
    """HPC pilot reservation for a stage."""
    partition:   str   = "cpu"          # cpu | gpu | mpi+gpu | largemem
    nodes:       int   = 1
    walltime_h:  float = 1.0

    def __post_init__(self) -> None:
        if self.nodes < 1:
            raise ValueError(f"PilotSpec.nodes must be ≥ 1, got {self.nodes}")
        if self.walltime_h <= 0:
            raise ValueError(f"PilotSpec.walltime_h must be > 0, got {self.walltime_h}")
        valid_partitions = {"cpu", "gpu", "mpi+gpu", "largemem"}
        if self.partition not in valid_partitions:
            raise ValueError(
                f"PilotSpec.partition={self.partition!r} not in {sorted(valid_partitions)}"
            )


@dataclass
class SurrogateSpec:
    """Surrogate model spec with nudgeable cutoffs.

    The Planner sets initial cutoff values AND the bounds the CM may nudge
    within.  At runtime, ``BudgetController.nudge_cutoffs`` adjusts the
    cutoffs to keep burn rate within the configured band.  Bounds are
    inclusive on both ends.

    Interpretation:
      score_cutoff:        reject candidates whose upstream score < this
      uncertainty_cutoff:  reject candidates whose surrogate σ > this
    """
    model_uri:                       Optional[str] = None
    score_cutoff:                    float = 0.0    # accept all by default
    score_cutoff_nudge_bounds:       tuple[float, float] = (0.0, 1.0)
    uncertainty_cutoff:              float = 1.0    # accept all by default
    uncertainty_cutoff_nudge_bounds: tuple[float, float] = (0.0, 1.0)
    # ADVANCE threshold — when a candidate's surrogate prediction is at
    # least this high AND its uncertainty is below uncertainty_cutoff, the
    # Triage returns ADVANCE.  The CM marks the candidate so the workflow
    # can skip the expensive computation (dreamer skips its sleep; real
    # workflows can short-circuit their pipeline).  Default ``inf`` disables
    # ADVANCE so legacy plans never fire it accidentally.
    advance_threshold:               float = float("inf")

    def __post_init__(self) -> None:
        sc_lo, sc_hi = self.score_cutoff_nudge_bounds
        if sc_lo > sc_hi:
            raise ValueError(
                f"score_cutoff_nudge_bounds must be (low, high); "
                f"got ({sc_lo}, {sc_hi})"
            )
        if not (sc_lo <= self.score_cutoff <= sc_hi):
            raise ValueError(
                f"score_cutoff={self.score_cutoff} outside bounds [{sc_lo}, {sc_hi}]"
            )
        un_lo, un_hi = self.uncertainty_cutoff_nudge_bounds
        if un_lo > un_hi:
            raise ValueError(
                f"uncertainty_cutoff_nudge_bounds must be (low, high); "
                f"got ({un_lo}, {un_hi})"
            )
        if not (un_lo <= self.uncertainty_cutoff <= un_hi):
            raise ValueError(
                f"uncertainty_cutoff={self.uncertainty_cutoff} outside bounds "
                f"[{un_lo}, {un_hi}]"
            )


@dataclass
class RetryPolicy:
    """Per-stage retry policy for failed replicas."""
    max_attempts:       int       = 1
    backoff_s:          float     = 0.0
    soft_failure_codes: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError(f"RetryPolicy.max_attempts must be ≥ 1, got {self.max_attempts}")
        if self.backoff_s < 0:
            raise ValueError(f"RetryPolicy.backoff_s must be ≥ 0, got {self.backoff_s}")


@dataclass
class BackpressureEdge:
    """Hysteresis watermarks for an inter-stage queue."""
    high_water: int
    low_water:  int

    def __post_init__(self) -> None:
        if self.high_water <= 0:
            raise ValueError(f"BackpressureEdge.high_water must be > 0, got {self.high_water}")
        if self.low_water < 0:
            raise ValueError(f"BackpressureEdge.low_water must be ≥ 0, got {self.low_water}")
        if self.low_water >= self.high_water:
            raise ValueError(
                f"BackpressureEdge.low_water ({self.low_water}) must be < "
                f"high_water ({self.high_water})"
            )


# ── Stage + edge ──────────────────────────────────────────────────────────────

@dataclass
class StageSpec:
    """Per-stage plan: resources, budget, surrogate, retry, scheduling bounds."""
    id:                       str
    variant:                  str   = "default"

    # Quality gate (upstream-score percentile gate at trigger time)
    threshold_top_fraction:   float = 1.0

    # Budget contract
    budget_node_hours:        float = 0.0
    burn_rate_band:           float = 0.15     # ±band tolerance before nudging
    downstream_input_target:  int   = 0        # BudgetController denominator (T = planned total)
    campaign_target:          int   = 0        # early-stop trigger: stop when finished >= N (0 = off)
    budget_kp:                float = 0.05     # BudgetController proportional gain
    budget_warmup_min:        int   = 3        # minimum finished replicas before controller acts

    # Nested specs
    pilot:                    PilotSpec   = field(default_factory=PilotSpec)
    surrogate:                Optional[SurrogateSpec] = None
    retry_policy:             RetryPolicy = field(default_factory=RetryPolicy)

    # Scheduling bounds (mirrors _WorkflowInfo at runtime)
    concurrency_floor:        int   = 0
    concurrency_cap:          int   = 0
    priority:                 int   = 0

    # Resource overlays — if 0, derive from pilot.partition mapping at load time
    required_cpus:            int   = 0
    required_gpus:            int   = 0
    required_memory_gb:       float = 0.0

    # Runtime population
    replicas:                 int   = 0       # 0 = dependent (filled by upstream triggers)
    dependencies:             list[str] = field(default_factory=list)
    dependency_threshold:     int   = 1

    # Sharder spec (raw dict — validated downstream by ShardingSpec.from_dict).
    # Kept loose so the plan schema doesn't have to mirror every Sharder knob.
    sharding:                 Optional[dict] = None

    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("StageSpec.id is required")
        if not (0.0 < self.threshold_top_fraction <= 1.0):
            raise ValueError(
                f"StageSpec.threshold_top_fraction must be in (0, 1], "
                f"got {self.threshold_top_fraction}"
            )
        if self.budget_node_hours < 0:
            raise ValueError(
                f"StageSpec.budget_node_hours must be ≥ 0, got {self.budget_node_hours}"
            )
        if not (0.0 <= self.burn_rate_band <= 1.0):
            raise ValueError(
                f"StageSpec.burn_rate_band must be in [0, 1], got {self.burn_rate_band}"
            )
        if self.downstream_input_target < 0:
            raise ValueError(
                f"StageSpec.downstream_input_target must be ≥ 0, got {self.downstream_input_target}"
            )
        if self.concurrency_floor < 0 or self.concurrency_cap < 0:
            raise ValueError("concurrency_floor / concurrency_cap must be ≥ 0")
        if self.concurrency_cap > 0 and self.concurrency_floor > self.concurrency_cap:
            raise ValueError(
                f"concurrency_floor ({self.concurrency_floor}) > "
                f"concurrency_cap ({self.concurrency_cap})"
            )
        if self.dependency_threshold < 1:
            raise ValueError(
                f"dependency_threshold must be ≥ 1, got {self.dependency_threshold}"
            )


@dataclass
class EdgeSpec:
    """Inter-stage edge: priority profile and (optional) backpressure."""
    upstream:     str
    downstream:   str
    profile:      str = "diverse_top"
    backpressure: Optional[BackpressureEdge] = None

    def __post_init__(self) -> None:
        if not self.upstream or not self.downstream:
            raise ValueError(
                f"EdgeSpec endpoints required; got upstream={self.upstream!r} "
                f"downstream={self.downstream!r}"
            )
        valid_profiles = {
            "pure_promise", "active_learning", "explore_exploit",
            "diverse_top", "round_robin",
        }
        if self.profile not in valid_profiles:
            raise ValueError(
                f"EdgeSpec.profile={self.profile!r} not in {sorted(valid_profiles)}"
            )


# ── Replan / Monitor thresholds ──────────────────────────────────────────────

@dataclass
class ReplanThresholds:
    """Thresholds that escalate to a Planner replan request.

    on_drift controls what happens when escalation fires:
      log_only          — just record the event (current default)
      drain_and_replan  — trigger DRIFT → DRAIN → RESUME handshake
    """
    budget_burn_deviation_pct:  float = 20.0
    pass_through_deviation_pct: float = 25.0
    surrogate_recall_floor:     float = 0.90
    breaches_to_escalate:       int   = 2
    on_drift:                   str   = "log_only"

    def __post_init__(self) -> None:
        valid = {"log_only", "drain_and_replan"}
        if self.on_drift not in valid:
            raise ValueError(
                f"ReplanThresholds.on_drift={self.on_drift!r} not in {sorted(valid)}"
            )
        if self.breaches_to_escalate < 1:
            raise ValueError(
                f"breaches_to_escalate must be ≥ 1, got {self.breaches_to_escalate}"
            )


# ── Top-level plan ────────────────────────────────────────────────────────────

@dataclass
class CampaignPlan:
    """Signed campaign plan — Planner contract with the CM.

    The CM treats ``signature`` and the structural fields as read-only.
    Only the surrogate cutoffs in StageSpec are CM-nudgeable, and only
    within their explicit bounds.
    """
    plan_id:          str
    stages:           list[StageSpec]
    plan_version:     int               = 1
    parent_plan_ref:  Optional[str]     = None
    signature:        Optional[str]     = None
    resources:        dict              = field(default_factory=dict)
    edges:            list[EdgeSpec]    = field(default_factory=list)
    replan:           ReplanThresholds  = field(default_factory=ReplanThresholds)
    features:         dict              = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.plan_id:
            raise ValueError("CampaignPlan.plan_id is required")
        if self.plan_version < 1:
            raise ValueError(
                f"CampaignPlan.plan_version must be ≥ 1, got {self.plan_version}"
            )
        if not self.stages:
            raise ValueError("CampaignPlan.stages must be non-empty")

        # Unique stage ids
        ids = [s.id for s in self.stages]
        dups = [i for i in ids if ids.count(i) > 1]
        if dups:
            raise ValueError(f"duplicate stage ids: {sorted(set(dups))}")

        valid_ids = set(ids)

        # Edge endpoints must reference existing stages
        for e in self.edges:
            if e.upstream not in valid_ids:
                raise ValueError(
                    f"EdgeSpec.upstream={e.upstream!r} not in stages {sorted(valid_ids)}"
                )
            if e.downstream not in valid_ids:
                raise ValueError(
                    f"EdgeSpec.downstream={e.downstream!r} not in stages {sorted(valid_ids)}"
                )

        # Stage dependencies must reference existing stages
        for s in self.stages:
            for dep in s.dependencies:
                if dep not in valid_ids:
                    raise ValueError(
                        f"stage {s.id!r} dependency {dep!r} not in stages "
                        f"{sorted(valid_ids)}"
                    )

        # Resource totals: cpus/gpus/memory_gb only — anything else is ignored
        valid_resources = {"total_cpus", "total_gpus", "total_memory_gb"}
        unknown = set(self.resources) - valid_resources
        if unknown:
            # Warning only — keep extra keys tolerant; production planners
            # may add fields we don't know about yet.
            pass

    # ── Helpers ────────────────────────────────────────────────────────────

    def stage(self, stage_id: str) -> StageSpec:
        """Look up a stage by id; raises KeyError if not found."""
        for s in self.stages:
            if s.id == stage_id:
                return s
        raise KeyError(f"no stage with id={stage_id!r}")

    def edge_for(self, downstream: str) -> Optional[EdgeSpec]:
        """Return the edge whose downstream is this stage (or None)."""
        for e in self.edges:
            if e.downstream == downstream:
                return e
        return None
