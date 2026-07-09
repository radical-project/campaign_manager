"""Plan loader: YAML → CampaignPlan, with back-compat for legacy flat configs.

Two YAML shapes are supported:

1. Structured plan (preferred — has top-level ``plan_id`` and ``stages``):

       plan_id: vaccine_screen_2026
       plan_version: 3
       resources: { total_cpus: 128, total_gpus: 4 }
       stages:
         - id: s1_ligand_filter
           pilot: { partition: cpu, nodes: 4, walltime_h: 2 }
           surrogate:
             score_cutoff: 0.65
             score_cutoff_nudge_bounds: [0.55, 0.80]
           ...
       edges:
         - { upstream: s1_ligand_filter, downstream: s2_ml_affinity,
             profile: diverse_top, backpressure: { high_water: 200, low_water: 50 } }
       cm:
         features: { sharder: true, ... }

2. Legacy flat config (still works — has top-level ``workflows`` dict):

       resources: { total_cpus: 128, total_gpus: 4 }
       workflows:
         s1: { replicas: 10, concurrency_floor: 1, ... }
         s2: { dependencies: [s1], ... }

Both formats produce a CampaignPlan internally so the rest of the CM only
sees structured input.  ``plan_to_workflows_dict`` is the inverse — used by
the existing flat-config-driven ``AsyncCampaignManager.from_config`` so the
structured plan can drive the same code path.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .schema import (
    BackpressureEdge,
    CampaignPlan,
    EdgeSpec,
    PilotSpec,
    ReplanThresholds,
    RetryPolicy,
    StageSpec,
    SurrogateSpec,
)

# Partition → default resource overlay.  Stage can override individual fields.
_PARTITION_RESOURCES: dict[str, dict[str, float]] = {
    "cpu": {"required_cpus": 16, "required_gpus": 0, "required_memory_gb": 0.0},
    "gpu": {"required_cpus": 4, "required_gpus": 1, "required_memory_gb": 0.0},
    "mpi+gpu": {"required_cpus": 16, "required_gpus": 2, "required_memory_gb": 0.0},
    "largemem": {"required_cpus": 8, "required_gpus": 1, "required_memory_gb": 64.0},
}


def _bp_from_dict(d: Any) -> BackpressureEdge | None:
    if d is None:
        return None
    if not isinstance(d, dict):
        raise TypeError(f"backpressure must be a dict, got {type(d).__name__}")
    return BackpressureEdge(
        high_water=int(d["high_water"]),
        low_water=int(d["low_water"]),
    )


def _surrogate_from_dict(d: Any) -> SurrogateSpec | None:
    if d is None:
        return None
    if not isinstance(d, dict):
        raise TypeError(f"surrogate must be a dict, got {type(d).__name__}")
    # Legacy alias: cutoff_nudge_bounds (cm-plan/1.0) → uncertainty_cutoff_nudge_bounds
    legacy_unc_bounds = d.get("cutoff_nudge_bounds")
    sc_lo, sc_hi = d.get("score_cutoff_nudge_bounds", [0.0, 1.0])
    un_lo, un_hi = d.get(
        "uncertainty_cutoff_nudge_bounds",
        legacy_unc_bounds if legacy_unc_bounds is not None else [0.0, 1.0],
    )
    return SurrogateSpec(
        # Legacy alias: model_ref (cm-plan/1.0) → model_uri
        model_uri=d.get("model_uri", d.get("model_ref")),
        score_cutoff=float(d.get("score_cutoff", 0.0)),
        score_cutoff_nudge_bounds=(float(sc_lo), float(sc_hi)),
        uncertainty_cutoff=float(d.get("uncertainty_cutoff", 1.0)),
        uncertainty_cutoff_nudge_bounds=(float(un_lo), float(un_hi)),
        advance_threshold=float(d.get("advance_threshold", float("inf"))),
    )


def _retry_from_dict(d: Any) -> RetryPolicy:
    if d is None:
        return RetryPolicy()
    return RetryPolicy(
        max_attempts=int(d.get("max_attempts", 1)),
        backoff_s=float(d.get("backoff_s", 0.0)),
        soft_failure_codes=list(d.get("soft_failure_codes", [])),
    )


def _pilot_from_dict(d: Any) -> PilotSpec:
    if d is None:
        return PilotSpec()
    return PilotSpec(
        partition=str(d.get("partition", "cpu")),
        nodes=int(d.get("nodes", 1)),
        walltime_h=float(d.get("walltime_h", 1.0)),
    )


def _stage_from_dict(d: dict, valid_stage_ids: set | None = None) -> StageSpec:
    pilot = _pilot_from_dict(d.get("pilot"))

    # Resource overlay: explicit dict keys win; otherwise derive from partition.
    res_default = _PARTITION_RESOURCES.get(pilot.partition, {})
    required_cpus = int(d.get("required_cpus", res_default.get("required_cpus", 0)))
    required_gpus = int(d.get("required_gpus", res_default.get("required_gpus", 0)))
    required_memory_gb = float(
        d.get("required_memory_gb", res_default.get("required_memory_gb", 0.0))
    )

    # Legacy alias: derive dependencies from upstream key (cm-plan/1.0 shape).
    # Only honour upstream when it refers to a real stage in the plan (the
    # dreamer config uses upstream="library" on the root, which is a virtual
    # source — that gets filtered out).
    deps = list(d.get("dependencies", []))
    if not deps and "upstream" in d:
        up = d["upstream"]
        if valid_stage_ids is None or up in valid_stage_ids:
            deps = [up]

    return StageSpec(
        id=str(d["id"]),
        variant=str(d.get("variant", "default")),
        threshold_top_fraction=float(d.get("threshold_top_fraction", 1.0)),
        budget_node_hours=float(d.get("budget_node_hours", 0.0)),
        burn_rate_band=float(d.get("burn_rate_band", 0.15)),
        downstream_input_target=int(d.get("downstream_input_target", 0)),
        campaign_target=int(d.get("campaign_target", 0)),
        budget_kp=float(d.get("budget_kp", 0.05)),
        budget_warmup_min=int(d.get("budget_warmup_min", 3)),
        pilot=pilot,
        surrogate=_surrogate_from_dict(d.get("surrogate")),
        retry_policy=_retry_from_dict(d.get("retry_policy")),
        concurrency_floor=int(d.get("concurrency_floor", 0)),
        concurrency_cap=int(d.get("concurrency_cap", 0)),
        priority=int(d.get("priority", 0)),
        required_cpus=required_cpus,
        required_gpus=required_gpus,
        required_memory_gb=required_memory_gb,
        replicas=int(d.get("replicas", 0)),
        dependencies=deps,
        dependency_threshold=int(d.get("dependency_threshold", 1)),
        sharding=(dict(d["sharding"]) if isinstance(d.get("sharding"), dict) else None),
    )


def _edge_from_dict(d: dict) -> EdgeSpec:
    return EdgeSpec(
        upstream=str(d["upstream"]),
        downstream=str(d["downstream"]),
        profile=str(d.get("profile", "diverse_top")),
        backpressure=_bp_from_dict(d.get("backpressure")),
    )


def _replan_from_dict(d: Any) -> ReplanThresholds:
    if d is None:
        return ReplanThresholds()
    return ReplanThresholds(
        budget_burn_deviation_pct=float(d.get("budget_burn_deviation_pct", 20.0)),
        pass_through_deviation_pct=float(d.get("pass_through_deviation_pct", 25.0)),
        surrogate_recall_floor=float(d.get("surrogate_recall_floor", 0.90)),
        breaches_to_escalate=int(d.get("breaches_to_escalate", 2)),
        on_drift=str(d.get("on_drift", "log_only")),
    )


def _is_structured(cfg: dict) -> bool:
    """Heuristic: structured plan has plan_id and stages; legacy has workflows."""
    return "plan_id" in cfg and "stages" in cfg


def load_plan(source: str | Path | dict) -> CampaignPlan:
    """Load a CampaignPlan from a YAML file path, YAML string, or dict.

    Accepts both structured and legacy formats; legacy gets a synthesized
    plan_id and stages are derived from workflows.  See module docstring
    for the two shapes.
    """
    if isinstance(source, dict):
        cfg = source
    else:
        path = Path(source)
        if path.exists():
            with open(path) as f:
                cfg = yaml.safe_load(f)
        else:
            # Treat as inline YAML string
            cfg = yaml.safe_load(str(source))

    if not isinstance(cfg, dict):
        raise TypeError(f"plan source must produce a dict; got {type(cfg).__name__}")

    if _is_structured(cfg):
        return _load_structured(cfg)
    return _load_legacy(cfg)


def _load_structured(cfg: dict) -> CampaignPlan:
    raw_stages = cfg.get("stages", [])
    # First pass: collect ids so dependency derivation from legacy
    # ``upstream`` keys can filter out virtual sources.
    valid_ids = {str(s["id"]) for s in raw_stages}
    stages = [_stage_from_dict(s, valid_stage_ids=valid_ids) for s in raw_stages]
    # Synthesize edges from stage upstream→downstream when no explicit edges
    # block is provided (legacy cm-plan/1.0 inferred edges from stage fields).
    # In either case, drop edges whose endpoints are virtual sources/sinks
    # (e.g., "library" feeding s1, "final_lead_set" off the terminal stage):
    # these aren't real CM stages and the validator would reject them.
    raw_edges = cfg.get("edges", [])
    if raw_edges:
        edges = []
        for e in raw_edges:
            up = str(e.get("upstream", ""))
            ds = str(e.get("downstream", ""))
            if up in valid_ids and ds in valid_ids:
                edges.append(_edge_from_dict(e))
    else:
        edges = []
        for s in raw_stages:
            ds = s.get("downstream")
            if ds and ds in valid_ids:
                edges.append(
                    EdgeSpec(
                        upstream=str(s["id"]),
                        downstream=str(ds),
                        profile=str(s.get("profile", "diverse_top")),
                    )
                )
    cm_cfg = cfg.get("cm", {})
    return CampaignPlan(
        plan_id=str(cfg["plan_id"]),
        plan_version=int(cfg.get("plan_version", 1)),
        parent_plan_ref=cfg.get("parent_plan_ref"),
        signature=cfg.get("signature"),
        resources=dict(cfg.get("resources", {})),
        stages=stages,
        edges=edges,
        replan=_replan_from_dict(cm_cfg.get("replan", cfg.get("replan"))),
        features=dict(cm_cfg.get("features", cfg.get("features", {}))),
    )


def _load_legacy(cfg: dict) -> CampaignPlan:
    """Adapt legacy ``workflows:`` dict format into a CampaignPlan.

    Synthesizes plan_id from a placeholder and converts each workflow entry
    into a StageSpec.  Edges are derived from the workflows' dependencies
    (one EdgeSpec per upstream → downstream pair) with a default profile.
    """
    wfs: dict[str, dict] = cfg.get("workflows", {})
    stages: list[StageSpec] = []
    edges: list[EdgeSpec] = []
    for name, wf in wfs.items():
        # Accept both new and legacy keys
        concurrency_floor = int(wf.get("concurrency_floor") or wf.get("min_replicas") or 0)
        concurrency_cap = int(wf.get("concurrency_cap") or wf.get("max_replicas") or 0)
        has_deps = bool(wf.get("dependencies", []))
        default_replicas = 0 if has_deps else 1
        stages.append(
            StageSpec(
                id=name,
                replicas=int(wf.get("replicas", default_replicas)),
                dependencies=list(wf.get("dependencies", [])),
                dependency_threshold=int(wf.get("dependency_threshold", 1)),
                concurrency_floor=concurrency_floor,
                concurrency_cap=concurrency_cap,
                priority=int(wf.get("priority", 0)),
                required_cpus=int(wf.get("required_cpus", 0)),
                required_gpus=int(wf.get("required_gpus", 0)),
                required_memory_gb=float(wf.get("required_memory_gb", 0.0)),
                threshold_top_fraction=float(wf.get("threshold_top_fraction", 1.0)),
                budget_node_hours=float(wf.get("budget_node_hours", 0.0)),
                burn_rate_band=float(wf.get("burn_rate_band", 0.15)),
                downstream_input_target=int(wf.get("downstream_input_target", 0)),
                campaign_target=int(wf.get("campaign_target", 0)),
                budget_kp=float(wf.get("budget_kp", 0.05)),
                budget_warmup_min=int(wf.get("budget_warmup_min", 3)),
                pilot=_pilot_from_dict(wf.get("pilot")),
            )
        )
        # Synthesize edges with backpressure from per-workflow keys
        for dep in wf.get("dependencies", []):
            bp = None
            hi = int(wf.get("backpressure_high") or 0)
            lo = int(wf.get("backpressure_low") or 0)
            if hi > 0 and lo > 0 and hi > lo:
                bp = BackpressureEdge(high_water=hi, low_water=lo)
            edges.append(
                EdgeSpec(
                    upstream=dep,
                    downstream=name,
                    profile=str(wf.get("profile", "diverse_top")),
                    backpressure=bp,
                )
            )

    cm_cfg = cfg.get("cm", {})
    return CampaignPlan(
        plan_id=cfg.get("plan_id", "legacy_plan"),
        plan_version=int(cfg.get("plan_version", 1)),
        resources=dict(cfg.get("resources", {})),
        stages=stages,
        edges=edges,
        replan=_replan_from_dict(cm_cfg.get("replan", cfg.get("replan"))),
        features=dict(cfg.get("features", cm_cfg.get("features", {}))),
    )


# ── Plan → flat workflows dict (legacy code path) ────────────────────────────


def plan_to_workflows_dict(plan: CampaignPlan) -> dict:
    """Render a CampaignPlan back into the legacy flat-config shape.

    Used by AsyncCampaignManager.from_config so a structured plan can drive
    the existing scheduler/executor without rewriting every code path.
    Downstream code receives the same dict it expects today.
    """
    workflows: dict[str, dict] = {}
    edge_by_downstream: dict[str, EdgeSpec] = {e.downstream: e for e in plan.edges}

    for s in plan.stages:
        wf: dict = {
            "replicas": s.replicas,
            "dependencies": list(s.dependencies),
            "dependency_threshold": s.dependency_threshold,
            "concurrency_floor": s.concurrency_floor,
            "concurrency_cap": s.concurrency_cap,
            "priority": s.priority,
            "required_cpus": s.required_cpus,
            "required_gpus": s.required_gpus,
            "required_memory_gb": s.required_memory_gb,
            # Workflow-config keys (forwarded to BaseWorkflow.config)
            "threshold_top_fraction": s.threshold_top_fraction,
            "budget_node_hours": s.budget_node_hours,
            "burn_rate_band": s.burn_rate_band,
            "downstream_input_target": s.downstream_input_target,
            "campaign_target": s.campaign_target,
            "budget_kp": s.budget_kp,
            "budget_warmup_min": s.budget_warmup_min,
            "pilot": {
                "partition": s.pilot.partition,
                "nodes": s.pilot.nodes,
                "walltime_h": s.pilot.walltime_h,
            },
        }
        # Edge metadata: profile + backpressure thresholds
        edge = edge_by_downstream.get(s.id)
        if edge is not None:
            wf["profile"] = edge.profile
            if edge.backpressure is not None:
                wf["backpressure_high"] = edge.backpressure.high_water
                wf["backpressure_low"] = edge.backpressure.low_water
        # Retry policy carried for the executor
        wf["retry_policy"] = {
            "max_attempts": s.retry_policy.max_attempts,
            "backoff_s": s.retry_policy.backoff_s,
            "soft_failure_codes": list(s.retry_policy.soft_failure_codes),
        }
        # Sharding spec carried through so the CM's Sharder feature can
        # consume it (ShardingSpec.from_dict validates the raw dict).
        if s.sharding is not None:
            wf["sharding"] = dict(s.sharding)
        # Surrogate spec carried as-is for the BudgetController to wire up
        if s.surrogate is not None:
            wf["surrogate"] = {
                "model_uri": s.surrogate.model_uri,
                "score_cutoff": s.surrogate.score_cutoff,
                "score_cutoff_nudge_bounds": list(s.surrogate.score_cutoff_nudge_bounds),
                "uncertainty_cutoff": s.surrogate.uncertainty_cutoff,
                "uncertainty_cutoff_nudge_bounds": list(
                    s.surrogate.uncertainty_cutoff_nudge_bounds
                ),
            }
        workflows[s.id] = wf

    return {
        "engine": "concurrent",  # caller may override
        "resources": dict(plan.resources),
        "features": dict(plan.features),
        "replan": {
            "budget_burn_deviation_pct": plan.replan.budget_burn_deviation_pct,
            "pass_through_deviation_pct": plan.replan.pass_through_deviation_pct,
            "surrogate_recall_floor": plan.replan.surrogate_recall_floor,
            "breaches_to_escalate": plan.replan.breaches_to_escalate,
            "on_drift": plan.replan.on_drift,
        },
        "workflows": workflows,
    }
