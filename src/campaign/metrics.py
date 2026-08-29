"""
CampaignMetrics — lightweight in-process event recorder.

Records timestamped events during a campaign run:
  - ReplicaEvent       per-replica start/finish with timing
  - BPEvent            backpressure state transitions per edge
  - ShardEvent         sharder dispatch metadata (shard quality)
  - SchedulingEvent    scheduling decisions (which stage chosen, bandit scores)
  - DecisionTraceEvent one entry per ADR tick: policy, full action list, elapsed time
  - ArtifactManifest   provenance records for remote artifacts (Pattern 7)

All timestamps are wall-clock seconds via time.time().
to_dict() serialises to JSON-compatible dicts for benchmark aggregation.
"""

import time
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class ReplicaEvent:
    group: str
    replica_id: str
    event: str  # "start" | "finish" | "failed"
    timestamp: float
    candidate_id: Optional[str] = None
    score: Optional[float] = None
    duration_s: Optional[float] = None  # set on finish event
    retry_of: Optional[str] = None      # original replica_id this is a retry of
    attempt: int = 0                    # 0 = first attempt, 1 = first retry, …


@dataclass
class BPEvent:
    group: str
    old_state: str
    new_state: str
    queue_depth: int
    timestamp: float


@dataclass
class ShardEvent:
    group: str
    shard_id: int
    n: int
    scores: list[float]  # raw candidate scores in dispatch order
    priorities: list[float]  # profile priority scores in dispatch order
    timestamp: float

    @property
    def mean_score(self) -> float:
        return sum(self.scores) / len(self.scores) if self.scores else 0.0

    @property
    def mean_priority(self) -> float:
        return sum(self.priorities) / len(self.priorities) if self.priorities else 0.0


@dataclass
class SchedulingEvent:
    chosen_groups: list[str]  # groups started this cycle (in order)
    eligible_groups: list[str]  # all eligible groups before selection
    bandit_scores: dict[str, float]  # Thompson sample per group (empty if no bandit)
    timestamp: float
    bandit_means: dict[str, float] = field(default_factory=dict)  # Beta posterior mean per arm


@dataclass
class DecisionTraceEvent:
    """One ADR tick decision — travels with the campaign JSON via CampaignMetrics.to_dict().

    Supplements (does not replace) the per-cycle JSONL written by PolicyRecorder.
    The JSONL is consumed by plot scripts; this trace is for post-run inspection
    without having to correlate a separate file.
    """

    cycle: int
    t: float                       # seconds since campaign start
    policy: str                    # e.g. "rule", "llm", "consensus"
    actions: list[dict]            # [{"name": "set_priority", "stage": "s", "priority": 9}, ...]
    llm_prompt_tokens: Optional[int] = None      # Phase 2: filled by LLM policy
    llm_completion_tokens: Optional[int] = None  # Phase 2: filled by LLM policy


@dataclass
class BudgetEventRecord:
    """One BudgetController.evaluate outcome, serialized for replay & plots.

    Captured from executor.py whenever a stage's BudgetController ticks.
    The fields mirror BudgetEvent (in budget_controller.py) plus the
    timestamp the executor recorded — together they form the trajectory
    plot_budget_control.py consumes.
    """

    stage_id: str
    kind: str  # in_band | nudged | bound_locked
    burn_ratio: float
    progress: float
    spend_node_hours: float
    finished: int
    score_cutoff: float
    uncertainty_cutoff: float
    score_at_bound: bool
    unc_at_bound: bool
    consecutive_hits: int
    frozen: bool
    timestamp: float


class CampaignMetrics:
    """Accumulates events during one campaign run."""

    def __init__(self) -> None:
        self.start_time: float = time.time()
        self.end_time: Optional[float] = None
        self.replica_events: list[ReplicaEvent] = []
        self.bp_events: list[BPEvent] = []
        self.shard_events: list[ShardEvent] = []
        self.scheduling_events: list[SchedulingEvent] = []
        self.budget_events: list[BudgetEventRecord] = []
        self.decision_events: list[DecisionTraceEvent] = []
        self.manifest_events: list[dict] = []
        self._replica_starts: dict[str, float] = {}  # replica_id → start time
        self._stage_wall_s: dict[str, float] = {}  # group → cumulative wall-time seconds

    # ── Writers ───────────────────────────────────────────────────────────────

    def record_replica_start(
        self,
        group: str,
        replica_id: str,
        candidate_id: Optional[str] = None,
        score: Optional[float] = None,
        retry_of: Optional[str] = None,
        attempt: int = 0,
    ) -> None:
        t = time.time()
        self._replica_starts[replica_id] = t
        self.replica_events.append(
            ReplicaEvent(
                group=group,
                replica_id=replica_id,
                event="start",
                timestamp=t,
                candidate_id=candidate_id,
                score=score,
                retry_of=retry_of,
                attempt=attempt,
            )
        )

    def record_replica_finish(
        self,
        group: str,
        replica_id: str,
        final_state: str,  # "done" | "failed"
    ) -> None:
        t = time.time()
        start = self._replica_starts.pop(replica_id, t)
        duration = t - start
        self.replica_events.append(
            ReplicaEvent(
                group=group,
                replica_id=replica_id,
                event="finish" if final_state == "done" else "failed",
                timestamp=t,
                duration_s=duration,
            )
        )
        self._stage_wall_s[group] = self._stage_wall_s.get(group, 0.0) + duration

    def stage_wall_s(self, group: str) -> float:
        """Cumulative finished-replica wall-time for a group (O(1))."""
        return self._stage_wall_s.get(group, 0.0)

    def record_bp_transition(
        self,
        group: str,
        old_state: str,
        new_state: str,
        queue_depth: int,
    ) -> None:
        self.bp_events.append(
            BPEvent(
                group=group,
                old_state=old_state.upper(),
                new_state=new_state.upper(),
                queue_depth=queue_depth,
                timestamp=time.time(),
            )
        )

    def record_shard(
        self,
        group: str,
        shard_id: int,
        n: int,
        scores: list[float],
        priorities: list[float],
    ) -> None:
        self.shard_events.append(
            ShardEvent(
                group=group,
                shard_id=shard_id,
                n=n,
                scores=scores,
                priorities=priorities,
                timestamp=time.time(),
            )
        )

    def record_budget(
        self,
        stage_id: str,
        kind: str,
        burn_ratio: float,
        progress: float,
        spend_node_hours: float,
        finished: int,
        score_cutoff: float,
        uncertainty_cutoff: float,
        score_at_bound: bool,
        unc_at_bound: bool,
        consecutive_hits: int,
        frozen: bool,
    ) -> None:
        """Record one BudgetController.evaluate outcome."""
        self.budget_events.append(
            BudgetEventRecord(
                stage_id=stage_id,
                kind=kind,
                burn_ratio=burn_ratio,
                progress=progress,
                spend_node_hours=spend_node_hours,
                finished=finished,
                score_cutoff=score_cutoff,
                uncertainty_cutoff=uncertainty_cutoff,
                score_at_bound=score_at_bound,
                unc_at_bound=unc_at_bound,
                consecutive_hits=consecutive_hits,
                frozen=frozen,
                timestamp=time.time(),
            )
        )

    def record_scheduling(
        self,
        chosen_groups: list[str],
        eligible_groups: list[str],
        bandit_scores: Optional[dict[str, float]] = None,
        bandit_means: Optional[dict[str, float]] = None,
    ) -> None:
        # bandit_* are legacy fields (the in-loop scheduling bandit was removed;
        # adaptive priority is now driven by the ADR layer). Kept optional so
        # older telemetry consumers still parse, defaulting to empty.
        self.scheduling_events.append(
            SchedulingEvent(
                chosen_groups=chosen_groups,
                eligible_groups=eligible_groups,
                bandit_scores=bandit_scores or {},
                bandit_means=bandit_means or {},
                timestamp=time.time(),
            )
        )

    def record_decision(
        self,
        cycle: int,
        t: float,
        policy: str,
        actions: list[dict],
        llm_prompt_tokens: Optional[int] = None,
        llm_completion_tokens: Optional[int] = None,
    ) -> None:
        self.decision_events.append(
            DecisionTraceEvent(
                cycle=cycle,
                t=t,
                policy=policy,
                actions=actions,
                llm_prompt_tokens=llm_prompt_tokens,
                llm_completion_tokens=llm_completion_tokens,
            )
        )

    def record_manifest(self, manifest: "Any") -> None:
        """Append an ArtifactManifest to the persistent record.

        Accepts any object with a to_dict() method so metrics.py stays
        import-free of artifacts.py at runtime (avoids circular imports).
        """
        d = manifest.to_dict()
        d["_recorded_at"] = time.time() - self.start_time
        self.manifest_events.append(d)

    def finish(self) -> None:
        self.end_time = time.time()

    # ── Summary ───────────────────────────────────────────────────────────────

    @property
    def wall_time_s(self) -> float:
        end = self.end_time or time.time()
        return end - self.start_time

    def group_stats(self) -> dict:
        """Per-group throughput and timing summary."""
        from collections import defaultdict

        starts: dict[str, list[float]] = defaultdict(list)
        finishes: dict[str, list[float]] = defaultdict(list)
        durations: dict[str, list[float]] = defaultdict(list)
        for ev in self.replica_events:
            if ev.event == "start":
                starts[ev.group].append(ev.timestamp)
            elif ev.event in ("finish", "failed"):
                finishes[ev.group].append(ev.timestamp)
                if ev.duration_s is not None:
                    durations[ev.group].append(ev.duration_s)
        groups = set(starts) | set(finishes)
        out = {}
        for g in groups:
            s_times = sorted(starts.get(g, []))
            f_times = sorted(finishes.get(g, []))
            durs = durations.get(g, [])
            span = (max(f_times) - min(s_times)) if s_times and f_times else 0.0
            out[g] = {
                "n_started": len(s_times),
                "n_finished": len(f_times),
                "first_start": min(s_times) - self.start_time if s_times else None,
                "last_finish": max(f_times) - self.start_time if f_times else None,
                "span_s": span,
                "mean_dur_s": sum(durs) / len(durs) if durs else None,
                "throughput_rps": len(f_times) / span if span > 0 else None,
            }
        return out

    def bp_state_fractions(self) -> dict:
        """Per-group fraction of inter-event time spent in each BP state."""
        from collections import defaultdict

        durations: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        # track last transition time and state
        last: dict[str, tuple[float, str]] = {}
        for ev in sorted(self.bp_events, key=lambda e: e.timestamp):
            if ev.group in last:
                prev_t, prev_state = last[ev.group]
                durations[ev.group][prev_state] += ev.timestamp - prev_t
            last[ev.group] = (ev.timestamp, ev.new_state)
        # close open intervals
        end = self.end_time or time.time()
        for g, (t, state) in last.items():
            durations[g][state] += end - t
        result = {}
        for g, d in durations.items():
            total = sum(d.values()) or 1.0
            result[g] = {k: v / total for k, v in d.items()}
        return result

    def to_dict(self) -> dict:
        """Full serialization for JSON storage."""
        return {
            "wall_time_s": self.wall_time_s,
            "group_stats": self.group_stats(),
            "bp_fractions": self.bp_state_fractions(),
            "shard_events": [
                {
                    "group": e.group,
                    "shard_id": e.shard_id,
                    "n": e.n,
                    "mean_score": e.mean_score,
                    "mean_priority": e.mean_priority,
                    "scores": e.scores,
                    "timestamp": e.timestamp - self.start_time,
                }
                for e in self.shard_events
            ],
            "scheduling_events": [
                {
                    "chosen": e.chosen_groups,
                    "eligible": e.eligible_groups,
                    "bandit": e.bandit_scores,
                    "bandit_means": e.bandit_means,
                    "timestamp": e.timestamp - self.start_time,
                }
                for e in self.scheduling_events
            ],
            "replica_events": [
                {
                    "group": e.group,
                    "replica_id": e.replica_id,
                    "event": e.event,
                    "t": e.timestamp - self.start_time,
                    "dur": e.duration_s,
                    "score": e.score,
                    **({"retry_of": e.retry_of, "attempt": e.attempt} if e.retry_of else {}),
                }
                for e in self.replica_events
            ],
            "budget_events": [
                {
                    "stage_id": e.stage_id,
                    "kind": e.kind,
                    "burn_ratio": e.burn_ratio,
                    "progress": e.progress,
                    "spend_node_hours": e.spend_node_hours,
                    "finished": e.finished,
                    "score_cutoff": e.score_cutoff,
                    "uncertainty_cutoff": e.uncertainty_cutoff,
                    "score_at_bound": e.score_at_bound,
                    "unc_at_bound": e.unc_at_bound,
                    "consecutive_hits": e.consecutive_hits,
                    "frozen": e.frozen,
                    "t": e.timestamp - self.start_time,
                }
                for e in self.budget_events
            ],
            "decision_events": [
                {
                    "cycle": e.cycle,
                    "t": e.t,
                    "policy": e.policy,
                    "actions": e.actions,
                    **({"llm_prompt_tokens": e.llm_prompt_tokens} if e.llm_prompt_tokens is not None else {}),
                    **({"llm_completion_tokens": e.llm_completion_tokens} if e.llm_completion_tokens is not None else {}),
                }
                for e in self.decision_events
            ],
            "manifest_events": list(self.manifest_events),
        }
