"""Unit tests for the src.campaign.adr bridge (Operator + policies + view).

All tests run against a FakeView — no live CampaignManager, no asyncflow
engine, and no LLM key required.
"""

import pytest

from src.campaign.adr import (
    DEFAULT_SCHEDULING_PROMPT,
    BanditSchedulingPolicy,
    CampaignOperator,
    DownstreamFirstPolicy,
    LLMSchedulingPolicy,
    ScheduleDecision,
    TelemetrySubscriber,
    make_scheduling_policy,
    resolve_system_prompt,
)
from src.campaign.adr.policies import _batch_for_bp, _downstream_bp, _stage_depth

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


# ── Fake view ───────────────────────────────────────────────────────────────

class FakeView:
    """In-memory CampaignViewProtocol implementation that records lever calls."""

    def __init__(self, stages: dict, hits: int = 0, target: int = 5,
                 free_gpus: int = 2):
        self._stages = stages
        self._hits = hits
        self._target = target
        self._free_gpus = free_gpus
        self.priority_calls: list = []
        self.batch_calls: list = []
        self.trigger_calls: list = []

    def observe(self) -> dict:
        return {
            "cycle": 0,
            "terminal": (list(self._stages)[-1] if self._stages else None),
            "hits": self._hits, "target": self._target,
            "free_cpus": 0, "free_gpus": self._free_gpus,
            "stages": self._stages,
        }

    def set_priority(self, stage: str, priority: int) -> bool:
        self.priority_calls.append((stage, priority))
        return True

    def set_batch_size(self, stage: str, size: int) -> bool:
        self.batch_calls.append((stage, size))
        return True

    async def trigger(self, stage: str, replicas: int) -> int:
        self.trigger_calls.append((stage, replicas))
        return replicas


def _stage(deps=(), queue_depth=0, bp_state="HOLD", priority=0,
           pending=0, running=0, cap=4):
    return {"status": "running", "priority": priority, "started": 0,
            "running": running, "finished": 0, "cap": cap, "ready": True,
            "deps": list(deps), "queue_depth": queue_depth, "bp_state": bp_state,
            "pending": pending,
            "starved": bool(pending > 0 and running < cap and deps),
            "is_source": not deps}


def _cascade(**overrides):
    stages = {
        "s1": _stage(),
        "s2": _stage(deps=["s1"]),
        "s3": _stage(deps=["s2"]),
    }
    for name, patch in overrides.items():
        stages[name].update(patch)
    return stages


# ── Helper functions ─────────────────────────────────────────────────────────

class TestHelpers:
    def test_stage_depth_orders_cascade(self):
        depth = _stage_depth(_cascade())
        assert depth["s1"] == 0
        assert depth["s2"] == 1
        assert depth["s3"] == 2

    def test_batch_shrinks_on_throttle(self):
        assert _batch_for_bp("THROTTLE", 50) == 25

    def test_batch_grows_on_widen(self):
        assert _batch_for_bp("WIDEN", 50) == 100

    def test_batch_holds_otherwise(self):
        assert _batch_for_bp("HOLD", 50) == 50


# ── Rule policy ───────────────────────────────────────────────────────────────

class TestDownstreamFirstPolicy:
    async def test_ranks_deepest_stage_highest(self):
        view = FakeView(_cascade(), free_gpus=2)
        op = CampaignOperator(view, engine=None, target=5)
        policy = DownstreamFirstPolicy(op)
        decision = await policy.run(view.observe())
        pr = {a.task_kwargs["stage"]: a.task_kwargs["priority"]
              for a in decision.actions if a.task_name == "set_priority"}
        # deepest (s3) highest, root (s1) lowest, every stage ranked
        assert pr["s3"] > pr["s2"] > pr["s1"]
        assert set(pr) == {"s1", "s2", "s3"}

    async def test_ranks_regardless_of_free_slots(self):
        # proactive: ranks even with no free slots / no queued work
        view = FakeView(_cascade(), free_gpus=0)
        op = CampaignOperator(view, engine=None, target=5)
        policy = DownstreamFirstPolicy(op)
        decision = await policy.run(view.observe())
        pr = [a for a in decision.actions if a.task_name == "set_priority"]
        assert len(pr) == 3

    async def test_batch_resized_under_throttle(self):
        view = FakeView(_cascade(s2={"bp_state": "THROTTLE"}))
        op = CampaignOperator(view, engine=None, target=5)
        policy = DownstreamFirstPolicy(op)
        decision = await policy.run(view.observe())
        batch = {a.task_kwargs["stage"]: a.task_kwargs["size"]
                 for a in decision.actions if a.task_name == "set_batch_size"}
        assert batch.get("s2") == 25

    async def test_empty_stages_is_noop(self):
        view = FakeView({}, target=0)
        op = CampaignOperator(view, engine=None, target=0)
        policy = DownstreamFirstPolicy(op)
        decision = await policy.run(view.observe())
        assert decision.actions == []


# ── Bandit policy (the in-CM bandit, wrapped) ─────────────────────────────────

class TestBanditSchedulingPolicy:
    async def test_emits_priority_for_every_stage(self):
        view = FakeView(_cascade())
        op = CampaignOperator(view, engine=None, target=5)
        policy = BanditSchedulingPolicy(op, seed=0)
        decision = await policy.run(view.observe())
        ranked = [a.task_kwargs["stage"] for a in decision.actions
                  if a.task_name == "set_priority"]
        assert set(ranked) == {"s1", "s2", "s3"}     # ranks all stages

    async def test_downstream_bp_reward_mapping(self):
        stages = _cascade(s2={"bp_state": "WIDEN"}, s3={"bp_state": "THROTTLE"})
        # s1 feeds s2 (WIDEN) ; s2 feeds s3 (THROTTLE) ; s3 terminal (None)
        assert _downstream_bp("s1", stages) == "WIDEN"
        assert _downstream_bp("s2", stages) == "THROTTLE"
        assert _downstream_bp("s3", stages) is None

    async def test_bandit_learns_downstream_first(self):
        # Reward s3 (terminal, neutral) low but s1 high via WIDEN downstream over
        # many cycles → bandit's posterior should rank the well-rewarded stage up.
        # Here s1's downstream (s2) is WIDEN (reward 0.8); s3 terminal (0.5).
        view = FakeView(_cascade(s2={"bp_state": "WIDEN"}))
        op = CampaignOperator(view, engine=None, target=999)
        policy = BanditSchedulingPolicy(op, seed=1)
        # Simulate many completions of s1 (each rewarded 0.8 via WIDEN downstream).
        for _ in range(40):
            view._stages["s1"]["finished"] += 1
            await policy.run(view.observe())
        means = policy.summary
        assert means["s1"] > means["s3"]    # learned to favour the rewarded stage

    async def test_warmstart_priors_depth_based(self):
        view = FakeView(_cascade())
        op = CampaignOperator(view, engine=None, target=5)
        policy = BanditSchedulingPolicy(op, seed=0, warmstart=True)
        await policy.run(view.observe())     # builds the bandit
        means = policy.summary
        # Deeper stages start with higher prior mean (Beta(depth+1, 1)).
        assert means["s3"] > means["s1"]


# ── Operator end-to-end (one cycle, rule policy) ──────────────────────────────

class TestOperatorCycle:
    async def test_one_cycle_applies_levers_to_view(self):
        view = FakeView(_cascade(s3={"queue_depth": 5}), hits=0, target=5)
        op = CampaignOperator(view, engine=None, target=5)
        op.policy = DownstreamFirstPolicy(op)

        async for _snapshot in op.run():
            break   # one cycle is enough

        # The @act levers ran and mutated the (fake) view: s3 (deepest) got the
        # highest priority of the three.
        pr = dict(view.priority_calls)
        assert pr["s3"] > pr["s2"] > pr["s1"]

    async def test_goal_stops_when_target_reached(self):
        view = FakeView(_cascade(), hits=5, target=5)   # already at target
        op = CampaignOperator(view, engine=None, target=5)
        op.policy = DownstreamFirstPolicy(op)

        cycles = 0
        async for _snapshot in op.run():
            cycles += 1
            if cycles > 3:
                break
        assert cycles == 1   # goal satisfied on first cycle → stop


# ── Composition factory + LLM guard ───────────────────────────────────────────

class TestFactory:
    def test_default_kind_returns_rule_policy(self):
        view = FakeView(_cascade())
        op = CampaignOperator(view, engine=None, target=5)
        assert isinstance(make_scheduling_policy(op), DownstreamFirstPolicy)

    def test_kind_bandit_returns_bandit_policy(self):
        view = FakeView(_cascade())
        op = CampaignOperator(view, engine=None, target=5)
        policy = make_scheduling_policy(op, kind="bandit", warmstart=True)
        assert isinstance(policy, BanditSchedulingPolicy)

    def test_kind_llm_requires_key(self):
        view = FakeView(_cascade())
        op = CampaignOperator(view, engine=None, target=5)
        with pytest.raises(ValueError):
            make_scheduling_policy(op, kind="llm", llm_api_key=None)

    def test_llm_policy_requires_optional_deps(self):
        # openai / instructor are not installed in CI → constructing the LLM
        # policy must raise a clear ImportError, not a cryptic one.
        view = FakeView(_cascade())
        op = CampaignOperator(view, engine=None, target=5)
        import importlib.util
        if importlib.util.find_spec("instructor") and importlib.util.find_spec("openai"):
            pytest.skip("openai+instructor installed — guard path not exercised")
        with pytest.raises(ImportError):
            LLMSchedulingPolicy("fake-key", op)


class TestPolicyRecorder:
    async def test_records_jsonl_per_cycle(self, tmp_path):
        import json
        from src.campaign.adr import PolicyRecorder

        view = FakeView(_cascade(s3={"queue_depth": 5}), hits=0, target=3)
        path = tmp_path / "decisions.jsonl"
        rec = PolicyRecorder(path, policy_kind="rule")
        op = CampaignOperator(view, engine=None, target=3, observer=rec)
        op.policy = DownstreamFirstPolicy(op)
        rec.bind(view=view, policy=op.policy)

        cycles = 0
        async for _snapshot in op.run():
            cycles += 1
            if cycles >= 2:
                break

        rows = [json.loads(l) for l in path.read_text().splitlines() if l.strip()]
        assert rows, "recorder wrote no rows"
        assert rows[0]["policy"] == "rule"
        assert "priorities" in rows[0] and "stages" in rows[0]
        # rule policy ranks the deepest stage (s3) above the root (s1)
        assert rows[0]["priorities"]["s3"] > rows[0]["priorities"]["s1"]

    async def test_bandit_summary_recorded(self, tmp_path):
        import json
        from src.campaign.adr import PolicyRecorder, BanditSchedulingPolicy

        view = FakeView(_cascade(), hits=0, target=999)
        path = tmp_path / "bandit.jsonl"
        rec = PolicyRecorder(path, policy_kind="bandit")
        op = CampaignOperator(view, engine=None, target=999, observer=rec)
        op.policy = BanditSchedulingPolicy(op, seed=0, warmstart=True)
        rec.bind(view=view, policy=op.policy)

        async for _snapshot in op.run():
            break

        rows = [json.loads(l) for l in path.read_text().splitlines() if l.strip()]
        assert rows[0]["summary"], "bandit posterior summary not recorded"


class TestLLMTimeoutFallback:
    async def test_timing_out_primary_falls_back_to_rule(self):
        # Reproduces the free-model failure mode: the LLM call hangs/times out.
        # The Policy(primary, fallback) composition must degrade to the rule
        # policy for that cycle instead of starving the operator.
        import asyncio
        from radical.adr import Policy, decide

        view = FakeView(_cascade(), free_gpus=2)
        op = CampaignOperator(view, engine=None, target=5)

        class _TimeoutPrimary(Policy):
            @decide
            async def run(self, obs):
                raise asyncio.TimeoutError("simulated hung LLM call")

        composed = Policy(primary=_TimeoutPrimary(),
                          fallback=DownstreamFirstPolicy(op))
        decision = await composed.decide(view.observe())
        # Rule fallback ran: every stage got a priority (deepest highest).
        pr = {a.task_kwargs["stage"]: a.task_kwargs["priority"]
              for a in decision.actions if a.task_name == "set_priority"}
        assert pr and pr["s3"] > pr["s1"]


def test_schedule_decision_defaults():
    sd = ScheduleDecision()
    assert sd.priorities == {}
    assert sd.batch_sizes is None  # None = "omit entirely" (no batch-size changes)
    assert sd.stop is False


def test_schedule_decision_priorities():
    sd = ScheduleDecision(
        priorities={"s1": 101, "s2": 102, "s5": 105},
        batch_sizes={"s2": 60},
    )
    assert sd.priorities["s5"] == 105
    assert sd.priorities["s1"] == 101
    assert sd.batch_sizes["s2"] == 60


# ── CampaignView observation surface (real view over a fake CM) ───────────────

class _FakeWf:
    def __init__(self, deps=(), replicas=0, started=0, finished=0,
                 cap=4, priority=0, ready=True, status="running"):
        self.dependencies = list(deps)
        self.replicas = replicas
        self.started_count = started
        self.finished_replicas = finished
        self.concurrency_cap = cap
        self.priority = priority
        self.ready = ready
        self.status = status


class _FakeResources:
    available_cpus = 32
    available_gpus = 2


class _FakeState:
    def __init__(self, workflows):
        self.workflows = workflows
        self.sharders = {}
        self.bp = {}
        self.resources = _FakeResources()


class _FakeCM:
    def __init__(self, workflows):
        self.state = _FakeState(workflows)
        self._plan = None


class TestCampaignView:
    def _view(self, telemetry_subscriber=None):
        from src.campaign.adr import CampaignView
        wfs = {
            # source stage: at cap with backlog → not starved (cap-limited)
            "s1": _FakeWf(deps=(), replicas=100, started=4, finished=2, cap=4),
            # dependent, resource-starved: pending>0 and running<cap
            "s2": _FakeWf(deps=["s1"], replicas=20, started=8, finished=2, cap=16),
            "s3": _FakeWf(deps=["s2"], replicas=2, started=2, finished=2, cap=12),
        }
        return CampaignView(_FakeCM(wfs), terminal="s3",
                            telemetry_subscriber=telemetry_subscriber)

    def test_exposes_pending_and_backlog(self):
        obs = self._view().observe()
        # pending = replicas - started_count
        assert obs["stages"]["s2"]["pending"] == 12
        assert obs["stages"]["s1"]["pending"] == 96

    def test_is_source_flag(self):
        st = self._view().observe()["stages"]
        assert st["s1"]["is_source"] is True
        assert st["s2"]["is_source"] is False

    def test_starved_only_for_resource_limited_dependent(self):
        st = self._view().observe()["stages"]
        # s2: dependent, pending>0, running(6)<cap(16) → starved
        assert st["s2"]["starved"] is True
        # s1: source → never flagged as a bottleneck
        assert st["s1"]["starved"] is False
        # s3: no pending (all started) → not starved
        assert st["s3"]["starved"] is False

    def test_telemetry_fields_merged_when_subscriber_present(self):
        sub = TelemetrySubscriber(None)
        sub._gpu_util, sub._cpu_util = 77.0, 40.0
        obs = self._view(telemetry_subscriber=sub).observe()
        assert obs["gpu_util"] == 77.0
        assert obs["cpu_util"] == 40.0
        assert "task_fail_rate" in obs

    def test_no_telemetry_fields_without_subscriber(self):
        obs = self._view().observe()
        assert "gpu_util" not in obs


# ── TelemetrySubscriber ───────────────────────────────────────────────────────

class _Ev:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class TestTelemetrySubscriber:
    def test_none_telemetry_is_noop_zeros(self):
        snap = TelemetrySubscriber(None).snapshot()
        assert snap["gpu_util"] == 0.0
        assert snap["cpu_util"] == 0.0
        assert snap["task_fail_rate"] == 0.0
        assert snap["avg_task_duration_s"] is None

    def test_resource_and_task_events_aggregate(self):
        s = TelemetrySubscriber(None, alpha=1.0)   # alpha=1 → no smoothing
        s._on_event(_Ev(event_type="ResourceUpdate", resource_scope="per_gpu",
                        gpu_id=0, gpu_percent=80.0))
        s._on_event(_Ev(event_type="ResourceUpdate", resource_scope="per_gpu",
                        gpu_id=1, gpu_percent=60.0))
        s._on_event(_Ev(event_type="ResourceUpdate", resource_scope="per_node",
                        gpu_id=None, gpu_percent=None,
                        cpu_percent=45.0, memory_percent=70.0))
        s._on_event(_Ev(event_type="TaskCompleted", duration_seconds=2.0))
        s._on_event(_Ev(event_type="TaskCompleted", duration_seconds=4.0))
        s._on_event(_Ev(event_type="TaskFailed"))
        snap = s.snapshot()
        assert snap["gpu_util"] == 70.0          # mean of 80,60
        assert snap["cpu_util"] == 45.0
        assert snap["mem_util"] == 70.0
        assert snap["avg_task_duration_s"] == 3.0   # mean of 2,4
        assert snap["task_fail_rate"] == 1 / 3      # 1 fail of 3 outcomes

    def test_subscribe_wired_when_telemetry_given(self):
        captured = []

        class _Mgr:
            def subscribe(self, cb):
                captured.append(cb)

        TelemetrySubscriber(_Mgr())
        assert len(captured) == 1   # registered its callback


# ── resolve_system_prompt ─────────────────────────────────────────────────────

class TestResolveSystemPrompt:
    def test_inline_wins(self):
        assert resolve_system_prompt({"system_prompt": "X",
                                      "system_prompt_file": "ignored.txt"}) == "X"

    def test_file_read_relative_to_config_dir(self, tmp_path):
        (tmp_path / "p.txt").write_text("PROMPT-FROM-FILE")
        out = resolve_system_prompt({"system_prompt_file": "p.txt"},
                                    config_dir=tmp_path)
        assert out == "PROMPT-FROM-FILE"

    def test_none_when_unset(self):
        assert resolve_system_prompt({}) is None


# ── LLM policy: prompt override + decision mapping (deps are installed) ────────

class TestLLMPolicyConfig:
    def _op(self):
        view = FakeView(_cascade())
        return CampaignOperator(view, engine=None, target=5)

    def test_default_prompt_used_when_not_overridden(self):
        pol = LLMSchedulingPolicy("k", self._op())
        assert pol.system_prompt == DEFAULT_SCHEDULING_PROMPT

    def test_system_prompt_override(self):
        pol = LLMSchedulingPolicy("k", self._op(), system_prompt="CUSTOM PROMPT")
        assert pol.system_prompt == "CUSTOM PROMPT"

    def test_to_decision_maps_both_levers(self):
        pol = LLMSchedulingPolicy("k", self._op())
        sd = ScheduleDecision(priorities={"s1": 1, "s3": 3},
                              batch_sizes={"s2": 40}, stop=False)
        decision = pol._to_decision(sd)
        pr = {a.task_kwargs["stage"]: a.task_kwargs["priority"]
              for a in decision.actions if a.task_name == "set_priority"}
        bz = {a.task_kwargs["stage"]: a.task_kwargs["size"]
              for a in decision.actions if a.task_name == "set_batch_size"}
        assert pr == {"s1": 1, "s3": 3}
        assert bz == {"s2": 40}

    def test_factory_passes_system_prompt(self):
        pol = make_scheduling_policy(self._op(), kind="llm", llm_api_key="k",
                                     system_prompt="ABC")
        # LLMSchedulingPolicy embeds its own fallback — no outer wrapper; the
        # returned policy itself carries the system_prompt.
        assert pol.system_prompt == "ABC"
