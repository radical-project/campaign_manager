"""Unit tests for the src.campaign.adr bridge (Operator + policies + view).

All tests run against a FakeView — no live CampaignManager, no asyncflow
engine, and no LLM key required.
"""

import pytest

pytest.importorskip("radical.adr")

from radical.adr import Decision, Policy, decide  # noqa: E402
from radical.adr import goals as _goals  # noqa: E402
from radical.adr.goals import Goal  # noqa: E402

from src.campaign.adr import (
    BanditSchedulingPolicy,
    CampaignOperator,
    DownstreamFirstPolicy,
    LLMSchedulingPolicy,
    RuleCorrectionsPolicy,
    ScheduleDecision,
    TelemetrySubscriber,
    make_scheduling_policy,
    resolve_system_prompt,
)
from src.campaign.adr.policies import _batch_for_bp, _stage_depth
from src.campaign.adr.policies.bandit import _downstream_bp
from src.campaign.monitor import Monitor

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


# ── Fake view ───────────────────────────────────────────────────────────────


class FakeView:
    """In-memory CampaignViewProtocol implementation that records lever calls."""

    def __init__(self, stages: dict, free_gpus: int = 2):
        self._stages = stages
        self._free_gpus = free_gpus
        self.priority_calls: list = []
        self.batch_calls: list = []
        self.trigger_calls: list = []

    def observe(self) -> dict:
        terminal = [list(self._stages)[-1]] if self._stages else []
        return {
            "cycle": 0,
            "terminal": terminal,
            "free_cpus": 0,
            "free_gpus": self._free_gpus,
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


def _stage(deps=(), queue_depth=0, bp_state="HOLD", priority=0, pending=0, running=0, cap=4,
           finished=0, stalls=0, avg_duration_s=None, score_p50=None, score_p90=None,
           requires_gpu=False):
    return {
        "status": "running",
        "priority": priority,
        "started": 0,
        "running": running,
        "finished": finished,
        "cap": cap,
        "ready": True,
        "deps": list(deps),
        "queue_depth": queue_depth,
        "bp_state": bp_state,
        "pending": pending,
        "starved": bool(pending > 0 and running < cap and deps),
        "is_source": not deps,
        "requires_gpu": requires_gpu,
        "stalls": stalls,
        "avg_duration_s": avg_duration_s,
        "score_p50": score_p50,
        "score_p90": score_p90,
    }


def _cascade(**overrides):
    stages = {
        "s1": _stage(),
        "s2": _stage(deps=["s1"]),
        "s3": _stage(deps=["s2"]),
    }
    for name, patch in overrides.items():
        stages[name].update(patch)
    return stages


def _priorities_from(decision) -> dict:
    """Extract {stage: priority} from a Decision's actions."""
    return {
        a.task_kwargs["stage"]: a.task_kwargs["priority"]
        for a in decision.actions
        if a.task_name == "set_priority"
    }


class _ConstantPriorityPolicy(Policy):
    """Always returns a fixed set of priority assignments.  Useful for isolating wrappers."""

    def __init__(self, op, priorities: dict, *, stop: bool = False) -> None:
        super().__init__()
        self._act = op.get_actions()
        self._priorities = priorities
        self._stop = stop

    @decide
    async def run(self, obs: dict) -> Decision:
        return Decision(
            actions=[self._act.set_priority(stage=s, priority=p) for s, p in self._priorities.items()],
            stop=self._stop,
        )


# ── Concrete test operator (CampaignOperator is a base class — no goals alone) ──


class _TestOp(CampaignOperator):
    """Minimal concrete campaign operator for tests."""

    n_target: int = 5

    def __init__(self, view, engine=None, *, n_target: int = 5, **kwargs):
        super().__init__(view, engine=engine, **kwargs)
        self.n_target = int(n_target)
        self._validate_stopping_condition()

    @_goals
    def criteria(self):
        if self.n_target <= 0:
            return []
        return Goal(name="done", metric="n_hits",
                    threshold=self.n_target - 0.5, direction="maximize")


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
        op = _TestOp(view, engine=None)
        policy = DownstreamFirstPolicy(op)
        decision = await policy.run(view.observe())
        pr = {
            a.task_kwargs["stage"]: a.task_kwargs["priority"]
            for a in decision.actions
            if a.task_name == "set_priority"
        }
        assert pr["s3"] > pr["s2"] > pr["s1"]
        assert set(pr) == {"s1", "s2", "s3"}

    async def test_ranks_regardless_of_free_slots(self):
        view = FakeView(_cascade(), free_gpus=0)
        op = _TestOp(view, engine=None)
        policy = DownstreamFirstPolicy(op)
        decision = await policy.run(view.observe())
        pr = [a for a in decision.actions if a.task_name == "set_priority"]
        assert len(pr) == 3

    async def test_batch_resized_under_throttle(self):
        view = FakeView(_cascade(s2={"bp_state": "THROTTLE"}))
        op = _TestOp(view, engine=None)
        policy = DownstreamFirstPolicy(op)
        decision = await policy.run(view.observe())
        batch = {
            a.task_kwargs["stage"]: a.task_kwargs["size"]
            for a in decision.actions
            if a.task_name == "set_batch_size"
        }
        assert batch.get("s2") == 25

    async def test_empty_stages_is_noop(self):
        view = FakeView({})
        op = _TestOp(view, engine=None, n_target=0, max_cycles=1)
        policy = DownstreamFirstPolicy(op)
        decision = await policy.run(view.observe())
        assert decision.actions == []


# ── Bandit policy ─────────────────────────────────────────────────────────────


class TestBanditSchedulingPolicy:
    async def test_emits_priority_for_every_stage(self):
        view = FakeView(_cascade())
        op = _TestOp(view, engine=None)
        policy = BanditSchedulingPolicy(op, seed=0)
        decision = await policy.run(view.observe())
        ranked = [a.task_kwargs["stage"] for a in decision.actions if a.task_name == "set_priority"]
        assert set(ranked) == {"s1", "s2", "s3"}

    async def test_downstream_bp_reward_mapping(self):
        stages = _cascade(s2={"bp_state": "WIDEN"}, s3={"bp_state": "THROTTLE"})
        assert _downstream_bp("s1", stages) == "WIDEN"
        assert _downstream_bp("s2", stages) == "THROTTLE"
        assert _downstream_bp("s3", stages) is None

    async def test_bandit_learns_downstream_first(self):
        view = FakeView(_cascade(s2={"bp_state": "WIDEN"}))
        op = _TestOp(view, engine=None, n_target=999)
        policy = BanditSchedulingPolicy(op, seed=1)
        for _ in range(40):
            view._stages["s1"]["finished"] += 1
            await policy.run(view.observe())
        means = policy.summary
        assert means["s1"] > means["s3"]

    async def test_warmstart_priors_depth_based(self):
        view = FakeView(_cascade())
        op = _TestOp(view, engine=None)
        policy = BanditSchedulingPolicy(op, seed=0, warmstart=True)
        await policy.run(view.observe())
        means = policy.summary
        assert means["s3"] > means["s1"]


# ── Operator end-to-end (one cycle, rule policy) ──────────────────────────────


class TestOperatorCycle:
    async def test_one_cycle_applies_levers_to_view(self):
        view = FakeView(_cascade(s3={"queue_depth": 5}))
        op = _TestOp(view, engine=None)
        op.policy = DownstreamFirstPolicy(op)

        async for _snapshot in op.run():
            break  # one cycle is enough

        pr = dict(view.priority_calls)
        assert pr["s3"] > pr["s2"] > pr["s1"]

    async def test_goal_stops_when_target_reached(self):
        # s3 is terminal; n_hits = s3.finished = 5 ≥ n_target=5 → goal fires cycle 1
        view = FakeView(_cascade(s3={"finished": 5}))
        op = _TestOp(view, engine=None, n_target=5)
        op.policy = DownstreamFirstPolicy(op)

        cycles = 0
        async for _snapshot in op.run():
            cycles += 1
            if cycles > 3:
                break
        assert cycles == 1  # goal satisfied on first cycle → stop

    async def test_n_hits_computed_from_terminal_stage(self):
        """Base @observe computes n_hits from terminal stage finished count."""

        view = FakeView(_cascade(s3={"finished": 7}))
        op = _TestOp(view, engine=None, n_target=10)

        # The @observe method is inherited; call it directly to inspect obs.
        snap = op.state.snapshot()
        obs = type(op)._adl_observe_fn(op, snap)
        assert obs["n_hits"] == 7
        assert obs["terminal"] == ["s3"]


# ── Inheritance: @act / @observe / @goals propagated to subclasses ────────────


class TestInheritance:
    def test_act_registry_inherited(self):
        view = FakeView(_cascade())
        op = _TestOp(view, engine=None)
        acts = op.get_actions()
        # These are defined on CampaignOperator and must be visible to subclass.
        assert callable(acts.set_priority)
        assert callable(acts.set_batch_size)
        assert callable(acts.trigger)

    def test_observe_fn_inherited_when_not_overridden(self):
        view = FakeView(_cascade())
        op = _TestOp(view, engine=None)
        # _TestOp doesn't define @observe; should inherit from CampaignOperator.
        assert type(op)._adl_observe_fn is not None

    def test_goals_fn_overridden_in_subclass(self):
        view = FakeView(_cascade())
        op = _TestOp(view, engine=None)
        # _TestOp defines @goals; its fn should be used (not CampaignOperator's).
        raw = type(op)._adl_goals_fn(op)
        goals_list = raw if isinstance(raw, list) else [raw]
        assert len(goals_list) == 1
        assert goals_list[0].metric == "n_hits"

    def test_validate_stopping_condition_raises_with_no_goals_no_cycles(self):
        view = FakeView({})

        class _NoGoalsOp(CampaignOperator):
            def __init__(self, view, engine=None, **kwargs):
                super().__init__(view, engine=engine, **kwargs)
                self._validate_stopping_condition()

        with pytest.raises(ValueError, match="no stopping condition"):
            _NoGoalsOp(view, engine=None)

    def test_validate_passes_with_max_cycles(self):
        view = FakeView({})

        class _NoGoalsOp(CampaignOperator):
            def __init__(self, view, engine=None, **kwargs):
                super().__init__(view, engine=engine, **kwargs)
                self._validate_stopping_condition()

        # Should not raise when max_cycles is set.
        _NoGoalsOp(view, engine=None, max_cycles=10)


# ── Composition factory + LLM guard ───────────────────────────────────────────


class TestFactory:
    def test_default_kind_wraps_rule_with_corrections(self):
        view = FakeView(_cascade())
        op = _TestOp(view, engine=None)
        policy = make_scheduling_policy(op)
        assert isinstance(policy, RuleCorrectionsPolicy)
        assert policy._correct_stalls is True
        assert policy._correct_budget is True

    def test_kind_downstream_first_wraps_with_corrections(self):
        view = FakeView(_cascade())
        op = _TestOp(view, engine=None)
        policy = make_scheduling_policy(op, kind="downstream_first")
        assert isinstance(policy, RuleCorrectionsPolicy)
        assert policy._correct_budget is True

    def test_kind_bandit_wraps_with_stalls_only(self):
        view = FakeView(_cascade())
        op = _TestOp(view, engine=None)
        policy = make_scheduling_policy(op, kind="bandit", warmstart=True)
        assert isinstance(policy, RuleCorrectionsPolicy)
        assert policy._correct_stalls is True
        assert policy._correct_budget is False  # bandit updates on outcomes — no budget demotion

    def test_kind_null_returns_null_policy(self):
        view = FakeView(_cascade())
        op = _TestOp(view, engine=None)
        from src.campaign.adr import NullSchedulingPolicy
        assert isinstance(make_scheduling_policy(op, kind="null"), NullSchedulingPolicy)

    def test_kind_llm_requires_key(self):
        view = FakeView(_cascade())
        op = _TestOp(view, engine=None)
        with pytest.raises(ValueError):
            make_scheduling_policy(op, kind="llm", llm_api_key=None)

    def test_llm_policy_requires_optional_deps(self):
        view = FakeView(_cascade())
        op = _TestOp(view, engine=None)
        import importlib.util

        if importlib.util.find_spec("instructor") and importlib.util.find_spec("openai"):
            pytest.skip("openai+instructor installed — guard path not exercised")
        with pytest.raises(ImportError):
            LLMSchedulingPolicy("fake-key", op, system_prompt="PROMPT")

    def test_llm_policy_requires_system_prompt(self):
        view = FakeView(_cascade())
        op = _TestOp(view, engine=None)
        import importlib.util

        if not (importlib.util.find_spec("instructor") and importlib.util.find_spec("openai")):
            pytest.skip("openai+instructor not installed")
        with pytest.raises(ValueError, match="system_prompt"):
            LLMSchedulingPolicy("fake-key", op)


class TestRuleCorrectionsPolicy:
    """Unit tests for RuleCorrectionsPolicy priority corrections."""

    def _op(self, stages=None):
        view = FakeView(stages or _cascade())
        return _TestOp(view, engine=None)

    async def test_no_correction_when_no_stalls_no_alerts(self):
        op = self._op()
        inner = _ConstantPriorityPolicy(op, {"s1": 101, "s2": 102, "s3": 103})
        policy = RuleCorrectionsPolicy(inner, op)
        d = await policy.run(FakeView(_cascade()).observe())
        pr = _priorities_from(d)
        assert pr == {"s1": 101, "s2": 102, "s3": 103}

    async def test_stall_boost_fires_at_threshold(self):
        stages = _cascade(s2={"stalls": 3})
        op = self._op(stages)
        inner = _ConstantPriorityPolicy(op, {"s1": 101, "s2": 102, "s3": 103})
        policy = RuleCorrectionsPolicy(inner, op)
        d = await policy.run(FakeView(stages).observe())
        pr = _priorities_from(d)
        assert pr["s2"] == 108   # 102 + 6 (stalls=3 → +6 with formula min(2*stalls,6))
        assert pr["s1"] == 101   # no stalls
        assert pr["s3"] == 103   # no stalls

    async def test_stall_boost_proportional_and_capped(self):
        # min(2*stalls, 6): fires at stall 1 (+2), reaches cap at stall 3 (+6)
        for stalls, expected_delta in [(0, 0), (1, 2), (2, 4), (3, 6), (4, 6), (99, 6)]:
            stages = _cascade(s2={"stalls": stalls})
            op = self._op(stages)
            inner = _ConstantPriorityPolicy(op, {"s2": 100})
            policy = RuleCorrectionsPolicy(inner, op)
            d = await policy.run(FakeView(stages).observe())
            pr = _priorities_from(d)
            assert pr["s2"] == 100 + expected_delta, f"stalls={stalls}: expected {100 + expected_delta}, got {pr['s2']}"

    async def test_frozen_demotion(self):
        stages = _cascade()
        obs = FakeView(stages).observe()
        obs["budget_controllers"] = {"s2": {"frozen": True, "consecutive_bound_hits": 3,
                                             "burn_ratio": 1.4, "progress": 0.5, "score_at_bound": False}}
        op = self._op(stages)
        inner = _ConstantPriorityPolicy(op, {"s1": 101, "s2": 102, "s3": 103})
        policy = RuleCorrectionsPolicy(inner, op, correct_budget=True)
        d = await policy.run(obs)
        pr = _priorities_from(d)
        assert pr["s2"] == 101   # 102 - 1
        assert pr["s1"] == 101   # unchanged
        assert pr["s3"] == 103   # unchanged

    async def test_budget_burn_demotion(self):
        stages = _cascade()
        obs = FakeView(stages).observe()
        obs["monitor_alerts"] = {"s2": ["budget_burn"]}
        op = self._op(stages)
        inner = _ConstantPriorityPolicy(op, {"s1": 101, "s2": 102, "s3": 103})
        policy = RuleCorrectionsPolicy(inner, op, correct_budget=True)
        d = await policy.run(obs)
        pr = _priorities_from(d)
        assert pr["s2"] == 101   # 102 - 1

    async def test_frozen_and_budget_burn_stack(self):
        stages = _cascade()
        obs = FakeView(stages).observe()
        obs["budget_controllers"] = {"s2": {"frozen": True, "consecutive_bound_hits": 1,
                                             "burn_ratio": 1.2, "progress": 0.3, "score_at_bound": False}}
        obs["monitor_alerts"] = {"s2": ["budget_burn"]}
        op = self._op(stages)
        inner = _ConstantPriorityPolicy(op, {"s2": 102})
        policy = RuleCorrectionsPolicy(inner, op, correct_budget=True)
        d = await policy.run(obs)
        pr = _priorities_from(d)
        assert pr["s2"] == 100   # 102 - 1 (frozen) - 1 (budget_burn)

    async def test_stall_and_frozen_stack(self):
        stages = _cascade(s2={"stalls": 3})
        obs = FakeView(stages).observe()
        obs["budget_controllers"] = {"s2": {"frozen": True, "consecutive_bound_hits": 1,
                                             "burn_ratio": 1.1, "progress": 0.4, "score_at_bound": False}}
        op = self._op(stages)
        inner = _ConstantPriorityPolicy(op, {"s2": 102})
        policy = RuleCorrectionsPolicy(inner, op, correct_stalls=True, correct_budget=True)
        d = await policy.run(obs)
        pr = _priorities_from(d)
        assert pr["s2"] == 107   # 102 + 6 (stalls=3) - 1 (frozen) = 107

    async def test_correct_budget_false_ignores_frozen_and_burn(self):
        stages = _cascade()
        obs = FakeView(stages).observe()
        obs["budget_controllers"] = {"s2": {"frozen": True, "consecutive_bound_hits": 5,
                                             "burn_ratio": 1.5, "progress": 0.2, "score_at_bound": True}}
        obs["monitor_alerts"] = {"s2": ["budget_burn"]}
        op = self._op(stages)
        inner = _ConstantPriorityPolicy(op, {"s2": 102})
        policy = RuleCorrectionsPolicy(inner, op, correct_stalls=True, correct_budget=False)
        d = await policy.run(obs)
        pr = _priorities_from(d)
        assert pr["s2"] == 102   # no budget corrections applied

    async def test_stop_preserved_from_inner(self):
        stages = _cascade()
        op = self._op(stages)
        inner = _ConstantPriorityPolicy(op, {"s1": 101}, stop=True)
        policy = RuleCorrectionsPolicy(inner, op)
        d = await policy.run(FakeView(stages).observe())
        assert d.stop is True

    async def test_batch_actions_preserved_through_correction(self):
        stages = _cascade(s2={"stalls": 3})
        op = self._op(stages)

        class _WithBatch(Policy):
            def __init__(self, op_):
                super().__init__()
                self._act = op_.get_actions()

            @decide
            async def run(self, obs):
                return Decision(actions=[
                    self._act.set_priority(stage="s2", priority=102),
                    self._act.set_batch_size(stage="s2", size=25),
                ])

        policy = RuleCorrectionsPolicy(_WithBatch(op), op)
        d = await policy.run(FakeView(stages).observe())

        batches = [a for a in d.actions if a.task_name == "set_batch_size"]
        assert len(batches) == 1
        assert batches[0].task_kwargs["size"] == 25
        assert _priorities_from(d)["s2"] == 108  # corrected: 102 + 6 (stalls=3)

    async def test_stage_not_in_inner_uses_live_obs_priority(self):
        # Inner only assigns s3; s2 has stalls but inner didn't touch it.
        stages = _cascade(s2={"stalls": 3, "priority": 50})
        op = self._op(stages)
        inner = _ConstantPriorityPolicy(op, {"s3": 103})
        policy = RuleCorrectionsPolicy(inner, op)
        d = await policy.run(FakeView(stages).observe())
        pr = _priorities_from(d)
        assert pr["s2"] == 56    # 50 (live obs) + 6 (stalls=3)
        assert pr["s3"] == 103   # untouched

    async def test_corrections_log_line_emitted(self, capsys):
        stages = _cascade(s2={"stalls": 3})
        op = self._op(stages)
        inner = _ConstantPriorityPolicy(op, {"s1": 101, "s2": 102, "s3": 103})
        policy = RuleCorrectionsPolicy(inner, op)
        await policy.run(FakeView(stages).observe())
        out = capsys.readouterr().out
        assert "[corrections]" in out
        assert "s2+6" in out

    async def test_no_log_line_when_no_corrections(self, capsys):
        stages = _cascade()  # all stalls=0, no alerts
        op = self._op(stages)
        inner = _ConstantPriorityPolicy(op, {"s1": 101, "s2": 102, "s3": 103})
        policy = RuleCorrectionsPolicy(inner, op)
        await policy.run(FakeView(stages).observe())
        out = capsys.readouterr().out
        assert "[corrections]" not in out

    def test_regime_forwarded_from_inner(self):
        view = FakeView(_cascade())
        op = _TestOp(view, engine=None)

        class _RegimeInner(Policy):
            _regime = "TEST_REGIME"

            def __init__(self, op_):
                super().__init__()
                self._act = op_.get_actions()

            @decide
            async def run(self, obs):
                return Decision(actions=[])

        policy = RuleCorrectionsPolicy(_RegimeInner(op), op)
        assert policy._regime == "TEST_REGIME"

    async def test_empty_stages_passes_through_unchanged(self):
        view = FakeView({})
        op = _TestOp(view, engine=None, n_target=0, max_cycles=1)
        inner = _ConstantPriorityPolicy(op, {})
        policy = RuleCorrectionsPolicy(inner, op)
        d = await policy.run(view.observe())
        assert d.actions == []


class TestMonitorActiveAlerts:
    def test_empty_when_no_breaches(self):
        assert Monitor().active_alerts() == {}

    def test_budget_burn_alert_appears_after_breach(self):
        m = Monitor(burn_dev_pct=10.0)
        m.check_budget("s1", spent=120.0, expected=100.0)  # 20% > 10% threshold
        alerts = m.active_alerts()
        assert "s1" in alerts
        assert "budget_burn" in alerts["s1"]

    def test_alert_cleared_when_deviation_normalises(self):
        m = Monitor(burn_dev_pct=10.0)
        m.check_budget("s1", spent=120.0, expected=100.0)  # breach
        m.check_budget("s1", spent=100.0, expected=100.0)  # back within bounds → reset
        assert m.active_alerts() == {}

    def test_multiple_kinds_per_stage(self):
        m = Monitor(burn_dev_pct=10.0, passthrough_dev_pct=10.0)
        m.check_budget("s1", spent=120.0, expected=100.0)
        m.check_passthrough("s1", observed=0.1, expected=0.5)
        kinds = set(m.active_alerts()["s1"])
        assert kinds == {"budget_burn", "pass_through"}

    def test_multiple_stages_independent(self):
        m = Monitor(burn_dev_pct=10.0)
        m.check_budget("s1", spent=120.0, expected=100.0)
        m.check_budget("s2", spent=100.0, expected=100.0)  # within bounds
        alerts = m.active_alerts()
        assert "s1" in alerts
        assert "s2" not in alerts


class TestPolicyRecorder:
    async def test_records_jsonl_per_cycle(self, tmp_path):
        import json

        from src.campaign.adr import PolicyRecorder

        view = FakeView(_cascade(s3={"queue_depth": 5}))
        path = tmp_path / "decisions.jsonl"
        rec = PolicyRecorder(path, policy_kind="rule")
        op = _TestOp(view, engine=None, n_target=3, observer=rec)
        op.policy = DownstreamFirstPolicy(op)
        rec.bind(view=view, policy=op.policy)

        cycles = 0
        async for _snapshot in op.run():
            cycles += 1
            if cycles >= 2:
                break

        rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        assert rows, "recorder wrote no rows"
        assert rows[0]["policy"] == "rule"
        assert "priorities" in rows[0] and "stages" in rows[0]
        assert rows[0]["priorities"]["s3"] > rows[0]["priorities"]["s1"]

    async def test_bandit_summary_recorded(self, tmp_path):
        import json

        from src.campaign.adr import BanditSchedulingPolicy, PolicyRecorder

        view = FakeView(_cascade())
        path = tmp_path / "bandit.jsonl"
        rec = PolicyRecorder(path, policy_kind="bandit")
        op = _TestOp(view, engine=None, n_target=999, observer=rec)
        op.policy = BanditSchedulingPolicy(op, seed=0, warmstart=True)
        rec.bind(view=view, policy=op.policy)

        async for _snapshot in op.run():
            break

        rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        assert rows[0]["summary"], "bandit posterior summary not recorded"


class TestLLMTimeoutFallback:
    async def test_timing_out_primary_falls_back_to_rule(self):
        import asyncio

        view = FakeView(_cascade(), free_gpus=2)
        op = _TestOp(view, engine=None)

        class _TimeoutPrimary(Policy):
            @decide
            async def run(self, obs):
                raise asyncio.TimeoutError("simulated hung LLM call")

        composed = Policy(primary=_TimeoutPrimary(), fallback=DownstreamFirstPolicy(op), timeout=1.0)
        decision = await composed.decide(view.observe())
        pr = {
            a.task_kwargs["stage"]: a.task_kwargs["priority"]
            for a in decision.actions
            if a.task_name == "set_priority"
        }
        assert pr and pr["s3"] > pr["s1"]


def test_schedule_decision_defaults():
    sd = ScheduleDecision()
    assert sd.priorities == {}
    assert sd.batch_sizes is None
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
    def __init__(
        self,
        deps=(),
        replicas=0,
        started=0,
        finished=0,
        cap=4,
        priority=0,
        ready=True,
        status="running",
        stalls=0,
        workflow_config=None,
    ):
        self.dependencies = list(deps)
        self.replicas = replicas
        self.started_count = started
        self.finished_replicas = finished
        self.concurrency_cap = cap
        self.priority = priority
        self.ready = ready
        self.status = status
        self._consecutive_stalls = stalls
        self.failed_replicas = 0
        self.workflow_config = workflow_config


class _FakeResources:
    available_cpus = 32
    available_gpus = 2


class _FakeMetrics:
    """Minimal metrics stub required by CampaignView.observe()."""
    shard_events = []
    budget_events = []

    def stage_wall_s(self, stage: str) -> float:
        return 0.0


class _FakeState:
    def __init__(self, workflows, triages=None, budget_controllers=None):
        self.workflows = workflows
        self.sharders = {}
        self.bp = {}
        self.resources = _FakeResources()
        self.triages = triages or {}
        self.budget_controllers = budget_controllers or {}


class _FakeCM:
    def __init__(self, workflows, triages=None, budget_controllers=None):
        self.state = _FakeState(workflows, triages=triages, budget_controllers=budget_controllers)
        self._plan = None

    def metrics(self) -> _FakeMetrics:
        return _FakeMetrics()


class TestCampaignView:
    def _view(self, telemetry_subscriber=None):
        from src.campaign.adr import CampaignView

        wfs = {
            "s1": _FakeWf(deps=(), replicas=100, started=4, finished=2, cap=4),
            "s2": _FakeWf(deps=["s1"], replicas=20, started=8, finished=2, cap=16),
            "s3": _FakeWf(deps=["s2"], replicas=2, started=2, finished=2, cap=12),
        }
        return CampaignView(_FakeCM(wfs), terminal="s3", telemetry_subscriber=telemetry_subscriber)

    def test_terminal_is_list(self):
        obs = self._view().observe()
        assert isinstance(obs["terminal"], list)
        assert obs["terminal"] == ["s3"]

    def test_no_hits_or_target_in_obs(self):
        obs = self._view().observe()
        assert "hits" not in obs
        assert "target" not in obs

    def test_exposes_pending_and_backlog(self):
        obs = self._view().observe()
        assert obs["stages"]["s2"]["pending"] == 12
        assert obs["stages"]["s1"]["pending"] == 96

    def test_is_source_flag(self):
        st = self._view().observe()["stages"]
        assert st["s1"]["is_source"] is True
        assert st["s2"]["is_source"] is False

    def test_starved_only_for_resource_limited_dependent(self):
        st = self._view().observe()["stages"]
        assert st["s2"]["starved"] is True
        assert st["s1"]["starved"] is False
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

    def test_terminal_inferred_as_list(self):
        from src.campaign.adr import CampaignView

        wfs = {
            "s1": _FakeWf(deps=()),
            "s2": _FakeWf(deps=["s1"]),
            "s3": _FakeWf(deps=["s2"]),
        }
        view = CampaignView(_FakeCM(wfs))
        obs = view.observe()
        assert obs["terminal"] == ["s3"]

    def test_terminal_string_coerced_to_list(self):
        from src.campaign.adr import CampaignView

        wfs = {"a": _FakeWf(), "b": _FakeWf()}
        view = CampaignView(_FakeCM(wfs), terminal="a")
        assert view.observe()["terminal"] == ["a"]

    def test_terminal_list_accepted_directly(self):
        from src.campaign.adr import CampaignView

        wfs = {"a": _FakeWf(), "b": _FakeWf()}
        view = CampaignView(_FakeCM(wfs), terminal=["a", "b"])
        assert view.observe()["terminal"] == ["a", "b"]


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
        s = TelemetrySubscriber(None, alpha=1.0)
        s._on_event(
            _Ev(event_type="ResourceUpdate", resource_scope="per_gpu", gpu_id=0, gpu_percent=80.0)
        )
        s._on_event(
            _Ev(event_type="ResourceUpdate", resource_scope="per_gpu", gpu_id=1, gpu_percent=60.0)
        )
        s._on_event(
            _Ev(
                event_type="ResourceUpdate",
                resource_scope="per_node",
                gpu_id=None,
                gpu_percent=None,
                cpu_percent=45.0,
                memory_percent=70.0,
            )
        )
        s._on_event(_Ev(event_type="TaskCompleted", duration_seconds=2.0))
        s._on_event(_Ev(event_type="TaskCompleted", duration_seconds=4.0))
        s._on_event(_Ev(event_type="TaskFailed"))
        snap = s.snapshot()
        assert snap["gpu_util"] == 70.0
        assert snap["cpu_util"] == 45.0
        assert snap["mem_util"] == 70.0
        assert snap["avg_task_duration_s"] == 3.0
        assert snap["task_fail_rate"] == 1 / 3

    def test_subscribe_wired_when_telemetry_given(self):
        captured = []

        class _Mgr:
            def subscribe(self, cb):
                captured.append(cb)

        TelemetrySubscriber(_Mgr())
        assert len(captured) == 1


# ── resolve_system_prompt ─────────────────────────────────────────────────────


class TestResolveSystemPrompt:
    def test_inline_wins(self):
        result = resolve_system_prompt({"system_prompt": "X", "system_prompt_file": "ignored.txt"})
        # Schema is auto-appended, so result starts with the inline prompt.
        assert result.startswith("X")

    def test_file_read_relative_to_config_dir(self, tmp_path):
        (tmp_path / "p.txt").write_text("PROMPT-FROM-FILE")
        out = resolve_system_prompt({"system_prompt_file": "p.txt"}, config_dir=tmp_path)
        assert out.startswith("PROMPT-FROM-FILE")

    def test_schema_auto_appended(self, tmp_path):
        (tmp_path / "p.txt").write_text("BASE")
        out = resolve_system_prompt({"system_prompt_file": "p.txt"}, config_dir=tmp_path)
        assert "OBSERVATION SCHEMA" in out

    def test_none_when_unset(self):
        assert resolve_system_prompt({}) is None


# ── LLM policy config (deps installed) ────────────────────────────────────────


class TestLLMPolicyConfig:
    def _op(self):
        view = FakeView(_cascade())
        return _TestOp(view, engine=None)

    def test_system_prompt_required(self):
        import importlib.util

        if not (importlib.util.find_spec("instructor") and importlib.util.find_spec("openai")):
            pytest.skip("openai+instructor not installed")
        with pytest.raises(ValueError, match="system_prompt"):
            LLMSchedulingPolicy("k", self._op())

    def test_system_prompt_override(self):
        import importlib.util

        if not (importlib.util.find_spec("instructor") and importlib.util.find_spec("openai")):
            pytest.skip("openai+instructor not installed")
        pol = LLMSchedulingPolicy("k", self._op(), system_prompt="CUSTOM PROMPT")
        assert pol.system_prompt == "CUSTOM PROMPT"

    def test_to_decision_maps_both_levers(self):
        import importlib.util

        if not (importlib.util.find_spec("instructor") and importlib.util.find_spec("openai")):
            pytest.skip("openai+instructor not installed")
        pol = LLMSchedulingPolicy("k", self._op(), system_prompt="PROMPT")
        sd = ScheduleDecision(priorities={"s1": 1, "s3": 3}, batch_sizes={"s2": 40}, stop=False)
        decision = pol._to_decision(sd)
        pr = {
            a.task_kwargs["stage"]: a.task_kwargs["priority"]
            for a in decision.actions
            if a.task_name == "set_priority"
        }
        bz = {
            a.task_kwargs["stage"]: a.task_kwargs["size"]
            for a in decision.actions
            if a.task_name == "set_batch_size"
        }
        assert pr == {"s1": 1, "s3": 3}
        assert bz == {"s2": 40}

    def test_factory_passes_system_prompt(self):
        import importlib.util

        if not (importlib.util.find_spec("instructor") and importlib.util.find_spec("openai")):
            pytest.skip("openai+instructor not installed")
        pol = make_scheduling_policy(self._op(), kind="llm", llm_api_key="k",
                                     system_prompt="ABC")
        assert pol.system_prompt == "ABC"


# ── #4: trigger_* routing in _triggered_stages / is_source / _infer_terminal ──


class TestCampaignViewTriggerRouting:
    """CampaignView correctly handles DAGs wired via trigger_* config keys
    (i.e. _on_completion routing) rather than config-level dependencies:.
    """

    def _view_trigger(self):
        """ddsim topology: ddsim_a/b → analysis, wired via trigger_analysis key."""
        from src.campaign.adr import CampaignView

        wfs = {
            "ddsim_a": _FakeWf(
                deps=(),
                replicas=10, started=2, finished=1, cap=4,
                workflow_config={"trigger_analysis": "analysis", "duration": 0.1},
            ),
            "ddsim_b": _FakeWf(
                deps=(),
                replicas=10, started=1, finished=0, cap=4,
                workflow_config={"trigger_analysis": "analysis", "duration": 0.3},
            ),
            "analysis": _FakeWf(
                deps=(),  # no config-level deps — routed via _on_completion
                replicas=5, started=3, finished=1, cap=8,
            ),
        }
        cm = _FakeCM(wfs)
        return CampaignView(cm, terminal="analysis")

    def test_triggered_stages_identifies_analysis(self):
        from src.campaign.adr import CampaignView
        wfs = {
            "ddsim_a": _FakeWf(workflow_config={"trigger_analysis": "analysis"}),
            "analysis": _FakeWf(),
        }
        view = CampaignView(_FakeCM(wfs), terminal="analysis")
        assert view._triggered_stages() == {"analysis"}

    def test_triggered_stages_ignores_float_trigger_keys(self):
        from src.campaign.adr import CampaignView
        wfs = {
            "src": _FakeWf(workflow_config={"trigger_fraction": 0.5, "trigger_next": "dst"}),
            "dst": _FakeWf(),
        }
        view = CampaignView(_FakeCM(wfs), terminal="dst")
        # trigger_fraction is a float, not a stage name — must be skipped
        assert view._triggered_stages() == {"dst"}

    def test_triggered_stages_ignores_unknown_stage_names(self):
        from src.campaign.adr import CampaignView
        wfs = {
            "src": _FakeWf(workflow_config={"trigger_ghost": "nonexistent_stage"}),
            "dst": _FakeWf(),
        }
        view = CampaignView(_FakeCM(wfs), terminal="dst")
        assert view._triggered_stages() == set()  # "nonexistent_stage" not in wfs

    def test_is_source_false_for_trigger_downstream(self):
        obs = self._view_trigger().observe()
        # analysis has no deps but is downstream via trigger_analysis → not a source
        assert obs["stages"]["analysis"]["is_source"] is False

    def test_is_source_true_for_trigger_origins(self):
        obs = self._view_trigger().observe()
        # ddsim_a/b have no deps and don't appear in any trigger_* value → source
        assert obs["stages"]["ddsim_a"]["is_source"] is True
        assert obs["stages"]["ddsim_b"]["is_source"] is True

    def test_starved_true_for_trigger_downstream_with_pending(self):
        obs = self._view_trigger().observe()
        # analysis: pending=5-3=2, running=3-1=2, cap=8 → running < cap and not source
        assert obs["stages"]["analysis"]["starved"] is True

    def test_infer_terminal_with_trigger_routing(self):
        from src.campaign.adr import CampaignView
        wfs = {
            "producer": _FakeWf(workflow_config={"trigger_consumer": "consumer"}),
            "consumer": _FakeWf(),
        }
        view = CampaignView(_FakeCM(wfs))
        # consumer is triggered by producer → it's not a source → terminal must be consumer
        assert view._terminal == ["consumer"]

    def test_infer_terminal_no_trigger_falls_back_to_last(self):
        from src.campaign.adr import CampaignView
        # No trigger_* keys, no deps → ambiguous; falls back to last workflow
        wfs = {"a": _FakeWf(), "b": _FakeWf()}
        view = CampaignView(_FakeCM(wfs))
        assert view._terminal == ["b"]

    def test_no_note_warning_in_stage_dict(self):
        # Regression: starved must be correct without any NOTE workaround
        obs = self._view_trigger().observe()
        for _name, st in obs["stages"].items():
            assert "starved" in st
            assert isinstance(st["starved"], bool)


# ── #9: set_score_cutoff lever and budget_controllers obs ─────────────────────


class _FakeTriage:
    """Minimal Triage stub exposing score_cutoff and nudge_cutoffs."""

    def __init__(self, score_cutoff=0.5, bounds=(0.0, 1.0)):
        self.score_cutoff = score_cutoff
        self.score_cutoff_bounds = bounds
        self.nudge_calls: list = []

    def nudge_cutoffs(self, score_delta: float, unc_delta: float):
        self.nudge_calls.append((score_delta, unc_delta))
        self.score_cutoff = max(
            self.score_cutoff_bounds[0],
            min(self.score_cutoff_bounds[1], self.score_cutoff + score_delta),
        )
        return False, False


class _FakeBudgetController:
    def __init__(self):
        self.frozen = False
        self._consecutive_bound_hits = 0


class TestSetScoreCutoff:
    def _view_with_triage(self, score_cutoff=0.5, bounds=(0.0, 1.0)):
        from src.campaign.adr import CampaignView
        triage = _FakeTriage(score_cutoff=score_cutoff, bounds=bounds)
        wfs = {"s1": _FakeWf()}
        cm = _FakeCM(wfs, triages={"s1": triage})
        return CampaignView(cm, terminal="s1"), triage

    def test_set_score_cutoff_calls_nudge_with_correct_delta(self):
        view, triage = self._view_with_triage(score_cutoff=0.5)
        result = view.set_score_cutoff("s1", 0.7)
        assert result is True
        assert len(triage.nudge_calls) == 1
        delta, unc = triage.nudge_calls[0]
        assert delta == pytest.approx(0.2)
        assert unc == pytest.approx(0.0)

    def test_set_score_cutoff_updates_triage_value(self):
        view, triage = self._view_with_triage(score_cutoff=0.5)
        view.set_score_cutoff("s1", 0.8)
        assert triage.score_cutoff == pytest.approx(0.8)

    def test_set_score_cutoff_clamped_to_bounds(self):
        view, triage = self._view_with_triage(score_cutoff=0.5, bounds=(0.0, 1.0))
        view.set_score_cutoff("s1", 1.5)  # exceeds upper bound
        assert triage.score_cutoff == pytest.approx(1.0)

    def test_set_score_cutoff_returns_false_for_unknown_stage(self):
        view, _ = self._view_with_triage()
        assert view.set_score_cutoff("nonexistent", 0.5) is False

    def test_set_score_cutoff_returns_false_when_no_triage(self):
        from src.campaign.adr import CampaignView
        wfs = {"s1": _FakeWf()}
        cm = _FakeCM(wfs, triages={})  # s1 has no triage
        view = CampaignView(cm, terminal="s1")
        assert view.set_score_cutoff("s1", 0.6) is False

    def test_set_score_cutoff_in_operator_act_registry(self):
        view = FakeView(_cascade())
        op = _TestOp(view, engine=None)
        acts = op.get_actions()
        assert callable(acts.set_score_cutoff)


class TestBudgetControllersObWithTriage:
    def _obs_with_triage(self, score_cutoff=0.6, bounds=(0.1, 0.9)):
        from src.campaign.adr import CampaignView
        triage = _FakeTriage(score_cutoff=score_cutoff, bounds=bounds)
        bc = _FakeBudgetController()
        wfs = {"s1": _FakeWf()}
        cm = _FakeCM(wfs, triages={"s1": triage}, budget_controllers={"s1": bc})
        view = CampaignView(cm, terminal="s1")
        return view.observe()

    def test_score_cutoff_present_when_triage_active(self):
        obs = self._obs_with_triage(score_cutoff=0.6)
        bc_obs = obs["budget_controllers"]["s1"]
        assert "score_cutoff" in bc_obs
        assert bc_obs["score_cutoff"] == pytest.approx(0.6)

    def test_score_cutoff_bounds_is_list(self):
        obs = self._obs_with_triage(bounds=(0.1, 0.9))
        bc_obs = obs["budget_controllers"]["s1"]
        assert isinstance(bc_obs["score_cutoff_bounds"], list)
        assert bc_obs["score_cutoff_bounds"] == [pytest.approx(0.1), pytest.approx(0.9)]

    def test_score_cutoff_absent_when_no_triage(self):
        from src.campaign.adr import CampaignView
        bc = _FakeBudgetController()
        wfs = {"s1": _FakeWf()}
        cm = _FakeCM(wfs, triages={}, budget_controllers={"s1": bc})
        view = CampaignView(cm, terminal="s1")
        obs = view.observe()
        bc_obs = obs["budget_controllers"]["s1"]
        assert "score_cutoff" not in bc_obs
        assert "score_cutoff_bounds" not in bc_obs

    def test_existing_budget_fields_still_present(self):
        obs = self._obs_with_triage()
        bc_obs = obs["budget_controllers"]["s1"]
        for field in ("frozen", "consecutive_bound_hits", "burn_ratio",
                      "progress", "score_at_bound"):
            assert field in bc_obs

