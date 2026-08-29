"""Tests for Pattern 1 — DecisionTraceEvent / record_decision in CampaignMetrics."""

import pytest

from src.campaign.metrics import CampaignMetrics, DecisionTraceEvent


class TestDecisionTraceEvent:
    def test_dataclass_fields(self):
        ev = DecisionTraceEvent(
            cycle=3, t=1.5, policy="rule",
            actions=[{"name": "set_priority", "stage": "sim", "priority": 9}],
        )
        assert ev.cycle == 3
        assert ev.t == pytest.approx(1.5)
        assert ev.policy == "rule"
        assert ev.actions[0]["name"] == "set_priority"
        assert ev.llm_prompt_tokens is None
        assert ev.llm_completion_tokens is None

    def test_llm_token_fields_accepted(self):
        ev = DecisionTraceEvent(
            cycle=1, t=0.0, policy="llm", actions=[],
            llm_prompt_tokens=512, llm_completion_tokens=128,
        )
        assert ev.llm_prompt_tokens == 512
        assert ev.llm_completion_tokens == 128


class TestRecordDecision:
    def test_appends_to_decision_events(self):
        m = CampaignMetrics()
        m.record_decision(cycle=0, t=0.1, policy="rule", actions=[])
        assert len(m.decision_events) == 1

    def test_fields_stored_correctly(self):
        m = CampaignMetrics()
        actions = [{"name": "set_batch_size", "stage": "sim", "size": 4}]
        m.record_decision(cycle=2, t=3.0, policy="consensus", actions=actions)
        ev = m.decision_events[0]
        assert ev.cycle == 2
        assert ev.t == pytest.approx(3.0)
        assert ev.policy == "consensus"
        assert ev.actions == actions

    def test_llm_tokens_optional_default_none(self):
        m = CampaignMetrics()
        m.record_decision(cycle=0, t=0.0, policy="rule", actions=[])
        ev = m.decision_events[0]
        assert ev.llm_prompt_tokens is None
        assert ev.llm_completion_tokens is None

    def test_llm_tokens_stored_when_provided(self):
        m = CampaignMetrics()
        m.record_decision(
            cycle=1, t=1.0, policy="llm", actions=[],
            llm_prompt_tokens=1024, llm_completion_tokens=256,
        )
        ev = m.decision_events[0]
        assert ev.llm_prompt_tokens == 1024
        assert ev.llm_completion_tokens == 256

    def test_multiple_decisions_ordered(self):
        m = CampaignMetrics()
        for i in range(4):
            m.record_decision(cycle=i, t=float(i), policy="rule", actions=[])
        cycles = [ev.cycle for ev in m.decision_events]
        assert cycles == [0, 1, 2, 3]

    def test_empty_actions_list_accepted(self):
        m = CampaignMetrics()
        m.record_decision(cycle=0, t=0.0, policy="rule", actions=[])
        assert m.decision_events[0].actions == []

    def test_multiple_actions_in_one_event(self):
        m = CampaignMetrics()
        actions = [
            {"name": "set_priority", "stage": "sim", "priority": 9},
            {"name": "set_batch_size", "stage": "sim", "size": 8},
        ]
        m.record_decision(cycle=0, t=0.0, policy="rule", actions=actions)
        assert len(m.decision_events[0].actions) == 2


class TestDecisionEventsSerialization:
    def test_decision_events_key_in_to_dict(self):
        m = CampaignMetrics()
        d = m.to_dict()
        assert "decision_events" in d

    def test_empty_decision_events_serializes(self):
        m = CampaignMetrics()
        d = m.to_dict()
        assert d["decision_events"] == []

    def test_decision_event_fields_in_to_dict(self):
        m = CampaignMetrics()
        actions = [{"name": "set_priority", "stage": "sim", "priority": 7}]
        m.record_decision(cycle=5, t=2.5, policy="llm", actions=actions)
        d = m.to_dict()
        assert len(d["decision_events"]) == 1
        ev = d["decision_events"][0]
        assert ev["cycle"] == 5
        assert ev["t"] == pytest.approx(2.5)
        assert ev["policy"] == "llm"
        assert ev["actions"] == actions

    def test_llm_tokens_omitted_when_none(self):
        """llm_prompt_tokens / llm_completion_tokens not included when None."""
        m = CampaignMetrics()
        m.record_decision(cycle=0, t=0.0, policy="rule", actions=[])
        ev = m.to_dict()["decision_events"][0]
        assert "llm_prompt_tokens" not in ev
        assert "llm_completion_tokens" not in ev

    def test_llm_tokens_included_when_set(self):
        m = CampaignMetrics()
        m.record_decision(
            cycle=0, t=0.0, policy="llm", actions=[],
            llm_prompt_tokens=800, llm_completion_tokens=200,
        )
        ev = m.to_dict()["decision_events"][0]
        assert ev["llm_prompt_tokens"] == 800
        assert ev["llm_completion_tokens"] == 200

    def test_multiple_events_preserved_in_order(self):
        m = CampaignMetrics()
        for i in range(3):
            m.record_decision(
                cycle=i, t=float(i), policy="rule",
                actions=[{"stage": f"s{i}"}],
            )
        events = m.to_dict()["decision_events"]
        assert [e["cycle"] for e in events] == [0, 1, 2]
