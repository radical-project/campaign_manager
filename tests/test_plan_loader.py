"""Unit tests for src.campaign.plan — structured + legacy config loading."""

import pytest

from src.campaign import CampaignPlan, StageSpec, load_plan, plan_to_workflows_dict


class TestStructuredLoad:
    def test_loads_structured_plan(self):
        cfg = {
            "plan_id": "test-plan",
            "plan_version": 2,
            "stages": [
                {"id": "s1", "concurrency_cap": 4, "priority": 10},
                {"id": "s2", "upstream": "s1", "downstream": None,
                 "campaign_target": 5, "budget_kp": 0.002, "budget_warmup_min": 20},
            ],
        }
        plan = load_plan(cfg)
        assert isinstance(plan, CampaignPlan)
        assert plan.plan_id == "test-plan"
        assert plan.plan_version == 2
        assert {s.id for s in plan.stages} == {"s1", "s2"}

    def test_per_stage_fields_wired(self):
        cfg = {
            "plan_id": "p",
            "stages": [
                {"id": "s2", "campaign_target": 5, "budget_kp": 0.002,
                 "budget_warmup_min": 20, "downstream_input_target": 200},
            ],
        }
        plan = load_plan(cfg)
        s2 = next(s for s in plan.stages if s.id == "s2")
        assert s2.campaign_target == 5
        assert s2.budget_kp == pytest.approx(0.002)
        assert s2.budget_warmup_min == 20
        assert s2.downstream_input_target == 200

    def test_dependency_derived_from_upstream(self):
        cfg = {"plan_id": "p", "stages": [
            {"id": "s1"},
            {"id": "s2", "upstream": "s1"},
        ]}
        plan = load_plan(cfg)
        s2 = next(s for s in plan.stages if s.id == "s2")
        assert "s1" in s2.dependencies

    def test_virtual_upstream_filtered(self):
        # "library" is a virtual source, not a real stage → not a dependency
        cfg = {"plan_id": "p", "stages": [{"id": "s1", "upstream": "library"}]}
        plan = load_plan(cfg)
        s1 = plan.stages[0]
        assert "library" not in s1.dependencies


class TestLegacyLoad:
    def test_loads_legacy_workflows_dict(self):
        cfg = {"workflows": {
            "s1": {"replicas": 8, "concurrency_cap": 4, "priority": 10},
            "s2": {"dependencies": ["s1"], "concurrency_cap": 2},
        }}
        plan = load_plan(cfg)
        assert isinstance(plan, CampaignPlan)
        assert {s.id for s in plan.stages} == {"s1", "s2"}

    def test_legacy_synthesizes_plan_id(self):
        plan = load_plan({"workflows": {"s1": {"replicas": 1}}})
        assert plan.plan_id  # non-empty synthesized id


class TestRoundTrip:
    def test_plan_to_workflows_dict_preserves_stages(self):
        cfg = {"plan_id": "p", "stages": [
            {"id": "s1", "concurrency_cap": 4, "priority": 10, "replicas": 8},
            {"id": "s2", "upstream": "s1", "concurrency_cap": 2},
        ]}
        plan = load_plan(cfg)
        out = plan_to_workflows_dict(plan)
        wfs = out["workflows"]
        assert set(wfs) == {"s1", "s2"}
        assert wfs["s1"]["priority"] == 10
        assert "s1" in wfs["s2"].get("dependencies", [])


def test_invalid_source_type_raises():
    with pytest.raises(TypeError):
        load_plan(["not", "a", "dict"])
