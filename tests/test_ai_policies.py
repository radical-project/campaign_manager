"""S1 — AI policies: the ranking base, LLMSchedulingPolicy (with a stubbed
client and a local HTTP endpoint), ADRController and ADRSchedulingPolicy."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import SimpleNamespace

import pytest

from src.aucam import (
    ADRController,
    ADRSchedulingPolicy,
    CampaignBudget,
    CampaignController,
    CampaignDecision,
    CampaignManager,
    CampaignSpec,
    LaunchRequest,
    LLMSchedulingPolicy,
    RankingSchedulingPolicy,
    SchedulingContext,
    WorkflowConfig,
    WorkflowReporter,
    parse_ranking,
)
from src.aucam.scheduler import _json_from, _openai_chat

CAPACITY = {"cpus": 1024, "gpus": 64}  # the allocation tests run on


async def quick(reporter: WorkflowReporter) -> None:
    await asyncio.sleep(0.02)
    await reporter.emit(score=1.0)


def _spec(policy, controller=None, n=12, decide_timeout_s=None):
    return CampaignSpec(
        name="ai",
        budget=CampaignBudget(
            capacity=CAPACITY, max_concurrent_workflows=1, max_concurrency_per_workflow=1
        ),
        controller=controller or CampaignController(launch=["a", "b"], goal_type="b", goal_n=n),
        workflows=[WorkflowConfig(name=t, block=quick) for t in ("a", "b")],
        scheduling_policy=policy,
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
        decide_timeout_s=decide_timeout_s,
    )


def _run(spec):
    return asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))


def _ctx(cycle=0, running=None):
    return SchedulingContext(
        running_by_type=running or {},
        max_concurrency=4,
        max_concurrency_per_type=4,
        telemetry=None,
        history=[],
        policy_cycles=cycle,
        campaign_name="t",
        budget_remaining_fraction=1.0,
    )


# ── Ranking ────────────────────────────────────────────────────────────────────


def test_parse_ranking_drops_unknown_types_and_clamps_factors():
    rank, factors = parse_ranking(
        {"rank": ["b", "x", "a", "b"], "value": {"a": 50, "b": "0.01", "x": 2, "c": "?"}},
        known={"a", "b", "c"},
    )
    assert rank == ["b", "a"]
    assert factors == {"a": 10.0, "b": 0.1}
    with pytest.raises(ValueError):
        parse_ranking({"order": []})


def test_json_from_finds_the_object_in_a_chatty_reply():
    assert _json_from('Sure!\n```json\n{"rank": ["a"], "value": {}}\n```') == {
        "rank": ["a"],
        "value": {},
    }
    with pytest.raises(ValueError):
        _json_from("no json here")


def test_fresh_ranking_orders_candidates_then_falls_back_when_stale():
    pol = RankingSchedulingPolicy(max_age_cycles=2)
    reqs = [LaunchRequest.make(t) for t in ("a", "b", "c")]
    pol.set_ranking(["c", "a"], {"c": 2.0}, cycle=5)
    assert [r.workflow_type for r in pol.select(reqs, _ctx(cycle=6))] == ["c", "a", "b"]
    assert pol.value("c", None) == 2.0 and pol.value("b", None) == 1.0
    stale = pol.select(reqs, _ctx(cycle=9))  # 4 cycles old: the bandit decides
    assert not pol.fresh() and len(stale) == 3


# ── LLMSchedulingPolicy ────────────────────────────────────────────────────────


class StubModel:
    def __init__(self, reply: str) -> None:
        self.reply = reply
        self.prompts: list[str] = []

    async def __call__(self, messages, *, model, max_tokens, temperature):
        self.prompts.append(messages[-1]["content"])
        return self.reply, {"prompt_tokens": 90, "completion_tokens": 10}


def _launched(caplog) -> dict[str, int]:
    n = {"a": 0, "b": 0}
    for rec in caplog.records:
        msg = rec.getMessage()
        if "] LAUNCH " in msg:
            n[msg.split("] LAUNCH ")[1].split()[0]] += 1
    return n


def test_llm_ranking_steers_which_type_starts(caplog):
    model = StubModel('{"rank": ["b", "a"], "value": {"b": 1.0}}')
    pol = LLMSchedulingPolicy(model="m", client=model)
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        out = _run(_spec(pol, n=10))
    assert out.goal_reached
    n = _launched(caplog)
    assert n["b"] >= 10 and n["a"] <= 3, n  # only before the first ranking
    assert out.model_usage["llm_tokens"] >= 100
    summary = json.loads(model.prompts[0])
    assert {"types", "queued", "running", "by_type", "budget_remaining_fraction"} <= set(summary)
    assert "[LLM] rank=['b', 'a']" in caplog.text


def test_llm_unusable_answer_uses_the_fallback(caplog):
    pol = LLMSchedulingPolicy(model="m", client=StubModel("I would run b first."))
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        out = _run(_spec(pol, n=4))
    assert out.goal_reached
    assert "refresh=error" in caplog.text and pol.ranking is None


def test_llm_without_endpoint_warns_and_uses_the_fallback(caplog, monkeypatch):
    for var in ("AUCAM_LLM_ENDPOINT", "OPENAI_BASE_URL"):
        monkeypatch.delenv(var, raising=False)
    pol = LLMSchedulingPolicy(model="m")
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        out = _run(_spec(pol, n=3))
    assert out.goal_reached
    assert caplog.text.count("[LLM] no endpoint") == 1


def test_openai_compatible_client_talks_to_an_endpoint():
    seen = {}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            seen["path"] = self.path
            seen["auth"] = self.headers.get("Authorization")
            seen["body"] = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            reply = {
                "choices": [{"message": {"content": '{"rank": ["a"]}'}}],
                "usage": {"total_tokens": 42},
            }
            data = json.dumps(reply).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        chat = _openai_chat(f"http://127.0.0.1:{server.server_port}/v1/", "k")
        text, usage = asyncio.run(
            chat([{"role": "user", "content": "hi"}], model="m", max_tokens=8, temperature=0.0)
        )
    finally:
        server.shutdown()
    assert seen["path"] == "/v1/chat/completions" and seen["auth"] == "Bearer k"
    assert seen["body"]["model"] == "m" and seen["body"]["max_tokens"] == 8
    assert text == '{"rank": ["a"]}' and usage == {"total_tokens": 42}


# ── ADR ────────────────────────────────────────────────────────────────────────


class FakeADR:
    """An ADR-like policy: async decide(obs) -> a Decision-like object."""

    def __init__(self, decide) -> None:
        self._decide = decide
        self.observations: list[dict] = []

    async def decide(self, obs: dict):
        self.observations.append(obs)
        await asyncio.sleep(0)
        return self._decide(obs)


def _decision(actions=(), directives=None, stop=False, goals=(), remove_goals=()):
    return SimpleNamespace(
        actions=list(actions),
        directives=directives or {},
        stop=stop,
        goals=list(goals),
        remove_goals=list(remove_goals),
    )


def _spawn(name, **kw):
    return SimpleNamespace(kind=SimpleNamespace(value="spawn_task"), task_name=name, task_kwargs=kw)


def test_adr_controller_translates_a_decision():
    ctl = ADRController(FakeADR(lambda o: None))
    res = ctl.translate(
        _decision(
            actions=[
                _spawn("b", seed=3, priority="high"),
                SimpleNamespace(kind="cancel_task", target_uid="ai.a.1"),
                SimpleNamespace(kind="update_param", param_key="temp", param_value=0.5),
                SimpleNamespace(kind="something_else"),
            ],
            directives={"rank": ["b"]},
            stop=True,
            goals=[SimpleNamespace(name="g", metric="best", threshold=1, direction="maximize")],
            remove_goals=["old"],
        )
    )
    (req,) = res.launch_requests
    assert req.workflow_type == "b" and req.context == {"seed": 3} and req.priority == "HIGH"
    assert res.decisions == {"ai.a.1": CampaignDecision.CANCEL}
    assert ctl.params == {"temp": 0.5}
    assert res.terminate and [g.name for g in res.goals] == ["g"] and res.remove_goals == ["old"]
    assert ctl.directives == {"rank": ["b"]} and ctl.directives_seq == 1


def test_adr_controller_and_scheduling_policy_share_one_decision(caplog):
    def decide(obs):
        q = obs["queue_snapshot"].by_type
        spawn = [_spawn(t) for t in ("a", "b") if q.get(t, 0) < 2]  # keep both queued
        return _decision(actions=spawn, directives={"rank": ["b", "a"]})

    adr = FakeADR(decide)
    ctl = ADRController(adr, goal_type="b", goal_n=8)
    pol = ADRSchedulingPolicy(controller=ctl)
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        out = _run(_spec(pol, controller=ctl))
    assert out.goal_reached and out.exit_reason == "controller_terminate"
    assert all(
        "cycle" in o for o in adr.observations
    )  # one ADR call per cycle, from the controller
    n = _launched(caplog)
    assert n["b"] >= 8 and n["a"] <= 2, n
    assert "[ADR] rank=['b', 'a']" in caplog.text


def test_adr_scheduling_policy_with_its_own_adr_policy(caplog):
    own = FakeADR(lambda obs: _decision(directives={"rank": ["b", "a"], "value": {"b": 2}}))
    pol = ADRSchedulingPolicy(policy=own)
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        out = _run(_spec(pol, n=8))
    assert out.goal_reached
    assert own.observations and "queued" in own.observations[0]
    assert _launched(caplog)["a"] <= 3
    assert pol.factors == {"b": 2.0}


def test_adr_controller_failure_falls_back_to_its_rules(caplog):
    def broken(obs):
        raise RuntimeError("adr down")

    ctl = ADRController(FakeADR(broken), launch=["b"], goal_type="b", goal_n=3)
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        out = _run(_spec(None, controller=ctl))
    assert out.goal_reached
    assert "decide=fallback(error)" in caplog.text


def test_adr_controller_with_a_real_radical_adr_policy():
    adr = pytest.importorskip("radical.adr")

    class Spawner(adr.Policy):
        @adr.policy.base.decide
        async def run(self, obs: dict):
            q = obs["queue_snapshot"].by_type
            actions = []
            if q.get("b", 0) == 0:
                actions.append(adr.Action(kind=adr.ActionKind.SPAWN_TASK, task_name="b"))
            return adr.Decision(actions=actions, directives={"rank": ["b"]})

    ctl = ADRController(Spawner(), goal_type="b", goal_n=4)
    out = _run(_spec(ADRSchedulingPolicy(controller=ctl), controller=ctl))
    assert out.goal_reached and out.completed.get("b", 0) >= 4
