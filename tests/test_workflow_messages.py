"""Messages from the controller to running workflows (PolicyResult.messages).

A message goes to one workflow id or to every running workflow of a type; a
sticky one is also kept as the type's latest value and given to workflows
launched later (context["messages"], CM_MESSAGES).  Workflows read messages
with reporter.messages() / aucam-report messages.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import threading
import types

import pytest

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    LaunchRequest,
    LocalDictBackend,
    PolicyResult,
    RadexDragonBackend,
    WorkflowConfig,
    WorkflowMessage,
    read_checkpoint,
)
from src.aucam.checkpoint import checkpoint_path

CAPACITY = {"cpus": 1024, "gpus": 64}

# What each sim saw: (context["messages"] at launch, messages read while running).
seen: list[tuple[dict | None, list]] = []


async def sim(reporter, context) -> None:
    at_launch = context.get("messages")
    got: list = []
    if at_launch is None:
        for _ in range(400):  # until the controller's message arrives (≤ 4 s)
            got += await reporter.messages()
            if got:
                break
            await reporter.emit(score=0.0)
            await asyncio.sleep(0.01)
    seen.append((at_launch, [(m.name, m.payload, m.seq) for m in got]))
    await reporter.emit(score=1.0)


class Promote(CampaignController):
    """Two sims; once both run, promote model v2 (sticky) to the type; then one more sim."""

    def __init__(self, extra: int = 1, sticky: bool = True) -> None:
        super().__init__()
        self.extra, self.sticky = extra, sticky
        self.sent = False
        self.launched = 0

    def decide(self, obs: dict) -> PolicyResult:
        res = PolicyResult()
        if self.launched < 2:
            res.launch_requests += [LaunchRequest.make("sim") for _ in range(2)]
            self.launched = 2
        running = obs["running"].get("sim", 0)
        if not self.sent and running == 2:
            res.messages.append(
                WorkflowMessage(to="sim", name="model", payload={"v": 2}, sticky=self.sticky)
            )
            self.sent = True
        done = sum(self.completed.values())
        if self.sent and done >= 2 and self.launched < 2 + self.extra:
            res.launch_requests.append(LaunchRequest.make("sim"))
            self.launched += 1
        res.terminate = done >= 2 + self.extra
        return res


def _spec(ctl, name="msgs", **kw):
    return CampaignSpec(
        name=name,
        budget=CampaignBudget(
            capacity=CAPACITY, max_concurrent_workflows=2, max_concurrency_per_workflow=2
        ),
        controller=ctl,
        workflows=[
            WorkflowConfig(
                name="sim",
                block=sim,
                spending_limit={"cpu_hours": 1000.0},
                resources={"cpus": 1},
                launch_on_start=False,
            )
        ],
        policy_interval_s=0.02,
        shutdown_timeout_s=5.0,
        **kw,
    )


def _run(spec):
    return asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=30.0))


def test_running_workflows_get_the_message_and_later_ones_the_sticky_value(caplog):
    seen.clear()
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        out = _run(_spec(Promote()))
    assert out.completed == {"sim": 3}
    running, later = seen[:2], seen[2]
    for at_launch, got in running:
        assert at_launch is None
        assert got == [("model", {"v": 2}, 1)]
    assert later == ({"model": {"v": 2}}, [])
    assert out.messages_sent == 2
    assert "[MSG] model -> sim: 2 running workflow(s) (sticky)" in caplog.text
    assert "messages=2" in caplog.text


def test_a_message_that_is_not_sticky_reaches_only_running_workflows():
    seen.clear()
    out = _run(_spec(Promote(sticky=False)))
    assert out.completed == {"sim": 3}
    assert [got for _, got in seen[:2]] == [[("model", {"v": 2}, 1)]] * 2
    assert seen[2][0] is None  # nothing at launch...
    assert seen[2][1] == []  # ...and nothing sent to it (it timed out waiting)


class ToOne(CampaignController):
    """Messages one running sim by id (sticky is ignored), and an unknown target."""

    def __init__(self) -> None:
        super().__init__()
        self.step = 0

    def decide(self, obs: dict) -> PolicyResult:
        res = PolicyResult()
        if self.step == 0:
            res.launch_requests.append(LaunchRequest.make("sim"))
            self.step = 1
        elif self.step == 1 and obs["wf_types"]:
            wf_id = next(iter(obs["wf_types"]))
            res.messages += [
                WorkflowMessage(to=wf_id, name="a", payload=1, sticky=True),
                WorkflowMessage(to="nobody", name="b"),
                WorkflowMessage(to=wf_id, name="c", payload=[1, 2]),
            ]
            self.step = 2
        res.terminate = sum(self.completed.values()) >= 1
        return res


def test_messages_to_one_workflow_in_order_and_bad_targets(caplog):
    seen.clear()
    with caplog.at_level(logging.INFO, logger="src.aucam"):
        out = _run(_spec(ToOne()))
    assert out.messages_sent == 2
    assert seen[0][1] in ([("a", 1, 1), ("c", [1, 2], 2)], [("a", 1, 1)])
    assert "sticky needs a workflow type" in caplog.text
    assert "'nobody' is neither a running workflow nor a workflow type" in caplog.text


def test_sticky_values_survive_a_restart(tmp_path):
    seen.clear()
    _run(_spec(Promote(extra=0), name="restart", checkpoint_dir=str(tmp_path)))
    ckpt = read_checkpoint(checkpoint_path(str(tmp_path), "restart"))
    assert ckpt.sticky_messages == {"sim": {"model": {"v": 2}}}

    seen.clear()

    class OneMore(CampaignController):
        """Counts only this run (a restore brings back the earlier counters)."""

        launched = finished = 0

        def on_complete(self, result):
            super().on_complete(result)
            self.finished += 1

        def decide(self, obs):
            res = PolicyResult(terminate=self.finished >= 1)
            if not self.launched:
                res.launch_requests.append(LaunchRequest.make("sim"))
                self.launched = 1
            return res

    _run(_spec(OneMore(), name="restart", checkpoint_dir=str(tmp_path)))
    assert seen == [({"model": {"v": 2}}, [])]


def test_payload_must_be_json():
    with pytest.raises(ValueError, match="JSON-serializable"):
        WorkflowMessage(to="sim", name="model", payload=object())
    with pytest.raises(ValueError, match="name"):
        WorkflowMessage(to="sim", name="")


def test_local_backend_queue_and_cleanup():
    async def go():
        b = LocalDictBackend()
        rep = b.make_reporter("w")
        await b.send_message("w", {"seq": 1, "name": "m", "payload": 1, "sent_at": 0.0})
        await b.send_message("w", {"seq": 2, "name": "m", "payload": 2, "sent_at": 0.0})
        assert [m.payload for m in await rep.messages()] == [1, 2]
        assert await rep.messages() == []
        await b.send_message("w", {"seq": 3, "name": "m", "payload": 3, "sent_at": 0.0})
        await b.delete_report("w")
        assert await rep.messages() == []

    asyncio.run(go())


# ── Dragon (a fake radex DragonClient over one shared dict) ─────────────────


@pytest.fixture()
def fake_radex(monkeypatch):
    store: dict = {}

    class DragonClient:
        def __init__(self, descriptor, timeout):
            pass

        def contains(self, key):
            return key in store

        def get_picklable(self, key):
            return store[key]

        def put_picklable(self, key, value):
            store[key] = value

    core = types.ModuleType("radex.clients.core")
    core.DragonClient = DragonClient
    for name, mod in (
        ("radex", types.ModuleType("radex")),
        ("radex.clients", types.ModuleType("radex.clients")),
        ("radex.clients.core", core),
    ):
        monkeypatch.setitem(sys.modules, name, mod)
    return store


def _msg(seq, payload):
    return {"seq": seq, "name": "model", "payload": payload, "sent_at": 1.0}


def test_dragon_backend_messages(fake_radex, monkeypatch):
    from src.aucam_client import Reporter

    async def go():
        b = RadexDragonBackend("desc")
        rep = b.make_reporter("w")
        await b.send_message("w", _msg(1, "a"))
        await b.send_message("w", _msg(2, "b"))
        assert [(m.seq, m.payload) for m in await rep.messages()] == [(1, "a"), (2, "b")]
        assert await rep.messages() == []
        assert fake_radex["w:messages_read"] == 2
        # Read messages are dropped from the stored list on the next send.
        await b.send_message("w", _msg(3, "c"))
        assert [m["seq"] for m in fake_radex["w:messages"]] == [3]
        # The command-line client reads the same keys.
        cli = Reporter(wf_id="w", endpoint="desc", backend="dragon")
        assert [m["payload"] for m in cli.messages()] == ["c"]
        assert await rep.messages() == []
        await b.delete_report("w")
        assert fake_radex["w:messages"] is None

    asyncio.run(go())


# ── Redis ────────────────────────────────────────────────────────────────────


@pytest.fixture()
def redis_endpoint():
    fakeredis = pytest.importorskip("fakeredis")
    pytest.importorskip("redis")
    server = fakeredis.TcpFakeServer(("127.0.0.1", 0), server_type="redis")
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    host, port = server.server_address
    yield f"{host}:{port}"
    server.shutdown()
    server.server_close()


def test_redis_backend_messages_and_cli(redis_endpoint, monkeypatch, capsys):
    from src.aucam import RhapsodyRedisBackend
    from src.aucam_client.cli import main as cli_main

    async def go():
        b = RhapsodyRedisBackend(redis_endpoint)
        rep = b.make_reporter("w")
        await b.send_message("w", _msg(1, {"path": "/m/1"}))
        await b.send_message("w", _msg(2, {"path": "/m/2"}))
        assert [m.payload["path"] for m in await rep.messages()] == ["/m/1", "/m/2"]
        assert await rep.messages() == []
        await b.send_message("w", _msg(3, {"path": "/m/3"}))
        return b

    b = asyncio.run(go())
    monkeypatch.setenv("CM_WORKFLOW_ID", "w")
    monkeypatch.setenv("CM_DATA_ENDPOINT", redis_endpoint)
    monkeypatch.setenv("CM_DATA_BACKEND", "redis")
    assert cli_main(["messages"]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert [json.loads(x)["payload"]["path"] for x in lines] == ["/m/3"]
    assert cli_main(["messages"]) == 0
    assert capsys.readouterr().out == ""
    asyncio.run(b.delete_report("w"))
    import redis

    host, port = redis_endpoint.rsplit(":", 1)
    assert redis.Redis(host=host, port=int(port)).get("w:messages") is None


def test_shell_workflow_gets_sticky_value_and_message(redis_endpoint, tmp_path):
    """A command-line workflow: CM_MESSAGES at launch, aucam-report messages while running."""
    from src.aucam import RhapsodyRedisBackend

    out_file = tmp_path / "got.txt"
    script = (
        f'echo "launch=$CM_MESSAGES" >> {out_file}\n'
        '[ -n "$CM_CTX_WAIT" ] || exit 0\n'
        "for i in $(seq 1 200); do\n"
        f"  m=$({sys.executable} -m src.aucam_client messages)\n"
        f'  if [ -n "$m" ]; then echo "msg=$m" >> {out_file}; break; fi\n'
        f"  {sys.executable} -m src.aucam_client emit --score 0\n"
        "  sleep 0.05\n"
        "done\n"
    )

    class Ctl(CampaignController):
        def __init__(self):
            super().__init__()
            self.step = 0

        def decide(self, obs):
            res = PolicyResult(terminate=sum(self.completed.values()) >= 2)
            if self.step == 0:
                # Sticky value before anything runs: the first launch gets it in CM_MESSAGES.
                res.messages.append(
                    WorkflowMessage(to="sh", name="model", payload="v1", sticky=True)
                )
                res.launch_requests.append(LaunchRequest.make("sh", context={"wait": 1}))
                self.step = 1
            elif self.step == 1 and obs["running"].get("sh"):
                res.messages.append(WorkflowMessage(to="sh", name="model", payload="v2"))
                self.step = 2
            elif self.step == 2 and sum(self.completed.values()) == 1:
                res.launch_requests.append(LaunchRequest.make("sh"))
                self.step = 3
            return res

    spec = CampaignSpec(
        name="shmsgs",
        budget=CampaignBudget(
            capacity=CAPACITY, max_concurrent_workflows=1, max_concurrency_per_workflow=1
        ),
        controller=Ctl(),
        workflows=[
            WorkflowConfig(
                name="sh",
                launch_cmd=["bash", "-c", script],
                spending_limit={"cpu_hours": 1000.0},
                resources={"cpus": 1},
                launch_on_start=False,
                stale_timeout_s=60.0,
            )
        ],
        data_backend=RhapsodyRedisBackend(redis_endpoint),
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )
    asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=60.0))
    lines = out_file.read_text().splitlines()
    assert lines[0] == 'launch={"model": "v1"}'
    assert json.loads(lines[1][len("msg=") :])["payload"] == "v2"
    # The second run started after the non-sticky v2: it gets the sticky v1 only.
    assert lines[2] == 'launch={"model": "v1"}'
