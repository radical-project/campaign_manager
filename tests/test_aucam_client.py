"""Language-neutral reporting for command-line workflows (src.aucam_client).

Uses a fakeredis TCP server, so both CM (RhapsodyRedisBackend) and a
command-line workflow in its own process talk to the same "Redis".
"""

from __future__ import annotations

import asyncio
import json
import pickle
import subprocess
import sys
import threading

import pytest

redis = pytest.importorskip("redis")
fakeredis = pytest.importorskip("fakeredis")

from src.aucam import (  # noqa: E402
    CampaignBudget,
    CampaignController,
    CampaignDecision,
    CampaignManager,
    CampaignSpec,
    LaunchPriority,
    LaunchRequest,
    PolicyResult,
    RhapsodyRedisBackend,
    WorkflowConfig,
)
from src.aucam_client import Reporter, ReporterConfigError, encode_report  # noqa: E402
from src.aucam_client.cli import main as cli_main  # noqa: E402


@pytest.fixture()
def redis_endpoint():
    server = fakeredis.TcpFakeServer(("127.0.0.1", 0), server_type="redis")
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    host, port = server.server_address
    yield f"{host}:{port}"
    server.shutdown()
    server.server_close()


@pytest.fixture()
def cm_env(monkeypatch, redis_endpoint):
    monkeypatch.setenv("CM_WORKFLOW_ID", "camp.wf.1")
    monkeypatch.setenv("CM_DATA_ENDPOINT", redis_endpoint)
    monkeypatch.setenv("CM_DATA_BACKEND", "redis")
    return redis_endpoint


def test_report_is_json_and_read_by_backend(cm_env) -> None:
    Reporter().emit(score={"energy": -8.5}, metrics={"cpu_hours": 0.2}, iteration=3)
    host, port = cm_env.rsplit(":", 1)
    raw = redis.Redis(host=host, port=int(port)).get("camp.wf.1")
    assert json.loads(raw)["score"] == {"energy": -8.5}  # plain JSON on the wire
    report = asyncio.run(RhapsodyRedisBackend(cm_env).get_report("camp.wf.1"))
    assert report.score == {"energy": -8.5}
    assert report.metrics == {"cpu_hours": 0.2} and report.iteration == 3


def test_backend_still_reads_pickled_reports(cm_env) -> None:
    host, port = cm_env.rsplit(":", 1)
    payload = {"wf_id": "w", "score": 1.0, "metrics": {}, "emitted_at": 1.0, "iteration": 0}
    redis.Redis(host=host, port=int(port)).set("w", pickle.dumps(payload))
    report = asyncio.run(RhapsodyRedisBackend(cm_env).get_report("w"))
    assert report.score == 1.0


def test_signals(cm_env) -> None:
    backend = RhapsodyRedisBackend(cm_env)
    rep = Reporter()
    assert rep.poll_signal() is None
    asyncio.run(backend.write_signal("camp.wf.1", CampaignDecision.PAUSE))
    assert rep.poll_signal() == "PAUSE"
    assert rep.poll_signal() is None  # consumed


def test_non_json_score_rejected() -> None:
    with pytest.raises(TypeError, match="JSON-serializable"):
        encode_report("w", score=object())


def test_missing_environment(monkeypatch) -> None:
    for k in ("CM_WORKFLOW_ID", "CM_DATA_ENDPOINT", "CM_DATA_BACKEND"):
        monkeypatch.delenv(k, raising=False)
    with pytest.raises(ReporterConfigError):
        Reporter()
    assert cli_main(["signal"]) == 2


def test_cli_emit_and_signal(cm_env, capsys) -> None:
    assert (
        cli_main(
            [
                "emit",
                "--score",
                "0.75",
                "--metric",
                "cpu_hours=1.5",
                "--metric",
                "stage=fold",
                "--iteration",
                "2",
            ]
        )
        == 0
    )
    report = asyncio.run(RhapsodyRedisBackend(cm_env).get_report("camp.wf.1"))
    assert report.score == 0.75
    assert report.metrics == {"cpu_hours": 1.5, "stage": "fold"}
    asyncio.run(RhapsodyRedisBackend(cm_env).write_signal("camp.wf.1", CampaignDecision.RESUME))
    assert cli_main(["signal"]) == 0
    assert capsys.readouterr().out.strip() == "RESUME"


# ── end to end: a bash workflow reports through the command-line tool ──────────


class Collect(CampaignController):
    def __init__(self) -> None:
        super().__init__()
        self.results: list = []
        self._sent = False

    def on_complete(self, result) -> None:
        super().on_complete(result)
        self.results.append(result)

    def decide(self, obs) -> PolicyResult:
        reqs = []
        if not self._sent:
            self._sent = True
            reqs = [LaunchRequest.make("sh", LaunchPriority.NORMAL)]
        return PolicyResult(launch_requests=reqs, terminate=bool(self.results))


def test_shell_workflow_reports_through_cli(redis_endpoint) -> None:
    script = (
        f"{sys.executable} -m src.aucam_client emit --score 0.9 "
        "--metric cpu_hours=0.01 --iteration 1"
    )
    ctl = Collect()
    spec = CampaignSpec(
        name="shellcamp",
        budget=CampaignBudget(max_concurrent_workflows=1, max_concurrency_per_workflow=1),
        controller=ctl,
        workflows=[
            WorkflowConfig(name="sh", launch_on_start=False, launch_cmd=["bash", "-c", script])
        ],
        data_backend=RhapsodyRedisBackend(redis_endpoint),
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )
    cm = CampaignManager()
    cm.submit_campaign(spec)
    asyncio.run(asyncio.wait_for(cm.run(), timeout=60.0))
    r = ctl.results[0]
    assert r.returncode == 0
    assert r.final_score == 0.9
    assert r.final_metrics == {"cpu_hours": 0.01}


def test_python_module_entry_point(cm_env) -> None:
    out = subprocess.run(
        [sys.executable, "-m", "src.aucam_client", "signal"], capture_output=True, text=True
    )
    assert out.returncode == 0 and out.stdout == ""
