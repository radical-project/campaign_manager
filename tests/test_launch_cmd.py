"""Command-line workflows: inline wrapper, exported variables, real exit codes."""
from __future__ import annotations

import asyncio
import inspect
import json
import sys

import pytest

from src.aucam import (
    CampaignBudget,
    CampaignController,
    CampaignManager,
    CampaignSpec,
    LaunchPriority,
    LaunchRequest,
    PolicyResult,
    WorkflowConfig,
)
from src.aucam.campaign import _CampaignInstance


class LaunchOnce(CampaignController):
    """Launch the given requests once; stop when all of them have finished."""

    def __init__(self, requests: list[LaunchRequest]) -> None:
        super().__init__()
        self._requests = requests
        self._sent = False
        self.results: list = []

    def on_complete(self, result) -> None:
        super().on_complete(result)
        self.results.append(result)

    def decide(self, obs: dict) -> PolicyResult:
        reqs: list[LaunchRequest] = []
        if not self._sent:
            self._sent = True
            reqs = self._requests
        done = len(self.results) >= len(self._requests)
        return PolicyResult(launch_requests=reqs, terminate=done)


def _run(workflows: list[WorkflowConfig], requests: list[LaunchRequest]) -> LaunchOnce:
    ctl = LaunchOnce(requests)
    spec = CampaignSpec(
        name="cmdcamp",
        budget=CampaignBudget(max_concurrent_workflows=4, max_concurrency_per_workflow=4),
        controller=ctl,
        workflows=workflows,
        policy_interval_s=0.05,
        shutdown_timeout_s=5.0,
    )
    cm = CampaignManager()
    cm.submit_campaign(spec)
    asyncio.run(asyncio.wait_for(cm.run(), timeout=60.0))
    return ctl


def test_wrap_cmd_is_inline_and_quotes_values() -> None:
    cmd = _CampaignInstance._wrap_cmd(["prog", "a b"], {"K": "it's"})
    assert cmd.startswith("bash -c ")
    assert ".sh" not in cmd  # no wrapper file


def _asyncflow_reports_exit_code() -> bool:
    from radical.asyncflow import workflow_manager

    return "exit_code" in inspect.getsource(workflow_manager)


@pytest.mark.skipif(not _asyncflow_reports_exit_code(),
                    reason="radical.asyncflow without task exit codes")
def test_exit_code_reaches_workflow_result() -> None:
    wfs = [
        WorkflowConfig(name=f"exit{c}", launch_on_start=False,
                       launch_cmd=[sys.executable, "-c", f"import sys; sys.exit({c})"])
        for c in (0, 3)
    ]
    reqs = [LaunchRequest.make(w.name, LaunchPriority.NORMAL) for w in wfs]
    ctl = _run(wfs, reqs)
    codes = {r.workflow_type: r.returncode for r in ctl.results}
    assert codes == {"exit0": 0, "exit3": 3}


def test_program_environment(tmp_path) -> None:
    out = tmp_path / "env.json"
    script = (
        "import json, os, sys; "
        "keys = ['CM_WORKFLOW_ID', 'CM_CAMPAIGN_ID', 'CM_CTX_SEED', 'MY_VAR']; "
        "json.dump({k: os.environ.get(k) for k in keys}, open(sys.argv[1], 'w'))"
    )
    wf = WorkflowConfig(
        name="envprog", launch_on_start=False, env={"MY_VAR": "x y 'z'"},
        launch_cmd=[sys.executable, "-c", script, str(out)],
    )
    ctl = _run([wf], [LaunchRequest.make("envprog", LaunchPriority.NORMAL,
                                         context={"seed": 7})])
    assert ctl.results[0].returncode == 0
    env = json.loads(out.read_text())
    assert env["CM_WORKFLOW_ID"] == ctl.results[0].workflow_id
    assert env["CM_CAMPAIGN_ID"] == "cmdcamp"
    assert env["CM_CTX_SEED"] == "7"
    assert env["MY_VAR"] == "x y 'z'"
