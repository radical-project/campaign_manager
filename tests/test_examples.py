"""Smoke tests for the example campaigns (U3): each runs, and its overuse use
cases end with AuCaM cancelling the workflows that overspend or use more CPUs
than they were given.  ddsim_aucam needs the DeepDriveSim pipeline and is not
run here."""

from __future__ import annotations

import asyncio
import importlib.util
import re
import subprocess
import sys
from pathlib import Path

import pytest

from src.aucam import CampaignController, CampaignManager

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "campaigns" / "demo_campaign"
HPC = ROOT / "campaigns" / "hpc_campaign"

pytestmark = pytest.mark.slow


def _demo_module():
    sys.path.insert(0, str(DEMO))
    try:
        spec = importlib.util.spec_from_file_location("demo_run_campaign", DEMO / "run_campaign.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    finally:
        sys.path.remove(str(DEMO))


@pytest.mark.parametrize(
    "flags, check",
    [
        ({}, lambda o: o.budget_cancels == 0 and o.resource_cancels == 0),
        # The overspending precise_sim needs ~1 s to pass its limit: run longer,
        # and do not let the score rule cancel it first.
        (
            {"overspend": True, "goal_analyses": 12, "cancel_threshold": 0.0},
            lambda o: o.budget_cancels >= 1,
        ),
        ({"overcpu": True}, lambda o: o.resource_cancels >= 1),
    ],
)
def test_demo_campaign(monkeypatch, flags, check):
    mod = _demo_module()
    # Only the analysis goal ends the run: a lucky perfect score could end it
    # before the overspending workflow passes its limit.
    monkeypatch.setattr(
        mod.ShowcaseController, "is_goal_reached", CampaignController.is_goal_reached
    )
    spec = mod.build_spec("demo", **{"goal_analyses": 4, **flags})
    outcome = asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=120))
    assert outcome.exit_reason == "controller_terminate"
    assert check(outcome), outcome


def _hpc(config: str) -> dict[str, int]:
    proc = subprocess.run(
        [sys.executable, "run_campaign.py", "--campaign-config", f"campaign_configs/{config}"],
        cwd=HPC,
        capture_output=True,
        text=True,
        timeout=150,
    )
    out = proc.stdout + proc.stderr
    assert proc.returncode == 0, out[-2000:]
    m = re.search(r"SUMMARY — exit=(\w+) .*?budget=(\d+) resources=(\d+)", out)
    assert m, out[-2000:]
    return {"exit": m.group(1), "budget": int(m.group(2)), "resources": int(m.group(3))}


def test_hpc_campaign_runs():
    r = _hpc("campaign.yaml")
    assert r["exit"] == "controller_terminate" and r["resources"] == 0


def test_hpc_campaign_overspending_workflows_are_cancelled():
    assert _hpc("campaign_overuse.yaml")["budget"] >= 1


def test_hpc_campaign_cpu_overuse_is_cancelled():
    assert _hpc("campaign_overuse_cpus.yaml")["resources"] >= 1
