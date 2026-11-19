"""The ASD prototype campaign: design -> label -> fine-tune -> trial -> promote.

Runs the real controller and the real training block (`impl="fake"`, so no ROME
and no Dragon) over stub design and label blocks, which stand in for the two
IMPRESS-A campaigns: a design's score improves with the model version it ran
with, which is what makes a trial either clear its bar or not.

What this pins is the part that is ours to get right — when a round is launched,
what a candidate has to prove before it is promoted, and that a promoted model
reaches both the trial designs (per launch) and every design launched afterwards
(the sticky message).
"""

from __future__ import annotations

import asyncio
import importlib.util
import sys
import uuid
from pathlib import Path

from src.aucam import CampaignBudget, CampaignManager, CampaignSpec, WorkflowConfig

ASD = Path(__file__).parent.parent / "campaigns" / "asd_campaign"
sys.path.insert(0, str(ASD))  # `workflows` imports `records`, as the campaign does


def _load(name: str, filename: str):
    """Load a campaign module under its own name (two campaigns own a controller.py)."""
    spec = importlib.util.spec_from_file_location(name, ASD / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


asd_controller = _load("asd_controller", "controller.py")
asd_workflows = _load("asd_workflows", "workflows.py")
records = sys.modules["records"]


def _stubs(corpus: Path, runs: Path, seen: list[dict]):
    """A design that remembers which model it ran with, and a label that scores it."""

    async def design(reporter, context=None, flow=None) -> None:
        ctx = context or {}
        # A trial candidate comes per launch; a promoted model in context["messages"].
        model = ctx.get("model") or (ctx.get("messages") or {}).get("model")
        version = int((model or {}).get("version") or 0)
        out = runs / f"design-{uuid.uuid4().hex[:8]}"
        out.mkdir(parents=True, exist_ok=True)
        seen.append({"tag": ctx.get("tag", "collect"), "version": version})
        await reporter.emit(
            score=1.0,
            metrics={
                "tag": ctx.get("tag", "collect"),
                "design_dir": str(out),
                "model_version": version,
                "cpu_hours": 0.001,
            },
        )

    async def label(reporter, context=None, flow=None) -> None:
        ctx = context or {}
        version = int(ctx.get("model_version") or 0)
        score = 0.5 + 0.2 * version  # a newer model designs better
        record = records.append(
            str(corpus),
            {
                "id": str(ctx["design_wf"]),
                "score": score,
                "design_dir": ctx.get("design_dir"),
                "model_version": version,
            },
        )
        await reporter.emit(
            score=score,
            metrics={
                "tag": ctx.get("tag", "collect"),
                "score": score,
                "record_id": record["id"],
                "cpu_hours": 0.001,
            },
        )

    return design, label


def _run(tmp_path: Path, seen: list[dict], **policy_kw):
    corpus, runs, ckpt = tmp_path / "corpus", tmp_path / "runs", tmp_path / "ckpt"
    design, label = _stubs(corpus, runs, seen)
    kw = {
        "design_in_flight": 2,
        "min_records": 3,
        "trial_n": 2,
        "goal_promotions": 1,
        "max_train_launches": 2,
        **policy_kw,
    }
    policy = asd_controller.AsdPolicy(**kw)
    train = asd_workflows.make_train_block(
        str(corpus), str(ckpt), impl="fake", min_records=3, train_seconds=0.0, gpus=0
    )
    controller = asd_controller.AsdController(policy)

    def wf(name, block):
        return WorkflowConfig(
            name=name,
            block=block,
            launch_on_start=False,
            spending_limit={"cpu_hours": 1.0},
            resources={"cpus": 1},
        )

    spec = CampaignSpec(
        name="asd-test",
        budget=CampaignBudget(
            capacity={"cpus": 8},
            max_concurrent_workflows=4,
            max_concurrency_per_workflow=1,
            spending={"cpu_hours": 50.0},
        ),
        controller=controller,
        workflows=[wf("design", design), wf("label", label), wf("train", train)],
        policy_interval_s=0.02,
        shutdown_timeout_s=5.0,
    )
    outcome = asyncio.run(asyncio.wait_for(CampaignManager().run(spec), timeout=60.0))
    return outcome, controller


def test_a_candidate_is_trialled_and_promoted_when_it_designs_better(tmp_path):
    seen: list[dict] = []
    outcome, controller = _run(tmp_path, seen)

    assert outcome.goal_reached and outcome.exit_reason == "goals_satisfied"
    assert controller.promotions == 1 and controller.rejected == 0
    # The trial ran the candidate, and only the candidate.
    trial = [d for d in seen if d["tag"] == "trial"]
    assert len(trial) == 2 and {d["version"] for d in trial} == {1}
    # Collecting designs before it ran the base model.
    assert {d["version"] for d in seen if d["tag"] == "collect"} == {0}
    # The promoted model is the trialled one, and the bar was the trial's own mean.
    assert controller.current_model["version"] == 1
    assert controller.baseline == 0.7
    # The round consumed the records it trained on, and only those: labels that
    # landed while it ran (the trial's own among them) are left for the next one.
    corpus = str(tmp_path / "corpus")
    used = records.consumed_ids(corpus)
    assert len(used) == 3
    assert not used & {str(r["id"]) for r in records.unconsumed(corpus)}


def test_a_promoted_model_reaches_designs_launched_afterwards(tmp_path):
    """The sticky message: a design launched after the promotion gets it in context."""
    seen: list[dict] = []
    outcome, controller = _run(tmp_path, seen, goal_promotions=2, max_train_launches=3)

    assert controller.promotions == 2, controller.get_summary()
    assert outcome.goal_reached
    # After the first promotion, collecting designs run v1 without being told again.
    assert any(d["tag"] == "collect" and d["version"] == 1 for d in seen)
    # And the second candidate was trialled on top of it.
    assert any(d["tag"] == "trial" and d["version"] == 2 for d in seen)


def test_a_candidate_under_the_target_is_rejected(tmp_path):
    """An absolute bar nothing can clear: the candidate is discarded, not promoted."""
    seen: list[dict] = []
    outcome, controller = _run(tmp_path, seen, target_score=0.99, max_train_launches=1)

    assert controller.promotions == 0 and controller.rejected == 1
    assert not outcome.goal_reached
    assert controller.current_model is None
    assert outcome.exit_reason == "controller_terminate"
