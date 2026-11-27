"""Run the ASD prototype campaign: design -> label -> ROME fine-tune -> promote.

    # laptop, nothing real: IMPRESS's mock chain and a fake training round
    python run_campaign.py \
        --design-spec $WORK_DIR/IMPRESS/agent/campaigns/mock-small-molecule-binding.yaml \
        --label-spec  $WORK_DIR/IMPRESS/agent/campaigns/mock-small-molecule-binding.yaml \
        --train-impl fake

    # Delta
    python run_campaign.py \
        --design-spec $WORK_DIR/IMPRESS/agent/campaigns/delta-small-molecule.yaml \
        --label-spec  $WORK_DIR/IMPRESS/agent/campaigns/delta-label.yaml

AuCaM owns the engine and passes it to every workflow (`flow=`), which is what
lets ROME submit its training round to the same engine — see README.md.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

from src.aucam import (  # noqa: E402
    CampaignBudget,
    CampaignManager,
    CampaignSpec,
    WorkflowConfig,
)

from controller import AsdController, AsdPolicy  # noqa: E402
from workflows import make_design_block, make_label_block, make_train_block  # noqa: E402

log = logging.getLogger("asd")

# `%(process)d` is not decoration.  Under Dragon the campaign's log stopped dead
# partway through two jobs, and "did the rest of it happen in another process?"
# could not be answered from the file, which is how a wrong diagnosis gets made.
LOG_FORMAT = "%(asctime)s %(levelname)-7s %(process)d %(name)s: %(message)s"


def _attach_log_file(path: str, level: int) -> None:
    """Add a `FileHandler` for `path` to the root logger, if one is not there.

    Called twice: once at startup, once after the execution backend is up.  The
    second call is the one that matters.  Dragon's own logging setup runs while
    the backend is being built, and anything that calls `basicConfig(force=True)`
    or re-levels the root logger would silently take this handler with it --- in
    jobs 24692415 and 24693358 every record after Dragon's handshake went
    missing, from both this file and the job's stderr.

    Re-asserting is cheap and idempotent, so it is done unconditionally rather
    than only when the handler has actually gone.
    """
    root = logging.getLogger()
    target = os.path.abspath(path)
    if not any(
        isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", None) == target
        for h in root.handlers
    ):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(path, mode="a", encoding="utf-8")
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        root.addHandler(handler)
    # A handler cannot see what the logger filtered out first, so the level is
    # half of the repair and the handler is the other half.
    if root.level > level or root.level == logging.NOTSET:
        root.setLevel(level)


def _describe_log_sinks() -> str:
    """Where the log is actually going, named rather than assumed."""
    root = logging.getLogger()
    sinks = []
    for h in root.handlers:
        where = getattr(h, "baseFilename", None) or getattr(
            getattr(h, "stream", None), "name", "?"
        )
        sinks.append(f"{type(h).__name__}({where})")
    return (
        f"pid={os.getpid()} root.level={logging.getLevelName(root.level)} "
        f"handlers=[{', '.join(sinks) or 'NONE'}]"
    )

HERE = Path(__file__).parent
DEFAULT_CONFIG = HERE / "campaign_configs" / "campaign.yaml"


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open() as fh:
        return yaml.safe_load(fh) or {}


def build_spec(cfg: dict[str, Any], args: argparse.Namespace) -> CampaignSpec:
    wf = cfg.get("workflows") or {}
    interval = float(cfg.get("report_interval_s", 30.0))
    rome_cfg = cfg.get("rome") or {}

    def workflow(name: str, block: Any) -> WorkflowConfig:
        own = dict(wf.get(name) or {})
        return WorkflowConfig(
            name=name,
            block=block,
            spending_limit=own.get("spending_limit") or {"cpu_hours": 1.0},
            # AuCaM DROPS a request whose sized limit would fall below this
            # (unfit_dropped), so it is the smallest budget worth starting with
            # — not a minimum grant.  Set it above the learned cost and every
            # launch disappears; left empty there is no floor.
            budget_floor=own.get("budget_floor") or {},
            # Learning off by default: AuCaM sizes a type from the cost of its
            # first few runs, and a design that then costs a little more is
            # cancelled at its own limit.  In a trial that throws away the
            # comparison the promotion depends on, so these three types get the
            # limit the config states, every launch.  Worth turning on per type
            # once its real cost on Delta is known.
            learn=bool(own.get("learn", False)),
            resources=own.get("resources") or {"cpus": 1},
            max_wall_time_s=own.get("max_wall_time_s"),
            # Everything is launched by the controller, with a reason.
            launch_on_start=False,
            # A design or label that stops reporting has stalled; the bound is a
            # multiple of the interval it reports on (AuCaM's own floor is 3x).
            stale_timeout_s=own.get("stale_timeout_s", 4 * interval),
        )

    design = make_design_block(
        args.design_spec,
        args.run_root,
        model=args.model,
        guard=not args.no_guard,
        report_interval_s=interval,
    )
    label = make_label_block(
        args.label_spec,
        args.run_root,
        args.corpus,
        input_param=cfg.get("label_input_param", "score.input_dir"),
        score_key=cfg.get("score_key", "complex_plddt"),
        model=args.model,
        guard=not args.no_guard,
        report_interval_s=interval,
    )
    train = make_train_block(
        args.corpus,
        args.checkpoints,
        impl=args.train_impl,
        min_records=int((cfg.get("policy") or {}).get("min_records", 8)),
        train_seconds=float(rome_cfg.get("train_seconds", 5.0)),
        gpus=int(rome_cfg.get("gpus", 1)),
        # None lets the block derive it from `train_seconds`.  Set it in the
        # config only when a real trainer's round is longer than that allows.
        round_timeout_s=(
            float(rome_cfg["round_timeout_s"]) if "round_timeout_s" in rome_cfg else None
        ),
    )

    return CampaignSpec(
        name=args.name,
        budget=CampaignBudget(
            capacity=cfg.get("capacity") or {"cpus": 8, "gpus": 1},
            max_concurrent_workflows=int(cfg.get("max_concurrent_workflows", 4)),
            max_concurrency_per_workflow=int(cfg.get("max_concurrency_per_workflow", 1)),
            spending=cfg.get("spending") or {},
        ),
        controller=AsdController(AsdPolicy(**(cfg.get("policy") or {}))),
        workflows=[workflow("design", design), workflow("label", label), workflow("train", train)],
        policy_interval_s=float(cfg.get("policy_interval_s", 5.0)),
        shutdown_timeout_s=float(cfg.get("shutdown_timeout_s", 300.0)),
        checkpoint_dir=args.checkpoint_dir,
    )


async def backends(design_spec: str) -> list[Any] | None:
    """AuCaM's engine gets IMPRESS-A's routes, so one engine serves every workflow.

    `impress_backends` builds `compute` and `local` (and `ai`) from the design
    spec's own backend settings — the same engine a standalone IMPRESS campaign
    would have built for itself.  None when IMPRESS is not installed: AuCaM then
    starts its own local backend and only the fake training path can run.
    """
    try:
        from impress_a.integrations.aucam import impress_backends
    except ImportError as exc:
        log.warning(
            "impress_a not importable (%s) — AuCaM will use its own local "
            "backend; IMPRESS workflows will fail",
            exc,
        )
        return None
    return await impress_backends(design_spec)


async def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--design-spec", required=True, help="IMPRESS-A YAML for one design")
    ap.add_argument("--label-spec", required=True, help="IMPRESS-A YAML for labelling")
    ap.add_argument("--config", default=str(DEFAULT_CONFIG))
    ap.add_argument("--name", default="asd-prototype")
    ap.add_argument("--run-root", default="_asd/runs", help="where IMPRESS runs land")
    ap.add_argument("--corpus", default="_asd/corpus", help="records.jsonl lives here")
    ap.add_argument("--checkpoints", default="_asd/checkpoints")
    ap.add_argument(
        "--checkpoint-dir", default="_asd/aucam_ckpt", help="AuCaM's own checkpoint, for restart"
    )
    ap.add_argument(
        "--train-impl",
        choices=("rome", "fake"),
        default="rome",
        help="'fake' writes a checkpoint without importing ROME (no Dragon)",
    )
    ap.add_argument("--model", default="D", help="IMPRESS-A control model")
    ap.add_argument("--no-guard", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument(
        "--log-file",
        default=None,
        help="also write the log here. Give it a path on a shared filesystem when "
        "running under a launcher that does not pass the head process's stderr "
        "through — Dragon does not, and job 24692415 lost its whole log that way.",
    )
    args = ap.parse_args(argv)

    # A stream handler AND, when asked, a file handler.
    #
    # Logging writes to stderr, and under `dragon -s` the head process's stderr does
    # not reach the job's error file: job 24534359 (concurrent) wrote 478 lines to
    # `asd_<jobid>.err` and job 24692415 (Dragon), running the same script, wrote 40
    # lines of mpbridge setup and nothing of the campaign's own. The worker count,
    # the policy cycles and the spend lines were all emitted and lost.
    #
    # A redirect in the launcher cannot fix that, because the stream never reaches
    # the launcher to be redirected. Only the process doing the logging can, which
    # is why this is an argument here rather than a `2>` in `delta_sbatch.sh`.
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format=LOG_FORMAT)
    if args.log_file:
        _attach_log_file(args.log_file, level)
        log.info("logging to %s — %s", args.log_file, _describe_log_sinks())
    for d in (args.run_root, args.corpus, args.checkpoints):
        Path(d).mkdir(parents=True, exist_ok=True)

    cfg = load_config(args.config)
    spec = build_spec(cfg, args)
    engine_backends = await backends(args.design_spec)

    # The backend is up, so Dragon's own logging setup has run.  Take the log back
    # if it was taken, and say where it is going either way --- the campaign's whole
    # value as evidence depends on this line being true.
    if args.log_file:
        _attach_log_file(args.log_file, level)
    log.info("execution backend(s) ready: %s", _describe_log_sinks())
    for backend in engine_backends or []:
        # asteer row 63: the Dragon pool size is the number this job exists to
        # report, and reading it off the object does not depend on rhapsody's own
        # log line surviving.
        batch = getattr(backend, "batch", None)
        log.info(
            "backend %s: workers=%s",
            type(backend).__name__,
            getattr(batch, "num_workers", "n/a") if batch is not None else "n/a",
        )

    cm = CampaignManager(backend=engine_backends)
    outcome = await cm.run(spec)

    log.info(
        "exit=%s promotions=%s spent=%s",
        outcome.exit_reason,
        outcome.summary.get("promotions"),
        outcome.spent,
    )
    log.info("summary: %s", outcome.summary)
    if engine_backends:
        # AuCaM leaves backends it was given to their owner.
        for backend in engine_backends:
            await backend.shutdown()
    return 0 if outcome.goal_reached else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
