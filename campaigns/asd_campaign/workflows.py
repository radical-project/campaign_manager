"""The three workflow blocks AuCaM launches.

`design` and `label` are each one IMPRESS-A campaign built from its own YAML;
`train` is one ROME fine-tuning round.  All three run on **AuCaM's engine**: it
is passed in as `flow`, which is what CON-1 bought us — ROME submits its round
to the same engine, so the round's tasks are inside AuCaM's block for the train
workflow, cancelled with it and visible to its accounting.

Two things are deliberately NOT `impress_a.integrations.aucam.make_impress_worker`:

* each launch needs its own IMPRESS `campaign_id`, or two designs from the same
  YAML would write into the same run directory;
* the controller needs fields the digest does not carry (which directory a design
  wrote, whether it was a trial, the score a label produced).

Both are small seams the IMPRESS adapter should grow once this shape settles
(`spec_patch=` and `extra_metrics=` on `make_impress_worker`); until then this
module does it here and imports the two adapter helpers it needs, which is why
the private names appear below.
"""

from __future__ import annotations

import json
import logging
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import records  # this directory is on sys.path, as in the other campaigns

log = logging.getLogger(__name__)


# ── IMPRESS-backed blocks ─────────────────────────────────────────────────────


def _impress():
    """IMPRESS's pieces, imported lazily and returned under plain names.

    Lazy because the ROME half of this campaign can run where IMPRESS is not
    installed, and because an import at module scope would make the controller
    untestable without it.
    """
    from impress_a.cli import build_roles, load_spec
    from impress_a.integrations.aucam import (  # private: see the module docstring
        _fit_resources,
        _narrow_budget,
        _ReporterSink,
    )
    from impress_a.integrations.reporting import run_with_sink
    from impress_a.manager import CampaignManager
    from impress_a.tools.registry import Registry

    return SimpleNamespace(
        build_roles=build_roles,
        load_spec=load_spec,
        sink_cls=_ReporterSink,
        fit_resources=_fit_resources,
        narrow_budget=_narrow_budget,
        run_with_sink=run_with_sink,
        manager_cls=CampaignManager,
        registry_cls=Registry,
    )


def _tagged_sink(base_cls, reporter: Any, extra: dict[str, Any]):
    """The adapter's sink with our own fields on every report it emits.

    On every report rather than only the last one, so the billed keys AuCaM
    charges from (`cpu_hours`, `gpu_hours`, `*_in_use`) are never dropped by a
    final report of our own.
    """

    class _Sink(base_cls):  # type: ignore[misc, valid-type]
        async def report(self, digest) -> None:
            metrics = digest.model_dump(mode="json")
            metrics.update(self._billed(digest))
            metrics.update(extra)
            await self.reporter.emit(score=digest.progress, metrics=metrics, iteration=digest.cycle)
            self.emitted += 1

    return _Sink(reporter)


def apply_model(spec: Any, model: dict[str, Any] | None) -> str | None:
    """Point the design chain at a checkpoint.  Returns the version it applied.

    **The one place the real weight swap goes.**  With ROME's `DummyTrainer` there
    is nothing to load, so the prototype records the version and leaves the
    design chain alone — which is enough to prove promotion reaches a design, and
    deliberately not enough to pretend the science changed.  With a real
    LigandMPNN trainer this sets the weights parameter of the sequence-design
    stage and nothing else in the campaign changes.
    """
    if not model:
        return None
    version = str(model.get("version", "?"))
    log.info("design: running with model v%s (%s)", version, model.get("path"))
    # TODO(real trainer): spec.params["ligandmpnn_design"]["<weights key>"] = model["path"]
    return version


def make_design_block(
    spec_path: str,
    run_root: str,
    *,
    model: str = "D",
    guard: bool = True,
    report_interval_s: float = 30.0,
):
    """One IMPRESS-A design campaign per launch, under its own run directory."""

    async def design(reporter: Any, context: dict | None = None, flow: Any = None) -> None:
        imp = _impress()
        ctx = context or {}
        spec = imp.load_spec(spec_path)
        # A directory per launch: two designs from one YAML must not collide.
        spec.campaign_id = f"{spec.campaign_id or 'design'}-{uuid.uuid4().hex[:8]}"
        spec.root = run_root
        design_dir = str(Path(run_root) / spec.campaign_id)

        if narrowed := imp.narrow_budget(spec, ctx.get("spending_limit")):
            log.info("design: caller's limit is now the budget: %s", "; ".join(narrowed))
        imp.fit_resources(spec, ctx.get("resources"))

        # A trial candidate arrives per launch in context["model"]; a promoted one
        # in context["messages"]["model"], which AuCaM fills from the sticky value.
        chosen = ctx.get("model") or (ctx.get("messages") or {}).get("model")
        version = apply_model(spec, chosen)

        extra = {
            "tag": ctx.get("tag", "collect"),
            "design_dir": design_dir,
            "model_version": version,
        }
        mgr = imp.manager_cls(
            spec, imp.build_roles(model, spec, guard), imp.registry_cls().load(), flow=flow
        )
        mgr.executor.cpu_share = int((ctx.get("resources") or {}).get("cpus", 0) or 0) or None
        sink = _tagged_sink(imp.sink_cls, reporter, extra)

        def on_message(manager: Any, name: str, payload: Any) -> None:
            """A model promoted while this design runs."""
            if name == "model":
                extra["model_version"] = apply_model(manager.spec, payload)

        result = await imp.run_with_sink(
            mgr, sink, period_s=report_interval_s, on_message=on_message
        )
        log.info("design %s ended: %s", spec.campaign_id, result.stop_reason)

    return design


def make_label_block(
    spec_path: str,
    run_root: str,
    corpus_dir: str,
    *,
    input_param: str,
    score_key: str,
    model: str = "D",
    guard: bool = True,
    report_interval_s: float = 30.0,
):
    """One IMPRESS-A labelling run over one design's outputs -> one corpus record.

    `score_key` is the objective in the label YAML whose best value is the
    design's score, or the literal `"progress"` for the digest's own scalar.  A
    chain that measured neither produces NO record: a design whose score is
    unknown must not become a 0.0 in the corpus, or a candidate gets promoted on
    the strength of labels that measured nothing.

    `input_param` is `"<stage>.<key>"` in the label YAML's — the one
    place the design's output directory is injected, named in the config rather
    than guessed here.  Empty: nothing is injected, which is only useful for a
    smoke run where the label chain ignores the design's outputs.
    """
    stage, _, key = input_param.partition(".")

    async def label(reporter: Any, context: dict | None = None, flow: Any = None) -> None:
        imp = _impress()
        ctx = context or {}
        design_dir = ctx.get("design_dir")
        if not design_dir:
            raise ValueError("label launched without context['design_dir']")

        spec = imp.load_spec(spec_path)
        spec.campaign_id = f"{spec.campaign_id or 'label'}-{uuid.uuid4().hex[:8]}"
        spec.root = run_root
        if input_param:
            spec.params.setdefault(stage, {})[key] = design_dir
        else:
            log.warning(
                "label: no input_param configured — this run scores its own chain, not %s",
                design_dir,
            )

        imp.narrow_budget(spec, ctx.get("spending_limit"))
        imp.fit_resources(spec, ctx.get("resources"))

        extra: dict[str, Any] = {
            "tag": ctx.get("tag", "collect"),
            "design_wf": ctx.get("design_wf"),
            "design_dir": design_dir,
            "model_version": ctx.get("model_version"),
        }
        mgr = imp.manager_cls(
            spec, imp.build_roles(model, spec, guard), imp.registry_cls().load(), flow=flow
        )
        sink = _tagged_sink(imp.sink_cls, reporter, extra)
        await imp.run_with_sink(mgr, sink, period_s=report_interval_s)

        digest = mgr.observe().digest()
        score = digest.progress if score_key == "progress" else digest.best.get(score_key)
        if score is None:
            log.warning(
                "label: the chain produced no %r (best=%s) — no record written",
                score_key,
                dict(digest.best),
            )
            extra["reason"] = "no_score"
            await sink.report(mgr.observe().digest(status="stopped"))
            return
        record = records.append(
            corpus_dir,
            {
                "id": str(ctx.get("design_wf") or spec.campaign_id),
                "score": float(score),
                "design_dir": design_dir,
                "label_dir": str(Path(run_root) / spec.campaign_id),
                "model_version": ctx.get("model_version"),
                "best": dict(digest.best),
            },
        )
        extra.update({"score": record["score"], "record_id": record["id"]})
        # One more report so the controller sees the score it has to judge.
        await sink.report(mgr.observe().digest(status="stopped"))
        log.info("label: design %s scored %.4f on %s", record["id"], record["score"], score_key)

    return label


# ── ROME ──────────────────────────────────────────────────────────────────────


def make_train_block(
    corpus_dir: str,
    checkpoint_dir: str,
    *,
    impl: str = "rome",
    min_records: int = 8,
    train_seconds: float = 5.0,
    gpus: int = 1,
):
    """One fine-tuning round over the records no round has used yet.

    `impl="rome"` is the real thing: ROME with `auto_train=False`, so a round
    happens only when this block asks for one — the controller decides, never
    ROME.  `impl="fake"` writes the same shape of checkpoint without importing
    ROME, because `import rome` needs Dragon and so cannot run off an
    allocation; it is what the test and a laptop run use.

    `context["target_score"]` is carried through and reported.  It does not gate
    rounds here: ROME's trainer returns a checkpoint path and no quality figure,
    so the bar is measured by the controller's trial.  Once a trainer reports a
    validation metric, this is where "keep training until it clears the target"
    belongs.
    """

    async def train(reporter: Any, context: dict | None = None, flow: Any = None) -> None:
        ctx = context or {}
        pending = records.unconsumed(corpus_dir)
        target = ctx.get("target_score")
        base = {
            "tag": "train",
            "target_score": target,
            "baseline": ctx.get("baseline"),
            "records": 0,
            "candidate": None,
            "version": None,
        }

        if len(pending) < min_records:
            log.warning("train: %d record(s), need %d — nothing to do", len(pending), min_records)
            await reporter.emit(score=0.0, metrics={**base, "reason": "not_enough_data"})
            return

        await reporter.emit(
            score=0.0, metrics={**base, "records": len(pending), "status": "running"}
        )
        if impl == "rome":
            path, version = await _rome_round(
                pending, checkpoint_dir, flow, min_records, train_seconds, gpus
            )
        else:
            path, version = _fake_round(pending, checkpoint_dir)

        consumed = [str(r["id"]) for r in pending]
        records.mark_consumed(corpus_dir, consumed)
        log.info("train: round published v%s at %s from %d record(s)", version, path, len(consumed))
        await reporter.emit(
            score=1.0,
            metrics={
                **base,
                "records": len(consumed),
                "candidate": str(path),
                "version": version,
                "status": "stopped",
                # The round's own cost, so AuCaM charges it rather than estimating.
                "gpu_hours": round(gpus * train_seconds / 3600.0, 6),
            },
        )

    return train


async def _rome_round(
    pending: list[dict],
    checkpoint_dir: str,
    flow: Any,
    min_records: int,
    train_seconds: float,
    gpus: int,
):
    """A ROME round on AuCaM's engine.  `rome` imports Dragon, so it is local."""
    import rome
    from rome.dummy import DummyTrainer

    manager = rome.Manager(
        asyncflow=flow,
        data_config=rome.DataConfig(
            min_samples=min_records, score_key="score", consume_on_train=False
        ),
        trainer_config=rome.TrainerConfig(
            trainer=DummyTrainer(train_seconds=train_seconds, gpus=gpus),
            checkpoint_dir=checkpoint_dir,
            # The controller decides when to train.  Left on, ROME would start a
            # round on its own as soon as the corpus crossed min_samples.
            auto_train=False,
        ),
    )
    await manager.start()
    try:
        for rec in pending:
            manager.add_training_data(
                score=rec.get("score"), **{k: v for k, v in rec.items() if k != "score"}
            )
        path = await manager.start_training(force=True)
        return path, manager.model_version
    finally:
        await manager.stop()


def _fake_round(pending: list[dict], checkpoint_dir: str):
    """What a DummyTrainer round leaves behind, without needing Dragon."""
    root = Path(checkpoint_dir) / "fake"
    root.mkdir(parents=True, exist_ok=True)
    version = 1 + sum(1 for p in root.glob("v*") if p.is_dir())
    out = root / f"v{version}"
    out.mkdir(parents=True, exist_ok=True)
    path = out / "checkpoint.json"
    path.write_text(
        json.dumps(
            {
                "version": version,
                "trained_on": [str(r["id"]) for r in pending],
                "mean_score": sum(float(r.get("score", 0.0)) for r in pending) / len(pending),
            },
            indent=2,
        )
    )
    (out / "train_complete").write_text("")  # ROME's own completion marker
    return path, version
