"""AsdController — design, label, fine-tune, and promote a model only once it has
been shown to design better.

The pipeline is three AuCaM workflow types:

    design  one IMPRESS-A design (one lineage), many launches, N in flight
    label   one IMPRESS-A labelling run over one design's outputs -> one corpus record
    train   one ROME fine-tuning round over the records no round has used yet

and the controller is the only thing that decides anything:

    collect   keep `design_in_flight` designs running; label each as it finishes
              -> once `min_records` labelled designs exist, launch `train`
    training  wait for the candidate checkpoint
    trial     run `trial_n` designs WITH the candidate (passed per launch in
              `context["model"]`, so nothing else switches), label them
              -> promote if their mean label score clears the bar, else discard
    promoted  WorkflowMessage(to="design", name="model", sticky=True): the
              running designs pick the new model up through reporter.messages(),
              and every design launched later gets it in context["messages"]

Why the trial: ROME reports a checkpoint path, never a quality score, and a
training metric (validation loss, sequence recovery) is not the campaign's bar.
The only honest answer to "is this model better" is to design with it and score
those designs the way every other design is scored — so that is what promotion
waits for.  The target score rides along in the train launch
(`context["target_score"]`) and is what this controller measures the trial
against; the number of rounds is never the criterion.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from statistics import mean
from typing import Any

from src.aucam import (
    CampaignController,
    LaunchRequest,
    PolicyResult,
    WorkflowMessage,
    WorkflowResult,
)

log = logging.getLogger(__name__)

DESIGN, LABEL, TRAIN = "design", "label", "train"
COLLECT, TRAINING, TRIAL = "collect", "training", "trial"


@dataclass
class AsdPolicy:
    """The knobs; `campaign_configs/campaign.yaml` fills them."""

    #: Designs kept in flight while collecting training data.
    design_in_flight: int = 3
    #: Labelled designs that must accumulate before a training round is worth it.
    #: Below ROME's own `DataConfig.min_samples` a round cannot start at all.
    min_records: int = 8
    #: Designs run with a candidate checkpoint before judging it.
    trial_n: int = 3
    #: An absolute bar the trial mean must clear.  None: beat the current model's
    #: own mean by `promote_margin` instead, which is the comparison that holds
    #: when nobody knows the achievable range yet.
    target_score: float | None = None
    promote_margin: float = 0.0
    #: The campaign's goal, and the backstop: rounds cost GPU-hours, and a score
    #: target with no cap is unbounded.  The per-workflow spending limit is the
    #: other half of that backstop.
    goal_promotions: int = 1
    max_train_launches: int = 4


class AsdController(CampaignController):
    def __init__(self, policy: AsdPolicy | None = None) -> None:
        # No LaunchRule: every launch here is a decision with a reason, so the
        # base rules would only duplicate (and race) the phase machine below.
        super().__init__()
        self.p = policy or AsdPolicy()
        self.phase = COLLECT
        #: The promoted model, and the mean label score it has earned so far.
        self.current_model: dict[str, Any] | None = None
        self.baseline: float | None = None
        self.candidate: dict[str, Any] | None = None
        self.promotions = 0
        self.train_launches = 0
        #: Labelled designs not yet used by a round (the train workflow reports
        #: how many it consumed, so this tracks the corpus without reading it).
        self.unconsumed = 0
        self.scores: list[float] = []  # label scores under the current model
        self.trial_scores: list[float] = []
        self.trial_launched = 0
        self.trial_resolved = 0
        self.rejected = 0
        self._to_label: list[dict[str, Any]] = []  # designs waiting for a label launch

    # ── What finished ─────────────────────────────────────────────────────────

    def on_complete(self, result: WorkflowResult) -> None:
        super().on_complete(result)
        m = dict(result.final_metrics or {})
        tag = m.get("tag", COLLECT)
        ok = result.returncode == 0

        if result.workflow_type == DESIGN:
            # Every design has to resolve, or a trial of three could wait forever
            # on a design that failed.
            if ok and m.get("design_dir"):
                self._to_label.append(
                    {
                        "tag": tag,
                        "design_dir": m["design_dir"],
                        "design_wf": result.workflow_id,
                        "model_version": m.get("model_version"),
                    }
                )
            else:
                log.warning(
                    "[ASD] design %s produced nothing to label (rc=%s)",
                    result.workflow_id,
                    result.returncode,
                )
                if tag == TRIAL:
                    self.trial_resolved += 1

        elif result.workflow_type == LABEL:
            # The label's own key only: `final_score` is the digest's progress,
            # which a label that measured nothing would still report.
            score = m.get("score")
            scored = ok and isinstance(score, (int, float))
            if tag == TRIAL:
                self.trial_resolved += 1
                if scored:
                    self.trial_scores.append(float(score))
            elif scored:
                self.unconsumed += 1
                self.scores.append(float(score))
            if not scored:
                log.warning(
                    "[ASD] label %s returned no score (rc=%s)",
                    result.workflow_id,
                    result.returncode,
                )

        elif result.workflow_type == TRAIN:
            self.unconsumed = max(0, self.unconsumed - int(m.get("records", 0) or 0))
            candidate = m.get("candidate")
            if ok and candidate:
                self.candidate = {"path": candidate, "version": m.get("version")}
                self.phase = TRIAL
                self.trial_scores, self.trial_launched, self.trial_resolved = [], 0, 0
                log.info(
                    "[ASD] candidate v%s at %s — trialling it on %d design(s)",
                    self.candidate.get("version"),
                    candidate,
                    self.p.trial_n,
                )
            else:
                log.warning(
                    "[ASD] training round produced no candidate (rc=%s) — back to collecting",
                    result.returncode,
                )
                self.phase = COLLECT

    # ── What to do next ───────────────────────────────────────────────────────

    def decide(self, obs: dict) -> PolicyResult:
        res = PolicyResult()
        running = obs.get("running") or {}
        queued = obs["queue_snapshot"].by_type if obs.get("queue_snapshot") else {}

        def pending(wf_type: str) -> int:
            return int(running.get(wf_type, 0)) + int(queued.get(wf_type, 0))

        # 1. Label every design that has finished.  Launched here rather than
        #    through a WorkflowDependency because the label needs the design's
        #    own output directory, which only its final report carries.
        for pend in self._to_label:
            res.launch_requests.append(LaunchRequest.make(LABEL, context=pend))
        self._to_label.clear()

        if self.phase == TRIAL:
            # Only trial designs: a trial is a comparison, and collect designs
            # launched now would both muddy it and compete for the same GPUs.
            for _ in range(max(0, self.p.trial_n - self.trial_launched)):
                res.launch_requests.append(
                    LaunchRequest.make(DESIGN, context={"tag": TRIAL, "model": self.candidate})
                )
                self.trial_launched += 1
            if self.trial_resolved >= self.p.trial_n and pending(LABEL) == 0:
                self._judge(res)

        elif self.phase == COLLECT and not self._spent():
            for _ in range(max(0, self.p.design_in_flight - pending(DESIGN))):
                res.launch_requests.append(LaunchRequest.make(DESIGN, context={"tag": COLLECT}))
            if (
                self.unconsumed >= self.p.min_records
                and pending(TRAIN) == 0
                and self.train_launches < self.p.max_train_launches
            ):
                res.launch_requests.append(
                    LaunchRequest.make(
                        TRAIN,
                        context={
                            "tag": "train",
                            "target_score": self.p.target_score,
                            "baseline": self.baseline,
                            "min_records": self.p.min_records,
                        },
                    )
                )
                self.train_launches += 1
                self.phase = TRAINING
                log.info(
                    "[ASD] %d labelled design(s) — training round %d",
                    self.unconsumed,
                    self.train_launches,
                )

        # Ending: the goal, or nothing left to try.  In the second case the
        # designs and labels already in flight finish first — their records are
        # what a later campaign would start from.
        idle = all(pending(t) == 0 for t in (DESIGN, LABEL, TRAIN))
        res.terminate = self.is_goal_reached() or (
            self._spent() and idle and not res.launch_requests
        )
        return res

    def _judge(self, res: PolicyResult) -> None:
        """Promote the candidate, or discard it, on what the trial designs scored."""
        got = mean(self.trial_scores) if self.trial_scores else None
        bar = self.p.target_score
        if bar is None and self.baseline is not None:
            bar = self.baseline + self.p.promote_margin
        promote = got is not None and (bar is None or got >= bar)

        if promote:
            # Sticky: the running designs get it through reporter.messages(), and
            # every design launched afterwards finds it in context["messages"].
            res.messages.append(
                WorkflowMessage(to=DESIGN, name="model", payload=self.candidate, sticky=True)
            )
            self.current_model = self.candidate
            self.baseline = got
            self.scores = list(self.trial_scores)
            self.promotions += 1
            log.info(
                "[ASD] PROMOTED model v%s — trial mean %.4f over bar %s (%d promotion(s))",
                self.candidate.get("version"),
                got,
                f"{bar:.4f}" if bar is not None else "none (first candidate)",
                self.promotions,
            )
        else:
            self.rejected += 1
            log.info(
                "[ASD] candidate v%s rejected — trial mean %s under bar %s",
                (self.candidate or {}).get("version"),
                f"{got:.4f}" if got is not None else "none",
                f"{bar:.4f}" if bar is not None else "none",
            )

        self.candidate = None
        self.trial_scores, self.trial_launched, self.trial_resolved = [], 0, 0
        self.phase = COLLECT

    def _spent(self) -> bool:
        """No round left to try, so collecting more records would buy nothing.

        The count of launches, not the corpus: records cannot be trained on once
        the launches are gone, and designs that went on being launched would hold
        the allocation for a campaign that can no longer promote anything.
        """
        return self.train_launches >= self.p.max_train_launches and self.phase == COLLECT

    def is_goal_reached(self) -> bool:
        return self.promotions >= self.p.goal_promotions

    def get_summary(self) -> dict:
        return {
            "phase": self.phase,
            "promotions": self.promotions,
            "rejected_candidates": self.rejected,
            "train_launches": self.train_launches,
            "labelled_designs": len(self.scores),
            "unconsumed_records": self.unconsumed,
            "baseline_score": self.baseline,
            "current_model": self.current_model,
        }
