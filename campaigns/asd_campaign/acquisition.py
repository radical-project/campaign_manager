"""Ranking and batch acquisition: item 14's rule, and the gate that protects the ATM budget.

**Rank on the lower confidence bound, never the posterior mean.** Model C is a GP recalibrated
on 10–20 ATM labels, and ranking a few hundred candidates by its mean spends the allocation on
exactly the designs it knows least about: a mean is most optimistic where the posterior is
widest. The LCB charges for that width. With ΔG minimised the conservative bound is the
**upper** one — `mean + k·sigma` — and getting that sign backwards would rank the model's blind
spots first, which is the failure this module exists to prevent rather than a detail of it.

**ABFE is gated on Model B's risk score, and the direction is the point.** ATM is the scarcest
label in the system (~8 GPU-h each, 15–30 per round against 40–60 MD labels) and it converges
poorly on exactly the designs B flags as badly preorganised — high `induced_fit_risk` means the
pocket reorganises on binding, which is the regime where alchemical transfer needs more windows
and more sampling to say anything. So spending ATM there wastes the scarcest thing twice: the
GPU-hours, and the label slot that a convergent design could have used.

**The holdout is by scaffold cluster, not at random.** A random 20% split puts near-duplicates
of held-out designs in the training set, so the model scores well on the holdout by having
memorised its neighbours and the evaluation reports a generalisation it does not have. Splitting
by cluster is the whole difference between a holdout and a decoration.

**Stdlib only, and no `impress_a` import.** This is arithmetic over posteriors: it belongs to the
campaign's policy rather than to any tool's adapter, and keeping it free of the dependency means
its tests run in an environment with neither impress-a nor a GPU — which is where a ranking rule
should be checkable.

**Moved here 2026-11-21 from `asteer/acquisition.py`** (episode H2 of the re-homing, which this
module's own sentence above decided: a campaign's policy belongs with the campaign). It is still
stdlib-only and still imports nothing from `asteer`, so AuCaM's library takes no dependency from
it. The sentence that used to close this docstring — *"`asteer.impress.roles` is the thin adapter
that hands it to impress-a"* — is deleted rather than carried: that module was removed in EI.14
and the reference had been stale since.

The call site that will replace the scripted one is `controller.py`'s `AsdController`; the
scripted dispatcher it is tested against stays in `asteer` (`asteer/dispatch.py`), because item 10
*replaces* that dispatcher rather than importing it.
"""
from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

__all__ = [
    "DEFAULT_LCB_K",
    "DEFAULT_RISK_CEILING",
    "AcquisitionPlan",
    "Candidate",
    "acquire",
    "lower_confidence_bound",
    "rank",
    "scaffold_holdout",
]

#: How many standard deviations the ranking charges for uncertainty. 1.0 is one sigma, which is
#: the conservative-but-not-timid default; the source spec does not fix a value, so it is a
#: declared parameter everywhere it is used rather than a constant anyone has to find.
DEFAULT_LCB_K = 1.0

#: Above this `induced_fit_risk`, a design is not sent to ABFE. 0.5 is the midpoint of B's
#: [0, 1] risk scale and is **a placeholder with a named replacement**: the right value is
#: whatever M3 measures as the risk above which ATM fails to converge, and until then any
#: number here is a guess. It is deliberately not tuned to look principled.
DEFAULT_RISK_CEILING = 0.5


@dataclass(frozen=True)
class Candidate:
    """One design, as the ranker sees it: an id, a posterior, and B's risk score.

    `sigma_dg` is `None` rather than 0.0 when Model C reported none, and the distinction is
    load-bearing: 0.0 would make the LCB equal the mean and the design would rank as if the
    model were certain about it. `rank` refuses a candidate with no sigma instead.
    """

    node_id: str
    dg_corrected: float
    sigma_dg: float | None = None
    induced_fit_risk: float | None = None
    #: The scaffold cluster this design belongs to, for the holdout split. `""` means
    #: unclustered, which `scaffold_holdout` refuses rather than treating as its own cluster.
    scaffold: str = ""

    def lcb(self, k: float = DEFAULT_LCB_K) -> float:
        if self.sigma_dg is None:
            raise ValueError(
                f"{self.node_id}: no sigma_dg, so there is no lower confidence bound. Ranking "
                "on the mean here would rank an uncertain design as a confident one -- which "
                "is the failure item 14's rule exists to prevent. Model C reports a sigma or "
                "the design is not rankable."
            )
        return lower_confidence_bound(self.dg_corrected, self.sigma_dg, k)


def lower_confidence_bound(mean: float, sigma: float, k: float = DEFAULT_LCB_K) -> float:
    """`mean + k·sigma`, because ΔG is minimised.

    Named "lower confidence bound" after the acquisition literature, where the objective is
    maximised and the conservative bound is below the mean. Here the objective is a binding
    free energy to be driven down, so the conservative value is **above** it. The name is kept
    because it is what the source spec and item 14 call it; the arithmetic follows the
    direction of the objective, and a test asserts the sign on a worked pair.
    """
    if sigma < 0:
        raise ValueError(f"sigma must be >= 0, got {sigma}")
    return mean + k * sigma


def rank(candidates: Iterable[Candidate], *, k: float = DEFAULT_LCB_K) -> list[Candidate]:
    """Best first, by LCB. Ties broken by node id so two runs agree.

    Every candidate must carry a sigma. A mixed batch — some with, some without — is refused
    rather than ranked with the sigma-less ones treated as certain, because that is precisely
    how a confident-looking blind spot reaches the top of a batch.
    """
    items = list(candidates)
    missing = [c.node_id for c in items if c.sigma_dg is None]
    if missing:
        raise ValueError(
            f"candidates with no sigma_dg: {sorted(missing)}. Ranking a mixed batch would "
            "treat them as certain and float them to the top; EI.7's calibration_in_range is "
            "the gate that should have caught this upstream of here."
        )
    return sorted(items, key=lambda c: (c.lcb(k), c.node_id))


@dataclass
class AcquisitionPlan:
    """One round's acquisition: who gets MD, who gets ABFE, and who was refused why."""

    md: list[str] = field(default_factory=list)
    abfe: list[str] = field(default_factory=list)
    #: node_id -> why it was not sent to ABFE. Kept rather than dropped: "the batch is what it
    #: is" makes an acquisition decision unauditable, and the risk gate is the one decision
    #: here that spends the scarcest label in the system.
    refused_abfe: dict[str, str] = field(default_factory=dict)

    @property
    def total(self) -> int:
        return len(self.md) + len(self.abfe)

    def as_risk_gate(self):
        """The `risk_gate` callable `asteer.dispatch.ScriptedDispatcher` already takes.

        **This is the seam, and naming it is what keeps the ABFE gate from being implemented
        twice.** The dispatcher holds node ids and no posteriors, so it cannot apply item 14's
        rule itself -- which is exactly why it takes an injected gate rather than a threshold.
        Handing it this closure makes `acquire` the single implementation and the dispatcher's
        `risk_gate` its one call site. Item 10 replaces the dispatcher and will consume an
        `AcquisitionPlan` directly; until then this is the join, and a test asserts the
        dispatcher's ABFE batch is exactly `self.abfe`.
        """
        admitted = set(self.abfe)
        return lambda node_id: node_id in admitted


def acquire(
    candidates: Iterable[Candidate],
    *,
    md_budget: int,
    abfe_budget: int,
    k: float = DEFAULT_LCB_K,
    risk_ceiling: float = DEFAULT_RISK_CEILING,
    require_risk: bool = True,
) -> AcquisitionPlan:
    """Rank, take the MD batch, then gate the ABFE subset on B's risk score.

    **ABFE is drawn from the MD batch, not from the whole pool**, and that is the science
    rather than a convenience: ATM wants the equilibrated holo frame `md_label` produces, so a
    design sent to ATM without MD would have to start from an unrelaxed structure. It is also
    why `asteer.dispatch` takes the MD batch as the ABFE candidate pool.

    **A candidate with no risk score is refused from ABFE by default, not admitted.** `B` is
    what makes the gate a gate; a missing score means the design was never assessed, and
    admitting it would spend the scarcest label on the one case nobody checked. `require_risk`
    exists so a caller can deliberately run without B — the unsteered control arm at M2, where
    there is no B to ask — and has to say so.
    """
    ranked = rank(candidates, k=k)
    md = ranked[: max(0, md_budget)]
    plan = AcquisitionPlan(md=[c.node_id for c in md])

    for candidate in md:
        if len(plan.abfe) >= max(0, abfe_budget):
            plan.refused_abfe[candidate.node_id] = "abfe budget exhausted"
            continue
        risk = candidate.induced_fit_risk
        if risk is None:
            if require_risk:
                plan.refused_abfe[candidate.node_id] = (
                    "no induced_fit_risk from Model B: unassessed, and ATM is too scarce to "
                    "spend on the one design nobody checked"
                )
                continue
        elif risk > risk_ceiling:
            plan.refused_abfe[candidate.node_id] = (
                f"induced_fit_risk {risk} > {risk_ceiling}: ATM converges poorly on exactly "
                "these, so the label would be spent twice over"
            )
            continue
        plan.abfe.append(candidate.node_id)
    return plan


def scaffold_holdout(
    candidates: Sequence[Candidate], *, fraction: float = 0.2,
) -> tuple[list[str], list[str]]:
    """Split by scaffold cluster, returning `(train, holdout)` node ids.

    **Whole clusters move together and the boundary never splits one.** A random split leaves
    near-duplicates of held-out designs in the training set, so the model scores well by having
    memorised a neighbour and the evaluation reports a generalisation it does not have. That is
    the single most common way a surrogate's reported accuracy is wrong, and it is invisible in
    the numbers.

    Clusters are taken largest-first until the holdout would exceed the fraction, which
    overshoots less than filling smallest-first and is deterministic. An unclustered candidate
    is **refused**, because treating `""` as one cluster would silently put every unclustered
    design on the same side of the boundary, and treating each as its own would make the split
    random by another name.
    """
    if not 0.0 < fraction < 1.0:
        raise ValueError(f"fraction must be in (0, 1), got {fraction}")
    unclustered = [c.node_id for c in candidates if not c.scaffold]
    if unclustered:
        raise ValueError(
            f"candidates with no scaffold cluster: {sorted(unclustered)}. Treating them as one "
            "cluster puts every unclustered design on the same side of the boundary; treating "
            "each as its own makes this a random split wearing a cluster's name."
        )
    clusters: dict[str, list[str]] = {}
    for candidate in candidates:
        clusters.setdefault(candidate.scaffold, []).append(candidate.node_id)

    target = math.floor(len(candidates) * fraction)
    holdout: list[str] = []
    # Largest first, by size then name: deterministic, and it reaches the target with fewer
    # clusters than filling from the smallest end.
    for name in sorted(clusters, key=lambda n: (-len(clusters[n]), n)):
        members = clusters[name]
        if holdout and len(holdout) + len(members) > target:
            continue
        if len(members) > target and holdout:
            continue
        holdout.extend(members)
    train = [c.node_id for c in candidates if c.node_id not in set(holdout)]
    return train, holdout
