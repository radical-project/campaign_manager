"""Item 14's ranking rule, the ABFE risk gate and the scaffold holdout (EI.11).

**The sign of the bound is the first test in the file, because getting it backwards is the
failure the rule exists to prevent** — not a cosmetic error. ΔG is minimised, so the
conservative bound is `mean + k·sigma`; inverted, the ranking would put the model's widest
posteriors first and spend the allocation on its blind spots, while every number in the run
looked ordinary.

Three refusals are asserted rather than the behaviours they replace, and each one replaces a
*plausible* default: a missing sigma could be read as zero (ranking a blind spot as a certainty),
a missing risk score could be read as admissible (spending the scarcest label on the one design
nobody assessed), and an unclustered candidate could be read as its own cluster (making the
holdout random by another name).

**This file imports no `impress_a`**, so it runs wherever Python does. A ranking rule should be
checkable without a GPU, a container or a dependency checkout.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

# The campaign's own modules are not a package — `tests/test_asd_campaign.py` reaches them the
# same way, and for the same reason: the campaign directory is what gets copied to a cluster.
ASD = Path(__file__).parent.parent / "campaigns" / "asd_campaign"
sys.path.insert(0, str(ASD))

from acquisition import (  # noqa: E402
    DEFAULT_LCB_K,
    DEFAULT_RISK_CEILING,
    Candidate,
    acquire,
    lower_confidence_bound,
    rank,
    scaffold_holdout,
)


def c(node_id: str, dg: float, sigma: float | None = 0.5, risk: float | None = 0.1,
      scaffold: str = "s1") -> Candidate:
    return Candidate(node_id=node_id, dg_corrected=dg, sigma_dg=sigma,
                     induced_fit_risk=risk, scaffold=scaffold)


# ---------------------------------------------------------------------------
# the rule
# ---------------------------------------------------------------------------


def test_the_bound_is_above_the_mean_because_delta_g_is_minimised():
    """The sign, on a worked pair. Inverted, the ranking prefers the model's blind spots."""
    assert lower_confidence_bound(-9.0, 0.8, k=2.0) == pytest.approx(-9.0 + 1.6)
    assert lower_confidence_bound(-9.0, 0.8) > -9.0


def test_an_uncertain_design_does_not_outrank_a_confident_one_of_equal_mean():
    """Item 14's rule in one assertion: equal means, different widths, the narrow one wins."""
    confident, uncertain = c("confident", -9.0, sigma=0.1), c("uncertain", -9.0, sigma=2.0)
    assert [x.node_id for x in rank([uncertain, confident])] == ["confident", "uncertain"]


def test_a_wide_posterior_can_lose_to_a_worse_mean_which_is_the_whole_point():
    """A design with a better mean and a wide posterior ranks BELOW a worse, tighter one.

    This is what ranking on the bound buys and what ranking on the mean would throw away: the
    better mean is better only if the model is to be believed about it.
    """
    optimistic = c("optimistic", -11.0, sigma=3.0)   # bound -8.0
    modest = c("modest", -9.5, sigma=0.2)            # bound -9.3
    assert [x.node_id for x in rank([optimistic, modest])] == ["modest", "optimistic"]


def test_k_zero_collapses_the_rule_to_mean_ranking_and_says_so_by_arithmetic():
    """Not a feature; the knob's zero exists so the comparison is reachable in an experiment."""
    a, b = c("a", -11.0, sigma=3.0), c("b", -9.5, sigma=0.2)
    assert [x.node_id for x in rank([a, b], k=0.0)] == ["a", "b"]
    assert [x.node_id for x in rank([a, b], k=DEFAULT_LCB_K)] == ["b", "a"]


def test_ties_break_on_node_id_so_two_runs_agree():
    ranked = rank([c("zz", -9.0, sigma=0.5), c("aa", -9.0, sigma=0.5)])
    assert [x.node_id for x in ranked] == ["aa", "zz"]


def test_a_candidate_with_no_sigma_is_refused_rather_than_treated_as_certain():
    with pytest.raises(ValueError, match="no sigma_dg"):
        rank([c("fine", -9.0), c("nosigma", -9.5, sigma=None)])
    with pytest.raises(ValueError, match="no lower confidence bound"):
        c("nosigma", -9.5, sigma=None).lcb()


def test_a_negative_sigma_is_refused():
    with pytest.raises(ValueError, match="sigma must be"):
        lower_confidence_bound(-9.0, -0.1)


# ---------------------------------------------------------------------------
# the ABFE gate
# ---------------------------------------------------------------------------


def test_abfe_is_drawn_from_the_md_batch_and_never_from_the_wider_pool():
    """ATM wants the equilibrated holo frame md_label produces, so a design sent to ATM
    without MD would start from an unrelaxed structure."""
    pool = [c(f"n{i}", -9.0 - i * 0.1, sigma=0.2) for i in range(10)]
    plan = acquire(pool, md_budget=4, abfe_budget=10)
    assert len(plan.md) == 4
    assert set(plan.abfe) <= set(plan.md)


def test_a_risky_design_never_reaches_abfe_and_the_reason_is_recorded():
    """ATM converges poorly on exactly the designs B flags, so the label is spent twice over."""
    plan = acquire([c("safe", -10.0, risk=0.1), c("risky", -11.0, risk=0.9)],
                   md_budget=2, abfe_budget=2)
    assert plan.md == ["risky", "safe"], "the risk gate must not change the MD ranking"
    assert plan.abfe == ["safe"]
    assert "induced_fit_risk 0.9" in plan.refused_abfe["risky"]


def test_the_ceiling_is_a_parameter_and_the_default_is_flagged_as_a_placeholder():
    borderline = c("borderline", -10.0, risk=0.6)
    assert acquire([borderline], md_budget=1, abfe_budget=1).abfe == []
    assert acquire([borderline], md_budget=1, abfe_budget=1, risk_ceiling=0.7).abfe == \
        ["borderline"]
    assert DEFAULT_RISK_CEILING == 0.5


def test_a_design_with_no_risk_score_is_refused_from_abfe_by_default():
    """B is what makes the gate a gate; a missing score means nobody assessed the design."""
    plan = acquire([c("unassessed", -10.0, risk=None)], md_budget=1, abfe_budget=1)
    assert plan.abfe == []
    assert "unassessed" in plan.refused_abfe["unassessed"]


def test_running_without_model_b_has_to_be_asked_for_explicitly():
    """The unsteered control arm at M2 has no B to ask, and must say so rather than inherit it."""
    plan = acquire([c("unassessed", -10.0, risk=None)], md_budget=1, abfe_budget=1,
                   require_risk=False)
    assert plan.abfe == ["unassessed"]


def test_an_exhausted_abfe_budget_is_recorded_as_a_reason_not_as_silence():
    pool = [c(f"n{i}", -9.0 - i * 0.1, sigma=0.2) for i in range(5)]
    plan = acquire(pool, md_budget=5, abfe_budget=2)
    assert len(plan.abfe) == 2
    exhausted = [n for n, why in plan.refused_abfe.items() if "budget exhausted" in why]
    assert len(exhausted) == 3, "every refusal needs a reason, including the boring one"


def test_zero_budgets_acquire_nothing_rather_than_everything():
    pool = [c(f"n{i}", -9.0) for i in range(3)]
    assert acquire(pool, md_budget=0, abfe_budget=0).total == 0


# ---------------------------------------------------------------------------
# the holdout
# ---------------------------------------------------------------------------


def test_a_scaffold_cluster_is_never_split_across_the_boundary():
    """The whole difference between a holdout and a decoration.

    A random split leaves near-duplicates of held-out designs in the training set, so the model
    scores well by having memorised a neighbour -- and the reported generalisation is one it
    does not have. Invisible in the numbers, which is why this is a test and not a comment.
    """
    candidates = [c(f"a{i}", -9.0, scaffold="A") for i in range(6)]
    candidates += [c(f"b{i}", -9.0, scaffold="B") for i in range(2)]
    candidates += [c(f"c{i}", -9.0, scaffold="C") for i in range(2)]
    train, holdout = scaffold_holdout(candidates, fraction=0.2)

    by_id = {x.node_id: x.scaffold for x in candidates}
    train_clusters = {by_id[n] for n in train}
    holdout_clusters = {by_id[n] for n in holdout}
    assert not (train_clusters & holdout_clusters), (
        f"cluster(s) {train_clusters & holdout_clusters} appear on both sides"
    )
    assert set(train) | set(holdout) == set(by_id)
    assert not set(train) & set(holdout)


def test_the_holdout_does_not_exceed_the_fraction():
    candidates = [c(f"n{i}", -9.0, scaffold=f"s{i // 2}") for i in range(20)]
    _, holdout = scaffold_holdout(candidates, fraction=0.2)
    assert 0 < len(holdout) <= 4


def test_the_split_is_deterministic():
    candidates = [c(f"n{i}", -9.0, scaffold=f"s{i % 5}") for i in range(20)]
    assert scaffold_holdout(candidates) == scaffold_holdout(candidates)


def test_an_unclustered_candidate_is_refused():
    with pytest.raises(ValueError, match="no scaffold cluster"):
        scaffold_holdout([c("clustered", -9.0), c("loose", -9.0, scaffold="")])


def test_a_fraction_outside_the_open_unit_interval_is_refused():
    candidates = [c("n1", -9.0)]
    for bad in (0.0, 1.0, -0.1, 1.5):
        with pytest.raises(ValueError, match="fraction must be"):
            scaffold_holdout(candidates, fraction=bad)


def test_the_holdout_is_asteer_s_rule_and_not_a_second_copy_of_it():
    """Item 50, closed on this side. One rule, two repos, and its failure is silent.

    The same rule was implemented here over `Candidate` objects and in ROME's
    `preorg_train_wrapper.scaffold_split` over corpus rows. What this asserts is not that the
    arithmetic is right -- asteer's own tests do that against both copies, over 2,160 splits --
    but that this function is a thin wrapper, so the two cannot drift apart again.

    `scaffold_holdout` keeps its own refusal: it **raises**, where ROME's returns `([], [])`
    into a manifest, because this one is about to spend ATM hours on the batch.
    """
    pytest.importorskip("asteer", reason="asteer is a sibling checkout, not a dependency of AuCaM")
    from asteer.grouping import holdout_indices

    candidates = [c(f"n{i}", -9.0, scaffold=f"s{i // 3}") for i in range(10)]
    train, holdout = scaffold_holdout(candidates, fraction=0.3)
    train_at, holdout_at = holdout_indices([x.scaffold for x in candidates], 0.3)

    assert [candidates[i].node_id for i in train_at] == train
    assert [candidates[i].node_id for i in holdout_at] == holdout


# ---------------------------------------------------------------------------
# against a reference implementation
# ---------------------------------------------------------------------------


def test_the_acquired_batch_matches_a_reference_written_independently():
    """The plan's acceptance check: a reference batch-BO over the same synthetic posteriors.

    Written as the obvious one-liner rather than reusing anything from the module, so the two
    agree only if the rule is what it says. Deliberately uses `sorted(..., key=mean + k*sigma)`
    with no tie-breaking subtlety and no gate, then applies the gate separately -- if the module
    ever folds the risk gate into the ranking, this disagrees.
    """
    import random

    rng = random.Random(20261120)
    pool = [
        Candidate(node_id=f"n{i:03d}",
                  dg_corrected=rng.uniform(-12.0, -6.0),
                  sigma_dg=rng.uniform(0.05, 2.5),
                  induced_fit_risk=rng.uniform(0.0, 1.0),
                  scaffold=f"s{i % 7}")
        for i in range(120)
    ]
    k, ceiling, md_budget, abfe_budget = 1.0, 0.5, 40, 12

    reference_md = [x.node_id for x in sorted(
        pool, key=lambda x: (x.dg_corrected + k * x.sigma_dg, x.node_id))][:md_budget]
    by_id = {x.node_id: x for x in pool}
    reference_abfe = [n for n in reference_md
                      if by_id[n].induced_fit_risk <= ceiling][:abfe_budget]

    plan = acquire(pool, md_budget=md_budget, abfe_budget=abfe_budget,
                   k=k, risk_ceiling=ceiling)
    assert plan.md == reference_md
    assert plan.abfe == reference_abfe
    assert len(plan.abfe) <= abfe_budget


def test_the_risk_gate_does_not_reorder_the_md_batch():
    """The gate selects a subset; it must not be folded into the ranking.

    Folding it in would mean a risky design ranked below a safe one of worse LCB, so the MD
    batch -- which is not risk-gated at all -- would silently inherit the ABFE policy.
    """
    import random

    rng = random.Random(7)
    pool = [Candidate(node_id=f"n{i:03d}", dg_corrected=rng.uniform(-12.0, -6.0),
                      sigma_dg=rng.uniform(0.05, 2.0),
                      induced_fit_risk=rng.uniform(0.0, 1.0), scaffold="s")
            for i in range(50)]
    ranked = [x.node_id for x in rank(pool)]
    plan = acquire(pool, md_budget=50, abfe_budget=0)
    assert plan.md == ranked


# ---------------------------------------------------------------------------
# the join to the dispatcher, which is where this rule reaches a campaign
# ---------------------------------------------------------------------------


def test_the_plan_becomes_the_dispatchers_risk_gate_and_they_agree_exactly():
    """One implementation of the ABFE gate, with the dispatcher as its call site.

    `ScriptedDispatcher` holds node ids and no posteriors, which is exactly why it takes an
    injected `risk_gate` rather than a threshold -- so the gate is item 14's to supply and the
    dispatcher's to apply. This asserts the dispatcher's ABFE batch is the plan's, which is
    what makes `acquire` the single implementation rather than the first of two.
    """
    pytest.importorskip("asteer", reason="asteer is a sibling checkout, not a dependency of AuCaM")
    from asteer.dispatch import ABFE_CAMPAIGN, MD_CAMPAIGN, ScriptedDispatcher

    pool = [c(f"n{i:02d}", -9.0 - i * 0.1, sigma=0.2, risk=0.9 if i % 3 == 0 else 0.1)
            for i in range(12)]
    plan = acquire(pool, md_budget=8, abfe_budget=4)
    assert plan.abfe, "the fixture must leave something admissible or this proves nothing"
    assert plan.refused_abfe, "and something refused"

    dispatcher = ScriptedDispatcher("asd-smb-mock", md_per_round=8, abfe_per_round=4,
                                    risk_gate=plan.as_risk_gate())
    batches = {b.campaign: b for b in dispatcher.batches(plan.md)}
    assert list(batches[MD_CAMPAIGN].node_ids) == plan.md
    assert list(batches[ABFE_CAMPAIGN].node_ids) == plan.abfe


def test_an_empty_abfe_plan_launches_no_abfe_campaign_at_all():
    """An empty batch would start a workflow that measures nothing, bill it, and report
    success -- the failure class this project has found five times."""
    pytest.importorskip("asteer", reason="asteer is a sibling checkout, not a dependency of AuCaM")
    from asteer.dispatch import ABFE_CAMPAIGN, ScriptedDispatcher

    pool = [c(f"n{i}", -9.0, sigma=0.2, risk=0.95) for i in range(4)]
    plan = acquire(pool, md_budget=4, abfe_budget=4)
    assert plan.abfe == []
    dispatcher = ScriptedDispatcher("asd-smb-mock", risk_gate=plan.as_risk_gate())
    assert ABFE_CAMPAIGN not in {b.campaign for b in dispatcher.batches(plan.md)}
