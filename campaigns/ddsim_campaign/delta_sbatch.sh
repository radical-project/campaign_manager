#!/bin/sh -l
#
# SPHERICAL Dummy Campaign — SLURM batch script (CPU-only, no GPU needed)
#
# Account: set SBATCH_ACCOUNT=<project>-delta-cpu before calling sbatch
#SBATCH --partition=cpu
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --time=00:30:00
#SBATCH --job-name=ddsim_campaign
#SBATCH --mail-user=mariya.goliyad@rutgers.edu
#SBATCH --mail-type=END,FAIL
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.err

# ── Environment ───────────────────────────────────────────────────────────────
if [ -z "${SBATCH_ACCOUNT:-}${SLURM_JOB_ACCOUNT:-}" ]; then
    echo "WARNING: SBATCH_ACCOUNT is not set — job may be charged to default account."
    echo "         Set it with: export SBATCH_ACCOUNT=<project>-delta-cpu"
fi
echo "Account: ${SLURM_JOB_ACCOUNT:-unknown}"

if [ -z "${SCRATCH:-}" ]; then
    echo "ERROR: SCRATCH is not set."
    echo "       export SCRATCH=/scratch/<allocation> && sbatch delta_sbatch.sh"
    exit 1
fi
export CM_DIR="${CM_DIR:-${SCRATCH}/${USER}/campaign_manager}"
export ENV_DIR="${ENV_DIR:-/u/${USER}/ve/ddsim_campaign}"

unset SLURM_EXPORT_ENV
module load anaconda3 2>/dev/null || true

source "${ENV_DIR}/bin/activate"

# Enable asyncio debug mode: logs slow callbacks (>100ms) to stderr
export PYTHONASYNCIODEBUG=1

# ── Run campaign ──────────────────────────────────────────────────────────────
CAMPAIGN_DIR="${CM_DIR}/campaigns/ddsim_campaign"
cd "${CAMPAIGN_DIR}"

BENCHMARK="${1:-}"
if [ -z "${BENCHMARK}" ]; then
    echo "ERROR: benchmark not specified."
    echo "       Usage: sbatch delta_sbatch.sh <bmark1|bmark2|bmark3>"
    exit 1
fi

case "${BENCHMARK}" in

bmark1)
    # Three-pool stall benchmark — rule has a blind spot on the fast feeder.
    # ConsensusPolicy (performance mode) uses obs-driven selection when no
    # majority exists; LLM classifies scheduling mode via ModeLLMSchedulingPolicy.
    #
    # Expected: consensus ≈ llm  <<  bandit  <<  rule
    python benchmark.py --config config_consensus_3stage.yaml --runs 5 \
        --policies rule bandit llm consensus \
        --out stall_benchmark_results_3stage.json
    python make_benchmark_plots.py --data stall_benchmark_results_3stage.json --out bmark1.png
    ;;

bmark1b)
    # LLM timeout degradation sweep.
    # none/rule/bandit are timeout-independent baselines.
    # llm degrades as timeout tightens; consensus stays robust via rule/bandit arms.

    # Reference: full-capability run (config timeout = 8.0 s).
    python benchmark.py --config config_consensus_3stage.yaml --runs 5 \
        --policies none rule bandit llm consensus \
        --out stall_benchmark_results_t8s.json
    python make_benchmark_plots.py --data stall_benchmark_results_t8s.json \
        --benchmark bmark1b --out bmark1b_t8s.png

    # Tight timeout sweeps.
    python benchmark.py --config config_consensus_3stage.yaml --runs 5 \
        --policies none rule bandit llm consensus \
        --llm-timeout 2.0 \
        --out stall_benchmark_results_t2s.json
    python make_benchmark_plots.py --data stall_benchmark_results_t2s.json \
        --benchmark bmark1b --out bmark1b_t2s.png

    python benchmark.py --config config_consensus_3stage.yaml --runs 5 \
        --policies none rule bandit llm consensus \
        --llm-timeout 1.0 \
        --out stall_benchmark_results_t1s.json
    python make_benchmark_plots.py --data stall_benchmark_results_t1s.json \
        --benchmark bmark1b --out bmark1b_t1s.png

    python benchmark.py --config config_consensus_3stage.yaml --runs 5 \
        --policies none rule bandit llm consensus \
        --llm-timeout 0.5 \
        --out stall_benchmark_results_t0s5.json
    python make_benchmark_plots.py --data stall_benchmark_results_t0s5.json \
        --benchmark bmark1b --out bmark1b_t0s5.png
    ;;

bmark2)
    # Pipeline-isolation concurrent-pipeline benchmark.
    # flat_global        — global throttle blind spot: THROTTLE from either analysis
    #                      stage demotes BOTH sims, stalling the unrelated pipeline.
    # isolated_delegated — each sim reacts only to its own analysis stage's backpressure.
    # isolated_centralized — rebalances hit-count imbalance every cycle when gap > 5%.
    # isolated_debounced — same as centralized but rebalances only when imbalance > 20%
    #                      for ≥2 consecutive cycles; ignores transient spikes.
    #
    # Expected: isolated_debounced ≈ isolated_centralized ≈ isolated_delegated  <<  flat_global
    python benchmark.py --benchmark bmark2 --config config_bmark2.yaml --runs 5 \
        --policies flat_global isolated_delegated isolated_centralized isolated_debounced \
        --out bmark2_results.json
    python make_benchmark_plots.py --data bmark2_results.json --out bmark2.png
    ;;

bmark3)
    # Temporal adaptation to stage depletion.
    #
    # fast_sim (200 rep, 0.10 s) → analysis_fast (cap=2, 0.20 s) — creates Phase-2 backlog.
    # slow_sim ( 30 rep, 0.50 s) → analysis_slow (cap=1, 0.05 s) — the target metric.
    # total_cpus=4.
    #
    # rule_static   — always gives analysis_fast priority 110; slow_sim priority 90.
    # adr_reactive  — detects fast_sim depletion; demotes analysis_fast, promotes slow_sim.
    # adr_proactive — predicts depletion via eta_s<2s; rebalances ~2 s before depletion.
    #
    # Expected: rule_static > adr_reactive > adr_proactive  (lower ttt = better)
    python benchmark.py --benchmark bmark3 --config config_temporal.yaml --runs 5 \
        --policies rule_static adr_reactive adr_proactive \
        --out temporal_benchmark_results.json
    python make_benchmark_plots.py --data temporal_benchmark_results.json --out bmark3.png
    ;;

bmark4)
    # Multi-operator + hierarchical + budget-control benchmark.
    #
    # Three conditions on two quality-asymmetric chains sharing 4 CPUs:
    #   Chain 1: fast_sim (0.10 s, 200 rep, score_mean≈0.55) → analysis_fast
    #   Chain 2: slow_sim (0.40 s,  30 rep, score_mean≈0.80) → analysis_slow
    #
    # flat_rule        — single operator; analysis_fast THROTTLE demotes BOTH sims.
    # multi_specialized — two chain-specialized operators; per-chain backpressure.
    # hier_parent      — parent boosts slow_sim from start; demotes fast_sim at
    #                    30 CPU-s budget exhaustion.
    #
    # Primary metric: quality_yield = analysis_slow completions at budget crossing.
    # Expected ordering (higher = better): flat_rule << multi_specialized < hier_parent
    python benchmark.py --benchmark bmark4 --config config_bmark4.yaml --runs 5 \
        --policies flat_rule multi_specialized hier_parent \
        --out bmark4_results.json
    python make_bmark4_plot.py --data bmark4_results.json --out bmark4.png
    ;;

*)
    echo "ERROR: unknown benchmark '${BENCHMARK}'."
    echo "       Valid options: bmark1  bmark1b  bmark2  bmark3  bmark4"
    exit 1
    ;;

esac

echo "=== DDSim campaign done: $(date) ==="

rm -rf asyncflow.session*
