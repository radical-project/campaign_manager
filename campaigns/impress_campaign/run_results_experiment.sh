#!/usr/bin/env bash
# Experiment 1: the steered campaign, its baseline and the two in-between runs,
# then the figures.
#
#   ./run_results_experiment.sh            # 60 results (default)
#   GOAL_N=30 ./run_results_experiment.sh
#
# Telemetry goes to runs/*.jsonl, each run's full log to logs/*.log, and the
# figures to figures/. The figures compare the steered run with the baseline;
# the other two show what the bandit and the cancel rule each give alone.
set -u
cd "$(dirname "$0")"
mkdir -p runs logs

GOAL_N="${GOAL_N:-60}"
COMMON=(--multi-arm --goal-n "$GOAL_N" --max-concurrent 2)

run() {  # run NAME [extra flags...]
    local name=$1; shift
    echo "$(date +%T)  $name ..."
    if python run_campaign.py "${COMMON[@]}" "$@" \
        --telemetry-out "runs/$name.jsonl" --aucam-log "logs/$name.aucam.log" \
        > "logs/$name.log" 2>&1; then
        grep -E "spent  |completed=" "logs/$name.log" | sed 's/^.*INFO  *[^ ]* *//'
    else
        echo "  FAILED — see logs/$name.log"
    fi
}

run bandit      --cost-cancel 0.008                  # steered: bandit + cancel rule
run even        --policy even                        # baseline: neither
run bandit_only                                      # the bandit alone
run even_cancel --policy even --cost-cancel 0.008    # the cancel rule alone

python plot_results.py --telemetry runs/bandit.jsonl --baseline runs/even.jsonl --out figures
