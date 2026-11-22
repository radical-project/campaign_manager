#!/usr/bin/env bash
# Experiment 2: the steered campaign and its three baselines, then the figures.
#
#   ./run_quality_experiment.sh            # 60 good designs (default)
#   GOAL_N=30 ./run_quality_experiment.sh
#
# Telemetry goes to runs/q_*.jsonl, each run's full log to logs/q_*.log,
# and the figures to figures/quality/.
set -u
cd "$(dirname "$0")"
mkdir -p runs logs

GOAL_N="${GOAL_N:-60}"
COMMON=(--multi-arm --quality-bar 0.7 --goal-n "$GOAL_N" --max-concurrent 2)

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

run q_bandit
run q_even           --policy even
run q_bandit_nostops --no-stops
run q_even_nostops   --policy even --no-stops

python plot_quality.py --runs runs --out figures/quality
