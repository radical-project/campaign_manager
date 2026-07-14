#!/bin/bash
# =============================================================================
# plot_telemetry.sh — Plot AsyncFlow workflow telemetry dashboard.
#
# Usage:
#   bash plot_telemetry.sh <telemetry.jsonl> [--out-dir DIR] [--split]
#
# Arguments:
#   <telemetry.jsonl>   Path to the JSONL telemetry checkpoint file (required).
#   --out-dir DIR       Output directory (default: plots/<wf_name>/ next to this
#                       script, where <wf_name> is inferred from the file path as
#                       the grandparent of the telemetry-output directory,
#                       e.g. miniapps_workflow/telemetry-output/... → plots/miniapps_workflow/).
#   --split             Save each subplot as a separate PNG instead of one combined
#                       dashboard image.
#
# Output (combined): workflow_dashboard_<YYYYMMDD_HHMMSS>.png
# Output (--split):  one <stem>.<panel>.png per subplot, in OUT_DIR.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -z "${SCRATCH:-}" ]; then
    echo "ERROR: SCRATCH is not set."
    echo "       export SCRATCH=/scratch/<allocation>"
    exit 1
fi
PLOT_SCRIPT="${SCRATCH}/${USER}/radical.asyncflow/examples/telemetry/plot_workflow_dashboard.py"

# ── Parse arguments ───────────────────────────────────────────────────────────
if [ $# -lt 1 ]; then
    echo "Usage: $0 <telemetry.jsonl> [--out-dir DIR] [--split]"
    exit 1
fi

JSONL_FILE="$1"
shift

# Derive workflow name from the grandparent directory of the JSONL file.
# Expected layout: <wf_name>/telemetry-output/<file>.jsonl
WF_NAME=$(basename "$(dirname "$(dirname "$(realpath "${JSONL_FILE}")")")")
if [ -z "${WF_NAME}" ] || [ "${WF_NAME}" = "." ]; then
    WF_NAME="workflow"
fi

OUT_DIR="${SCRIPT_DIR}/plots/${WF_NAME}"
SPLIT=0

while [[ $# -gt 0 ]]; do
    case $1 in
        --out-dir) OUT_DIR="$2"; shift 2 ;;
        --split)   SPLIT=1; shift ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

# ── Validate inputs ───────────────────────────────────────────────────────────
if [ ! -f "${JSONL_FILE}" ]; then
    echo "ERROR: telemetry file not found: ${JSONL_FILE}"
    exit 1
fi

if [ ! -f "${PLOT_SCRIPT}" ]; then
    echo "ERROR: plot script not found: ${PLOT_SCRIPT}"
    exit 1
fi

# ── Plot ──────────────────────────────────────────────────────────────────────
mkdir -p "${OUT_DIR}"

if [ "${SPLIT}" -eq 1 ]; then
    python "${PLOT_SCRIPT}" "${JSONL_FILE}" --split --out "${OUT_DIR}"
    echo "Telemetry panels saved to ${OUT_DIR}/"
else
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
    OUT_FILE="${OUT_DIR}/workflow_dashboard_${TIMESTAMP}.png"
    python "${PLOT_SCRIPT}" "${JSONL_FILE}" --out "${OUT_FILE}"
    echo "Telemetry plot saved to ${OUT_FILE}"
fi


# --- Plot telemetry ---
#bash plot_telemetry.sh telemetry-output/out.jsonl
#bash plot_telemetry.sh telemetry-output/out.jsonl --split
