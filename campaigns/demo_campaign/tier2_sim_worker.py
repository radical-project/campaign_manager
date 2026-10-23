"""Tier 2 subprocess worker for the demo campaign.

Launched by the CM via WorkflowConfig(launch_cmd=[sys.executable, __file__]).
Reads CM_WORKFLOW_ID / CM_DATA_ENDPOINT / CM_DATA_BACKEND from the environment,
runs a synthetic simulation identical to the in-process sim_worker, and emits
scores via the language-neutral client (src.aucam_client); a non-Python
program would call the `aucam-report` command instead.

Exit 0 = success, non-zero = failure.
"""

from __future__ import annotations

# Locate the campaign_manager package root so src.aucam_client is importable.
import os
import random
import sys
import time

_here = os.path.dirname(os.path.abspath(__file__))
_root = os.path.join(_here, "..", "..")  # campaigns/demo_campaign → campaign_manager
sys.path.insert(0, os.path.abspath(_root))

from src.aucam_client import Reporter  # noqa: E402


def main() -> int:
    reporter = Reporter()

    n_iters = int(os.environ.get("CM_T2_N_ITERS", "10"))
    sleep_s = float(os.environ.get("CM_T2_SLEEP_S", "0.08"))
    converge = random.random() < 0.65  # 65% convergence rate (same as sim_worker)

    score = 0.5
    for step in range(n_iters):
        # Check for a CM signal before each step.
        if reporter.poll_signal() == "PAUSE":
            reporter.wait_for_resume()

        # Simulate score drift.
        delta = random.gauss(+0.05, 0.1) if converge else random.gauss(-0.08, 0.1)
        score = max(0.0, min(1.0, score + delta))

        reporter.emit(
            score=score,
            metrics={"step": step, "converging": converge},
            iteration=step,
        )
        time.sleep(sleep_s)

    # Final emit with the terminal score.
    reporter.emit(
        score=score,
        metrics={"step": n_iters, "converging": converge, "score": score},
        iteration=n_iters,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
