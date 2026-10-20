"""Backward-compatible Python reporter for command-line (Tier 2) workflows.

New code should use :mod:`src.aucam_client` (``Reporter`` or the
``aucam-report`` command): it implements CM's language-neutral reporting
protocol and does not import the rest of AuCaM.  ``SubprocessReporter`` is the
same reporter under its old name.

    from src.aucam_client import Reporter

    reporter = Reporter()                    # reads CM_WORKFLOW_ID / CM_DATA_*
    reporter.emit(score=0.85, metrics={...}) # JSON report to the CM's data backend
    sig = reporter.poll_signal()             # non-blocking: None | "PAUSE" | "RESUME"
"""

from __future__ import annotations

from src.aucam_client import Reporter


class SubprocessReporter(Reporter):
    """Deprecated alias of :class:`src.aucam_client.Reporter`."""
