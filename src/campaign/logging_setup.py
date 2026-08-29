"""Campaign Manager stdlib logging initialisation.

Mirrors the enable_logging() API from radical.adr so the full stack
(Campaign Manager, ADR, AsyncFlow, RHAPSODY) can be configured to the
same format and level with a single call.

Usage
-----
    import logging
    from src.campaign import enable_logging

    enable_logging(logging.INFO)              # campaign namespace only
    enable_logging(logging.DEBUG,             # full stack + file
                   output_file="run.log",
                   configure_stack=True)

Compatible with radical.adr, radical.asyncflow, and RHAPSODY
-------------------------------------------------------------
All four projects use the same pipe-delimited format, so their lines
interleave uniformly and remain filterable by namespace prefix.  Like
radical.adr, each namespace logger is configured with propagate=False
so lines are never duplicated and survive a sibling reconfiguring the
root logger.

Log levels
----------
INFO    Lifecycle milestones — CM startup, group status transitions,
        replica failures, monitor drift warnings, ADR goal-reached / stop.
DEBUG   Per-cycle detail — scheduling decisions, sharder dispatch,
        ADR per-action trace, task settle events.
WARNING Anomalies only — resource stalls, surrogate drift, replanning
        state changes, endpoint failures.
"""

from __future__ import annotations

import logging
import sys
from typing import Optional

_PIPE_FMT = "%(asctime)s | %(levelname)-8s | %(name)-36s | %(message)s"
_DETAIL_FMT = (
    "%(asctime)s | %(levelname)-8s | %(name)-36s"
    " | [pid=%(process)d tid=%(thread)d] | %(message)s"
)
_DATE_FMT = "%H:%M:%S"

_LEVEL_COLORS: dict[str, str] = {
    "DEBUG":    "\033[90m",        # dim grey
    "INFO":     "\033[96m",        # cyan
    "WARNING":  "\033[93m",        # yellow
    "ERROR":    "\033[91m",        # bright red
    "CRITICAL": "\033[1m\033[91m", # bold bright red
}
_RESET = "\033[0m"


class _ColorFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        line = super().format(record)
        color = _LEVEL_COLORS.get(record.levelname, "")
        if color:
            line = line.replace(record.levelname, f"{color}{record.levelname}{_RESET}", 1)
        return line


def enable_logging(
    level: int = logging.INFO,
    *,
    output_file: Optional[str] = None,
    use_colors: bool = True,
    show_details: bool = False,
    configure_stack: bool = False,
) -> None:
    """Configure stdlib logging for the Campaign Manager.

    Parameters
    ----------
    level:
        Minimum log level, e.g. ``logging.INFO`` or ``logging.DEBUG``.
    output_file:
        If given, also write to this path (ANSI codes stripped).
    use_colors:
        Colorize the level name on a TTY console.
    show_details:
        Append ``pid`` / ``tid`` to each line.
    configure_stack:
        Also configure ``radical.adr``, ``radical.asyncflow``, and
        ``rhapsody`` (when installed) at the same level and format.
    """
    fmt = _DETAIL_FMT if show_details else _PIPE_FMT

    # ── console handler ──────────────────────────────────────────────────────
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(level)
    if use_colors and sys.stderr.isatty():
        console.setFormatter(_ColorFormatter(fmt, datefmt=_DATE_FMT))
    else:
        console.setFormatter(logging.Formatter(fmt, datefmt=_DATE_FMT))

    handlers: list[logging.Handler] = [console]

    # ── optional file handler (plain text, no ANSI) ──────────────────────────
    if output_file:
        fh = logging.FileHandler(output_file)
        fh.setLevel(level)
        fh.setFormatter(logging.Formatter(fmt, datefmt=_DATE_FMT))
        handlers.append(fh)

    # ── namespaces to configure ──────────────────────────────────────────────
    # Both "src.campaign" and "campaign" cover the two common import roots
    # (runner adds project root to sys.path → "src.campaign.*"; library users
    # install the package → "campaign.*").
    namespaces = ["src.campaign", "campaign"]
    if configure_stack:
        namespaces += ["radical.adr", "radical.asyncflow"]
        try:
            import rhapsody as _rh  # noqa: F401
            namespaces.append("rhapsody")
        except ImportError:
            pass

    for ns in namespaces:
        lg = logging.getLogger(ns)
        lg.setLevel(level)
        lg.handlers.clear()
        for h in handlers:
            lg.addHandler(h)
        lg.propagate = False
