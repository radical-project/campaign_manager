"""``aucam-report`` — report to CM from any program (shell, C++, Fortran, ...).

    aucam-report emit [--score VALUE] [--metric KEY=VALUE]... [--metrics JSON]
                      [--iteration N]
    aucam-report signal                 # prints PAUSE or RESUME, or nothing
    aucam-report wait-resume [--interval SECONDS]

Values are parsed as JSON when possible (``0.8``, ``true``, ``{"a": 1}``) and
kept as strings otherwise.  Settings come from the CM_* environment variables
that CM exports to the program.  Exit status: 0 on success, 2 when the
environment is not a CM workflow or a value is invalid, 1 on backend errors.
"""

from __future__ import annotations

import argparse
import json
import sys

from . import Reporter, ReporterConfigError


def _value(text: str):
    try:
        return json.loads(text)
    except ValueError:
        return text


def _metric(text: str) -> tuple[str, object]:
    if "=" not in text:
        raise argparse.ArgumentTypeError(f"expected KEY=VALUE, got {text!r}")
    key, val = text.split("=", 1)
    return key, _value(val)


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="aucam-report", description="Report to AuCaM from a command-line workflow."
    )
    sub = p.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("emit", help="send a report (score and metrics)")
    e.add_argument("--score", type=_value, default=None, help="score (JSON or text)")
    e.add_argument(
        "--metric",
        type=_metric,
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="one metric; repeatable",
    )
    e.add_argument(
        "--metrics",
        type=json.loads,
        default=None,
        help="metrics as a JSON object (merged with --metric)",
    )
    e.add_argument("--iteration", type=int, default=0)
    sub.add_parser("signal", help="print a pending PAUSE/RESUME signal, if any")
    w = sub.add_parser("wait-resume", help="block until RESUME arrives")
    w.add_argument("--interval", type=float, default=1.0)
    return p


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        rep = Reporter()
        if args.cmd == "emit":
            metrics = dict(args.metrics or {})
            if not isinstance(metrics, dict):
                raise TypeError("--metrics must be a JSON object")
            metrics.update(dict(args.metric))
            rep.emit(score=args.score, metrics=metrics, iteration=args.iteration)
        elif args.cmd == "signal":
            sig = rep.poll_signal()
            if sig:
                print(sig)
        else:
            rep.wait_for_resume(args.interval)
    except (ReporterConfigError, TypeError) as exc:
        print(f"aucam-report: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # backend unreachable, missing client library, ...
        print(f"aucam-report: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
