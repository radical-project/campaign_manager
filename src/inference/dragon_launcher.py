#!/usr/bin/env python3
"""
Standalone launcher for Dragon server processes.

Dragon spawns processes on remote nodes via SSH. Those processes need to
unpickle the target function, but 'src' is not installed as a package and
isn't on the remote Python's sys.path. This launcher script solves that by:

1. Computing the project root from its own file path
2. Adding project root + script directory to sys.path
3. Dynamically importing the service class
4. Running the server via _server_node_main

Usage (called by Dragon, not directly):
    python dragon_launcher.py --config-json /path/to/config.json \
        --node-rank 0 --hostname node1.example.com --port 8000 \
        --service-module esm2_service --service-class ESM2InferenceService \
        --script-dir /path/to/examples/esm2
"""

import sys
from pathlib import Path

# Add project root to sys.path BEFORE any src imports.
# This file lives in SPHERICAL/src/inference/, so project root is three levels up.
_project_root = str(Path(__file__).resolve().parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import argparse  # noqa: E402
import ctypes  # noqa: E402
import importlib  # noqa: E402
import json  # noqa: E402
import signal  # noqa: E402


def _set_pdeathsig():
    """Request SIGTERM when parent process dies (Linux only).

    Dragon launches this script via subprocess.run in a managed process.
    When Dragon terminates that parent process, the kernel will send us
    SIGTERM so we can shut down gracefully instead of becoming an orphan.
    """
    try:
        pr_set_pdeathsig = 1
        libc = ctypes.CDLL("libc.so.6", use_errno=True)
        libc.prctl(pr_set_pdeathsig, signal.SIGTERM)
    except OSError:
        pass  # Not on Linux or libc unavailable; skip


def main():
    _set_pdeathsig()

    parser = argparse.ArgumentParser(description="Dragon server node launcher")
    parser.add_argument(
        "--config-json", required=True, help="Path to JSON config file on shared filesystem"
    )
    parser.add_argument("--node-rank", type=int, required=True)
    parser.add_argument("--hostname", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument(
        "--service-module",
        required=True,
        help="Python module containing the service class (e.g. esm2_service)",
    )
    parser.add_argument(
        "--service-class",
        required=True,
        help="Name of the InferenceService subclass (e.g. ESM2InferenceService)",
    )
    parser.add_argument(
        "--script-dir",
        default="",
        help="Directory to add to sys.path for finding the service module",
    )
    parser.add_argument("--use-https", action="store_true")
    args = parser.parse_args()

    # Add the script directory to sys.path so the service module is importable
    # (e.g. examples/esm2/ contains esm2_service.py)
    if args.script_dir and args.script_dir not in sys.path:
        sys.path.insert(0, args.script_dir)

    # Load config from shared filesystem
    with open(args.config_json) as f:
        config = json.load(f)

    # Dynamically import the service class
    mod = importlib.import_module(args.service_module)
    service_class = getattr(mod, args.service_class)

    # Now import and run the server
    from src.inference.orchestrator import _server_node_main

    _server_node_main(
        config,
        args.node_rank,
        args.hostname,
        args.port,
        service_class,
        args.use_https,
    )


if __name__ == "__main__":
    main()
