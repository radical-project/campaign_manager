#!/usr/bin/env python3
"""
run_orbit_workflow.py — run the full DummyWorkflow on a remote orbit endpoint.

Submits run_workflow.py as a single rhapsody task on the compute node.
The entire asyncflow engine, simulation, training, and prediction loop
run remotely; this script just connects, submits, waits, and prints the result.

Prerequisites
-------------
  # Terminal 1 — broker (login node)
  ./bin/radical-orbit-broker.py --port 8020

  # Terminal 2 — endpoint (compute node inside allocation)
  export RADICAL_ORBIT_BROKER_URL=https://<login-node>:8020
  ./bin/radical-orbit-endpoint.py --name my-endpoint -p rhapsody

Usage
-----
  export RADICAL_ORBIT_BROKER_URL=https://dt-login03.delta.ncsa.illinois.edu:8020
  python run_orbit_workflow.py
  python run_orbit_workflow.py --config-file config.yaml --output-dir /scratch/bblj/$USER/ddsim_output
"""

import argparse
import asyncio
from pathlib import Path


DUMMY_DIR = Path(__file__).parent.resolve()


async def main(args: argparse.Namespace) -> None:
    import rhapsody

    python_exe  = args.python_exe
    work_dir    = args.work_dir
    config_file = args.config_file if Path(args.config_file).is_absolute() \
                  else str(Path(work_dir) / args.config_file)
    output_dir  = args.output_dir
    log_file    = str(Path(output_dir) / "run_workflow.log")

    print("Connecting to orbit ...")
    backend = await rhapsody.get_backend("orbit", backends=["concurrent"])
    session = rhapsody.Session(backends=[backend])
    print(f"Connected → broker='{backend._broker_url}'  endpoint='{backend._endpoint_name}'")

    # cd into the dummy_workflow directory so relative paths in config.yaml
    # (home_dir, telemetry_dir) resolve correctly on the compute node.
    cmd = (
        f"mkdir -p {output_dir} && "
        f"cd {work_dir} && "
        f"{python_exe} run_workflow.py --config_file {config_file} "
        f"> {log_file} 2>&1"
    )

    task = rhapsody.ComputeTask(executable="/bin/bash", arguments=["-c", cmd])

    print("Submitting DummyWorkflow ...")
    print(f"  python     = {python_exe}")
    print(f"  work_dir   = {work_dir}")
    print(f"  config     = {config_file}")
    print(f"  log        = {log_file}")
    print()

    await session.submit_tasks([task])
    print("Waiting for completion ...")
    await session.wait_tasks([task])

    exit_code = task.get("exit_code", 0)
    if exit_code != 0:
        print(f"Workflow FAILED (exit={exit_code})")
        print(f"Check log: {log_file}")
    else:
        print("Workflow completed successfully.")
        print(f"Log: {log_file}")
        # Print last few lines of the remote log so the result is visible here.
        try:
            lines = Path(log_file).read_text().splitlines()
            print()
            print("\n".join(lines[-20:]))
        except OSError:
            pass

    await session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the full DummyWorkflow on a remote orbit endpoint"
    )
    parser.add_argument(
        "--python-exe",
        default="/u/mgoliyad1/ve/orbit_campaign/bin/python",
        help="Python interpreter on the compute node (default: ve/orbit_campaign)",
    )
    parser.add_argument(
        "--work-dir",
        default=str(DUMMY_DIR),
        help="Working directory for run_workflow.py on the compute node "
             "(default: this script's directory)",
    )
    parser.add_argument(
        "--config-file",
        default="config.yaml",
        help="Config file — relative to work-dir or absolute (default: config.yaml)",
    )
    parser.add_argument(
        "--output-dir",
        default="/scratch/bblj/mgoliyad1/orbit_ddsim_output",
        help="Shared scratch directory for the remote log file",
    )
    args = parser.parse_args()
    asyncio.run(main(args))
