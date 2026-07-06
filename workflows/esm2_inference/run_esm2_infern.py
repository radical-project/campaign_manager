#!/usr/bin/env python3
"""
ESM2 Inference Runner

Example script demonstrating how to run ESM2 inference using the spherical framework.
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from radical.asyncflow import WorkflowEngine

from src.inference.esm2_service import ESM2Client, ESM2InferenceService
from src.inference.orchestrator import init_clients, start_services, start_services_local
from src.inference.utils import load_config
from src.utils.logger import Logger

logger = Logger(use_colors=True)


async def init_asyncflow(config: dict):
    engine_name = config.get("engine", "concurrent").lower()
    if "dragon" in engine_name:
        from rhapsody.backends import DragonExecutionBackendV3

        engine = await DragonExecutionBackendV3()
        logger.info("Asyncflow enabled with DragonExecutionBackendV3")
    elif "dask" in engine_name:
        from rhapsody.backends import DaskExecutionBackend

        engine = await DaskExecutionBackend
        logger.info("Asyncflow enabled with DaskExecutionBackend")
    else:
        from rhapsody.backends import ConcurrentExecutionBackend

        engine = await ConcurrentExecutionBackend()
        logger.info("Asyncflow enabled with ConcurrentExecutionBackend")
    return await WorkflowEngine.create(engine)


async def main(config_file: str, mode: str):
    config = load_config(config_file)

    asyncflow = await init_asyncflow(config)

    telemetry = None
    if config.get("collect_telemetry", False):
        telemetry_dir = config.get("telemetry_dir", "telemetry_output")
        if hasattr(asyncflow, "start_telemetry"):
            telemetry = await asyncflow.start_telemetry(
                resource_poll_interval=0.5,
                checkpoint_path=telemetry_dir,
            )
            print(f"Started Asyncflow telemetry → {telemetry_dir}")

    if mode == "server":
        services = await start_services(config, ESM2InferenceService)
    else:
        services = await start_services_local(config, ESM2InferenceService)

    clients = await init_clients(config, services, ESM2Client, asyncflow)

    try:
        if clients is None:
            logger.error("Unable to initiate clients")
        else:
            await asyncio.gather(
                asyncio.gather(*(client.run() for client in clients)),
            )

    except Exception as e:
        logger.error(f"An error occurred while running inference: {e}")
    finally:
        # Close clients first
        for client in clients:
            if hasattr(client, "close"):
                await client.close()

        # Then close services
        for service in services:
            if hasattr(service, "close"):
                await service.close()

        await asyncflow.shutdown()

        if telemetry:
            await telemetry.stop()
            print("Asyncflow telemetry stopped")

    print("All work has been completed...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ESM2 inference")
    parser.add_argument(
        "--config_file", type=str, default="config.yaml", help="Path to configuration file"
    )
    parser.add_argument(
        "--mode",
        choices=["server", "local"],
        default="server",
        help="Run mode: server (hosting model on server, default) or use model locally",
    )
    args = parser.parse_args()

    asyncio.run(main(args.config_file, args.mode))

# Hint: use this command to run with dragon dragon -w ssh --network-config slurm.yaml  run_esm2_infern.py
