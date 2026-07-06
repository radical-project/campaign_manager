#!/usr/bin/env python3
"""
Inference Orchestrator

Manages multi-node deployment of inference services.

Features:
- Launch servers on multiple nodes
- Dragon process placement for true multi-node deployment
- Wait for servers to become healthy
- Create clients for remote inference
- Graceful shutdown coordination
"""

import asyncio
import itertools
import socket
import sys
from typing import Any, Optional

from aiohttp import web

from ..utils.logger import Logger
from .server import get_app, init_server
from .utils import get_devices_for_node, get_slurm_nodes

logger = Logger(use_colors=True)

__all__ = [
    "launch_server",
    "launch_servers",
    "wait_for_healthy",
    "start_services",
    "start_services_local",
    "init_clients",
    "ServiceHandle",
]


# ---------------------------------------------------------------------------
# Health checks
# ---------------------------------------------------------------------------


async def wait_for_healthy(
    endpoints: list[str],
    timeout: int = 60,
    check_interval: int = 2,
) -> list[str]:
    """
    Wait for servers to be ready and perform health checks.

    Args:
        endpoints: List of server endpoints
        timeout: Maximum time to wait in seconds
        check_interval: Time between health checks in seconds

    Returns:
        List of healthy endpoints
    """
    import time

    import aiohttp

    logger.separator(title="WAITING FOR SERVERS TO BE READY")

    start_time = time.time()

    async def check_endpoint(endpoint: str) -> bool:
        """Check if endpoint is healthy."""
        health_url = f"{endpoint}/health"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(health_url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        return True
                    return False
        except Exception:
            return False

    healthy = []
    prev_healthy = -1
    last_log_time = 0
    while (time.time() - start_time) < timeout:
        health_checks = [check_endpoint(ep) for ep in endpoints]
        health_results = await asyncio.gather(*health_checks)

        healthy = [
            ep for ep, is_healthy in zip(endpoints, health_results, strict=False) if is_healthy
        ]

        if len(healthy) == len(endpoints):
            logger.info(f"All {len(healthy)} servers are healthy!")
            return healthy

        elapsed = int(time.time() - start_time)
        now = time.time()
        # Log when status changes or every 30s to avoid flooding
        if len(healthy) != prev_healthy or (now - last_log_time) >= 30:
            logger.info(
                f"{len(healthy)}/{len(endpoints)} servers ready, waiting... ({elapsed}s/{timeout}s)"
            )
            prev_healthy = len(healthy)
            last_log_time = now

        await asyncio.sleep(check_interval)

    if healthy:
        logger.warning(f"Timeout: {len(healthy)}/{len(endpoints)} servers healthy")
    else:
        logger.error("Timeout: No healthy servers found!")

    return healthy


# ---------------------------------------------------------------------------
# Service handle with Dragon process lifecycle support
# ---------------------------------------------------------------------------


class ServiceHandle:
    """
    Wrapper for a running inference service with its endpoint.

    Supports two modes:
    - In-process: holds local service + aiohttp runner
    - Remote (Dragon): holds client-mode service + remote process reference
    """

    def __init__(
        self,
        endpoint: Optional[str],
        service: Optional[Any],
        runner: Optional[web.AppRunner] = None,
        dragon_process: Optional[Any] = None,
    ):
        self.endpoint = endpoint
        self.service = service
        self.runner = runner
        self.dragon_process = dragon_process

    async def close(self):
        """Shutdown the service and cleanup resources."""
        logger.separator(title="SHUTTING DOWN SERVERS")
        try:
            logger.info(f"Shutting down {self.endpoint}...")
            if self.dragon_process is not None:
                # Remote mode (Dragon): terminate the server process
                self.dragon_process.terminate()
                self.dragon_process.join(timeout=30)
                logger.info(f"Remote process for {self.endpoint} terminated")
            else:
                # In-process mode: shutdown service and runner locally
                if self.service and hasattr(self.service, "shutdown"):
                    await self.service.shutdown()
                if self.runner:
                    await self.runner.cleanup()
        except Exception as e:
            logger.error(f"Error shutting down {self.endpoint}: {e}")

    def __iter__(self):
        """Allow unpacking as (endpoint, service) for backward compatibility."""
        return iter((self.endpoint, self.service))


# ---------------------------------------------------------------------------
# Remote server process entry point (Dragon)
# ---------------------------------------------------------------------------


def _server_node_main(config, node_rank, hostname, port, service_class, use_https):
    """
    Entry point for a remote server process on a single node.

    Runs a complete, self-contained inference server:
    1. Detects local GPUs
    2. Loads model onto devices
    3. Starts GPU workers (via aiohttp startup handler)
    4. Starts HTTP server for /health, /generate, etc.
    5. Blocks until SIGTERM/SIGINT

    Batch preprocessing is handled client-side. The server accepts
    pre-tokenized batch data in /generate requests.

    Spawned by Dragon with placement policy.
    """
    import asyncio
    import signal

    from aiohttp import web as _web

    from src.inference.server import get_app as _get_app
    from src.inference.server import init_server as _init_server
    from src.inference.utils import get_devices_for_node as _get_devices
    from src.utils.logger import Logger as _Logger

    log = _Logger(use_colors=True)

    async def _run():
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)

        devices = _get_devices(config)
        if not devices:
            log.error(f"[Server {node_rank}] No devices found on {hostname}")
            return

        node_name = f"node{node_rank}_{hostname.split('.')[0]}"
        log.info(
            f"[Server {node_rank}] Dragon process starting on {hostname}:{port} "
            f"with devices {devices}"
        )

        # 1. Initialize service (loads model onto devices -- slowest step)
        svc = _init_server(config, devices, node_rank, node_name, hostname, port, service_class)

        # 2. Create HTTP app and trigger startup (starts GPU workers)
        app = _get_app()
        runner = _web.AppRunner(app)
        await runner.setup()

        # 3. Start accepting HTTP connections so health checks pass
        #    while init_queue runs. Workers are started but idle until
        #    batch data is ready and /generate requests arrive.
        site = _web.TCPSite(runner, host="0.0.0.0", port=port)
        await site.start()

        protocol = "https" if use_https else "http"
        log.info(f"[Server {node_rank}] HTTP server listening at {protocol}://{hostname}:{port}")
        log.info(f"[Server {node_rank}] Server ready, waiting for requests")

        # Block until termination signal
        await stop.wait()

        log.info(f"[Server {node_rank}] Shutting down...")
        if svc:
            await svc.shutdown()
        await runner.cleanup()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Single server launch (in-process, for non-Dragon path)
# ---------------------------------------------------------------------------


async def launch_server(
    config: dict[str, Any],
    node_rank: int,
    hostname: str,
    port: int,
    service_class: type[Any],
    use_https: bool = False,
) -> tuple[Optional[str], Optional[Any], Optional[web.AppRunner]]:
    """
    Launch an inference server on a single node (in-process).

    Args:
        config: Configuration dictionary
        node_rank: Rank/index of this node
        hostname: Hostname of the node
        port: Port to use for this server
        service_class: subclass to instantiate
        use_https: Whether to use HTTPS in endpoint URLs

    Returns:
        Tuple of (endpoint_url, inference_service, app_runner) or (None, None, None) if failed
    """
    engine = config.get("engine", "").lower()
    if "dragon" in engine and hostname:
        fqdn = hostname
    else:
        fqdn = socket.getfqdn(hostname) if hostname else socket.getfqdn()
    logger.info(f"Launching server on node {node_rank} ({fqdn}:{port})...")

    try:
        devices = get_devices_for_node(config)
        if not devices:
            logger.error(f"[Server {node_rank}] No GPUs allocated!")
            return None, None, None

        node_name = f"node{node_rank}_{hostname.split('.')[0]}"
        logger.info(f"[Server {node_rank}] Using devices: {devices}")

        inference_service = init_server(
            config, devices, node_rank, node_name, fqdn, port, service_class=service_class
        )

        app = get_app()

        runner = web.AppRunner(app)
        await runner.setup()

        site = web.TCPSite(runner, host="0.0.0.0", port=port)
        await site.start()

        protocol = "https" if use_https else "http"
        endpoint = f"{protocol}://{fqdn}:{port}"

        logger.info(f"[Server {node_rank}] Server started on {endpoint}")

        return endpoint, inference_service, runner

    except Exception as e:
        logger.error(f"Error launching server on {fqdn}: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return None, None, None


# ---------------------------------------------------------------------------
# Multi-node launch
# ---------------------------------------------------------------------------


async def launch_servers(
    config: dict[str, Any],
    service_class: type[Any],
    nodes: Optional[list[str]] = None,
    base_port: int = 8000,
    use_https: bool = False,
) -> list[tuple[str, Any, Any]]:
    """
    Launch servers on all available nodes.

    For Dragon engine: spawns one process per node using Dragon process
    placement, so each server runs on actual target hardware.
    Otherwise: launches servers concurrently in the current process.

    Args:
        config: Configuration dictionary
        service_class: subclass to instantiate
        nodes: List of node hostnames (auto-detected if None)
        base_port: Base port number (each node gets base_port + node_rank)
        use_https: Whether to use HTTPS in endpoint URLs

    Returns:
        List of (endpoint, service_or_None, runner_or_process) tuples
    """
    if nodes is None:
        nodes = get_slurm_nodes(config)
        logger.info(f"Auto-detected {len(nodes)} nodes: {nodes}")

    if not nodes:
        logger.error("No nodes available!")
        return []

    logger.separator(title=f"LAUNCHING SERVERS ON {len(nodes)} NODES")

    num_services = config.get("num_services", len(nodes))
    nodes = nodes[:num_services]

    engine = config.get("engine", "").lower()

    if "dragon" in engine:
        return await _launch_servers_dragon(config, service_class, nodes, base_port, use_https)
    else:
        return await _launch_servers_async(config, service_class, nodes, base_port, use_https)


async def _launch_servers_dragon(
    config: dict[str, Any],
    service_class: type[Any],
    nodes: list[str],
    base_port: int,
    use_https: bool,
) -> list[tuple[str, None, Any]]:
    """
    Launch one server per node using Dragon process placement.

    Each node gets a dedicated Dragon process that:
    - Runs on the target node's hardware (via placement policy)
    - Loads the model onto that node's GPUs
    - Starts GPU workers and HTTP server
    - Prepares batch data via init_queue

    To avoid pickle errors ("No module named 'src'"), we use subprocess.run
    as the Dragon Process target (stdlib, always picklable) and have it
    execute dragon_launcher.py which handles sys.path setup before importing.
    The launcher uses prctl(PR_SET_PDEATHSIG) so it receives SIGTERM when
    the parent (subprocess.run) process is terminated by Dragon.
    """
    import inspect
    import json
    import os
    import subprocess
    import sys
    from pathlib import Path

    try:
        from dragon.infrastructure.policy import Policy
        from dragon.native.process import Process
    except ImportError:
        logger.error("Dragon is not available. Install Dragon or set engine to 'concurrent'.")
        return []

    logger.info(f"Using Dragon process placement for {len(nodes)} nodes")

    # Path to the standalone launcher script (avoids pickling src objects)
    launcher_script = str(Path(__file__).resolve().parent / "dragon_launcher.py")

    # Resolve the directory containing the service module so remote nodes
    # can import it (e.g. examples/esm2/ for esm2_service.py)
    service_module_name = service_class.__module__
    service_class_name = service_class.__name__
    try:
        service_file = inspect.getfile(service_class)
        script_dir = str(Path(service_file).resolve().parent)
    except (TypeError, OSError):
        script_dir = os.getcwd()

    # Write config to shared filesystem so remote processes can read it
    config_dir = Path(config.get("output_dir", config.get("ESM2_dir", ".")))
    config_dir.mkdir(parents=True, exist_ok=True)
    config_json_path = str(config_dir / f"_dragon_config_{os.getpid()}.json")
    with open(config_json_path, "w") as f:
        json.dump(config, f)
    logger.info(f"Config written to {config_json_path}")

    servers = []
    for rank, hostname in enumerate(nodes):
        port = base_port + rank
        protocol = "https" if use_https else "http"

        # Under `dragon -s` (single-node) Dragon reports 'localhost'.
        # Use the real hostname for the HTTP endpoint URL so health checks work.
        endpoint_host = socket.gethostname() if hostname == "localhost" else hostname
        endpoint = f"{protocol}://{endpoint_host}:{port}"

        # Policy placement:
        # - 'localhost' (dragon -s): DEFAULT avoids the ~54s host_id=-1 timeout
        #   and the "not found" error from specifying the real short hostname.
        # - real hostname: HOST_NAME with short name (strip FQDN domain suffix).
        if hostname == "localhost":
            policy = Policy(placement=Policy.Placement.DEFAULT)
        else:
            policy = Policy(
                placement=Policy.Placement.HOST_NAME,
                host_name=hostname.split(".")[0],
            )

        service_python = config.get("service_python", sys.executable)
        cmd = [
            service_python,
            launcher_script,
            "--config-json",
            config_json_path,
            "--node-rank",
            str(rank),
            "--hostname",
            endpoint_host,
            "--port",
            str(port),
            "--service-module",
            service_module_name,
            "--service-class",
            service_class_name,
            "--script-dir",
            script_dir,
        ]
        if use_https:
            cmd.append("--use-https")

        # subprocess.run is from stdlib (always picklable by Dragon).
        # The launcher script uses prctl(PR_SET_PDEATHSIG) to receive
        # SIGTERM when this parent process is terminated by Dragon.
        proc = Process(
            target=subprocess.run,
            args=(cmd,),
            policy=policy,
        )
        proc.start()

        logger.info(f"[Server {rank}] Dragon process spawned on {endpoint_host}")
        servers.append((endpoint, None, proc))

    logger.info(f"Spawned {len(servers)} Dragon server processes")
    return servers


async def _launch_servers_async(
    config: dict[str, Any],
    service_class: type[Any],
    nodes: list[str],
    base_port: int,
    use_https: bool,
) -> list[tuple[str, Any, web.AppRunner]]:
    """Launch servers concurrently.

    If ``service_python`` is set in config and differs from ``sys.executable``,
    each server is launched as a plain subprocess (same launcher as the Dragon
    path) so the service runs in its own conda environment.  Otherwise the
    server runs in-process as before.
    """
    service_python = config.get("service_python", sys.executable)
    use_subprocess = service_python != sys.executable

    if use_subprocess:
        # Reuse the Dragon-path subprocess launch logic (no Dragon placement).
        import inspect
        import json
        import os
        from pathlib import Path

        launcher_script = str(Path(__file__).parent / "dragon_launcher.py")
        service_module_name = service_class.__module__
        service_class_name = service_class.__name__
        try:
            script_dir = str(Path(inspect.getfile(service_class)).resolve().parent)
        except (TypeError, OSError):
            script_dir = os.getcwd()
        config_dir = Path(config.get("output_dir", config.get("ESM2_dir", ".")))
        config_dir.mkdir(parents=True, exist_ok=True)
        config_json_path = str(config_dir / f"_async_config_{os.getpid()}.json")
        with open(config_json_path, "w") as f:
            json.dump(config, f)

        procs = []
        servers = []
        for rank, hostname in enumerate(nodes):
            port = base_port + rank
            protocol = "https" if use_https else "http"
            fqdn = socket.getfqdn(hostname) if hostname else socket.getfqdn()
            endpoint = f"{protocol}://{fqdn}:{port}"
            cmd = [
                service_python,
                launcher_script,
                "--config-json",
                config_json_path,
                "--node-rank",
                str(rank),
                "--hostname",
                hostname,
                "--port",
                str(port),
                "--service-module",
                service_module_name,
                "--service-class",
                service_class_name,
                "--script-dir",
                script_dir,
            ]
            if use_https:
                cmd.append("--use-https")
            proc = await asyncio.create_subprocess_exec(*cmd)
            procs.append(proc)
            servers.append((endpoint, None, proc))
            logger.info(f"Launched service subprocess (rank={rank}) on {endpoint} pid={proc.pid}")

        logger.info(f"Successfully launched {len(servers)}/{len(nodes)} servers (subprocess mode)")
        return servers

    # In-process path (original behaviour).
    launch_tasks = []
    for rank, hostname in enumerate(nodes):
        port = base_port + rank
        task = launch_server(config, rank, hostname, port, service_class, use_https)
        launch_tasks.append(task)

    results = await asyncio.gather(*launch_tasks, return_exceptions=True)

    servers = []
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Launch failed with exception: {result}")
        elif result[0] is not None:
            servers.append(result)

    logger.info(f"Successfully launched {len(servers)}/{len(nodes)} servers")
    return servers


# ---------------------------------------------------------------------------
# Service orchestration
# ---------------------------------------------------------------------------


async def start_services_local(
    config: dict[str, Any],
    service_class: type[Any],
) -> list[ServiceHandle]:
    """
    Initialize the inference service for single node.

    Args:
        config: Configuration dictionary
        service_class: service_class subclass to instantiate
    Returns:
        List of ServiceHandle objects for healthy servers
    """

    num_services = config.get("num_services", 1)
    num_gpus_per_service = config.get("num_gpus_per_service", 1)

    # Build device list: prefer CM-injected GPU IDs (assigned_gpu_ids) so that
    # each service uses exactly the GPU the campaign manager allocated to it,
    # rather than defaulting to cuda:0.  Falls back to auto-detection when no
    # assignment is present (e.g. local testing without the CM).
    from .utils import detect_device_type, get_available_device_count

    device_type = detect_device_type()
    if device_type == "cuda":
        # Prefer group_gpu_ids (all GPUs held by the inference group) so that
        # each service is placed on a distinct CM-assigned GPU.  Fall back to
        # assigned_gpu_ids (single replica) or full auto-detection.
        group_ids = config.get("group_gpu_ids")
        assigned_ids = config.get("assigned_gpu_ids")
        if group_ids:
            all_devices = [f"cuda:{i}" for i in group_ids]
            logger.info(f"Using CM group GPU IDs for services: {group_ids}")
        elif assigned_ids:
            all_devices = [f"cuda:{i}" for i in assigned_ids]
            logger.info(f"Using CM-assigned GPU IDs for services: {assigned_ids}")
        else:
            total_gpus = get_available_device_count()
            all_devices = [f"cuda:{i}" for i in range(total_gpus)]
    else:
        all_devices = ["cpu"] * (num_services * num_gpus_per_service)
    devices_cycle = itertools.cycle(all_devices)
    handles = []

    for rank in range(num_services):
        devices = [next(devices_cycle) for _ in range(num_gpus_per_service)]

        # Run the constructor in a thread so that synchronous model loading
        # (tokenizer + model weights, possibly downloading from HuggingFace)
        # does not block the asyncio event loop.
        service = await asyncio.to_thread(
            lambda r=rank, d=devices: service_class(config=config, devices=d, rank=r)
        )
        handles.append(ServiceHandle(endpoint=None, service=service))
        logger.info(f"[Server {rank}] Local Inference service initialized on devices: {devices}")

    return handles


async def start_services(
    config: dict[str, Any],
    service_class: type[Any],
) -> list[ServiceHandle]:
    """
    Launch servers on all nodes and wait for them to be healthy.

    For Dragon engine: spawns remote processes, then creates lightweight
    client-mode service instances for client-side preprocessing.
    For other engines: launches servers in-process.

    Args:
        config: Configuration dictionary
        service_class: service_class subclass to instantiate

    Returns:
        List of ServiceHandle objects for healthy servers
    """
    nodes = config.get("nodes", None)
    base_port = config.get("server_port", 8000)
    use_https = config.get("use_https", False)

    if nodes is None:
        nodes = get_slurm_nodes(config)
        logger.info(f"Auto-detected {len(nodes)} nodes: {nodes}")

    if not nodes:
        logger.error("No nodes available!")
        return []

    servers = await launch_servers(config, service_class, nodes, base_port, use_https)
    if not servers:
        logger.error("Failed to launch any servers!")
        return []

    endpoints = [ep for ep, _, _ in servers]

    health_timeout = config.get("health_check_timeout", 300)
    healthy_endpoints = await wait_for_healthy(endpoints, timeout=health_timeout)
    if not healthy_endpoints:
        logger.error("No healthy servers available!")
        return []

    engine = config.get("engine", "").lower()
    is_remote = "dragon" in engine

    handles = []
    for rank, (ep, svc, proc_or_runner) in enumerate(servers):
        if ep in healthy_endpoints:
            if is_remote or svc is None:
                # Remote mode (Dragon) or subprocess mode: service lives in a
                # separate process.  Create a lightweight client-mode service
                # for client-side preprocessing (loads tokenizer only, no model/GPU).
                client_svc = service_class(
                    config=config, devices=["cpu"], rank=rank, client_mode=True
                )
                handles.append(ServiceHandle(ep, client_svc, None, dragon_process=proc_or_runner))
            else:
                handles.append(ServiceHandle(ep, svc, proc_or_runner))

    logger.info(f"{len(handles)} services ready")
    return handles


# ---------------------------------------------------------------------------
# Client initialization
# ---------------------------------------------------------------------------


async def init_clients(
    config: dict[str, Any],
    services: list[ServiceHandle],
    client_class: type,
    asyncflow: Any,
) -> Optional[list[Any]]:
    """
    Initialize clients for the given services.

    Args:
        config: Configuration dictionary
        services: List of ServiceHandle objects from start_services()
        client_class: Client class to instantiate

    Returns:
        Tuple of (list of client instances, telemetry collector or None)
    """

    clients = []
    for rank, handle in enumerate(services):
        try:
            if isinstance(handle, ServiceHandle):
                endpoint = handle.endpoint
                service = handle.service
            else:
                endpoint, service = handle
            if endpoint is not None:
                endpoints = [endpoint] if isinstance(endpoint, str) else endpoint
                client = client_class(
                    endpoints=endpoints,
                    rank=rank,
                    service=service,
                    config=config,
                    asyncflow=asyncflow,
                )
                clients.append(client)
            else:
                if service is not None:
                    client = client_class(
                        endpoints=[],
                        rank=rank,
                        service=service,
                        config=config,
                        asyncflow=asyncflow,
                    )
                    clients.append(client)
                    await service.start_workers()
        except Exception as e:
            logger.error(f"Unable to initiate client for rank {rank}: {e}")

    if not clients:
        logger.error("No clients were created!")
        return None

    logger.info(f"Created {len(clients)} clients")
    return clients
