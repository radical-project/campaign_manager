import asyncio
import sys
import time
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional


class Colors:
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    BRIGHT_BLACK = "\033[90m"
    BRIGHT_RED = "\033[91m"
    BRIGHT_GREEN = "\033[92m"
    BRIGHT_YELLOW = "\033[93m"
    BRIGHT_BLUE = "\033[94m"
    BRIGHT_MAGENTA = "\033[95m"
    BRIGHT_CYAN = "\033[96m"
    BRIGHT_WHITE = "\033[97m"
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"


class LogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Logger:
    def __init__(
        self,
        name="workflow",
        use_colors=True,
        output_stream=None,
        config=None,
        rank=0,
        devices=None,
        min_level: str = "INFO",
    ):
        self.name = name
        self.use_colors = use_colors
        self.output_stream = output_stream or sys.stdout
        self.rank = rank
        self._min_level = LogLevel[min_level.upper()]

        self.level_colors = {
            LogLevel.DEBUG: Colors.BRIGHT_BLACK,
            LogLevel.INFO: Colors.BRIGHT_CYAN,
            LogLevel.WARNING: Colors.BRIGHT_YELLOW,
            LogLevel.ERROR: Colors.BRIGHT_RED,
            LogLevel.CRITICAL: Colors.RED + Colors.BOLD,
        }

        self.component_colors = {
            "task": Colors.BRIGHT_GREEN,
            "manager": Colors.BRIGHT_RED,
            "workflow": Colors.BRIGHT_GREEN,
            "simulation": Colors.BLUE,
            "training": Colors.BRIGHT_YELLOW,
            "prediction": Colors.GREEN,
        }

        self.metrics: Optional[dict[str, Any]] = None
        self.metrics_output: Optional[Path] = None
        self.metrics_log_interval: int = 10
        self.metrics_lock: Optional[asyncio.Lock] = None
        self.metrics_task: Optional[asyncio.Task] = None

        if config:
            self.metrics_dir = config.get("metrics_dir", "outputs")
            metrics_file_prefix = config.get("metrics_file_prefix", "metrics")
            self.metrics_output = Path(self.metrics_dir, f"{metrics_file_prefix}_{self.rank}.json")
            self.metrics = self.init_metrics(devices or [])
            self.metrics_log_interval = config.get("metrics_log_interval", 10)
            self.metrics_lock = asyncio.Lock()

    def _colorize(self, text, color):
        return f"{color}{text}{Colors.RESET}" if self.use_colors else text

    def _format_message(self, level, component, message, task_name=None):
        if task_name is None:
            task_name = self.name
        if component == "manager":
            component = self.name
        timestamp = self._colorize(datetime.now().strftime("%H:%M:%S.%f")[:-3], Colors.DIM)
        colored_level = self._colorize(
            f"[{level.value}]", self.level_colors.get(level, Colors.WHITE)
        )

        if component.lower().startswith("task-"):
            component_color = Colors.BRIGHT_GREEN
        else:
            component_color = self.component_colors.get(component.lower(), Colors.WHITE)

        colored_component = self._colorize(f"[{component.upper()}]", component_color)

        task_part = ""
        if task_name and task_name.upper() != component.upper():
            task_part = f" {self._colorize(f'[{task_name}]', Colors.BRIGHT_WHITE)}"

        return f"{timestamp} {colored_level} {colored_component}{task_part} {message}"

    def _write_log(self, message, to_stderr=False):
        stream = sys.stderr if to_stderr else self.output_stream
        stream.write(message + "\n")
        stream.flush()

    def debug(self, message, component="manager", task_name=None):
        if self._min_level != LogLevel.DEBUG:
            return
        if task_name is None:
            task_name = self.name
        formatted = self._format_message(LogLevel.DEBUG, component, message, task_name)
        self._write_log(formatted)

    def info(self, message, component="manager", task_name=None):
        if task_name is None:
            task_name = self.name
        formatted = self._format_message(LogLevel.INFO, component, message, task_name)
        self._write_log(formatted)

    def warning(self, message, component="manager", task_name=None):
        if task_name is None:
            task_name = self.name
        formatted = self._format_message(LogLevel.WARNING, component, message, task_name)
        self._write_log(formatted)

    def error(self, message, component="manager", task_name=None):
        if task_name is None:
            task_name = self.name
        formatted = self._format_message(LogLevel.ERROR, component, message, task_name)
        self._write_log(formatted, to_stderr=True)

    def critical(self, message, component="manager", task_name=None):
        if task_name is None:
            task_name = self.name
        formatted = self._format_message(LogLevel.CRITICAL, component, message, task_name)
        self._write_log(formatted, to_stderr=True)

    def task_started(self, task_name, component="task"):
        message = f"Task started: {self._colorize(task_name, Colors.BRIGHT_WHITE)}"
        self.info(message, component)

    def task_completed(self, task_name, component="task"):
        message = f"Task completed: {self._colorize(task_name, Colors.BRIGHT_WHITE)}"
        self.info(message, component)

    def task_killed(self, task_name, component="task"):
        message = f"Task killed: {self._colorize(task_name, Colors.BRIGHT_WHITE)}"
        self.warning(message, component)

    def manager_starting(self, task_count):
        message = (
            f"Starting with {self._colorize(str(task_count), Colors.BRIGHT_WHITE)} initial tasks"
        )
        self.info(message, "manager")

    def manager_exiting(self):
        self.info("All tasks finished. Exiting.", "manager")

    def separator(self, title=None):
        if title:
            separator = f"{'=' * 20} {title} {'=' * 20}"
        else:
            separator = "=" * 50
        self._write_log(self._colorize(separator, Colors.BRIGHT_BLUE))

    def init_metrics(self, devices) -> dict[str, Any]:
        """Initialize metrics tracking structure."""
        now = time.monotonic()
        return {
            "requests": 0,
            "queue_tokens": 0,
            "total_tokens": 0,
            "errors": 0,
            "t_start": now,
            "t_last": now,
            "last_requests": 0,
            "last_tokens": 0,
            "timeseries": [],
            "gpu_stats": {device: {"processed": 0} for device in devices},
        }

    def start_metrics_logging(self, service) -> asyncio.Task:
        """
        Start background metrics logging task.

        Args:
            service: InferenceService instance with shutting_down, work_queue, workers attributes

        Returns:
            The created asyncio Task
        """
        self.metrics_task = asyncio.create_task(self._log_metrics(service))
        return self.metrics_task

    async def _log_metrics(self, service):
        """Background task to periodically log performance metrics."""
        while self.metrics["requests"] == 0 and not service.shutting_down.is_set():
            await asyncio.sleep(0.1)
        try:
            while not service.shutting_down.is_set():
                # await asyncio.sleep(self.metrics_log_interval)
                try:
                    await asyncio.wait_for(
                        service.shutting_down.wait(), timeout=self.metrics_log_interval
                    )
                    break
                except asyncio.TimeoutError:
                    pass

                now = time.monotonic()
                dt = now - self.metrics["t_last"]
                if dt <= 0:
                    continue

                async with self.metrics_lock:
                    if (
                        self.metrics["requests"] == self.metrics["last_requests"]
                        and self.metrics["total_tokens"] == self.metrics["last_tokens"]
                    ):
                        continue

                    d_req = self.metrics["requests"] - self.metrics["last_requests"]
                    d_tok = self.metrics["total_tokens"] - self.metrics["last_tokens"]
                    req_s = d_req / dt
                    tok_s = d_tok / dt

                    record = {
                        "timestamp": time.time(),
                        "dt": dt,
                        "requests": d_req,
                        "tokens": d_tok,
                        "req_per_sec": req_s,
                        "tok_per_sec": tok_s,
                        "total_tokens": self.metrics["total_tokens"],
                        "queue_tokens": self.metrics["queue_tokens"],
                        "work_queue_depth": service.work_queue.qsize(),
                        "gpu_stats": dict(self.metrics["gpu_stats"]),
                    }
                    self.metrics["timeseries"].append(record)

                    self.metrics["t_last"] = now
                    self.metrics["last_requests"] = self.metrics["requests"]
                    self.metrics["last_tokens"] = self.metrics["total_tokens"]

                    active_workers = sum(1 for w in service.workers if w.is_busy)

                    log = (
                        f"[service {self.rank}] | "
                        f"elapsed={now - self.metrics['t_start']:.2f}s | "
                        f"req/s={req_s:.2f} | "
                        f"tok/s={tok_s:.2f} | "
                        f"work_q={service.work_queue.qsize()} | "
                        f"active_workers={active_workers}/{len(service.workers)} | "
                    )

                self.info(log)

        except asyncio.CancelledError:
            self.info(f"[service {self.rank}] Metrics logging cancelled")
