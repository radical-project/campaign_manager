"""Tests for logger module."""

import re
import sys
from io import StringIO

from src.utils.logger import Colors, Logger, LogLevel


class TestColors:
    def test_colors_defined(self):
        assert Colors.RED.startswith("\033[")
        assert Colors.GREEN.startswith("\033[")
        assert Colors.RESET == "\033[0m"
        assert Colors.BOLD == "\033[1m"

    def test_bright_colors_defined(self):
        assert Colors.BRIGHT_RED.startswith("\033[")
        assert Colors.BRIGHT_GREEN.startswith("\033[")
        assert Colors.BRIGHT_CYAN.startswith("\033[")


class TestLogLevel:
    def test_log_levels(self):
        assert LogLevel.DEBUG.value == "DEBUG"
        assert LogLevel.INFO.value == "INFO"
        assert LogLevel.WARNING.value == "WARNING"
        assert LogLevel.ERROR.value == "ERROR"
        assert LogLevel.CRITICAL.value == "CRITICAL"


class TestLogger:
    def test_logger_init(self, output_stream):
        logger = Logger(name="test", use_colors=True, output_stream=output_stream)
        assert logger.name == "test"
        assert logger.use_colors is True

    def test_logger_no_colors(self, output_stream):
        logger = Logger(use_colors=False, output_stream=output_stream)
        logger.info("test message")
        output = output_stream.getvalue()
        assert "test message" in output
        assert "\033[" not in output

    def test_logger_info(self, output_stream):
        logger = Logger(use_colors=False, output_stream=output_stream)
        logger.info("info message")
        output = output_stream.getvalue()
        assert "[INFO]" in output
        assert "info message" in output

    def test_logger_warning(self, output_stream):
        logger = Logger(use_colors=False, output_stream=output_stream)
        logger.warning("warning message")
        output = output_stream.getvalue()
        assert "[WARNING]" in output
        assert "warning message" in output

    def test_logger_debug(self, output_stream):
        logger = Logger(use_colors=False, output_stream=output_stream)
        logger.debug("debug message")
        output = output_stream.getvalue()
        assert "[DEBUG]" in output
        assert "debug message" in output

    def test_logger_with_component(self, output_stream):
        logger = Logger(use_colors=False, output_stream=output_stream)
        logger.info("test message", component="workflow")
        output = output_stream.getvalue()
        assert "[WORKFLOW]" in output

    def test_error_writes_to_stderr(self, output_stream):
        """error() writes to stderr, not to the injected output_stream."""
        stderr = StringIO()
        logger = Logger(use_colors=False, output_stream=output_stream)
        old_stderr = sys.stderr
        sys.stderr = stderr
        try:
            logger.error("err msg")
        finally:
            sys.stderr = old_stderr
        assert "err msg" in stderr.getvalue()
        assert "err msg" not in output_stream.getvalue()

    def test_critical_writes_to_stderr(self, output_stream):
        """critical() writes to stderr."""
        stderr = StringIO()
        logger = Logger(use_colors=False, output_stream=output_stream)
        old_stderr = sys.stderr
        sys.stderr = stderr
        try:
            logger.critical("crit msg")
        finally:
            sys.stderr = old_stderr
        assert "crit msg" in stderr.getvalue()
        assert "crit msg" not in output_stream.getvalue()

    def test_init_metrics_structure(self):
        """init_metrics returns dict with all required keys and correct defaults."""
        logger = Logger(use_colors=False)
        metrics = logger.init_metrics(["cuda:0", "cuda:1"])
        assert metrics["requests"] == 0
        assert metrics["total_tokens"] == 0
        assert metrics["errors"] == 0
        assert metrics["queue_tokens"] == 0
        assert "cuda:0" in metrics["gpu_stats"]
        assert "cuda:1" in metrics["gpu_stats"]
        assert metrics["gpu_stats"]["cuda:0"] == {"processed": 0}
        assert metrics["gpu_stats"]["cuda:1"] == {"processed": 0}
        assert isinstance(metrics["timeseries"], list)
        assert "t_start" in metrics
        assert "t_last" in metrics

    def test_format_message_fields(self, output_stream):
        """Formatted output contains HH:MM:SS.mmm timestamp, level, component, message."""
        logger = Logger(use_colors=False, output_stream=output_stream)
        logger.info("hello world", component="mycomp")
        output = output_stream.getvalue()
        assert "[INFO]" in output
        assert "[MYCOMP]" in output
        assert "hello world" in output
        assert re.search(r"\d{2}:\d{2}:\d{2}\.\d{3}", output)

    def test_task_started(self, output_stream):
        logger = Logger(use_colors=False, output_stream=output_stream)
        logger.task_started("my_task")
        output = output_stream.getvalue()
        assert "Task started" in output
        assert "my_task" in output

    def test_task_completed(self, output_stream):
        logger = Logger(use_colors=False, output_stream=output_stream)
        logger.task_completed("my_task")
        output = output_stream.getvalue()
        assert "Task completed" in output
        assert "my_task" in output

    def test_task_killed(self, output_stream):
        logger = Logger(use_colors=False, output_stream=output_stream)
        logger.task_killed("my_task")
        output = output_stream.getvalue()
        assert "Task killed" in output
        assert "[WARNING]" in output

    def test_manager_starting(self, output_stream):
        logger = Logger(use_colors=False, output_stream=output_stream)
        logger.manager_starting(5)
        output = output_stream.getvalue()
        assert "Starting with" in output
        assert "5" in output

    def test_manager_exiting(self, output_stream):
        logger = Logger(use_colors=False, output_stream=output_stream)
        logger.manager_exiting()
        output = output_stream.getvalue()
        assert "Exiting" in output

    def test_separator(self, output_stream):
        logger = Logger(use_colors=False, output_stream=output_stream)
        logger.separator("Test Section")
        output = output_stream.getvalue()
        assert "Test Section" in output
        assert "=" in output

    def test_colorize_enabled(self):
        logger = Logger(use_colors=True)
        result = logger._colorize("test", Colors.RED)
        assert Colors.RED in result
        assert Colors.RESET in result

    def test_colorize_disabled(self):
        logger = Logger(use_colors=False)
        result = logger._colorize("test", Colors.RED)
        assert result == "test"
        assert Colors.RED not in result

    def test_metrics_initialized_from_config(self, temp_dir):
        """Logger with config dict initializes metrics and metrics_lock."""
        config = {
            "metrics_dir": str(temp_dir / "metrics"),
            "metrics_file_prefix": "test",
            "metrics_log_interval": 5,
        }
        logger = Logger(use_colors=False, config=config, devices=["cuda:0"])
        assert logger.metrics is not None
        assert logger.metrics_lock is not None
        assert logger.metrics_log_interval == 5
        assert "cuda:0" in logger.metrics["gpu_stats"]
