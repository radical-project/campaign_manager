"""Tests for src.campaign.logging_setup.enable_logging."""

import logging

from src.campaign.logging_setup import enable_logging


def _reset_loggers(*names):
    """Remove all handlers and re-enable propagation on named loggers."""
    for name in names:
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = True
        lg.setLevel(logging.NOTSET)


class TestEnableLogging:
    def teardown_method(self):
        _reset_loggers("src.campaign", "campaign")

    def test_src_campaign_logger_configured(self):
        enable_logging(logging.INFO)
        lg = logging.getLogger("src.campaign")
        assert lg.level == logging.INFO

    def test_campaign_logger_also_configured(self):
        enable_logging(logging.DEBUG)
        lg = logging.getLogger("campaign")
        assert lg.level == logging.DEBUG

    def test_propagate_disabled(self):
        """propagate=False prevents log duplication when root logger is also configured."""
        enable_logging(logging.WARNING)
        assert logging.getLogger("src.campaign").propagate is False
        assert logging.getLogger("campaign").propagate is False

    def test_handler_attached(self):
        enable_logging(logging.INFO)
        lg = logging.getLogger("src.campaign")
        assert len(lg.handlers) >= 1

    def test_second_call_does_not_duplicate_handlers(self):
        """Calling enable_logging twice must not stack handlers."""
        enable_logging(logging.INFO)
        enable_logging(logging.DEBUG)
        lg = logging.getLogger("src.campaign")
        assert len(lg.handlers) == 1

    def test_output_file_adds_file_handler(self, tmp_path):
        log_file = tmp_path / "run.log"
        enable_logging(logging.INFO, output_file=str(log_file))
        lg = logging.getLogger("src.campaign")
        file_handlers = [h for h in lg.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) == 1

    def test_output_file_receives_log_records(self, tmp_path):
        log_file = tmp_path / "run.log"
        enable_logging(logging.INFO, output_file=str(log_file))
        logging.getLogger("src.campaign").info("hello from test")
        text = log_file.read_text()
        assert "hello from test" in text

    def test_configure_stack_false_does_not_touch_radical(self):
        """With configure_stack=False, radical.adr logger is untouched."""
        enable_logging(logging.INFO, configure_stack=False)
        # radical.adr logger should NOT be set by us (may not exist at all)
        adr = logging.getLogger("radical.adr")
        assert not any(isinstance(h, logging.StreamHandler)
                       and h in logging.getLogger("src.campaign").handlers
                       for h in adr.handlers)
