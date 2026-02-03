"""Tests for discord_archive.utils.pipeline_logger."""

from __future__ import annotations

from io import StringIO
from typing import Any
from unittest.mock import MagicMock

from rich.console import Console

from discord_archive.utils.pipeline_logger import BasePipelineLogger, StructuredBlock


class ConcreteLogger(BasePipelineLogger):
    """Concrete implementation for testing the abstract base class."""

    def __init__(self) -> None:
        super().__init__("test_logger")
        # Replace console with a string-capturing one for assertions
        self.console = Console(file=StringIO(), force_terminal=True, width=120)

    def summary(self, **kwargs: Any) -> None:
        self.print_summary("Test", elapsed=0.0, stats={})

    def get_output(self) -> str:
        self.console.file.seek(0)
        return self.console.file.read()


# ---------------------------------------------------------------------------
# TestStructuredBlock
# ---------------------------------------------------------------------------


class TestStructuredBlock:
    """Tests for StructuredBlock context manager."""

    def test_prints_title_on_enter(self) -> None:
        logger = ConcreteLogger()

        with logger.block("My Block"):
            pass

        output = logger.get_output()
        assert "My Block" in output

    def test_field_prints_key_value(self) -> None:
        logger = ConcreteLogger()

        with logger.block("Block") as b:
            b.field("channel ID", 12345)

        output = logger.get_output()
        assert "channel ID:" in output
        assert "12345" in output

    def test_field_with_color(self) -> None:
        logger = ConcreteLogger()

        with logger.block("Block") as b:
            b.field("mode", "backfill", color="magenta")

        output = logger.get_output()
        assert "mode:" in output
        assert "backfill" in output

    def test_result_success(self) -> None:
        logger = ConcreteLogger()

        with logger.block("Block") as b:
            b.result("ingested 100 messages")

        output = logger.get_output()
        assert "ingested" in output
        assert "100" in output
        assert "messages" in output

    def test_result_failure(self) -> None:
        logger = ConcreteLogger()

        with logger.block("Block") as b:
            b.result("failed", success=False)

        output = logger.get_output()
        assert "failed" in output

    def test_skip(self) -> None:
        logger = ConcreteLogger()

        with logger.block("Block") as b:
            b.skip("no access")

        output = logger.get_output()
        assert "Skipped: no access" in output

    def test_empty(self) -> None:
        logger = ConcreteLogger()

        with logger.block("Block") as b:
            b.empty()

        output = logger.get_output()
        assert "Empty, skipping" in output

    def test_progress_sets_flag(self) -> None:
        logger = ConcreteLogger()

        with logger.block("Block") as b:
            b.progress("processing...")
            assert logger._has_progress_line is True


# ---------------------------------------------------------------------------
# TestBasePipelineLogger
# ---------------------------------------------------------------------------


class TestBasePipelineLogger:
    """Tests for BasePipelineLogger base class."""

    def test_clear_progress_line_resets_flag(self) -> None:
        logger = ConcreteLogger()
        logger._has_progress_line = True

        logger._clear_progress_line()

        assert logger._has_progress_line is False

    def test_clear_progress_line_noop_when_no_progress(self) -> None:
        logger = ConcreteLogger()
        logger._has_progress_line = False

        logger._clear_progress_line()

        assert logger._has_progress_line is False

    def test_info_delegates_to_python_logger(self) -> None:
        logger = ConcreteLogger()
        logger._logger = MagicMock()

        logger.info("test message")

        logger._logger.info.assert_called_once_with("test message")

    def test_warning_delegates_to_python_logger(self) -> None:
        logger = ConcreteLogger()
        logger._logger = MagicMock()

        logger.warning("warn message")

        logger._logger.warning.assert_called_once_with("warn message")

    def test_error_delegates_to_python_logger(self) -> None:
        logger = ConcreteLogger()
        logger._logger = MagicMock()

        logger.error("error message")

        logger._logger.error.assert_called_once_with("error message")

    def test_debug_delegates_to_python_logger(self) -> None:
        logger = ConcreteLogger()
        logger._logger = MagicMock()

        logger.debug("debug message")

        logger._logger.debug.assert_called_once_with("debug message")

    def test_success_prints_checkmark(self) -> None:
        logger = ConcreteLogger()

        logger.success("done")

        output = logger.get_output()
        assert "done" in output

    def test_batch_progress_sets_flag(self) -> None:
        logger = ConcreteLogger()

        logger.batch_progress(50, total=100)

        assert logger._has_progress_line is True

    def test_batch_progress_with_dates(self) -> None:
        logger = ConcreteLogger()

        logger.batch_progress(
            50, oldest_date="2024-01-01", newest_date="2024-06-01"
        )

        assert logger._has_progress_line is True

    def test_block_yields_structured_block(self) -> None:
        logger = ConcreteLogger()

        with logger.block("test") as b:
            assert isinstance(b, StructuredBlock)

    def test_print_summary_outputs_panel(self) -> None:
        logger = ConcreteLogger()

        logger.print_summary(
            "Test Pipeline",
            elapsed=12.3,
            stats={"Messages": 100, "Channels": 5},
        )

        output = logger.get_output()
        assert "Test Pipeline Complete" in output
        assert "12.3s" in output

    def test_print_summary_with_extra_sections(self) -> None:
        logger = ConcreteLogger()

        logger.print_summary(
            "Test",
            elapsed=1.0,
            stats={"Total": 10},
            extra_sections={"Details": {"Sub-item": 5}},
        )

        output = logger.get_output()
        assert "Test Complete" in output


# ---------------------------------------------------------------------------
# TestIngestLogger
# ---------------------------------------------------------------------------


class TestIngestLogger:
    """Tests for IngestLogger (the concrete subclass in ingest/logger.py)."""

    def test_rate_limit_logs_warning(self) -> None:
        from discord_archive.ingest.logger import IngestLogger

        logger = IngestLogger()
        logger._logger = MagicMock()

        logger.rate_limit(1.5)

        logger._logger.warning.assert_called_once()
        assert "1.5" in logger._logger.warning.call_args[0][0]

    def test_retry_without_reason(self) -> None:
        from discord_archive.ingest.logger import IngestLogger

        logger = IngestLogger()
        logger._logger = MagicMock()

        logger.retry(1, 3, 2.0)

        msg = logger._logger.warning.call_args[0][0]
        assert "1/3" in msg
        assert "2.0" in msg

    def test_retry_with_reason(self) -> None:
        from discord_archive.ingest.logger import IngestLogger

        logger = IngestLogger()
        logger._logger = MagicMock()

        logger.retry(2, 5, 3.0, reason="timeout")

        msg = logger._logger.warning.call_args[0][0]
        assert "timeout" in msg

    def test_summary_calls_print_summary(self) -> None:
        from discord_archive.ingest.logger import IngestLogger

        logger = IngestLogger()
        logger.console = Console(file=StringIO(), force_terminal=True, width=120)

        logger.summary(guilds=2, channels=10, messages=500, elapsed=5.5)

        logger.console.file.seek(0)
        output = logger.console.file.read()
        assert "Ingest Complete" in output
