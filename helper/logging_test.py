"""
Test cases for logging utilities.
"""

import io
import logging
import unittest

from helper.logging import ColorFormatter, QueuerLogger, get_logger, setup_logging


class TestColorFormatter(unittest.TestCase):
    """Test cases for ColorFormatter class."""

    def test_init_with_colors(self):
        """Test ColorFormatter initialization with colors enabled."""
        formatter = ColorFormatter(use_colors=True, include_timestamp=True)
        self.assertTrue(formatter.include_timestamp)
        # Colors depend on terminal detection

    def test_init_without_colors(self):
        """Test ColorFormatter initialization with colors disabled."""
        formatter = ColorFormatter(use_colors=False, include_timestamp=False)
        self.assertFalse(formatter.use_colors)
        self.assertFalse(formatter.include_timestamp)

    def test_format_with_timestamp(self):
        """Test formatting with timestamp."""
        formatter = ColorFormatter(use_colors=False, include_timestamp=True)

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        formatted = formatter.format(record)
        self.assertIn("INFO: Test message", formatted)
        # Should include timestamp
        self.assertRegex(formatted, r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")

    def test_format_without_timestamp(self):
        """Test formatting without timestamp."""
        formatter = ColorFormatter(use_colors=False, include_timestamp=False)

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="Error message",
            args=(),
            exc_info=None,
        )

        formatted = formatter.format(record)
        self.assertEqual(formatted, "ERROR: Error message")

    def test_color_codes_defined(self):
        """Test that color codes are properly defined."""
        formatter = ColorFormatter()

        expected_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "RESET"]
        for level in expected_levels:
            self.assertIn(level, formatter.COLORS)
            self.assertIsInstance(formatter.COLORS[level], str)


class TestQueuerLogger(unittest.TestCase):
    """Test cases for QueuerLogger class."""

    def setUp(self):
        """Set up test fixtures."""
        self.stream = io.StringIO()

    def test_init_default(self):
        """Test QueuerLogger initialization with defaults."""
        logger = QueuerLogger(stream=self.stream)
        self.assertEqual(logger.logger.name, "queuer")
        self.assertEqual(logger.logger.level, logging.INFO)

    def test_init_custom(self):
        """Test QueuerLogger initialization with custom settings."""
        logger = QueuerLogger(
            name="custom", level=logging.DEBUG, use_colors=False, stream=self.stream
        )
        self.assertEqual(logger.logger.name, "custom")
        self.assertEqual(logger.logger.level, logging.DEBUG)

    def test_debug_logging(self):
        """Test debug level logging."""
        logger = QueuerLogger(level=logging.DEBUG, stream=self.stream)
        logger.debug("Debug message")

        output = self.stream.getvalue()
        self.assertIn("DEBUG: Debug message", output)

    def test_info_logging(self):
        """Test info level logging."""
        logger = QueuerLogger(stream=self.stream)
        logger.info("Info message")

        output = self.stream.getvalue()
        self.assertIn("INFO: Info message", output)

    def test_warning_logging(self):
        """Test warning level logging."""
        logger = QueuerLogger(stream=self.stream)
        logger.warning("Warning message")

        output = self.stream.getvalue()
        self.assertIn("WARNING: Warning message", output)

    def test_error_logging(self):
        """Test error level logging."""
        logger = QueuerLogger(stream=self.stream)
        logger.error("Error message")

        output = self.stream.getvalue()
        self.assertIn("ERROR: Error message", output)

    def test_error_logging_with_exception(self):
        """Test error logging with exception."""
        logger = QueuerLogger(stream=self.stream)
        error = ValueError("test error")
        logger.error("Operation failed", error=error)

        output = self.stream.getvalue()
        self.assertIn("ERROR: Operation failed: test error", output)

    def test_critical_logging(self):
        """Test critical level logging."""
        logger = QueuerLogger(stream=self.stream)
        logger.critical("Critical message")

        output = self.stream.getvalue()
        self.assertIn("CRITICAL: Critical message", output)

    def test_critical_logging_with_exception(self):
        """Test critical logging with exception."""
        logger = QueuerLogger(stream=self.stream)
        error = RuntimeError("critical error")
        logger.critical("System failure", error=error)

        output = self.stream.getvalue()
        self.assertIn("CRITICAL: System failure: critical error", output)

    def test_logging_with_context(self):
        """Test logging with context information."""
        logger = QueuerLogger(stream=self.stream)
        logger.info("Processing job", job_id="123", worker="worker-1")

        output = self.stream.getvalue()
        self.assertIn("INFO: Processing job | job_id=123 worker=worker-1", output)

    def test_set_level(self):
        """Test setting log level."""
        logger = QueuerLogger(level=logging.INFO, stream=self.stream)

        # Debug should not appear at INFO level
        logger.debug("Debug message")
        self.assertEqual(self.stream.getvalue(), "")

        # Change to DEBUG level
        logger.set_level(logging.DEBUG)
        logger.debug("Debug message")

        output = self.stream.getvalue()
        self.assertIn("DEBUG: Debug message", output)

    def test_no_duplicate_handlers(self):
        """Test that no duplicate handlers are added."""
        logger1 = QueuerLogger(name="test", stream=self.stream)
        initial_handler_count = len(logger1.logger.handlers)

        logger2 = QueuerLogger(name="test", stream=self.stream)
        final_handler_count = len(logger2.logger.handlers)

        self.assertEqual(initial_handler_count, final_handler_count)


class TestLoggerFunctions(unittest.TestCase):
    """Test cases for module-level logger functions."""

    def test_get_logger_default(self):
        """Test getting default logger."""
        logger = get_logger()
        self.assertIsInstance(logger, QueuerLogger)
        self.assertEqual(logger.logger.name, "queuer")

    def test_get_logger_custom_name(self):
        """Test getting logger with custom name."""
        logger = get_logger("custom")
        self.assertIsInstance(logger, QueuerLogger)
        self.assertEqual(logger.logger.name, "custom")

    def test_get_logger_singleton(self):
        """Test that get_logger returns same instance for same name."""
        logger1 = get_logger("test")
        logger2 = get_logger("test")
        self.assertIs(logger1, logger2)

    def test_get_logger_different_names(self):
        """Test that get_logger returns different instances for different names."""
        logger1 = get_logger("test1")
        logger2 = get_logger("test2")
        self.assertIsNot(logger1, logger2)

    def test_setup_logging(self):
        """Test setup_logging function."""
        logger = setup_logging(level=logging.DEBUG, use_colors=False, name="setup_test")

        self.assertIsInstance(logger, QueuerLogger)
        self.assertEqual(logger.logger.name, "setup_test")
        self.assertEqual(logger.logger.level, logging.DEBUG)

    def test_setup_logging_sets_global(self):
        """Test that setup_logging sets global logger."""
        new_logger = setup_logging(name="global_test")
        current_logger = get_logger("global_test")

        self.assertIs(new_logger, current_logger)


if __name__ == "__main__":
    unittest.main()
