"""
Pretty logging utilities for Python queuer implementation.
Mirrors Go's helper/prettyLog.go with Python logging enhancements.
"""

import logging
import sys
from typing import Any, Callable, Optional, TextIO
from datetime import datetime


class ColorFormatter(logging.Formatter):
    """
    Colored log formatter for console output.
    Mirrors Go's PrettyHandler functionality.
    """

    # ANSI color codes
    COLORS = {
        "DEBUG": "\033[95m",  # Magenta
        "INFO": "\033[94m",  # Blue
        "WARNING": "\033[93m",  # Yellow
        "ERROR": "\033[91m",  # Red
        "CRITICAL": "\033[95m",  # Magenta
        "RESET": "\033[0m",  # Reset
    }

    def __init__(self, use_colors: bool = True, include_timestamp: bool = True):
        """
        Initialize the color formatter.

        :param use_colors: Whether to use colors in the output.
        :param include_timestamp: Whether to include timestamps in log messages.
        """
        self.use_colors = use_colors and sys.stdout.isatty()
        self.include_timestamp = include_timestamp

        if include_timestamp:
            fmt = "%(asctime)s %(levelname)s: %(message)s"
        else:
            fmt = "%(levelname)s: %(message)s"

        super().__init__(fmt, datefmt="%Y-%m-%d %H:%M:%S")

    def format(self, record: logging.LogRecord) -> str:
        """
        Format the log record with colors.

        :param record: The log record to format.
        :returns: The formatted log message string.
        """
        formatted = super().format(record)

        if self.use_colors and record.levelname in self.COLORS:
            color = self.COLORS[record.levelname]
            reset = self.COLORS["RESET"]
            parts = formatted.split(": ", 1)
            if len(parts) == 2:
                level_part, message_part = parts
                formatted = f"{color}{level_part}: {reset}{message_part}"

        return formatted


class QueuerLogger:
    """
    Enhanced logger for queuer operations.
    Provides structured logging with context information.
    """

    def __init__(
        self,
        name: str = "queuer",
        level: int = logging.INFO,
        use_colors: bool = True,
        stream: Optional[TextIO] = None,
    ):
        """
        Initialize the queuer logger.

        :param name: Logger name.
        :param level: Logging level.
        :param use_colors: Whether to use colored output.
        :param stream: Output stream (defaults to sys.stdout).
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Remove existing handlers to avoid duplicates
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        # Create console handler with color formatter
        handler = logging.StreamHandler(stream or sys.stdout)
        formatter = ColorFormatter(use_colors=use_colors)
        handler.setFormatter(formatter)

        self.logger.addHandler(handler)
        self.logger.propagate = False

    def debug(self, message: str, **kwargs: Any) -> None:
        """
        Log debug message with optional context.

        :param message: The log message.
        :param kwargs: Additional context to include in the log.
        """
        self.log_with_context(logging.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        """
        Log info message with optional context.

        :param message: The log message.
        :param kwargs: Additional context to include in the log.
        """
        self.log_with_context(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        """
        Log warning message with optional context.

        :param message: The log message.
        :param kwargs: Additional context to include in the log.
        """
        self.log_with_context(logging.WARNING, message, **kwargs)

    def error(
        self, message: str, error: Optional[Exception] = None, **kwargs: Any
    ) -> None:
        """
        Log error message with optional exception and context.

        :param message: The log message.
        :param error: Optional exception to include in the log.
        :param kwargs: Additional context to include in the log.
        """
        if error:
            message = f"{message}: {error}"
        self.log_with_context(logging.ERROR, message, **kwargs)

    def critical(
        self, message: str, error: Optional[Exception] = None, **kwargs: Any
    ) -> None:
        """
        Log critical message with optional exception and context.

        :param message: The log message.
        :param error: Optional exception to include in the log.
        :param kwargs: Additional context to include in the log.
        """
        if error:
            message = f"{message}: {error}"
        self.log_with_context(logging.CRITICAL, message, **kwargs)

    def log_with_context(self, level: int, message: str, **kwargs: Any) -> None:
        """
        Log message with additional context information.

        :param level: Logging level.
        :param message: The log message.
        :param kwargs: Additional context to include in the log.
        """
        if kwargs:
            context_parts = [f"{k}={v}" for k, v in kwargs.items()]
            context_str = " | " + " ".join(context_parts)
            message += context_str

        self.logger.log(level, message)

    def set_level(self, level: int) -> None:
        """Set the logging level."""
        self.logger.setLevel(level)


# Default logger instance
_default_logger: Optional[QueuerLogger] = None


def get_logger(name: str = "queuer") -> QueuerLogger:
    """
    Get or create a logger instance.

    :param name: Logger name.
    :returns: QueuerLogger instance.
    """
    global _default_logger
    if _default_logger is None or _default_logger.logger.name != name:
        _default_logger = QueuerLogger(name)
    return _default_logger


def setup_logging(
    level: int = logging.INFO, use_colors: bool = True, name: str = "queuer"
) -> QueuerLogger:
    """
    Setup logging for the queuer application.

    :param level: Logging level.
    :param use_colors: Whether to use colors in logs.
    :param name: Logger name.
    :returns: Configured QueuerLogger instance.
    """
    logger = QueuerLogger(name, level, use_colors)

    # Set the global logger
    global _default_logger
    _default_logger = logger

    return logger


# Performance logging utilities
class PerformanceLogger:
    """Logger for performance metrics and timing."""

    def __init__(self, logger: QueuerLogger):
        self.logger = logger
        self.start_time: Optional[datetime] = None

    def start_timer(self, operation: str) -> None:
        """Start timing an operation."""
        self.start_time = datetime.now()
        self.logger.debug(f"Started {operation}")

    def end_timer(self, operation: str) -> float:
        """End timing and log duration."""
        if self.start_time is None:
            self.logger.warning(f"Timer not started for {operation}")
            return 0.0

        duration = (datetime.now() - self.start_time).total_seconds()
        self.logger.info(
            f"Completed {operation}", duration_seconds=duration, operation=operation
        )
        self.start_time = None
        return duration


def time_operation(
    operation: str, logger: Optional[QueuerLogger] = None
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator to time function execution.

    @time_operation("database_query")
    def my_function():
        pass
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            perf_logger = PerformanceLogger(logger or get_logger())
            perf_logger.start_timer(operation)
            try:
                result = func(*args, **kwargs)
                perf_logger.end_timer(operation)
                return result
            except Exception as e:
                perf_logger.logger.error(f"Failed {operation}", error=e)
                raise

        return wrapper

    return decorator
