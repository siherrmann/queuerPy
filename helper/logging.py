"""
Pretty logging utilities for Python queuer implementation.
Mirrors Go's helper/prettyLog.go with Python logging enhancements.
"""

import logging
import sys
from typing import Optional, TextIO
from datetime import datetime


class ColorFormatter(logging.Formatter):
    """
    Colored log formatter for console output.
    Mirrors Go's PrettyHandler functionality.
    """
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[95m',    # Magenta
        'INFO': '\033[94m',     # Blue
        'WARNING': '\033[93m',  # Yellow
        'ERROR': '\033[91m',    # Red
        'CRITICAL': '\033[95m', # Magenta
        'RESET': '\033[0m'      # Reset
    }
    
    def __init__(self, use_colors: bool = True, include_timestamp: bool = True):
        """Initialize the color formatter."""
        self.use_colors = use_colors and sys.stdout.isatty()
        self.include_timestamp = include_timestamp
        
        if include_timestamp:
            fmt = '%(asctime)s %(levelname)s: %(message)s'
        else:
            fmt = '%(levelname)s: %(message)s'
        
        super().__init__(fmt, datefmt='%Y-%m-%d %H:%M:%S')
    
    def format(self, record: logging.LogRecord) -> str:
        """Format the log record with colors."""
        # Format the base message
        formatted = super().format(record)
        
        # Add colors if enabled
        if self.use_colors and record.levelname in self.COLORS:
            color = self.COLORS[record.levelname]
            reset = self.COLORS['RESET']
            
            # Color only the level name
            parts = formatted.split(':', 1)
            if len(parts) == 2:
                level_part, message_part = parts
                formatted = f"{color}{level_part}:{reset}{message_part}"
        
        return formatted


class QueuerLogger:
    """
    Enhanced logger for queuer operations.
    Provides structured logging with context information.
    """
    
    def __init__(self, name: str = "queuer", level: int = logging.INFO,
                 use_colors: bool = True, stream: Optional[TextIO] = None):
        """Initialize the queuer logger."""
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
    
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message with optional context."""
        self._log_with_context(logging.DEBUG, message, **kwargs)
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message with optional context."""
        self._log_with_context(logging.INFO, message, **kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message with optional context."""
        self._log_with_context(logging.WARNING, message, **kwargs)
    
    def error(self, message: str, error: Optional[Exception] = None, **kwargs) -> None:
        """Log error message with optional exception and context."""
        if error:
            message = f"{message}: {error}"
        self._log_with_context(logging.ERROR, message, **kwargs)
    
    def critical(self, message: str, error: Optional[Exception] = None, **kwargs) -> None:
        """Log critical message with optional exception and context."""
        if error:
            message = f"{message}: {error}"
        self._log_with_context(logging.CRITICAL, message, **kwargs)
    
    def _log_with_context(self, level: int, message: str, **kwargs) -> None:
        """Log message with additional context information."""
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
    """Get or create a logger instance."""
    global _default_logger
    if _default_logger is None or _default_logger.logger.name != name:
        _default_logger = QueuerLogger(name)
    return _default_logger


def setup_logging(level: int = logging.INFO, use_colors: bool = True,
                 name: str = "queuer") -> QueuerLogger:
    """
    Setup logging for the queuer application.
    Returns configured logger instance.
    """
    logger = QueuerLogger(name, level, use_colors)
    
    # Set the global logger
    global _default_logger
    _default_logger = logger
    
    return logger


# Convenience functions for direct logging
def debug(message: str, **kwargs) -> None:
    """Log debug message using default logger."""
    get_logger().debug(message, **kwargs)


def info(message: str, **kwargs) -> None:
    """Log info message using default logger."""
    get_logger().info(message, **kwargs)


def warning(message: str, **kwargs) -> None:
    """Log warning message using default logger."""
    get_logger().warning(message, **kwargs)


def error(message: str, error: Optional[Exception] = None, **kwargs) -> None:
    """Log error message using default logger."""
    get_logger().error(message, error, **kwargs)


def critical(message: str, error: Optional[Exception] = None, **kwargs) -> None:
    """Log critical message using default logger."""
    get_logger().critical(message, error, **kwargs)


# Context manager for structured logging
class LogContext:
    """
    Context manager for adding context to log messages.
    Automatically adds context information to all log messages within the block.
    """
    
    def __init__(self, logger: QueuerLogger, **context):
        """Initialize log context."""
        self.logger = logger
        self.context = context
        self.original_log_method = None
    
    def __enter__(self):
        """Enter the log context."""
        # Store original log method
        self.original_log_method = self.logger._log_with_context
        
        # Create new log method that includes context
        def enhanced_log(level: int, message: str, **kwargs):
            # Merge context with message kwargs
            merged_context = {**self.context, **kwargs}
            return self.original_log_method(level, message, **merged_context)
        
        # Replace the log method
        self.logger._log_with_context = enhanced_log
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the log context."""
        # Restore original log method
        if self.original_log_method:
            self.logger._log_with_context = self.original_log_method


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
        self.logger.info(f"Completed {operation}", duration_seconds=duration)
        self.start_time = None
        return duration


def time_operation(operation: str, logger: Optional[QueuerLogger] = None):
    """
    Decorator to time function execution.
    
    @time_operation("database_query")
    def my_function():
        pass
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
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