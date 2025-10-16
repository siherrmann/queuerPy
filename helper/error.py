"""
Error handling utilities for Python queuer implementation.
Mirrors Go's helper/error.go with Python exception handling.
"""

import traceback
from typing import List, Optional, Union
import inspect


class QueuerError(Exception):
    """
    Enhanced error class with trace information.
    Mirrors Go's Error struct for better error tracking.
    """
    
    def __init__(self, original: Union[Exception, str], trace: Optional[List[str]] = None):
        """Initialize QueuerError with original error and trace."""
        if isinstance(original, str):
            self.original = Exception(original)
        else:
            self.original = original
        
        self.trace = trace or []
        super().__init__(str(self.original))
    
    def __str__(self) -> str:
        """Return formatted error message with trace."""
        if self.trace:
            return f"{str(self.original)} | Trace: {', '.join(self.trace)}"
        return str(self.original)
    
    def add_trace(self, trace_message: str) -> 'QueuerError':
        """Add a trace message to the error."""
        # Get caller information
        frame = inspect.currentframe().f_back
        if frame:
            function_name = frame.f_code.co_name
            trace_message = f"{function_name} - {trace_message}"
        
        self.trace.append(trace_message)
        return self


def new_error(trace: str, original: Union[Exception, str]) -> QueuerError:
    """
    Create a new QueuerError with trace information.
    Mirrors Go's NewError function.
    """
    # Get caller information
    frame = inspect.currentframe().f_back
    if frame:
        function_name = frame.f_code.co_name
        trace = f"{function_name} - {trace}"
    
    if isinstance(original, QueuerError):
        # Add to existing trace
        original.trace.append(trace)
        return original
    else:
        # Create new error with trace
        return QueuerError(original, [trace])


def wrap_error(error: Exception, context: str) -> QueuerError:
    """
    Wrap an existing exception with additional context.
    Python-specific helper for better error handling.
    """
    if isinstance(error, QueuerError):
        return error.add_trace(context)
    else:
        return new_error(context, error)


def safe_execute(func, *args, **kwargs):
    """
    Safely execute a function and wrap any exceptions.
    Returns (result, error) tuple similar to Go error handling.
    """
    try:
        result = func(*args, **kwargs)
        return result, None
    except Exception as e:
        wrapped_error = wrap_error(e, f"executing {func.__name__}")
        return None, wrapped_error


class ErrorContext:
    """
    Context manager for adding trace information to errors.
    Python-specific enhancement for error handling.
    """
    
    def __init__(self, context: str):
        self.context = context
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None and issubclass(exc_type, Exception):
            # Modify the exception to add our context
            if isinstance(exc_val, QueuerError):
                exc_val.add_trace(self.context)
            else:
                # Replace with wrapped error
                wrapped = wrap_error(exc_val, self.context)
                # Note: We can't replace the exception in __exit__, 
                # but we can log or handle it
                return False  # Re-raise the original exception
        return False


def format_exception_with_trace(exc: Exception) -> str:
    """
    Format an exception with full stack trace.
    Helper for logging and debugging.
    """
    if isinstance(exc, QueuerError):
        base_trace = traceback.format_exception(type(exc.original), exc.original, None)
        trace_info = "\nQueuer Trace: " + " -> ".join(reversed(exc.trace))
        return "".join(base_trace) + trace_info
    else:
        return traceback.format_exception(type(exc), exc, exc.__traceback__)


# Exception types for specific queuer errors
class TaskError(QueuerError):
    """Error related to task execution or validation."""
    pass


class DatabaseError(QueuerError):
    """Error related to database operations."""
    pass


class ConfigurationError(QueuerError):
    """Error related to configuration or environment setup."""
    pass


class WorkerError(QueuerError):
    """Error related to worker operations."""
    pass


class JobError(QueuerError):
    """Error related to job operations."""
    pass


# Convenience functions for creating specific error types
def task_error(message: str, original: Exception = None) -> TaskError:
    """Create a task-related error."""
    return TaskError(original or message)


def database_error(message: str, original: Exception = None) -> DatabaseError:
    """Create a database-related error."""
    return DatabaseError(original or message)


def configuration_error(message: str, original: Exception = None) -> ConfigurationError:
    """Create a configuration-related error."""
    return ConfigurationError(original or message)


def worker_error(message: str, original: Exception = None) -> WorkerError:
    """Create a worker-related error."""
    return WorkerError(original or message)


def job_error(message: str, original: Exception = None) -> JobError:
    """Create a job-related error."""
    return JobError(original or message)