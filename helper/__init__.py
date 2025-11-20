"""
Helper package for Python queuer implementation.
Provides utility functions and classes that mirror Go's helper package.
"""

from .task import (
    check_valid_task,
    check_valid_task_with_parameters,
    get_task_name_from_function,
    get_task_name_from_interface,
    get_input_parameters_from_task,
    get_output_parameters_from_task,
    validate_task_signature,
    get_task_name,
)

from .error import (
    QueuerError,
)

from .database import (
    Database,
    DatabaseConfiguration,
    new_database,
    new_database_from_env,
    new_database_with_connection,
    validate_connection_string,
    get_database_version,
    check_extension_exists,
)

from .logging import (
    QueuerLogger,
    ColorFormatter,
    PerformanceLogger,
    get_logger,
    setup_logging,
    time_operation,
)

__all__ = [
    # Task utilities
    "check_valid_task",
    "check_valid_task_with_parameters",
    "get_task_name_from_function",
    "get_task_name_from_interface",
    "get_input_parameters_from_task",
    "get_output_parameters_from_task",
    "validate_task_signature",
    "get_task_name",
    # Error handling
    "QueuerError",
    # Database utilities
    "Database",
    "DatabaseConfiguration",
    "new_database",
    "new_database_from_env",
    "new_database_with_connection",
    "validate_connection_string",
    "get_database_version",
    "check_extension_exists",
    # Logging utilities
    "QueuerLogger",
    "ColorFormatter",
    "PerformanceLogger",
    "get_logger",
    "setup_logging",
    "time_operation",
]
