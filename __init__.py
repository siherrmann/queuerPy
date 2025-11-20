"""
QueuerPy - A Python implementation of the queuer system

A job queuing and processing system with PostgreSQL backend that provides:
- Asynchronous job processing
- Retry mechanisms with configurable policies
- Job scheduling with interval functions
- Worker management and monitoring
- Database-backed persistence
- Event-driven notifications
"""

from ._version import __version__

__author__ = "Simon Herrmann"
__email__ = "siherrmann@users.noreply.github.com"

# Core exports
from .queuer import (
    Queuer,
    new_queuer,
    new_queuer_with_db,
)

from .helper.database import (
    DatabaseConfiguration,
)

from .model.job import (
    Job,
    JobStatus,
)

from .model.worker import (
    Worker,
    WorkerStatus,
)

from .model.options_on_error import (
    OnError,
    RetryBackoff,
)

from .helper.error import (
    QueuerError,
)

# Convenience imports for common use cases
__all__ = [
    # Core classes
    "Queuer",
    "new_queuer",
    "new_queuer_with_db",
    # Configuration
    "DatabaseConfiguration",
    # Models
    "Job",
    "JobStatus",
    "Worker",
    "WorkerStatus",
    "OnError",
    "RetryBackoff",
    # Exceptions
    "QueuerError",
    # Version info
    "__version__",
    "__author__",
    "__email__",
]
