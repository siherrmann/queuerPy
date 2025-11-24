"""
Database package for Python queuer implementation.
Provides database handlers and SQL loading using helper.database classes.
"""

# Import from helper for core database functionality
from ..helper.database import (
    Database,
    DatabaseConfiguration,
    new_database,
    new_database_from_env,
)

# Import deprecated SQL loader from helper.sql module
from ..helper.sql import (
    SQLLoader,
)

from .db_job import (
    JobDBHandler,
)

from .db_worker import (
    WorkerDBHandler,
)

from .db_listener import (
    QueuerListener,
    new_queuer_db_listener,
)

__all__ = [
    # Core database classes (from helper)
    "Database",
    "DatabaseConfiguration",
    "new_database",
    "new_database_from_env",
    # Deprecated SQL loading utilities
    "SQLLoader",
    # Job handler
    "JobDBHandler",
    # Worker handler
    "WorkerDBHandler",
    # Listeners
    "QueuerListener",
    "new_queuer_db_listener",
]
