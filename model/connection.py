"""
Connection model for Python queuer implementation.
Mirrors Go's model.Connection struct.
"""

from dataclasses import dataclass


@dataclass
class Connection:
    """
    Represents a database connection with its details.
    Used to monitor and manage active connections to the database.
    """

    pid: int
    database: str
    username: str
    application_name: str
    query: str
    state: str

    def __post_init__(self):
        """Validate the connection after initialization."""
        if self.pid < 0:
            raise ValueError("PID cannot be negative")
        if not self.database:
            raise ValueError("Database name cannot be empty")
        if not self.username:
            raise ValueError("Username cannot be empty")
