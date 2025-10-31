"""
Database test utilities for Python queuer implementation.
Simplified testcontainers implementation for reliable testing.
"""

import os
from testcontainers.postgres import PostgresContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from helper.database import Database, DatabaseConfiguration

# Disable testcontainers Reaper for more reliable container management
os.environ["TESTCONTAINERS_REAPER_DISABLED"] = "true"
os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"


class DatabaseTestMixin:
    """
    Simple mixin class for test cases that need PostgreSQL database containers.
    """

    @classmethod
    def setup_class(cls):
        """Set up PostgreSQL container for the entire test class."""
        cls.container = PostgresContainer("timescale/timescaledb:latest-pg16")

        # Use modern structured wait strategy
        cls.container = cls.container.waiting_for(
            LogMessageWaitStrategy("database system is ready to accept connections")
        )
        cls.container.start()

        # Set environment variables for database configuration
        cls._set_database_env_vars()

        # Create database configuration
        cls.db_config = DatabaseConfiguration(
            host=cls.container.get_container_host_ip(),
            port=cls.container.get_exposed_port(5432),
            database=cls.container.dbname,
            username=cls.container.username,
            password=cls.container.password,
            schema="public",
            sslmode="disable",
            with_table_drop=True,
        )

    @classmethod
    def teardown_class(cls):
        """Clean up PostgreSQL container after all tests."""
        if hasattr(cls, "container"):
            try:
                cls.container.stop()
            except Exception:
                pass  # Ignore cleanup errors

    def setup_method(self, method=None):
        """Set up fresh database connection for each test method."""
        self.db = Database("test_db", self.db_config)

    def teardown_method(self, method=None):
        """Clean up database connection after each test method."""
        if hasattr(self, "db") and self.db:
            try:
                if hasattr(self.db, "instance") and self.db.instance:
                    self.db.instance.rollback()
                self.db.close()
            except Exception:
                pass  # Ignore cleanup errors

    @classmethod
    def _set_database_env_vars(cls):
        """Set environment variables for database configuration."""
        os.environ["QUEUER_DB_HOST"] = cls.container.get_container_host_ip()
        os.environ["QUEUER_DB_PORT"] = str(cls.container.get_exposed_port(5432))
        os.environ["QUEUER_DB_DATABASE"] = cls.container.dbname
        os.environ["QUEUER_DB_USERNAME"] = cls.container.username
        os.environ["QUEUER_DB_PASSWORD"] = cls.container.password
        os.environ["QUEUER_DB_SCHEMA"] = "public"
        os.environ["QUEUER_DB_SSLMODE"] = "disable"
        os.environ["QUEUER_DB_WITH_TABLE_DROP"] = "true"
