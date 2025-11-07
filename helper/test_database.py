"""
Database test utilities for Python queuer implementation.
Simplified testcontainers implementation for reliable testing.
"""

import os
from testcontainers.postgres import PostgresContainer
from helper.database import Database, DatabaseConfiguration


class DatabaseTestMixin:
    """
    Simple mixin class for test cases that need PostgreSQL database containers.
    Uses lazy initialization to avoid creating containers during test discovery.
    """

    @classmethod
    def setup_class(cls):
        """Set up PostgreSQL container for the entire test class (lazy initialization)."""
        # Only create container if we don't already have one for this class
        if not hasattr(cls, "_container_initialized"):
            cls.container = PostgresContainer("timescale/timescaledb:latest-pg16")
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
            cls._container_initialized = True

    @classmethod
    def teardown_class(cls):
        """Clean up PostgreSQL container after all tests."""
        if hasattr(cls, "container") and cls.container:
            try:
                cls.container.stop()
            except Exception:
                pass
            # Clean up the initialization flag
            if hasattr(cls, "_container_initialized"):
                delattr(cls, "_container_initialized")

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
                pass

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
