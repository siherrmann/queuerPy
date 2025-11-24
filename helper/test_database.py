"""
Database test utilities for Python queuer implementation.
Simplified testcontainers implementation for reliable testing.
"""

import os
import threading
import time
from typing import Any
import psutil
from testcontainers.postgres import PostgresContainer

from .database import Database, DatabaseConfiguration
from .logging import get_logger


logger = get_logger(__name__)


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
            cls.container: PostgresContainer = PostgresContainer(
                "timescale/timescaledb:latest-pg16",
                dbname="test_db",
                username="test_user",
                password="test_password",
            )
            cls.container.start()

            # Set environment variables for database configuration
            cls._set_database_env_vars()

            # Create database configuration
            cls.db_config = DatabaseConfiguration(
                host=cls.container.get_container_host_ip(),
                port=cls.container.get_exposed_port(5432),
                database="test_db",
                username="test_user",
                password="test_password",
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

    def setup_method(self, method: Any = None):
        """Set up fresh database connection for each test method."""
        self.db = Database("test_db", self.db_config)

        self._initial_thread_count = threading.active_count()
        logger.debug(
            f"🔍 DIAGNOSTIC - Test setup - Active threads: {self._initial_thread_count}"
        )

    def teardown_method(self, method: Any = None):
        """Clean up database connection after each test method."""
        if hasattr(self, "db") and self.db:
            try:
                if hasattr(self.db, "instance") and self.db.instance:
                    self.db.instance.rollback()
                self.db.close()
            except Exception:
                pass

        # Give threads time to clean up
        time.sleep(0.2)

        try:
            process = psutil.Process(os.getpid())

            memory_mb = process.memory_info().rss / 1024 / 1024
            open_files = len(process.open_files())
            connections = len(process.net_connections())

            logger.warning(
                f"📊 DIAGNOSTIC - Memory: {memory_mb:.1f} MB, Open files: {open_files}, Connections: {connections}"
            )

            final_thread_count = threading.active_count()
            thread_diff = final_thread_count - getattr(self, "_initial_thread_count", 1)

            logger.warning(
                f"📊 Test cleanup - Thread increase: +{thread_diff} threads ({getattr(self, '_initial_thread_count', 1)} -> {final_thread_count})"
            )

            for thread in threading.enumerate():
                if thread != threading.main_thread():
                    logger.warning(
                        f"🧵 Active thread: {thread.name} ({type(thread).__name__})"
                    )
        except Exception:
            logger.warning("📊 DIAGNOSTIC - monitoring not available")

    @classmethod
    def _set_database_env_vars(cls):
        """Set environment variables for database configuration."""
        os.environ["QUEUER_DB_HOST"] = cls.container.get_container_host_ip()
        os.environ["QUEUER_DB_PORT"] = str(cls.container.get_exposed_port(5432))
        os.environ["QUEUER_DB_DATABASE"] = "test_db"
        os.environ["QUEUER_DB_USERNAME"] = "test_user"
        os.environ["QUEUER_DB_PASSWORD"] = "test_password"
        os.environ["QUEUER_DB_SCHEMA"] = "public"
        os.environ["QUEUER_DB_SSLMODE"] = "disable"
        os.environ["QUEUER_DB_WITH_TABLE_DROP"] = "true"
