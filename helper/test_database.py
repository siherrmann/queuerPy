"""
Database test utilities for Python queuer implementation.
Mirrors Go's helper/databaseTest.go using testcontainers-python.
"""

import os
import time
import atexit
from typing import Tuple, Optional, Callable
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from helper.database import Database, DatabaseConfiguration


# Test database constants
DB_NAME = "database"
DB_USER = "user"
DB_PASSWORD = "password"

# Global container tracking for cleanup
_active_containers = []


def cleanup_orphaned_containers():
    """Clean up any orphaned test containers on exit."""
    global _active_containers
    for container in _active_containers[
        :
    ]:  # Copy list to avoid modification during iteration
        _safe_container_cleanup(container, "orphaned container cleanup")
    _active_containers.clear()  # Clear the list after cleanup


# Register cleanup function to run on exit
atexit.register(cleanup_orphaned_containers)


def _safe_cleanup_with_fallback(
    primary_action, fallback_action=None, context="cleanup"
):
    """
    Utility function to safely execute cleanup actions with fallback.

    Args:
        primary_action: Main cleanup action to attempt
        fallback_action: Optional fallback action if primary fails
        context: Description of the cleanup context for error messages
    """
    try:
        primary_action()
    except Exception as e:
        print(f"Warning: Error during {context}: {e}")
        if fallback_action:
            try:
                fallback_action()
            except Exception as e2:
                print(f"Warning: Fallback {context} also failed: {e2}")


def _safe_container_cleanup(container, context="container cleanup"):
    """
    Unified container cleanup logic with proper error handling.

    Args:
        container: Container object with stop() method
        context: Description for error messages
    """
    if not container:
        return

    def primary_cleanup():
        if hasattr(container, "stop"):
            container.stop()

    def fallback_cleanup():
        # Force remove if normal stop failed
        if (
            hasattr(container, "container")
            and hasattr(container.container, "_container")
            and container.container._container
        ):
            container.container._container.remove(force=True)

    _safe_cleanup_with_fallback(primary_cleanup, fallback_cleanup, context)


class PostgresTestContainer:
    """
    Test container wrapper for PostgreSQL.
    Mirrors Go's testcontainer setup.
    """

    def __init__(self):
        """Initialize the PostgreSQL test container using structured wait strategies."""
        global _active_containers
        # Use DockerContainer directly with structured wait strategies
        self.container = (
            DockerContainer("timescale/timescaledb:latest-pg16")
            .with_exposed_ports(5432)
            .with_env("POSTGRES_DB", DB_NAME)
            .with_env("POSTGRES_USER", DB_USER)
            .with_env("POSTGRES_PASSWORD", DB_PASSWORD)
            .with_env("POSTGRES_HOST_AUTH_METHOD", "trust")
            .waiting_for(
                LogMessageWaitStrategy("database system is ready to accept connections")
            )
        )
        self._is_started = False
        # Track this container for cleanup
        _active_containers.append(self)

    def start(self) -> Tuple[str, int]:
        """
        Start the PostgreSQL container with structured wait strategies.
        Returns (host, port) tuple.
        """
        if not self._is_started:
            self.container.start()
            self._is_started = True

            # Additional wait for full readiness
            time.sleep(1)

        return self.container.get_container_host_ip(), self.container.get_exposed_port(
            5432
        )

    def stop(self) -> None:
        """Stop the PostgreSQL container with proper cleanup."""
        global _active_containers
        if self._is_started:

            def primary_stop():
                self.container.stop()
                self._is_started = False

            def fallback_stop():
                if hasattr(self.container, "_container") and self.container._container:
                    self.container._container.remove(force=True)
                    self._is_started = False

            _safe_cleanup_with_fallback(primary_stop, fallback_stop, "container stop")

        # Remove from active containers list
        try:
            if self in _active_containers:
                _active_containers.remove(self)
        except ValueError:
            pass  # Already removed, ignore

    def _get_host_and_port(self) -> Tuple[str, int]:
        """Get the current host and port of the container, starting if needed."""
        if not self._is_started:
            return self.start()
        else:
            host = self.container.get_container_host_ip()
            port = self.container.get_exposed_port(5432)
            return host, port

    def get_connection_string(self) -> str:
        """Get connection string for the test container."""
        host, port = self._get_host_and_port()
        return f"postgresql://{DB_USER}:{DB_PASSWORD}@{host}:{port}/{DB_NAME}?sslmode=disable"

    def get_config(self) -> DatabaseConfiguration:
        """Get DatabaseConfiguration for the test container."""
        host, port = self._get_host_and_port()
        return DatabaseConfiguration(
            host=host,
            port=port,
            database=DB_NAME,
            username=DB_USER,
            password=DB_PASSWORD,
            schema="public",
            sslmode="disable",
            with_table_drop=True,
        )


def must_start_postgres_container() -> Tuple[Callable[[], None], PostgresTestContainer]:
    """
    Start a PostgreSQL container for testing.
    Returns (terminate_function, container) tuple.
    Mirrors Go's MustStartPostgresContainer function.
    """
    container = PostgresTestContainer()
    container.start()

    def terminate():
        container.stop()

    return terminate, container


def new_test_database(config: DatabaseConfiguration) -> Database:
    """
    Create a new Database instance for testing.
    Mirrors Go's NewTestDatabase function.
    """
    return Database("test_db", config)


def set_test_database_config_envs(container: PostgresTestContainer) -> None:
    """
    Set environment variables for test database configuration.
    Mirrors Go's SetTestDatabaseConfigEnvs function.
    """
    host, port = container._get_host_and_port()

    os.environ["QUEUER_DB_HOST"] = host
    os.environ["QUEUER_DB_PORT"] = str(port)
    os.environ["QUEUER_DB_DATABASE"] = DB_NAME
    os.environ["QUEUER_DB_USERNAME"] = DB_USER
    os.environ["QUEUER_DB_PASSWORD"] = DB_PASSWORD
    os.environ["QUEUER_DB_SCHEMA"] = "public"
    os.environ["QUEUER_DB_SSLMODE"] = "disable"
    os.environ["QUEUER_DB_WITH_TABLE_DROP"] = "true"


class DatabaseTestMixin:
    """
    Mixin class for test cases that need database containers.
    Provides setup and teardown for PostgreSQL test containers.
    """

    @classmethod
    def setup_class(cls):
        """Set up the test container for the entire test class."""
        cls.terminate_func, cls.postgres_container = must_start_postgres_container()
        cls.db_config = cls.postgres_container.get_config()

    @classmethod
    def teardown_class(cls):
        """Tear down the test container after all tests with improved cleanup."""
        if hasattr(cls, "terminate_func") and cls.terminate_func:

            def primary_teardown():
                cls.terminate_func()

            def fallback_teardown():
                if hasattr(cls, "postgres_container") and cls.postgres_container:
                    cls.postgres_container.stop()

            _safe_cleanup_with_fallback(
                primary_teardown, fallback_teardown, "class teardown"
            )

    def setup_method(self, method=None):
        """Set up for each test method."""
        # Set environment variables for each test
        set_test_database_config_envs(self.postgres_container)

        # Create fresh database instance
        self.db = new_test_database(self.db_config)

    def teardown_method(self, method=None):
        """Clean up after each test method with improved error handling."""
        if hasattr(self, "db") and self.db and self.db.instance:

            def primary_db_cleanup():
                # Force close any active transactions
                try:
                    self.db.instance.rollback()
                except Exception:
                    pass  # Ignore if no active transaction
                # Close the database connection
                self.db.close()

            def fallback_db_cleanup():
                # Force close the connection
                if hasattr(self.db, "instance") and self.db.instance:
                    self.db.instance.close()
                    self.db.instance = None

            _safe_cleanup_with_fallback(
                primary_db_cleanup, fallback_db_cleanup, "database cleanup"
            )


# Utility functions for testing
def create_test_database_from_env() -> Database:
    """
    Create test database using environment variables.
    Assumes environment has been set up with set_test_database_config_envs.
    """
    config = DatabaseConfiguration.from_env()
    return new_test_database(config)


def wait_for_container_ready(
    container: PostgresTestContainer, timeout: int = 30
) -> bool:
    """
    Wait for the container to be ready to accept connections.

    Args:
        container: The PostgreSQL test container
        timeout: Maximum time to wait in seconds

    Returns:
        bool: True if container is ready, False if timeout
    """
    import time

    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            # Try to create a connection to test readiness
            connection_string = container.get_connection_string()
            test_db = Database("test_readiness", container.get_config())
            test_db.connect()
            test_db.close()
            return True
        except Exception:
            time.sleep(0.5)

    return False


def check_orphaned_containers():
    """
    Check for any Docker containers that might be left running from tests.
    This is a utility function for debugging container cleanup issues.
    """
    try:
        import subprocess

        result = subprocess.run(
            [
                "docker",
                "ps",
                "--filter",
                "ancestor=timescale/timescaledb:latest-pg16",
                "--format",
                "table {{.ID}}\\t{{.Names}}\\t{{.Status}}",
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode == 0 and result.stdout.strip():
            lines = result.stdout.strip().split("\n")
            if len(lines) > 1:  # More than just header
                print("Warning: Found potentially orphaned test containers:")
                print(result.stdout)
                return lines[1:]  # Return container lines (excluding header)
        return []
    except Exception as e:
        print(f"Could not check for orphaned containers: {e}")
        return []


# End of file
