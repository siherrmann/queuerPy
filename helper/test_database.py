"""
Database test utilities for Python queuer implementation.
Mirrors Go's helper/databaseTest.go using testcontainers-python.
"""

import os
import time
from typing import Tuple, Optional, Callable
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from helper.database import Database, DatabaseConfiguration


# Test database constants
DB_NAME = "database"
DB_USER = "user"
DB_PASSWORD = "password"


class PostgresTestContainer:
    """
    Test container wrapper for PostgreSQL.
    Mirrors Go's testcontainer setup.
    """
    
    def __init__(self):
        """Initialize the PostgreSQL test container using structured wait strategies."""
        # Use DockerContainer directly with structured wait strategies
        self.container = (DockerContainer("timescale/timescaledb:latest-pg16")
            .with_exposed_ports(5432)
            .with_env("POSTGRES_DB", DB_NAME)
            .with_env("POSTGRES_USER", DB_USER)
            .with_env("POSTGRES_PASSWORD", DB_PASSWORD)
            .with_env("POSTGRES_HOST_AUTH_METHOD", "trust")
            .waiting_for(LogMessageWaitStrategy("database system is ready to accept connections"))
        )
        self._is_started = False
    
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
        
        return self.container.get_container_host_ip(), self.container.get_exposed_port(5432)
    
    def stop(self) -> None:
        """Stop the PostgreSQL container."""
        if self._is_started:
            self.container.stop()
            self._is_started = False
    
    def get_connection_string(self) -> str:
        """Get connection string for the test container."""
        if not self._is_started:
            host, port = self.start()
        else:
            host = self.container.get_container_host_ip()
            port = self.container.get_exposed_port(5432)
        
        return f"postgresql://{DB_USER}:{DB_PASSWORD}@{host}:{port}/{DB_NAME}?sslmode=disable"
    
    def get_config(self) -> DatabaseConfiguration:
        """Get DatabaseConfiguration for the test container."""
        if not self._is_started:
            host, port = self.start()
        else:
            host = self.container.get_container_host_ip()
            port = self.container.get_exposed_port(5432)
        
        return DatabaseConfiguration(
            host=host,
            port=port,
            database=DB_NAME,
            username=DB_USER,
            password=DB_PASSWORD,
            schema="public",
            sslmode="disable",
            with_table_drop=True
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
    if not container._is_started:
        host, port = container.start()
    else:
        host = container.container.get_container_host_ip()
        port = container.container.get_exposed_port(5432)
    
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
        """Tear down the test container after all tests."""
        if hasattr(cls, 'terminate_func'):
            cls.terminate_func()
    
    def setup_method(self, method=None):
        """Set up for each test method."""
        # Set environment variables for each test
        set_test_database_config_envs(self.postgres_container)
        
        # Create fresh database instance
        self.db = new_test_database(self.db_config)
    
    def teardown_method(self, method=None):
        """Clean up after each test method."""
        if hasattr(self, 'db') and self.db.instance:
            self.db.close()


# Context manager for temporary test database
class TempPostgresContainer:
    """
    Context manager for temporary PostgreSQL containers.
    Automatically starts and stops the container.
    """
    
    def __init__(self):
        self.container: Optional[PostgresTestContainer] = None
        self.terminate_func: Optional[Callable[[], None]] = None
    
    def __enter__(self) -> PostgresTestContainer:
        """Start the container and return it."""
        self.terminate_func, self.container = must_start_postgres_container()
        return self.container
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop the container."""
        if self.terminate_func:
            self.terminate_func()


# Utility functions for testing
def create_test_database_from_env() -> Database:
    """
    Create test database using environment variables.
    Assumes environment has been set up with set_test_database_config_envs.
    """
    config = DatabaseConfiguration.from_env()
    return new_test_database(config)


def wait_for_container_ready(container: PostgresTestContainer, timeout: int = 30) -> bool:
    """
    Wait for the container to be ready to accept connections.
    Returns True if ready, False if timeout.
    """
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            # Try to create a database connection
            config = container.get_config()
            db = Database("test_connection", config)
            db.instance.execute("SELECT 1")
            db.close()
            return True
        except Exception:
            time.sleep(1)
    
    return False