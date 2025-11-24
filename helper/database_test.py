"""
Test cases for database configuration and utilities.
"""

import os
import unittest
from unittest.mock import patch, Mock, call

from .database import DatabaseConfiguration, Database
from .error import QueuerError


class TestDatabaseConfiguration(unittest.TestCase):
    """Test cases for DatabaseConfiguration class."""

    def test_init_with_defaults(self):
        """Test initialization with default values."""
        config = DatabaseConfiguration(
            host="localhost",
            port=5432,
            database="test",
            username="user",
            password="pass",
        )

        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.port, 5432)
        self.assertEqual(config.database, "test")
        self.assertEqual(config.username, "user")
        self.assertEqual(config.password, "pass")
        self.assertEqual(config.schema, "public")
        self.assertEqual(config.sslmode, "require")
        self.assertFalse(config.with_table_drop)

    def test_init_with_custom_values(self):
        """Test initialization with custom values."""
        config = DatabaseConfiguration(
            host="db.example.com",
            port=5433,
            database="custom_db",
            username="custom_user",
            password="custom_pass",
            schema="custom_schema",
            sslmode="disable",
            with_table_drop=True,
        )

        self.assertEqual(config.host, "db.example.com")
        self.assertEqual(config.port, 5433)
        self.assertEqual(config.database, "custom_db")
        self.assertEqual(config.username, "custom_user")
        self.assertEqual(config.password, "custom_pass")
        self.assertEqual(config.schema, "custom_schema")
        self.assertEqual(config.sslmode, "disable")
        self.assertTrue(config.with_table_drop)

    @patch.dict(
        os.environ,
        {
            "QUEUER_DB_HOST": "test_host",
            "QUEUER_DB_PORT": "5433",
            "QUEUER_DB_DATABASE": "test_db",
            "QUEUER_DB_USERNAME": "test_user",
            "QUEUER_DB_PASSWORD": "test_pass",
            "QUEUER_DB_SCHEMA": "test_schema",
            "QUEUER_DB_SSLMODE": "disable",
            "QUEUER_DB_WITH_TABLE_DROP": "true",
        },
    )
    def test_from_env_all_variables(self):
        """Test creating configuration from environment variables."""
        config = DatabaseConfiguration.from_env()

        self.assertEqual(config.host, "test_host")
        self.assertEqual(config.port, 5433)
        self.assertEqual(config.database, "test_db")
        self.assertEqual(config.username, "test_user")
        self.assertEqual(config.password, "test_pass")
        self.assertEqual(config.schema, "test_schema")
        self.assertEqual(config.sslmode, "disable")
        self.assertTrue(config.with_table_drop)

    @patch.dict(os.environ, {}, clear=True)
    def test_from_env_defaults(self):
        """Test creating configuration with default environment values."""
        config = DatabaseConfiguration.from_env()

        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.port, 5432)
        self.assertEqual(config.database, "queuer")
        self.assertEqual(config.username, "postgres")
        self.assertEqual(config.password, "")
        self.assertEqual(config.schema, "public")
        self.assertEqual(config.sslmode, "require")
        self.assertFalse(config.with_table_drop)

    @patch.dict(
        os.environ,
        {
            "QUEUER_DB_HOST": "",
            "QUEUER_DB_DATABASE": "test",
            "QUEUER_DB_USERNAME": "user",
        },
    )
    def test_from_env_missing_required(self):
        """Test from_env with missing required variables."""
        with self.assertRaises(ValueError) as cm:
            DatabaseConfiguration.from_env()

        self.assertIn("Required environment variables missing", str(cm.exception))

    @patch.dict(os.environ, {"QUEUER_DB_WITH_TABLE_DROP": "TRUE"})
    def test_from_env_boolean_parsing_true(self):
        """Test boolean parsing for table drop (TRUE)."""
        config = DatabaseConfiguration.from_env()
        self.assertTrue(config.with_table_drop)

    @patch.dict(os.environ, {"QUEUER_DB_WITH_TABLE_DROP": "False"})
    def test_from_env_boolean_parsing_false(self):
        """Test boolean parsing for table drop (False)."""
        config = DatabaseConfiguration.from_env()
        self.assertFalse(config.with_table_drop)

    def test_connection_string(self):
        """Test connection string generation."""
        config = DatabaseConfiguration(
            host="localhost",
            port=5432,
            database="test_db",
            username="test_user",
            password="test_pass",
            schema="test_schema",
            sslmode="require",
        )

        conn_str = config.connection_string()

        expected_parts = [
            "host=localhost",
            "port=5432",
            "dbname=test_db",
            "user=test_user",
            "password=test_pass",
            "sslmode=require",
            "application_name=queuer",
            "options='-c search_path=test_schema'",
        ]

        for part in expected_parts:
            self.assertIn(part, conn_str)

    def test_connection_string_with_empty_password(self):
        """Test connection string with empty password."""
        config = DatabaseConfiguration(
            host="localhost",
            port=5432,
            database="test_db",
            username="test_user",
            password="",
            schema="public",
        )

        conn_str = config.connection_string()
        self.assertIn("password=", conn_str)


class TestDatabase(unittest.TestCase):
    """Test cases for Database class."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = DatabaseConfiguration(
            host="localhost",
            port=5432,
            database="test",
            username="user",
            password="pass",
        )

    def test_init_minimal(self):
        """Test Database initialization with minimal parameters."""
        db = Database("test_db", auto_connect=False)

        self.assertEqual(db.name, "test_db")
        self.assertIsNone(db.instance)
        self.assertIsNone(db.config)

    def test_init_with_config(self):
        """Test Database initialization with configuration."""
        mock_logger = Mock()
        db = Database("test_db", self.config, mock_logger, auto_connect=False)

        self.assertEqual(db.name, "test_db")
        self.assertEqual(db.config, self.config)
        self.assertEqual(db.logger, mock_logger)
        self.assertIsNone(db.instance)

    @patch("queuerPy.helper.database.psycopg.connect")
    @patch("queuerPy.helper.database.run_ddl")
    def test_connect_to_database_success(self, mock_run_ddl: Mock, mock_connect: Mock):
        """Test successful database connection."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection

        db = Database("test_db", self.config, auto_connect=False)
        db.connect_to_database()

        self.assertEqual(db.instance, mock_connection)
        mock_connect.assert_called_once_with(
            self.config.connection_string(), autocommit=False
        )
        # Check that both execute calls were made
        expected_calls = [call("SET application_name = 'queuer'"), call("SELECT 1")]
        mock_connection.execute.assert_has_calls(expected_calls, any_order=False)
        mock_run_ddl.assert_called_once_with(
            mock_connection, "CREATE EXTENSION IF NOT EXISTS pg_trgm;"
        )

    @patch("queuerPy.helper.database.psycopg.connect")
    def test_connect_to_database_failure(self, mock_connect: Mock):
        """Test database connection failure."""
        original_error = Exception("Connection failed")
        mock_connect.side_effect = original_error

        db = Database("test_db", self.config, auto_connect=False)

        with self.assertRaises(QueuerError) as cm:
            db.connect_to_database()

        self.assertIn("Failed to connect to database", str(cm.exception))
        self.assertIsNone(db.instance)

    def test_connect_to_database_without_config(self):
        """Test connecting without configuration."""
        db = Database("test_db", auto_connect=False)

        with self.assertRaises(QueuerError) as cm:
            db.connect_to_database()

        self.assertIn(
            "Database configuration is required for connection", str(cm.exception)
        )

    @patch("queuerPy.helper.database.psycopg.connect")
    @patch("queuerPy.helper.database.run_ddl")
    def test_auto_connect_enabled(self, mock_run_ddl: Mock, mock_connect: Mock):
        """Test auto-connection on initialization."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection

        db = Database("test_db", self.config, auto_connect=True)

        self.assertEqual(db.instance, mock_connection)
        mock_connect.assert_called_once()

    def test_auto_connect_disabled(self):
        """Test auto-connection disabled."""
        db = Database("test_db", self.config, auto_connect=False)

        self.assertIsNone(db.instance)


class TestDatabaseUtilities(unittest.TestCase):
    """Test cases for database utility functions."""

    def test_new_database_function_exists(self):
        """Test that new_database function exists and is importable."""
        from .database import new_database

        self.assertTrue(callable(new_database))

    @patch("queuerPy.helper.database.Database")
    def test_new_database_creates_instance(self, mock_database_class: Mock):
        """Test new_database function creates Database instance."""
        from .database import new_database, DatabaseConfiguration

        mock_instance = Mock()
        mock_database_class.return_value = mock_instance

        config = DatabaseConfiguration(
            host="test", port=5432, database="test", username="user", password="pass"
        )
        logger = Mock()

        result = new_database("test_db", config, logger)

        mock_database_class.assert_called_once_with("test_db", config, logger, True)
        self.assertEqual(result, mock_instance)


if __name__ == "__main__":
    unittest.main()
