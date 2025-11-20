"""
Test cases for SQL loader functionality.
"""

import shutil
import unittest
from unittest.mock import Mock, patch, MagicMock
import os
import tempfile
from pathlib import Path
from psycopg import errors

from helper.sql import SQLLoader, run_ddl


class TestRunDDL(unittest.TestCase):
    """Test cases for run_ddl function."""

    @patch("helper.sql._DDL_LOCK")
    def test_run_ddl_success(self, mock_lock: MagicMock):
        """Test successful DDL execution."""
        mock_conn = Mock()
        mock_cursor = MagicMock()
        # Set up cursor as a context manager
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None

        # Set up lock as context manager
        mock_lock.__enter__ = Mock()
        mock_lock.__exit__ = Mock()

        run_ddl(mock_conn, "CREATE TABLE test (id INT);")

        mock_conn.rollback.assert_called_once()
        mock_cursor.execute.assert_called_once_with(b"CREATE TABLE test (id INT);")
        mock_conn.commit.assert_called_once()

    @patch("helper.sql.time.sleep")
    def test_run_ddl_retry_on_deadlock(self, mock_sleep: MagicMock):
        """Test DDL retry on deadlock."""
        mock_conn = Mock()
        mock_cursor = MagicMock()
        # Set up cursor as a context manager
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None

        # First call raises deadlock, second succeeds
        mock_cursor.execute.side_effect = [errors.DeadlockDetected("deadlock"), None]

        run_ddl(mock_conn, "CREATE TABLE test (id INT);", max_retries=3)

        self.assertEqual(mock_cursor.execute.call_count, 2)
        mock_sleep.assert_called_once_with(0.5)

    def test_run_ddl_max_retries_exceeded(self):
        """Test max retries exceeded."""
        mock_conn = Mock()
        mock_cursor = MagicMock()
        # Set up cursor as a context manager
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None
        mock_cursor.execute.side_effect = errors.DeadlockDetected("deadlock")

        with self.assertRaises(errors.DeadlockDetected):
            run_ddl(mock_conn, "CREATE TABLE test (id INT);", max_retries=2)

    def test_run_ddl_other_exception(self):
        """Test DDL with other exception."""
        mock_conn = Mock()
        mock_cursor = MagicMock()
        # Set up cursor as a context manager
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None
        mock_cursor.execute.side_effect = ValueError("some error")

        with self.assertRaises(ValueError):
            run_ddl(mock_conn, "CREATE TABLE test (id INT);")

        mock_conn.rollback.assert_called()


class TestSQLLoader(unittest.TestCase):
    """Test cases for SQLLoader class."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.sql_loader = SQLLoader(self.temp_dir)

    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_init_with_default_path(self):
        """Test SQLLoader initialization with default path."""
        loader = SQLLoader()
        self.assertTrue(loader.sql_base_path.endswith("sql"))

    def test_init_with_custom_path(self):
        """Test SQLLoader initialization with custom path."""
        loader = SQLLoader("/custom/path")
        self.assertEqual(loader.sql_base_path, "/custom/path")

    def test_load_sql_file_success(self):
        """Test loading SQL file content."""
        # Create a test SQL file
        sql_content = "CREATE TABLE test (id INT);"
        sql_file = Path(self.temp_dir) / "test.sql"
        sql_file.write_text(sql_content)

        content = self.sql_loader.load_sql_file(str(sql_file))
        self.assertEqual(content, sql_content)

    def test_load_sql_file_not_found(self):
        """Test loading non-existent SQL file."""
        with self.assertRaises(ValueError) as cm:
            self.sql_loader.load_sql_file("/nonexistent/file.sql")
        self.assertIn("SQL file not found", str(cm.exception))

    def test_check_functions_all_exist(self):
        """Test checking functions when all exist."""
        mock_conn = Mock()
        mock_cursor = MagicMock()
        # Set up cursor as a context manager
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None
        mock_cursor.fetchone.return_value = [True]

        result = self.sql_loader.check_functions(mock_conn, ["func1", "func2"])
        self.assertTrue(result)
        self.assertEqual(mock_cursor.execute.call_count, 2)

    def test_check_functions_some_missing(self):
        """Test checking functions when some are missing."""
        mock_conn = Mock()
        mock_cursor = MagicMock()
        # Set up cursor as a context manager
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None
        mock_cursor.fetchone.side_effect = [[True], [False]]

        result = self.sql_loader.check_functions(mock_conn, ["func1", "func2"])
        self.assertFalse(result)

    @patch.object(SQLLoader, "execute_sql_file")
    @patch.object(SQLLoader, "check_functions")
    def test_load_job_sql_functions_exist(
        self, mock_check: MagicMock, mock_execute: MagicMock
    ):
        """Test loading job SQL when functions already exist."""
        mock_check.return_value = True

        mock_conn = Mock()
        self.sql_loader.load_job_sql(mock_conn, force=False)

        mock_check.assert_called_once_with(mock_conn, self.sql_loader.JOB_FUNCTIONS)
        mock_execute.assert_not_called()

    @patch.object(SQLLoader, "execute_sql_file")
    @patch.object(SQLLoader, "check_functions")
    def test_load_job_sql_force_reload(
        self, mock_check: MagicMock, mock_execute: MagicMock
    ):
        """Test loading job SQL with force=True."""
        mock_check.return_value = True

        mock_conn = Mock()
        expected_path = os.path.join(self.temp_dir, "job.sql")

        self.sql_loader.load_job_sql(mock_conn, force=True)

        mock_execute.assert_called_once_with(mock_conn, expected_path)

    @patch.object(SQLLoader, "execute_sql_file")
    @patch.object(SQLLoader, "check_functions")
    def test_load_job_sql_verification_fails(
        self, mock_check: MagicMock, mock_execute: MagicMock
    ):
        """Test loading job SQL when verification fails."""
        mock_check.side_effect = [
            False,
            False,
        ]  # Not exist, then still not exist after load

        mock_conn = Mock()

        with self.assertRaises(RuntimeError) as cm:
            self.sql_loader.load_job_sql(mock_conn)

        self.assertIn(
            "Not all required job SQL functions were created", str(cm.exception)
        )

    def test_execute_sql_file(self):
        """Test executing SQL file."""
        # Create a test SQL file
        sql_content = "CREATE TABLE test (id INT);"
        sql_file = Path(self.temp_dir) / "test.sql"
        sql_file.write_text(sql_content)

        mock_conn = Mock()

        with patch("helper.sql.run_ddl") as mock_run_ddl:
            self.sql_loader.execute_sql_file(mock_conn, str(sql_file))
            mock_run_ddl.assert_called_once_with(mock_conn, sql_content)

    def test_function_lists_defined(self):
        """Test that function lists are properly defined."""
        self.assertIsInstance(self.sql_loader.JOB_FUNCTIONS, list)
        self.assertIsInstance(self.sql_loader.WORKER_FUNCTIONS, list)
        self.assertIsInstance(self.sql_loader.NOTIFY_FUNCTIONS, list)

        self.assertGreater(len(self.sql_loader.JOB_FUNCTIONS), 0)
        self.assertGreater(len(self.sql_loader.WORKER_FUNCTIONS), 0)
        self.assertGreater(len(self.sql_loader.NOTIFY_FUNCTIONS), 0)


if __name__ == "__main__":
    unittest.main()
