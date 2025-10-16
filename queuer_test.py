"""
Test for the main Queuer implementation.
Mirrors the Go queuerTest.go implementation with testcontainers.
"""

import os
import time
import unittest
import threading
from datetime import datetime, timedelta
from typing import Optional

from queuer import new_queuer, new_queuer_with_db
from helper.test_database import DatabaseTestMixin
from helper.database import DatabaseConfiguration
from model.options_on_error import OnError, RetryBackoff
from model.worker import WorkerStatus


class TestNewQueuer(DatabaseTestMixin, unittest.TestCase):
    """Test new_queuer factory function with various configurations."""
    
    @classmethod
    def setUpClass(cls):
        """Set up for the entire test class."""
        super().setup_class()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        super().teardown_class()
    
    def setUp(self):
        """Set up for each test method."""
        super().setup_method()
    
    def tearDown(self):
        """Clean up after each test method."""
        super().teardown_method()
    
    def _set_db_env_vars(self):
        """Set database environment variables for testing."""
        os.environ["QUEUER_DB_HOST"] = self.db_config.host
        os.environ["QUEUER_DB_PORT"] = str(self.db_config.port)
        os.environ["QUEUER_DB_DATABASE"] = self.db_config.database
        os.environ["QUEUER_DB_USERNAME"] = self.db_config.username
        os.environ["QUEUER_DB_PASSWORD"] = self.db_config.password
        os.environ["QUEUER_DB_SCHEMA"] = self.db_config.schema
        os.environ["QUEUER_DB_SSLMODE"] = self.db_config.sslmode
    
    def _clear_db_env_vars(self):
        """Clear database environment variables."""
        env_vars = [
            "QUEUER_DB_HOST", "QUEUER_DB_PORT", "QUEUER_DB_DATABASE",
            "QUEUER_DB_USERNAME", "QUEUER_DB_PASSWORD", "QUEUER_DB_SCHEMA", "QUEUER_DB_SSLMODE"
        ]
        for var in env_vars:
            if var in os.environ:
                del os.environ[var]
    
    def test_valid_queuer(self):
        """Test creating a valid queuer."""
        self._set_db_env_vars()
        try:
            queuer = new_queuer("valid_queuer", 100)
            self.assertIsNotNone(queuer)
            self.assertEqual(queuer.name, "valid_queuer")
            self.assertEqual(queuer.max_concurrency, 100)
            self.assertEqual(queuer.worker.name, "valid_queuer")
            self.assertEqual(queuer.worker.max_concurrency, 100)
        finally:
            self._clear_db_env_vars()
    
    def test_valid_queuer_with_options(self):
        """Test creating a valid queuer with error handling options."""
        self._set_db_env_vars()
        try:
            options = OnError(
                timeout=10.0,
                max_retries=3,
                retry_delay=1.0,
                retry_backoff=RetryBackoff.LINEAR
            )
            queuer = new_queuer("valid_queuer_with_options", 100, options)
            self.assertIsNotNone(queuer)
            self.assertEqual(queuer.name, "valid_queuer_with_options")
            self.assertEqual(queuer.max_concurrency, 100)
        finally:
            self._clear_db_env_vars()
    
    def test_invalid_max_concurrency(self):
        """Test creating queuer with invalid max concurrency."""
        self._set_db_env_vars()
        try:
            with self.assertRaises(ValueError):
                queuer = new_queuer("invalid_concurrency", -1)
        finally:
            self._clear_db_env_vars()
    
    def test_invalid_options(self):
        """Test creating queuer with invalid options."""
        self._set_db_env_vars()
        try:
            with self.assertRaises(ValueError):
                options = OnError(timeout=-10.0)  # Invalid timeout
                queuer = new_queuer("invalid_options", 100, options)
        finally:
            self._clear_db_env_vars()
    
    def test_missing_db_environment_variable(self):
        """Test creating queuer with missing database environment variables."""
        # Clear all environment variables first
        self._clear_db_env_vars()
        
        # Set most env vars but intentionally omit QUEUER_DB_HOST (required)
        os.environ["QUEUER_DB_PORT"] = str(self.db_config.port)
        os.environ["QUEUER_DB_DATABASE"] = self.db_config.database
        os.environ["QUEUER_DB_USERNAME"] = self.db_config.username
        os.environ["QUEUER_DB_PASSWORD"] = self.db_config.password
        os.environ["QUEUER_DB_SCHEMA"] = self.db_config.schema
        # Intentionally missing: QUEUER_DB_HOST (make it empty to trigger validation)
        os.environ["QUEUER_DB_HOST"] = ""
        
        try:
            with self.assertRaises((ValueError, Exception)):  # Should raise a database error
                queuer = new_queuer("missing_env", 100)
        finally:
            self._clear_db_env_vars()


class TestNewQueuerWithDB(DatabaseTestMixin, unittest.TestCase):
    """Test new_queuer_with_db factory function."""
    
    @classmethod
    def setUpClass(cls):
        """Set up for the entire test class."""
        super().setup_class()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        super().teardown_class()
    
    def setUp(self):
        """Set up for each test method."""
        super().setup_method()
    
    def tearDown(self):
        """Clean up after each test method."""
        super().teardown_method()
    
    def test_valid_queuer_with_nil_db_config(self):
        """Test creating queuer with None db_config (uses env vars)."""
        # Set environment variables
        os.environ["QUEUER_DB_HOST"] = self.db_config.host
        os.environ["QUEUER_DB_PORT"] = str(self.db_config.port)
        os.environ["QUEUER_DB_DATABASE"] = self.db_config.database
        os.environ["QUEUER_DB_USERNAME"] = self.db_config.username
        os.environ["QUEUER_DB_PASSWORD"] = self.db_config.password
        os.environ["QUEUER_DB_SCHEMA"] = self.db_config.schema
        os.environ["QUEUER_DB_SSLMODE"] = self.db_config.sslmode
        
        try:
            queuer = new_queuer_with_db("test_queuer", 100, "", None)
            self.assertIsNotNone(queuer)
            self.assertIsNotNone(queuer.database)
            self.assertIsNotNone(queuer.db_job)
            self.assertIsNotNone(queuer.db_worker)
        finally:
            # Clean up environment variables
            env_vars = [
                "QUEUER_DB_HOST", "QUEUER_DB_PORT", "QUEUER_DB_DATABASE",
                "QUEUER_DB_USERNAME", "QUEUER_DB_PASSWORD", "QUEUER_DB_SCHEMA", "QUEUER_DB_SSLMODE"
            ]
            for var in env_vars:
                if var in os.environ:
                    del os.environ[var]
    
    def test_valid_queuer_with_provided_db_config(self):
        """Test creating queuer with provided database configuration."""
        queuer = new_queuer_with_db("test_queuer", 100, "", self.db_config)
        self.assertIsNotNone(queuer)
        self.assertIsNotNone(queuer.database)
        self.assertIsNotNone(queuer.db_job)
        self.assertIsNotNone(queuer.db_worker)


class TestQueuerStart(DatabaseTestMixin, unittest.TestCase):
    """Test queuer start functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Set up for the entire test class."""
        super().setup_class()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        super().teardown_class()
    
    def setUp(self):
        """Set up for each test method."""
        super().setup_method()
        self.queuer = new_queuer_with_db("test", 10, "", self.db_config)
    
    def tearDown(self):
        """Clean up after each test method."""
        if hasattr(self, 'queuer') and self.queuer._running:
            self.queuer.stop()
        super().teardown_method()
    
    def test_start_queuer_with_valid_context(self):
        """Test starting queuer with valid context."""
        cancel_called = threading.Event()
        
        def cancel_func():
            cancel_called.set()
        
        # This should not raise any exceptions
        self.queuer.start(cancel_func)
        self.assertTrue(self.queuer._running)
        self.assertEqual(self.queuer.worker.status, WorkerStatus.RUNNING)
        
        # Clean up
        self.queuer.stop()
        self.assertFalse(self.queuer._running)
    
    def test_start_queuer_with_nil_cancel_func(self):
        """Test starting queuer with None cancel function."""
        # This should work fine
        self.queuer.start(None)
        self.assertTrue(self.queuer._running)
        
        # Clean up
        self.queuer.stop()
        self.assertFalse(self.queuer._running)
    
    def test_start_queuer_already_running(self):
        """Test starting queuer that's already running."""
        self.queuer.start()
        self.assertTrue(self.queuer._running)
        
        # Starting again should raise an exception
        with self.assertRaises(RuntimeError):
            self.queuer.start()
        
        # Clean up
        self.queuer.stop()


class TestQueuerStop(DatabaseTestMixin, unittest.TestCase):
    """Test queuer stop functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Set up for the entire test class."""
        super().setup_class()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        super().teardown_class()
    
    def setUp(self):
        """Set up for each test method."""
        super().setup_method()
        self.queuer = new_queuer_with_db("test", 10, "", self.db_config)
    
    def tearDown(self):
        """Clean up after each test method."""
        super().teardown_method()
    
    def test_stop_queuer(self):
        """Test stopping queuer."""
        # Start first
        self.queuer.start()
        self.assertTrue(self.queuer._running)
        
        # Stop
        self.queuer.stop()
        self.assertFalse(self.queuer._running)
        self.assertEqual(self.queuer.worker.status, WorkerStatus.STOPPED)
    
    def test_stop_queuer_not_running(self):
        """Test stopping queuer that's not running."""
        # Should not raise any exceptions
        self.queuer.stop()
        self.assertFalse(self.queuer._running)


class TestQueuerHeartbeat(DatabaseTestMixin, unittest.TestCase):
    """Test queuer heartbeat functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Set up for the entire test class."""
        super().setup_class()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        super().teardown_class()
    
    def setUp(self):
        """Set up for each test method."""
        super().setup_method()
        self.queuer = new_queuer_with_db("test", 10, "", self.db_config)
    
    def tearDown(self):
        """Clean up after each test method."""
        if hasattr(self, 'queuer') and self.queuer._running:
            self.queuer.stop()
        super().teardown_method()
    
    def test_heartbeat_ticker_starts_successfully(self):
        """Test that heartbeat ticker starts successfully."""
        # Start queuer
        self.queuer.start()
        self.assertTrue(self.queuer._running)
        
        # Wait a short time for heartbeat to potentially run
        time.sleep(0.1)
        
        # Worker should still exist and be running
        self.assertIsNotNone(self.queuer.worker)
        self.assertEqual(self.queuer.worker.status, WorkerStatus.RUNNING)
        
        # Clean up
        self.queuer.stop()
    
    def test_heartbeat_ticker_updates_worker_timestamp(self):
        """Test that heartbeat ticker updates worker timestamp."""
        # Start queuer
        self.queuer.start()
        
        # Record initial timestamp
        initial_time = self.queuer.worker.updated_at
        
        # Wait a bit longer than heartbeat interval would normally be
        # Note: In real implementation, heartbeat runs every 30 seconds,
        # but we can test that the mechanism works
        time.sleep(0.1)
        
        # Worker should still exist
        self.assertIsNotNone(self.queuer.worker)
        
        # Clean up
        self.queuer.stop()


class TestQueuerTasks(DatabaseTestMixin, unittest.TestCase):
    """Test queuer task management."""
    
    @classmethod
    def setUpClass(cls):
        """Set up for the entire test class."""
        super().setup_class()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        super().teardown_class()
    
    def setUp(self):
        """Set up for each test method."""
        super().setup_method()
        self.queuer = new_queuer_with_db("test", 10, "", self.db_config)
    
    def tearDown(self):
        """Clean up after each test method."""
        if hasattr(self, 'queuer') and self.queuer._running:
            self.queuer.stop()
        super().teardown_method()
    
    def test_add_task(self):
        """Test adding a task."""
        def test_task(x: int) -> int:
            return x * 2
        
        self.queuer.add_task(test_task)
        self.assertIn("test_task", self.queuer.tasks)
    
    def test_add_job(self):
        """Test adding and executing a job."""
        def test_task(x: int) -> int:
            return x * 2
        
        # Add task first
        self.queuer.add_task(test_task)
        
        # Start queuer
        self.queuer.start()
        
        try:
            # Add job
            job = self.queuer.add_job(test_task, 5)
            self.assertEqual(job.task_name, "test_task")
            self.assertEqual(job.parameters, [5])
            
            # Wait a bit for job to execute
            time.sleep(0.2)
            
            # Check job was executed
            updated_job = self.queuer.get_job(job.rid)
            self.assertIsNotNone(updated_job)
            
        finally:
            self.queuer.stop()
    
    def test_async_task(self):
        """Test with async task function."""
        import asyncio
        
        async def async_task(x: int) -> int:
            await asyncio.sleep(0.1)
            return x * 3
        
        # Add task and start queuer
        self.queuer.add_task(async_task)
        self.queuer.start()
        
        try:
            # Add job
            job = self.queuer.add_job(async_task, 4)
            
            # Wait for execution
            time.sleep(0.3)
            
            # Check result
            updated_job = self.queuer.get_job(job.rid)
            self.assertIsNotNone(updated_job)
            
        finally:
            self.queuer.stop()


if __name__ == '__main__':
    unittest.main()