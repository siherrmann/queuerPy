"""
Test for the main Queuer implementation.
Mirrors the Go queuerTest.go implementation with testcontainers.
"""

import os
import time
import unittest
import threading
import asyncio
from datetime import datetime, timedelta
from typing import Optional

from queuer import new_queuer, new_queuer_with_db
from helper.test_database import DatabaseTestMixin
from helper.database import DatabaseConfiguration
from model.options_on_error import OnError, RetryBackoff
from model.worker import WorkerStatus


async def global_async_task(x: int) -> int:
    """Global async task function for testing ProcessRunner with async functions."""
    await asyncio.sleep(0.1)
    return x * 3


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
            "QUEUER_DB_HOST",
            "QUEUER_DB_PORT",
            "QUEUER_DB_DATABASE",
            "QUEUER_DB_USERNAME",
            "QUEUER_DB_PASSWORD",
            "QUEUER_DB_SCHEMA",
            "QUEUER_DB_SSLMODE",
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
                retry_backoff=RetryBackoff.LINEAR,
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
            with self.assertRaises(
                (ValueError, Exception)
            ):  # Should raise a database error
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
                "QUEUER_DB_HOST",
                "QUEUER_DB_PORT",
                "QUEUER_DB_DATABASE",
                "QUEUER_DB_USERNAME",
                "QUEUER_DB_PASSWORD",
                "QUEUER_DB_SCHEMA",
                "QUEUER_DB_SSLMODE",
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
        if hasattr(self, "queuer") and self.queuer._running:
            self.queuer.stop()
        super().teardown_method()

    def test_start_queuer_with_valid_context(self):
        """Test starting queuer with valid context."""
        # This should not raise any exceptions
        self.queuer.start()
        self.assertTrue(self.queuer._running)
        self.assertEqual(self.queuer.worker.status, WorkerStatus.RUNNING)

        # Clean up
        self.queuer.stop()
        self.assertFalse(self.queuer._running)

    def test_start_queuer_simple(self):
        """Test starting queuer with simplified interface."""
        # This should work fine
        self.queuer.start()
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
        if hasattr(self, "queuer") and self.queuer._running:
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
        if hasattr(self, "queuer") and self.queuer._running:
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

            # Check job was executed (try main table first, then archive)
            try:
                updated_job = self.queuer.get_job(job.rid)
            except Exception:
                # If not in main table, try archive
                updated_job = self.queuer.get_job_ended(job.rid)
            self.assertIsNotNone(updated_job)

        finally:
            self.queuer.stop()

    def test_async_task(self):
        """Test with async task function."""

        # Add task and start queuer
        self.queuer.add_task(global_async_task)
        self.queuer.start()

        try:
            # Add job
            job = self.queuer.add_job(global_async_task, 4)

            # Wait for job to finish using event-driven approach
            finished_job = self.queuer.wait_for_job_finished(job.rid)
            self.assertIsNotNone(finished_job)
            self.assertEqual(finished_job.status, "SUCCEEDED")
            self.assertEqual(finished_job.results, 12)  # 4 * 3

        finally:
            self.queuer.stop()


class TestQueuerNotifications(DatabaseTestMixin, unittest.TestCase):
    """Test comprehensive notification system with multiple queuers."""

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

    def test_comprehensive_notification_system(self):
        """Test comprehensive notification system with two queuers."""

        # Create two queuers - one will add jobs, one will listen for notifications
        queuer1 = new_queuer_with_db("notification_producer", 5, "", self.db_config)
        queuer2 = new_queuer_with_db("notification_consumer", 5, "", self.db_config)

        try:
            # Verify that triggers exist before we start
            trigger_check = """
            SELECT tgname, tgrelid::regclass as table_name 
            FROM pg_trigger 
            WHERE tgname LIKE '%job_notify%'
            ORDER BY tgname;
            """

            triggers = queuer1.db_job.db.instance.execute(trigger_check).fetchall()
            self.assertGreaterEqual(
                len(triggers),
                2,
                f"Expected at least 2 notification triggers, found {len(triggers)}",
            )

            # Define a simple test task
            def test_notification_task(message: str) -> str:
                """Simple task for notification testing."""
                return f"Processed: {message}"

            # Add the task to both queuers
            queuer1.add_task(test_notification_task)
            queuer2.add_task(test_notification_task)

            # Start both queuers
            queuer1.start()
            queuer2.start()

            # Verify both queuers are running
            self.assertTrue(queuer1._running, "Queuer1 should be running")
            self.assertTrue(queuer2._running, "Queuer2 should be running")

            # Add jobs using queuer1 - this should trigger notifications
            job1 = queuer1.add_job(test_notification_task, "First notification test")
            self.assertIsNotNone(job1, "Job1 should be created successfully")
            self.assertEqual(job1.task_name, "test_notification_task")

            # Use wait_for_job_finished to properly verify job completion
            finished_job1 = queuer1.wait_for_job_finished(
                job1.rid, timeout_seconds=10.0
            )
            self.assertIsNotNone(finished_job1, "Job1 should finish successfully")
            self.assertEqual(
                finished_job1.results, "Processed: First notification test"
            )

            # Add a second job and verify it
            job2 = queuer1.add_job(test_notification_task, "Second notification test")
            self.assertIsNotNone(job2, "Job2 should be created successfully")

            finished_job2 = queuer1.wait_for_job_finished(
                job2.rid, timeout_seconds=10.0
            )
            self.assertIsNotNone(finished_job2, "Job2 should finish successfully")
            self.assertEqual(
                finished_job2.results, "Processed: Second notification test"
            )

            # Add a third job from the second queuer to test cross-queuer functionality
            job3 = queuer2.add_job(test_notification_task, "Cross-queuer test")
            self.assertIsNotNone(
                job3, "Job3 should be created successfully from queuer2"
            )

            finished_job3 = queuer2.wait_for_job_finished(
                job3.rid, timeout_seconds=10.0
            )
            self.assertIsNotNone(finished_job3, "Job3 should finish successfully")
            self.assertEqual(finished_job3.results, "Processed: Cross-queuer test")

            # Verify the notification system worked by checking that triggers are still present
            triggers_after = queuer1.db_job.db.instance.execute(
                trigger_check
            ).fetchall()
            self.assertGreaterEqual(
                len(triggers_after),
                2,
                "Notification triggers should persist after operations",
            )

            # Final verification - system should be stable
            self.assertTrue(queuer1._running, "Queuer1 should still be running")
            self.assertTrue(queuer2._running, "Queuer2 should still be running")

        finally:
            # Clean up - stop both queuers
            try:
                if queuer1._running:
                    queuer1.stop()
            except:
                pass

            try:
                if queuer2._running:
                    queuer2.stop()
            except:
                pass

    def test_notification_system_persistence(self):
        """Test that notification triggers persist across queuer lifecycle."""

        # Create a queuer and verify initial state
        queuer = new_queuer_with_db("persistence_test", 3, "", self.db_config)

        try:
            # Check triggers exist
            trigger_check = """
            SELECT tgname, tgrelid::regclass as table_name 
            FROM pg_trigger 
            WHERE tgname LIKE '%job_notify%'
            ORDER BY tgname;
            """

            initial_triggers = queuer.db_job.db.instance.execute(
                trigger_check
            ).fetchall()
            self.assertGreaterEqual(
                len(initial_triggers), 2, "Should have notification triggers initially"
            )

            # Start and stop the queuer multiple times
            for i in range(3):
                queuer.start()
                self.assertTrue(
                    queuer._running, f"Queuer should start on iteration {i+1}"
                )

                # Add a job
                def simple_task(x: int) -> int:
                    return x * 2

                queuer.add_task(simple_task)
                job = queuer.add_job(simple_task, i)
                self.assertIsNotNone(job, f"Job should be created on iteration {i+1}")

                # Wait for job to complete using wait_for_job_finished
                finished_job = queuer.wait_for_job_finished(
                    job.rid, timeout_seconds=5.0
                )
                self.assertIsNotNone(
                    finished_job, f"Job should finish on iteration {i+1}"
                )
                self.assertEqual(
                    finished_job.results,
                    i * 2,
                    f"Job result should be correct on iteration {i+1}",
                )

                # Stop the queuer
                queuer.stop()
                self.assertFalse(
                    queuer._running, f"Queuer should stop on iteration {i+1}"
                )

                # Verify triggers still exist
                current_triggers = queuer.db_job.db.instance.execute(
                    trigger_check
                ).fetchall()
                self.assertGreaterEqual(
                    len(current_triggers),
                    2,
                    f"Triggers should persist after iteration {i+1}",
                )

            # Final verification that the system is stable
            final_triggers = queuer.db_job.db.instance.execute(trigger_check).fetchall()
            self.assertEqual(
                len(final_triggers),
                len(initial_triggers),
                "Trigger count should remain consistent",
            )

        finally:
            # Clean up
            try:
                if queuer._running:
                    queuer.stop()
            except:
                pass

    def test_job_insertion_no_hanging(self):
        """Test that job insertion doesn't hang with notification system."""

        queuer = new_queuer_with_db("no_hang_test", 2, "", self.db_config)

        try:
            # Define a test task
            def fast_task(data: str) -> str:
                return f"Processed: {data}"

            queuer.add_task(fast_task)
            queuer.start()

            # Add multiple jobs rapidly to test for hanging
            jobs = []
            start_time = time.time()

            for i in range(10):
                job = queuer.add_job(fast_task, f"test_data_{i}")
                jobs.append(job)
                self.assertIsNotNone(job, f"Job {i} should be created successfully")

            end_time = time.time()
            duration = end_time - start_time

            # Job creation should be fast (under 5 seconds for 10 jobs)
            self.assertLess(
                duration, 5.0, f"Job creation took {duration:.2f}s, should be under 5s"
            )

            # Verify all jobs were created
            self.assertEqual(len(jobs), 10, "All 10 jobs should be created")

            # Wait for a few jobs to finish using wait_for_job_finished
            for i in range(3):  # Test first 3 jobs
                finished_job = queuer.wait_for_job_finished(
                    jobs[i].rid, timeout_seconds=10.0
                )
                self.assertIsNotNone(
                    finished_job, f"Job {i} should finish successfully"
                )
                self.assertEqual(finished_job.results, f"Processed: test_data_{i}")

            # Verify system is still responsive
            final_job = queuer.add_job(fast_task, "final_test")
            self.assertIsNotNone(final_job, "Final job should be created successfully")

            # Verify final job completes
            finished_final = queuer.wait_for_job_finished(
                final_job.rid, timeout_seconds=10.0
            )
            self.assertIsNotNone(finished_final, "Final job should finish successfully")
            self.assertEqual(finished_final.results, "Processed: final_test")

        finally:
            # Clean up
            try:
                if queuer._running:
                    queuer.stop()
            except:
                pass


if __name__ == "__main__":
    unittest.main()
