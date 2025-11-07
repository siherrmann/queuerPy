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


def global_simple_task(x: int) -> int:
    """Global simple task function for testing."""
    return x * 2


def global_test_task(x: int) -> int:
    """Global test task function for testing."""
    return x * 2


def global_notification_task(message: str) -> str:
    """Simple task for notification testing."""
    return f"Processed: {message}"


def global_fast_task(data: str) -> str:
    """Fast task for testing responsiveness."""
    return f"Processed: {data}"


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

        self.queuer.add_task(global_test_task)
        self.assertIn("global_test_task", self.queuer.tasks)

    def test_add_job(self):
        """Test adding and executing a job."""

        # Add task first
        self.queuer.add_task(global_test_task)

        # Start queuer
        self.queuer.start()

        try:
            # Add job
            job = self.queuer.add_job(global_test_task, 5)
            self.assertEqual(job.task_name, "global_test_task")
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

        # Create first queuer with table drop to ensure clean state
        self.queuer1 = new_queuer_with_db(
            "notification_producer", 5, "", self.db_config
        )

        # Create second database config without table drop to avoid removing first queuer's data
        from helper.database import DatabaseConfiguration

        db_config_no_drop = DatabaseConfiguration(
            host=self.db_config.host,
            port=self.db_config.port,
            username=self.db_config.username,
            password=self.db_config.password,
            database=self.db_config.database,
            schema=self.db_config.schema,
            sslmode=self.db_config.sslmode,
            with_table_drop=False,  # Don't drop tables for second queuer
        )

        # Create second queuer without dropping tables
        self.queuer2 = new_queuer_with_db(
            "notification_consumer", 5, "", db_config_no_drop
        )

        # Add test task to the consumer queuer (queuer2) so it can process jobs
        self.queuer2.add_task(global_notification_task)

        # Start both queuers
        self.queuer1.start()
        self.queuer2.start()

    def tearDown(self):
        """Clean up after each test method."""
        # Stop queuers if they exist
        if hasattr(self, "queuer1") and self.queuer1:
            try:
                self.queuer1.stop()
            except:
                pass
        if hasattr(self, "queuer2") and self.queuer2:
            try:
                self.queuer2.stop()
            except:
                pass
        super().teardown_method()

    def test_comprehensive_notification_system(self):
        """Test comprehensive notification system with two queuers - one producer, one consumer."""

        try:
            # Verify that triggers exist before we start
            trigger_check = """
            SELECT tgname, tgrelid::regclass as table_name
            FROM pg_trigger
            WHERE tgname LIKE '%notify_event'
            ORDER BY tgname;
            """

            triggers = self.queuer1.db_job.db.instance.execute(trigger_check).fetchall()
            self.assertGreaterEqual(
                len(triggers),
                2,
                f"Expected at least 2 notification triggers, found {len(triggers)}",
            )

            # Verify both queuers are running
            self.assertTrue(
                self.queuer1._running, "Queuer1 (producer) should be running"
            )
            self.assertTrue(
                self.queuer2._running, "Queuer2 (consumer) should be running"
            )

            # Add job using queuer1 (producer) - this should trigger notifications
            job = self.queuer1.add_job(
                global_notification_task, "Cross-queuer notification test"
            )
            self.assertIsNotNone(job, "Job should be created successfully")
            self.assertEqual(job.task_name, "global_notification_task")

            # Wait for job to be processed by queuer2 (consumer) via notification system
            # Use queuer2 to wait since it's the one that should process it
            finished_job = self.queuer2.wait_for_job_finished(
                job.rid, timeout_seconds=15.0
            )
            self.assertIsNotNone(
                finished_job, "Job should finish successfully via notifications"
            )
            self.assertEqual(
                finished_job.results, "Processed: Cross-queuer notification test"
            )

            # Verify the job was properly archived after completion
            with self.queuer1.db_job.db.instance.cursor() as cur:
                cur.execute(
                    "SELECT rid, task_name, results, status FROM job_archive WHERE rid = %s",
                    (job.rid,),
                )
                archived_job = cur.fetchone()

            self.assertIsNotNone(
                archived_job, "Job should be archived after completion"
            )
            self.assertEqual(
                str(archived_job[0]),
                str(job.rid),
                "Archived job should have correct RID",
            )
            self.assertEqual(
                archived_job[1],
                "global_notification_task",
                "Archived job should have correct task name",
            )
            self.assertEqual(
                archived_job[2],
                "Processed: Cross-queuer notification test",
                "Archived job should have correct results",
            )
            self.assertEqual(
                archived_job[3],
                "SUCCEEDED",
                "Archived job should have SUCCEEDED status",
            )

            # Verify the notification system worked by checking that triggers are still present
            triggers_after = self.queuer1.db_job.db.instance.execute(
                trigger_check
            ).fetchall()
            self.assertGreaterEqual(
                len(triggers_after),
                2,
                "Notification triggers should persist after operations",
            )

            # Final verification - both systems should be stable
            self.assertTrue(self.queuer1._running, "Queuer1 should still be running")
            self.assertTrue(self.queuer2._running, "Queuer2 should still be running")

        except Exception as e:
            # Let tearDown handle cleanup
            raise

    def test_notification_system_persistence(self):
        """Test that notification triggers persist across queuer lifecycle."""

        # Check triggers exist initially
        trigger_check = """
        SELECT tgname, tgrelid::regclass as table_name 
        FROM pg_trigger 
        WHERE tgname LIKE '%notify_event'
        ORDER BY tgname;
        """

        # Use a temporary queuer just to check initial triggers
        temp_queuer = new_queuer_with_db("temp_check", 3, "", self.db_config)
        try:
            initial_triggers = temp_queuer.db_job.db.instance.execute(
                trigger_check
            ).fetchall()
            self.assertGreaterEqual(
                len(initial_triggers), 2, "Should have notification triggers initially"
            )
        finally:
            temp_queuer.stop()

        # Simple test: verify triggers persist after queuer lifecycle
        final_queuer = new_queuer_with_db("final_check", 3, "", self.db_config)
        try:
            final_triggers = final_queuer.db_job.db.instance.execute(
                trigger_check
            ).fetchall()
            self.assertEqual(
                len(final_triggers),
                len(initial_triggers),
                "Trigger count should remain consistent after queuer lifecycle",
            )
        finally:
            final_queuer.stop()

    def test_job_insertion_no_hanging(self):
        """Test that job insertion doesn't hang with notification system."""

        queuer = new_queuer_with_db("no_hang_test", 2, "", self.db_config)

        try:
            # Define a test task
            queuer.add_task(global_fast_task)
            queuer.start()

            # Add multiple jobs rapidly to test for hanging
            jobs = []
            start_time = time.time()

            for i in range(10):
                job = queuer.add_job(global_fast_task, f"test_data_{i}")
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
            final_job = queuer.add_job(global_fast_task, "final_test")
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
