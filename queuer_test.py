"""
Comprehensive test for the main Queuer implementation.
Based on the Go queuerTest.go implementation.
"""

import threading
import time
from typing import List
import unittest

from .core.runner import SmallRunner, go_func
from .helper.logging import get_logger
from .model.batch_job import BatchJob
from .model.job import Job
from .queuer import new_queuer_with_db
from .helper.database import DatabaseConfiguration
from .helper.error import QueuerError
from .helper.test_database import DatabaseTestMixin
from .model.options_on_error import OnError, RetryBackoff
from .model.worker import WorkerStatus

logger = get_logger(__name__)


def global_simple_task(x: int) -> int:
    """Global simple task function for testing."""
    return x * 2


def global_test_task(message: str) -> str:
    """Global test task function for testing."""
    return f"Processed: {message}"


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

    def test_valid_queuer(self):
        """Test creating a valid queuer."""
        queuer = new_queuer_with_db("test_valid", 100, "", self.db_config)

        self.assertIsNotNone(queuer, "Expected Queuer to be created successfully")
        self.assertEqual(
            queuer.name,
            "test_valid",
            "Expected Queuer name to be test_valid",
        )
        self.assertEqual(
            100, queuer.max_concurrency, "Expected Queuer max concurrency to match"
        )
        self.assertIsNotNone(queuer.worker, "Expected worker to be initialized")
        self.assertEqual(
            queuer.worker.name,
            "test_valid",
            "Expected worker name to be test_valid",
        )
        self.assertEqual(
            100,
            queuer.worker.max_concurrency,
            "Expected worker max concurrency to match",
        )

    def test_valid_queuer_with_options(self):
        """Test creating a valid queuer with error handling options."""
        options = OnError(
            timeout=10.0,
            max_retries=3,
            retry_delay=1.0,
            retry_backoff=RetryBackoff.LINEAR,
        )

        queuer = new_queuer_with_db(
            "test_with_options", 100, "", self.db_config, options
        )

        self.assertIsNotNone(queuer, "Expected Queuer to be created successfully")
        self.assertEqual(
            queuer.name,
            "test_with_options",
            "Expected Queuer name to be test_with_options",
        )
        self.assertEqual(100, queuer.max_concurrency)
        self.assertIsNotNone(queuer.worker)

        # Check that worker has the options applied
        self.assertIsNotNone(queuer.worker.options)
        if queuer.worker.options:
            self.assertEqual(10.0, queuer.worker.options.timeout)
            self.assertEqual(3, queuer.worker.options.max_retries)
            self.assertEqual(1.0, queuer.worker.options.retry_delay)
            self.assertEqual(RetryBackoff.LINEAR, queuer.worker.options.retry_backoff)

    def test_invalid_max_concurrency(self):
        """Test that invalid max concurrency raises error."""
        with self.assertRaises((ValueError, RuntimeError)):
            new_queuer_with_db("test_invalid", -1, "", self.db_config)

    def test_invalid_options(self):
        """Test that invalid options raise error."""
        # In Python, the validation happens when creating OnError object
        with self.assertRaises(ValueError):
            OnError(
                timeout=-10.0,  # Invalid timeout value
                max_retries=3,
                retry_delay=1.0,
                retry_backoff=RetryBackoff.LINEAR,
            )

    def test_invalid_database_config(self):
        """Test that invalid database configuration raises error."""
        invalid_config = DatabaseConfiguration(
            host="",  # Invalid empty host
            port=5432,
            username="user",
            password="password",
            database="database",
            schema="public",
            sslmode="disable",
            with_table_drop=True,
        )

        with self.assertRaises(
            (ValueError, RuntimeError, ConnectionError, QueuerError)
        ):
            new_queuer_with_db("test_invalid_db", 100, "", invalid_config)


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

    def test_start_queuer_basic(self):
        """Test starting queuer with basic configuration."""
        queuer = new_queuer_with_db("test_start", 10, "", self.db_config)
        self.assertIsNotNone(queuer, "Expected Queuer to be created successfully")

        try:
            # Start should not raise an exception
            queuer.start()
            self.assertTrue(queuer.running, "Expected queuer to be running after start")

            # Worker status should be RUNNING
            self.assertEqual(WorkerStatus.RUNNING, queuer.worker.status)

        finally:
            if queuer.running:
                queuer.stop()

    def test_start_queuer_already_running(self):
        """Test that starting an already running queuer raises error."""
        queuer = new_queuer_with_db("test_start_twice", 10, "", self.db_config)

        try:
            queuer.start()
            self.assertTrue(queuer.running)

            # Starting again should raise RuntimeError
            with self.assertRaises(RuntimeError):
                queuer.start()
        finally:
            if queuer.running:
                queuer.stop()


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

    def test_stop_queuer(self):
        """Test stopping queuer."""
        queuer = new_queuer_with_db("test_stop", 10, "", self.db_config)

        # Stop should work even if not started
        queuer.stop()  # Should not raise error

        # Start and then stop
        queuer.start()
        self.assertTrue(queuer.running)

        queuer.stop()
        self.assertFalse(queuer.running, "Expected queuer to not be running after stop")

    def test_stop_not_running_queuer(self):
        """Test stopping a queuer that's not running."""
        queuer = new_queuer_with_db("test_stop_not_running", 10, "", self.db_config)

        # Stop should work even if not started - should not raise error
        queuer.stop()
        self.assertFalse(queuer.running)


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

    def test_heartbeat_ticker_starts(self):
        """Test that heartbeat ticker starts successfully."""
        queuer = new_queuer_with_db("test_heartbeat", 10, "", self.db_config)

        try:
            queuer.start()
            self.assertTrue(queuer.running)
            self.assertIsNotNone(queuer.heartbeat_ticker)
            if queuer.heartbeat_ticker:
                self.assertTrue(queuer.heartbeat_ticker.is_running())

            time.sleep(0.5)
            self.assertIsNotNone(queuer.worker)
        finally:
            if queuer.running:
                queuer.stop()

    def test_heartbeat_ticker_stops(self):
        """Test that heartbeat ticker stops when queuer stops."""
        queuer = new_queuer_with_db("test_heartbeat_stop", 10, "", self.db_config)

        queuer.start()
        self.assertTrue(queuer.running)
        self.assertIsNotNone(queuer.heartbeat_ticker)
        if queuer.heartbeat_ticker:
            self.assertTrue(queuer.heartbeat_ticker.is_running())

        queuer.stop()
        self.assertFalse(queuer.running)

        if queuer.heartbeat_ticker:
            self.assertFalse(
                queuer.heartbeat_ticker.is_running(),
                "Expected heartbeat ticker to be stopped",
            )


class TestQueuerTasks(DatabaseTestMixin, unittest.TestCase):
    """Test queuer task management functionality."""

    @classmethod
    def setUpClass(cls):
        """Set up for the entire test class."""
        super().setup_class()

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        super().teardown_class()

    def test_add_task(self):
        """Test adding tasks to queuer."""
        queuer = new_queuer_with_db("test_add_task", 10, "", self.db_config)

        try:
            queuer.add_task(global_simple_task)
            self.assertIn("global_simple_task", queuer.tasks)

            task_obj = queuer.tasks["global_simple_task"]
            self.assertEqual(global_simple_task, task_obj.task)
        finally:
            queuer.stop()

    def test_add_multiple_tasks(self):
        """Test adding multiple tasks to queuer."""
        queuer = new_queuer_with_db("test_multiple_tasks", 10, "", self.db_config)

        try:
            queuer.add_task(global_simple_task)
            queuer.add_task(global_test_task)

            self.assertIn("global_simple_task", queuer.tasks)
            self.assertIn("global_test_task", queuer.tasks)
            self.assertEqual(2, len(queuer.tasks))
        finally:
            queuer.stop()

    def test_add_job_basic(self):
        """Test adding a basic job."""
        queuer = new_queuer_with_db("test_add_job", 10, "", self.db_config)

        try:
            queuer.add_task(global_simple_task)

            job = queuer.add_job(global_simple_task, 42)
            self.assertIsNotNone(job, "Expected job to be created")
            self.assertEqual("global_simple_task", job.task_name)
            self.assertIsNotNone(job.rid, "Expected job to have a RID")

            job = queuer.get_job(job.rid)
            self.assertIsNotNone(job, "Expected job to be found in database")
        finally:
            queuer.stop()

    def test_add_job_without_task_registration(self):
        """Test adding job without registering task."""
        queuer = new_queuer_with_db("test_job_no_task", 10, "", self.db_config)

        try:
            job = queuer.add_job("global_simple_task", 42)
            self.assertIsNotNone(job)
            self.assertEqual("global_simple_task", job.task_name)
            logger.debug(
                f"job with non existing task inserted: {job.rid if job else 'None'}"
            )
        finally:
            queuer.stop()

    def test_job_creation_performance(self):
        """Test that job creation is fast and doesn't hang."""
        queuer = new_queuer_with_db("test_job_performance", 10, "", self.db_config)

        try:
            queuer.add_task(global_test_task)

            jobs: List[Job] = []
            start_time = time.time()

            for i in range(5):
                job = queuer.add_job(global_test_task, f"test_data_{i}")
                jobs.append(job)
                self.assertIsNotNone(job, f"Job {i} should be created successfully")

            end_time = time.time()
            duration = end_time - start_time

            job_ids = [job.rid for job in jobs]
            self.assertLess(
                duration, 2.0, f"Job creation took {duration:.3f}s, should be under 2s"
            )
            self.assertEqual(len(jobs), 5, "All 5 jobs should be created")
            self.assertEqual(
                len(job_ids), len(set(job_ids)), "All job IDs should be unique"
            )
        finally:
            queuer.stop()


class TestQueuerNotifications(DatabaseTestMixin, unittest.TestCase):
    """Test queuer notification system and rapid job insertion."""

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

    def test_rapid_job_insertion_no_hanging(self):
        """Test that rapid job insertion doesn't hang with notification system."""
        queuer = new_queuer_with_db("test_no_hang", 5, "", self.db_config)
        listener_runner: SmallRunner = SmallRunner(queuer.listen_for_job_insert)

        try:
            queuer.add_task(global_simple_task)
            queuer.start()

            jobs: List[Job] = []
            jobs_lock = threading.Lock()

            def on_job_inserted(job: Job):
                logger.debug(f"Received job notification: {job.rid if job else 'None'}")
                with jobs_lock:
                    jobs.append(job)

            runner = go_func(
                queuer.listen_for_job_insert,
                use_mp=False,
                notify_function=on_job_inserted,
            )
            if runner and isinstance(runner, SmallRunner):
                listener_runner: SmallRunner = runner

            # Give the listener a moment to start up
            time.sleep(0.5)

            batchJobs = [BatchJob(global_simple_task, [i * 2]) for i in range(10)]
            queuer.add_jobs(batchJobs)

            final_job = queuer.add_job(global_simple_task, 999)
            self.assertIsNotNone(final_job, "Final job should be created successfully")

            time.sleep(2.0)

            with jobs_lock:
                jobs_received = len(jobs)

            self.assertGreater(
                jobs_received,
                0,
                f"Should have received at least some job notifications, got {jobs_received}",
            )

            jobs = queuer.get_jobs(0, 100)
            jobs_ended = queuer.get_jobs_ended(0, 100)
            self.assertGreaterEqual(
                len(jobs) + len(jobs_ended),
                9,
                f"Should have at least 9 jobs in database, found {len(jobs) + len(jobs_ended)}",
            )
        finally:
            listener_runner.get_results()
            queuer.stop()


if __name__ == "__main__":
    unittest.main()
