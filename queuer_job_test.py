"""
Test cases for the queuer job functionality.
Mirrors Go's queuerJob_test.go with testcontainers for end-to-end testing.
"""

import pytest
import time
import unittest
from uuid import uuid4, UUID

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queuer import new_queuer
from model.job import Job, JobStatus
from model.options import Options
from model.options_on_error import OnError, RetryBackoff
from helper.test_database import DatabaseTestMixin
from database.db_job import JobDBHandler
from database.db_worker import WorkerDBHandler


def task_mock(duration: int, param2: str) -> int:
    """Short running example task function."""
    # Simulate some work
    time.sleep(duration)

    # Example for some error handling
    try:
        param2_int = int(param2)
    except ValueError as e:
        raise e

    return duration + param2_int


class MockFailer:
    """Mock class that fails a certain number of times."""

    def __init__(self):
        self.count = 0

    def task_mock_failing(self, duration: int, max_fail_count: str) -> int:
        """Task that fails until max_fail_count is reached."""
        self.count += 1

        # Simulate some work
        time.sleep(duration)

        # Example for some error handling
        try:
            max_fail_count_int = int(max_fail_count)
        except ValueError as e:
            raise e

        if self.count < max_fail_count_int:
            raise Exception(f"fake fail max count reached: {max_fail_count_int}")

        return self.count


class TestQueuerJob(DatabaseTestMixin, unittest.TestCase):
    """Test cases for queuer job functionality with real database integration."""

    @classmethod
    def setUpClass(cls):
        """Set up for the entire test class."""
        super().setup_class()

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        super().teardown_class()

    def setUp(self):
        """Set up test fixtures."""
        super().setup_method()

        # Create queuer with real database handlers
        self.queuer = new_queuer("TestQueuer", 100)
        self.queuer.db_job = JobDBHandler(self.db, with_table_drop=True)
        self.queuer.db_worker = WorkerDBHandler(self.db, with_table_drop=True)

        # Re-insert the worker since we changed the database handlers
        self.queuer.worker = self.queuer.db_worker.insert_worker(self.queuer.worker)

        # Add test tasks
        self.queuer.add_task(task_mock)
        mock_failer = MockFailer()
        self.queuer.add_task(mock_failer.task_mock_failing)

    def tearDown(self):
        """Clean up after each test method."""
        super().teardown_method()

    def test_add_job_success_with_nil_options(self):
        """Test successfully adding a job with nil options."""
        # Add a job with real database
        job = self.queuer.add_job(task_mock, 1, "2")

        # Verify job was created and inserted
        self.assertIsNotNone(job)
        self.assertIsNotNone(job.rid)
        self.assertEqual(job.task_name, "task_mock")
        self.assertEqual(job.parameters, [1, "2"])
        self.assertEqual(job.status, JobStatus.QUEUED)

        # Verify job exists in database
        retrieved_job = self.queuer.db_job.select_job(job.rid)
        self.assertIsNotNone(retrieved_job)
        self.assertEqual(retrieved_job.rid, job.rid)

    def test_add_job_error_for_nil_function(self):
        """Test returning error for nil function."""
        # Test with None function
        with self.assertRaises(Exception) as context:
            self.queuer.add_job(None, "param1")

        self.assertIn("task must be callable", str(context.exception))

    def test_add_job_error_for_invalid_task_type(self):
        """Test returning error for invalid task type."""
        # Test with invalid integer type instead of function
        invalid_task = 123
        with self.assertRaises(Exception) as context:
            self.queuer.add_job(invalid_task, "param1")

        self.assertIn("task must be callable", str(context.exception))

    def test_add_job_with_options_success(self):
        """Test successfully adding a job with options."""
        options = Options(
            on_error=OnError(
                timeout=5.0,
                max_retries=3,
                retry_delay=1.0,
                retry_backoff=RetryBackoff.EXPONENTIAL,
            )
        )

        # Add job with options
        job = self.queuer.add_job_with_options(options, task_mock, 1, "2")

        # Verify job was created with options
        self.assertIsNotNone(job)
        self.assertIsNotNone(job.options)
        self.assertEqual(job.task_name, "task_mock")
        self.assertEqual(job.parameters, [1, "2"])
        self.assertEqual(job.options.on_error.timeout, 5.0)
        self.assertEqual(job.options.on_error.max_retries, 3)
        self.assertEqual(job.options.on_error.retry_delay, 1.0)
        self.assertEqual(job.options.on_error.retry_backoff, RetryBackoff.EXPONENTIAL)

        # Verify job exists in database
        retrieved_job = self.queuer.db_job.select_job(job.rid)
        self.assertIsNotNone(retrieved_job)
        self.assertEqual(retrieved_job.rid, job.rid)

    def test_add_job_with_nil_options_success(self):
        """Test successfully adding a job with nil options."""
        # Add job with None options
        job = self.queuer.add_job_with_options(None, task_mock, 1, "2")

        # Verify job was created
        self.assertIsNotNone(job)
        self.assertEqual(job.task_name, "task_mock")
        self.assertEqual(job.parameters, [1, "2"])

        # Verify job exists in database
        retrieved_job = self.queuer.db_job.select_job(job.rid)
        self.assertIsNotNone(retrieved_job)
        self.assertEqual(retrieved_job.rid, job.rid)

    def test_add_job_with_invalid_options_error(self):
        """Test returning error for invalid options."""
        # Should raise error for invalid options during OnError creation
        with self.assertRaises(ValueError) as context:
            options = Options(
                on_error=OnError(
                    timeout=-1.0,  # Invalid negative timeout
                    max_retries=3,
                    retry_delay=1.0,
                    retry_backoff=RetryBackoff.LINEAR,
                )
            )

        self.assertIn("timeout cannot be negative", str(context.exception))

    def test_get_job_success(self):
        """Test retrieving a job by RID."""
        # Create a real job in the database
        job = self.queuer.add_job(task_mock, 1, "2")
        self.assertIsNotNone(job)

        # Retrieve the job
        result = self.queuer.get_job(job.rid)

        # Verify we got the correct job
        self.assertEqual(result.rid, job.rid)
        self.assertEqual(result.task_name, "task_mock")
        self.assertEqual(result.parameters, [1, "2"])

    def test_get_job_not_found_error(self):
        """Test retrieving a job that doesn't exist."""
        job_rid = uuid4()

        # Try to get a job that doesn't exist
        with self.assertRaises(Exception) as context:
            self.queuer.get_job(job_rid)

        self.assertIn("Job not found", str(context.exception))

    def test_get_jobs_success(self):
        """Test retrieving all jobs."""
        # Create multiple jobs in the database
        job1 = self.queuer.add_job(task_mock, 1, "2")
        job2 = self.queuer.add_job(task_mock, 3, "4")

        # Get all jobs
        jobs = self.queuer.get_jobs()

        # Verify we got both jobs
        self.assertGreaterEqual(len(jobs), 2)
        job_rids = [job.rid for job in jobs]
        self.assertIn(job1.rid, job_rids)
        self.assertIn(job2.rid, job_rids)

    def test_get_jobs_by_worker_rid_success(self):
        """Test retrieving jobs by worker RID."""
        # Create jobs in the database
        job1 = self.queuer.add_job(task_mock, 1, "2")
        job2 = self.queuer.add_job(task_mock, 3, "4")

        # Assign jobs to worker through update_jobs_initial (simulating job pickup)
        picked_jobs = self.queuer.db_job.update_jobs_initial(self.queuer.worker)
        self.assertGreaterEqual(len(picked_jobs), 2)

        # Get jobs for this worker
        jobs = self.queuer.get_jobs_by_worker_rid(self.queuer.worker.rid)

        # Verify we got the jobs for this worker
        self.assertGreaterEqual(len(jobs), 2)
        job_rids = [job.rid for job in jobs]
        self.assertIn(job1.rid, job_rids)
        self.assertIn(job2.rid, job_rids)

        # Verify all jobs belong to the correct worker
        for job in jobs:
            self.assertEqual(job.worker_rid, self.queuer.worker.rid)

    def test_cancel_job_success(self):
        """Test cancelling a job."""
        # Create a real job in the database
        job = self.queuer.add_job(task_mock, 1, "2")
        self.assertIsNotNone(job)

        # Cancel the job
        cancelled_job = self.queuer.cancel_job(job.rid)

        # Verify job was cancelled
        self.assertIsNotNone(cancelled_job)
        self.assertEqual(cancelled_job.rid, job.rid)
        self.assertEqual(cancelled_job.status, JobStatus.CANCELLED)

    def test_cancel_job_not_found_error(self):
        """Test cancelling a job that doesn't exist."""
        job_rid = uuid4()

        # Try to cancel a job that doesn't exist
        with self.assertRaises(Exception) as context:
            self.queuer.cancel_job(job_rid)

        self.assertIn("Job not found", str(context.exception))

    def test_readd_job_from_archive_success(self):
        """Test readding a job from archive."""
        # Create and cancel a job to get it in archive
        job = self.queuer.add_job(task_mock, 1, "2")
        cancelled_job = self.queuer.cancel_job(job.rid)

        # Readd the job from archive using the task function, not task name
        # Get the task function from the tasks registry
        task_function = self.queuer.tasks[cancelled_job.task_name].task
        new_job = self.queuer.add_job_with_options(
            cancelled_job.options, task_function, *cancelled_job.parameters
        )

        # Verify new job was created
        self.assertIsNotNone(new_job)
        self.assertNotEqual(new_job.rid, cancelled_job.rid)  # Should have new RID
        self.assertEqual(new_job.task_name, "task_mock")
        self.assertEqual(new_job.parameters, [1, "2"])
        self.assertEqual(new_job.status, JobStatus.QUEUED)

    def test_readd_job_from_archive_not_found_error(self):
        """Test readding a job that doesn't exist in archive."""
        job_rid = uuid4()

        # Try to readd a job that doesn't exist in archive
        with self.assertRaises(Exception) as context:
            self.queuer.readd_job_from_archive(job_rid)

        self.assertIn("Job not found in archive", str(context.exception))


# Test cases that require running queuer (integration tests)
class TestQueuerJobRunning(DatabaseTestMixin, unittest.TestCase):
    """Test cases for queuer job functionality with running queuer."""

    @classmethod
    def setUpClass(cls):
        """Set up for the entire test class."""
        super().setup_class()

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        super().teardown_class()

    def setUp(self):
        """Set up test fixtures."""
        super().setup_method()

        # Create queuer with real database handlers
        self.queuer = new_queuer("TestQueuerRunning", 100)
        self.queuer.db_job = JobDBHandler(self.db, with_table_drop=True)
        self.queuer.db_worker = WorkerDBHandler(self.db, with_table_drop=True)

        # Re-insert the worker since we changed the database handlers
        self.queuer.worker = self.queuer.db_worker.insert_worker(self.queuer.worker)

        # Add test tasks
        self.queuer.add_task(task_mock)

        # Start the queuer
        self.queuer.start()

    def tearDown(self):
        """Clean up after each test method."""
        if hasattr(self, "queuer"):
            self.queuer.stop()
        super().teardown_method()

    def test_job_execution_success(self):
        """Test successful job execution using wait_for_job_finished."""
        # Add a job
        job = self.queuer.add_job(task_mock, 1, "2")
        self.assertIsNotNone(job)

        # Wait for job to finish using the new method with timeout
        print(f"Waiting for job to finish: {job.rid}")
        finished_job = self.queuer.wait_for_job_finished(
            job.rid,
            timeout_seconds=15.0,  # Increased timeout for better reliability under load
        )
        print(f"Finished job: {finished_job}")

        # Verify job finished successfully
        self.assertIsNotNone(
            finished_job, "wait_for_job_finished should return the finished job"
        )
        self.assertEqual(finished_job.status, JobStatus.SUCCEEDED)
        self.assertEqual(finished_job.results, 3)  # 1 + 2 = 3

        # Verify job is no longer in main table (moved to archive)
        with self.assertRaises(Exception):
            self.queuer.get_job(job.rid)


if __name__ == "__main__":
    unittest.main()
