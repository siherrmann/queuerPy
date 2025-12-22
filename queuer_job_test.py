"""
Test cases for the queuer job functionality.
Mirrors Go's queuerJob_test.go with testcontainers for end-to-end testing.
"""

from typing import List
import pytest
import time
import unittest
from uuid import uuid4

from .queuer import Queuer, new_queuer_with_db
from .model.job import Job, JobStatus
from .model.options import Options
from .model.options_on_error import OnError, RetryBackoff
from .helper.test_database import DatabaseTestMixin


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


def task_with_kwargs(a: int, b: str = "default", c: int = 0) -> dict:
    """Task function that accepts both positional and keyword arguments."""
    return {"a": a, "b": b, "c": c}


def task_kwargs_only(**kwargs) -> dict:
    """Task function that accepts only keyword arguments."""
    return kwargs


class MockFailer:
    """Mock class that fails a certain number of times."""

    def __init__(self):
        self.count = 0

    def task_mock_failing(self, duration: int, max_fail_count: str) -> int:
        """Task that fails until max_fail_count is reached."""
        self.count += 1

        time.sleep(duration)

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

    def tearDown(self):
        """Clean up after each test method."""
        super().tearDown()

    def test_add_job_success_with_nil_options(self):
        """Test successfully adding a job with nil options."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_mock)

        # Add a job with real database
        job = queuer.add_job(task_mock, 1, "2")

        # Verify job was created and inserted
        self.assertIsNotNone(job)
        self.assertIsNotNone(job.rid)
        self.assertEqual(job.task_name, "task_mock")
        self.assertEqual(job.parameters, [1, "2"])
        self.assertEqual(job.status, JobStatus.QUEUED)

        # Verify job exists in database
        retrieved_job = queuer.db_job.select_job(job.rid)
        self.assertIsNotNone(retrieved_job)
        if retrieved_job:
            self.assertEqual(retrieved_job.rid, job.rid)

        queuer.stop()

    def test_add_job_with_options_success(self):
        """Test successfully adding a job with options."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_mock)

        options = Options(
            on_error=OnError(
                timeout=5.0,
                max_retries=3,
                retry_delay=1.0,
                retry_backoff=RetryBackoff.EXPONENTIAL,
            )
        )
        job = queuer.add_job_with_options(options, task_mock, 1, "2")

        # Verify job was created with options
        self.assertIsNotNone(job)
        self.assertIsNotNone(job.options)
        self.assertEqual(job.task_name, "task_mock")
        self.assertEqual(job.parameters, [1, "2"])
        if job.options and job.options.on_error:
            self.assertEqual(job.options.on_error.timeout, 5.0)
            self.assertEqual(job.options.on_error.max_retries, 3)
            self.assertEqual(job.options.on_error.retry_delay, 1.0)
            self.assertEqual(
                job.options.on_error.retry_backoff, RetryBackoff.EXPONENTIAL
            )

        # Verify job exists in database
        retrieved_job = queuer.db_job.select_job(job.rid)
        self.assertIsNotNone(retrieved_job)
        if retrieved_job:
            self.assertEqual(retrieved_job.rid, job.rid)

        queuer.stop()

    def test_add_job_with_nil_options_success(self):
        """Test successfully adding a job with nil options."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_mock)

        job = queuer.add_job_with_options(None, task_mock, 1, "2")
        self.assertIsNotNone(job)
        self.assertEqual(job.task_name, "task_mock")
        self.assertEqual(job.parameters, [1, "2"])

        retrieved_job = queuer.db_job.select_job(job.rid)
        self.assertIsNotNone(retrieved_job)
        if retrieved_job:
            self.assertEqual(retrieved_job.rid, job.rid)

        queuer.stop()

    def test_add_job_only_positional_args_no_options(self):
        """Test add_job with only positional arguments and no options."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_with_kwargs)

        job = queuer.add_job(task_with_kwargs, 42)

        self.assertIsNotNone(job)
        self.assertEqual(job.task_name, "task_with_kwargs")
        self.assertEqual(job.parameters, [42])
        self.assertEqual(job.parameters_keyed, {})
        self.assertIsNone(job.options)

        queuer.stop()

    def test_add_job_only_kwargs_no_options(self):
        """Test add_job with only keyword arguments and no options."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_kwargs_only)

        job = queuer.add_job(task_kwargs_only, name="test", value=42, flag=True)

        self.assertIsNotNone(job)
        self.assertEqual(job.task_name, "task_kwargs_only")
        self.assertEqual(job.parameters, [])
        self.assertEqual(
            job.parameters_keyed, {"name": "test", "value": 42, "flag": True}
        )
        self.assertIsNone(job.options)

        queuer.stop()

    def test_add_job_both_args_and_kwargs_no_options(self):
        """Test add_job with both positional and keyword arguments, no options."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_with_kwargs)

        job = queuer.add_job(task_with_kwargs, 42, b="custom", c=100)

        self.assertIsNotNone(job)
        self.assertEqual(job.task_name, "task_with_kwargs")
        self.assertEqual(job.parameters, [42])
        self.assertEqual(job.parameters_keyed, {"b": "custom", "c": 100})
        self.assertIsNone(job.options)

        queuer.stop()

    def test_add_job_with_options_only_positional(self):
        """Test add_job_with_options with only positional arguments."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_with_kwargs)

        options = Options(on_error=OnError(retry_delay=2.0))
        job = queuer.add_job_with_options(options, task_with_kwargs, 42)

        self.assertIsNotNone(job)
        self.assertEqual(job.parameters, [42])
        self.assertEqual(job.parameters_keyed, {})
        self.assertIsNotNone(job.options)

        queuer.stop()

    def test_add_job_with_options_only_kwargs(self):
        """Test add_job_with_options with only keyword arguments."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_kwargs_only)

        options = Options(on_error=OnError(retry_delay=2.0))
        job = queuer.add_job_with_options(
            options, task_kwargs_only, name="test", value=42
        )

        self.assertIsNotNone(job)
        self.assertEqual(job.parameters, [])
        self.assertEqual(job.parameters_keyed, {"name": "test", "value": 42})

        queuer.stop()

    def test_add_job_with_options_both_args_and_kwargs(self):
        """Test add_job_with_options with both positional and keyword arguments."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_with_kwargs)

        options = Options(on_error=OnError(max_retries=10))
        job = queuer.add_job_with_options(options, task_with_kwargs, 42, b="test", c=99)

        self.assertIsNotNone(job)
        self.assertEqual(job.parameters, [42])
        self.assertEqual(job.parameters_keyed, {"b": "test", "c": 99})
        self.assertIsNotNone(job.options)

        queuer.stop()

    def test_get_job_success(self):
        """Test retrieving a job by RID."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_mock)

        job = queuer.add_job(task_mock, 1, "2")
        self.assertIsNotNone(job)

        result = queuer.get_job(job.rid)
        self.assertEqual(result.rid, job.rid)
        self.assertEqual(result.task_name, "task_mock")
        self.assertEqual(result.parameters, [1, "2"])

        queuer.stop()

    def test_get_job_not_found_error(self):
        """Test retrieving a job that doesn't exist."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_mock)

        job_rid = uuid4()

        # Try to get a job that doesn't exist
        with self.assertRaises(Exception) as context:
            queuer.get_job(job_rid)
        self.assertIn("Job not found", str(context.exception))

        queuer.stop()

    def test_get_jobs_success(self):
        """Test retrieving all jobs."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_mock)

        job1 = queuer.add_job(task_mock, 1, "2")
        job2 = queuer.add_job(task_mock, 3, "4")

        # Get all jobs
        jobs = queuer.get_jobs()
        self.assertGreaterEqual(len(jobs), 2)
        job_rids = [job.rid for job in jobs]
        self.assertIn(job1.rid, job_rids)
        self.assertIn(job2.rid, job_rids)

        queuer.stop()

    def test_get_jobs_by_worker_rid_success(self):
        """Test retrieving jobs by worker RID."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_mock)

        # Clean job db
        jobs = queuer.get_jobs(0, 100)
        for job in jobs:
            queuer.db_job.delete_job(job.rid)

        # Add new jobs
        job1 = queuer.add_job(task_mock, 1, "2")
        job2 = queuer.add_job(task_mock, 3, "4")

        jobs = queuer.db_job.update_jobs_initial(queuer.worker)
        self.assertEqual(len(jobs), 2)

        jobs = queuer.get_jobs_by_worker_rid(queuer.worker.rid)
        self.assertGreaterEqual(len(jobs), 2)
        job_rids = [job.rid for job in jobs]
        self.assertIn(job1.rid, job_rids)
        self.assertIn(job2.rid, job_rids)
        for job in jobs:
            self.assertEqual(job.worker_rid, queuer.worker.rid)

        queuer.stop()

    def test_cancel_job_success(self):
        """Test cancelling a job."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_mock)

        job = queuer.add_job(task_mock, 10, "2")
        self.assertIsNotNone(job)

        cancelled_job = queuer.cancel_job(job.rid)
        self.assertIsNotNone(cancelled_job)
        self.assertEqual(cancelled_job.rid, job.rid)
        self.assertEqual(cancelled_job.status, JobStatus.CANCELLED)

        time.sleep(0.5)  # Small delay to ensure DB update

        job_ended = queuer.get_job_ended(job.rid)
        self.assertIsNotNone(job_ended, "Cancelled job should be moved to archive")
        self.assertEqual(job_ended.status, JobStatus.CANCELLED)
        self.assertEqual(len(job_ended.results), 0)

        queuer.stop()

    def test_cancel_job_not_found_error(self):
        """Test cancelling a job that doesn't exist."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_mock)

        job_rid = uuid4()

        # Try to cancel a job that doesn't exist
        with self.assertRaises(Exception) as context:
            queuer.cancel_job(job_rid)
        self.assertIn("Job not found", str(context.exception))

        queuer.stop()

    def test_readd_job_from_archive_success(self):
        """Test readding a job from archive."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_mock)

        job = queuer.add_job(task_mock, 1, "2")

        cancelled_job = queuer.cancel_job(job.rid)
        new_job = queuer.readd_job_from_archive(cancelled_job.rid)

        # Verify new job was created
        self.assertIsNotNone(new_job)
        self.assertNotEqual(new_job.rid, cancelled_job.rid)  # Should have new RID
        self.assertEqual(new_job.task_name, "task_mock")
        self.assertEqual(new_job.parameters, [1, "2"])
        self.assertEqual(new_job.status, JobStatus.QUEUED)

        queuer.stop()

    def test_readd_job_from_archive_not_found_error(self):
        """Test readding a job that doesn't exist in archive."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_mock)

        job_rid = uuid4()

        # Try to readd a job that doesn't exist in archive
        with self.assertRaises(Exception) as context:
            queuer.readd_job_from_archive(job_rid)
        self.assertIn("Job not found in archive", str(context.exception))

        queuer.stop()

    def test_select_all_jobs_from_archive(self):
        """Test selecting all jobs from archive with pagination."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_mock)

        archived_jobs: List[Job] = []
        for i in range(3):
            job = Job(
                task_name=f"archive_test_task_{i}",
                parameters=[f"param_{i}"],
                status=JobStatus.RUNNING,
            )
            inserted_job = queuer.db_job.insert_job(job)

            # Update each job with different final status
            if i == 0:
                inserted_job.status = "SUCCEEDED"
                inserted_job.results = [{"success": True}]
            elif i == 1:
                inserted_job.status = "FAILED"
                inserted_job.error = f"Error in job {i}"
            else:
                inserted_job.status = "CANCELLED"

            archived_job = queuer.db_job.update_job_final(inserted_job)
            archived_jobs.append(archived_job)

        jobs_ended = queuer.db_job.select_all_jobs_from_archive(entries=10)
        self.assertGreaterEqual(len(jobs_ended), 3)

        # Verify the jobs have the correct statuses and error information
        succeeded_job = None
        failed_job = None
        cancelled_job = None

        for job in jobs_ended:
            if job.status == "SUCCEEDED":
                succeeded_job = job
            elif job.status == "FAILED":
                failed_job = job
            elif job.status == "CANCELLED":
                cancelled_job = job

        self.assertIsNotNone(succeeded_job, "Should have found succeeded job")
        self.assertIsNotNone(failed_job, "Should have found failed job")
        self.assertIsNotNone(cancelled_job, "Should have found cancelled job")

        # Verify error information is preserved
        if failed_job:
            self.assertIsNotNone(failed_job.error)
            self.assertIn("Error in job", failed_job.error if failed_job.error else "")

        queuer.stop()


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

    def tearDown(self):
        """Clean up after each test method."""
        super().tearDown()

    def test_job_execution_success(self):
        """Test successful job execution using wait_for_job_finished."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_mock)
        queuer.start()

        job = queuer.add_job(task_mock, 1, "2")
        self.assertIsNotNone(job)

        finished_job = queuer.wait_for_job_finished(job.rid, timeout_seconds=15.0)
        if finished_job:
            self.assertEqual(finished_job.status, "SUCCEEDED")
            self.assertEqual(finished_job.results, [3])
            self.assertEqual(finished_job.status, JobStatus.SUCCEEDED)
            self.assertEqual(finished_job.results, [3])

        with self.assertRaises(Exception):
            queuer.get_job(job.rid)

        queuer.stop()

    def test_execute_job_with_only_kwargs(self):
        """Test executing a job with only keyword arguments."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_kwargs_only)
        queuer.start()

        job = queuer.add_job(task_kwargs_only, name="test", value=42, flag=True)
        self.assertIsNotNone(job)

        finished_job = queuer.wait_for_job_finished(job.rid, timeout_seconds=10.0)
        self.assertIsNotNone(finished_job)
        if finished_job:
            self.assertEqual(finished_job.status, JobStatus.SUCCEEDED)
            self.assertEqual(
                finished_job.results, [{"name": "test", "value": 42, "flag": True}]
            )

        queuer.stop()

    def test_execute_job_with_both_args_and_kwargs(self):
        """Test executing a job with both positional and keyword arguments."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_with_kwargs)
        queuer.start()

        job = queuer.add_job(task_with_kwargs, 10, b="custom", c=5)
        self.assertIsNotNone(job)

        finished_job = queuer.wait_for_job_finished(job.rid, timeout_seconds=10.0)
        self.assertIsNotNone(finished_job)
        if finished_job:
            self.assertEqual(finished_job.status, JobStatus.SUCCEEDED)
            self.assertEqual(finished_job.results, [{"a": 10, "b": "custom", "c": 5}])

        queuer.stop()

    def test_execute_job_with_only_positional_args(self):
        """Test executing a job with only positional arguments."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_with_kwargs)
        queuer.start()

        job = queuer.add_job(task_with_kwargs, 20)
        self.assertIsNotNone(job)

        finished_job = queuer.wait_for_job_finished(job.rid, timeout_seconds=10.0)
        self.assertIsNotNone(finished_job)
        if finished_job:
            self.assertEqual(finished_job.status, JobStatus.SUCCEEDED)
            self.assertEqual(finished_job.results, [{"a": 20, "b": "default", "c": 0}])

        queuer.stop()

    def test_job_execution_timeout(self):
        """Test job execution timeout handling using wait_for_job_finished."""
        queuer: Queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_mock)
        queuer.start()

        job = queuer.add_job(task_mock, 5, "2")
        self.assertIsNotNone(job)

        with pytest.raises(Exception):
            queuer.wait_for_job_finished(job.rid, timeout_seconds=2.0)

        job_running = queuer.get_job(job.rid)
        self.assertEqual(job_running.status, JobStatus.RUNNING)

        time.sleep(5)

        final_job = queuer.get_job_ended(job.rid)
        self.assertEqual(final_job.status, JobStatus.SUCCEEDED)
        self.assertEqual(final_job.results, [7])

        queuer.stop()

    def test_job_execution_failing_job(self):
        """Test job execution with a failing job using wait_for_job_finished."""
        mock_failer = MockFailer()

        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(mock_failer.task_mock_failing)
        queuer.start()

        job = queuer.add_job(mock_failer.task_mock_failing, 1, "2")
        self.assertIsNotNone(job)

        finished_job = queuer.wait_for_job_finished(job.rid, timeout_seconds=5)
        if finished_job:
            self.assertEqual(finished_job.status, JobStatus.FAILED)
            self.assertIn(
                "fake fail max count reached: 2",
                finished_job.error if finished_job.error else "",
            )

        with self.assertRaises(Exception):
            queuer.get_job(job.rid)

        queuer.stop()
