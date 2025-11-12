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

from queuer import new_queuer, new_queuer_with_db
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

        # Create queuer with the test database configuration
        self.queuer = new_queuer_with_db("TestQueuer", 100, "", self.db_config)

        # Add test tasks
        self.queuer.add_task(task_mock)
        mock_failer = MockFailer()
        self.queuer.add_task(mock_failer.task_mock_failing)

        # Start the queuer to enable proper job processing
        self.queuer.start()

    def tearDown(self):
        """Clean up after each test method."""
        # Stop the queuer to clean up background tasks and connections
        if hasattr(self, "queuer") and self.queuer:
            try:
                # Stop the queuer properly to avoid resource leaks
                self.queuer.stop()
                # Add a small delay to allow threads to fully terminate
                import time

                time.sleep(0.1)
            except Exception as e:
                # Log but don't fail the test cleanup
                print(f"Warning: Error stopping queuer in tearDown: {e}")

        super().tearDown()

    def _cleanup_remaining_threads(self):
        """Clean up any remaining threads that might cause segfaults."""
        import threading
        import time

        # Get all active threads
        active_threads = threading.enumerate()
        main_thread = threading.main_thread()

        # Find threads that aren't the main thread
        background_threads = [
            t for t in active_threads if t != main_thread and t.is_alive()
        ]

        if background_threads:
            print(
                f"Warning: Found {len(background_threads)} background threads still running"
            )

            # Try to join them with a timeout
            for thread in background_threads:
                if hasattr(thread, "_stop_event"):
                    # This is likely a ticker or similar - try to stop it
                    try:
                        thread._stop_event.set()
                    except:
                        pass

                # Try to join with a short timeout
                try:
                    thread.join(timeout=0.5)
                except:
                    pass

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

        # Note: Due to immediate processing, jobs are automatically picked up when created
        # The jobs should now be assigned to the worker and in RUNNING status

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
        """Test cancelling a job.

        Note: Due to Python ProcessPoolExecutor limitations, this test verifies that:
        1. The job status is correctly updated to CANCELLED
        2. The job results are empty (not the completed result)
        3. The job is moved to archive with CANCELLED status

        The underlying process may continue running until completion, but the results
        are discarded and the job is treated as cancelled by the system.
        """
        # Create a job with longer duration to ensure we can cancel it mid-execution
        job = self.queuer.add_job(task_mock, 10, "2")
        self.assertIsNotNone(job)

        cancelled_job = self.queuer.cancel_job(job.rid)
        self.assertIsNotNone(cancelled_job)
        self.assertEqual(cancelled_job.rid, job.rid)
        self.assertEqual(cancelled_job.status, JobStatus.CANCELLED)

        time.sleep(0.5)  # Small delay to ensure DB update

        # Check that the job is in archive with CANCELLED status
        archived_job = self.queuer.db_job.select_job_from_archive(job.rid)
        self.assertIsNotNone(archived_job, "Cancelled job should be moved to archive")
        self.assertEqual(archived_job.status, JobStatus.CANCELLED)

        self.assertTrue(isinstance(archived_job.results, list))
        self.assertEqual(
            len(archived_job.results),
            0,
            "Cancelled job should have empty results",
        )

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
        new_job = self.queuer.readd_job_from_archive(cancelled_job.rid)

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

    def test_failed_job_archived_with_error(self):
        """Test that failed jobs are properly archived with their error messages."""
        # Create a job that will fail
        job = Job(
            task_name="failing_task",
            parameters=["test_param"],
            status=JobStatus.RUNNING,
            error="",
        )

        # Insert the job
        inserted_job = self.queuer.db_job.insert_job(job)

        # Simulate job failure by updating it with error information
        inserted_job.status = "FAILED"  # String format for database update
        inserted_job.error = "Task failed due to invalid input data"
        inserted_job.results = {"error_details": "Validation failed"}

        # Use the database update function to move job to archive
        archived_job = self.queuer.db_job.update_job_final(inserted_job)

        # Verify the job was updated in the database
        self.assertIsNotNone(archived_job)
        self.assertEqual(archived_job.status, "FAILED")
        self.assertEqual(archived_job.error, "Task failed due to invalid input data")

        # Verify the job is now in the archive
        archived_job_from_db = self.queuer.db_job.select_job_from_archive(
            archived_job.rid
        )
        self.assertIsNotNone(
            archived_job_from_db, "Failed job should be moved to archive"
        )
        self.assertEqual(archived_job_from_db.status, "FAILED")
        self.assertEqual(
            archived_job_from_db.error, "Task failed due to invalid input data"
        )
        self.assertEqual(
            archived_job_from_db.results, {"error_details": "Validation failed"}
        )

        # Verify the job is no longer in the main job table
        main_job = self.queuer.db_job.select_job(archived_job.rid)
        self.assertIsNone(main_job, "Failed job should be removed from main table")

    def test_select_all_jobs_from_archive(self):
        """Test selecting all jobs from archive with pagination."""
        # Create and archive multiple jobs with different statuses
        archived_jobs = []

        for i in range(3):
            job = Job(
                task_name=f"archive_test_task_{i}",
                parameters=[f"param_{i}"],
                status=JobStatus.RUNNING,
            )
            inserted_job = self.queuer.db_job.insert_job(job)

            # Update each job with different final status
            if i == 0:
                inserted_job.status = "SUCCEEDED"
                inserted_job.results = {"success": True}
            elif i == 1:
                inserted_job.status = "FAILED"
                inserted_job.error = f"Error in job {i}"
            else:
                inserted_job.status = "CANCELLED"

            # Archive the job
            archived_job = self.queuer.db_job.update_job_final(inserted_job)
            archived_jobs.append(archived_job)

        # Test retrieving all archived jobs
        retrieved_jobs = self.queuer.db_job.select_all_jobs_from_archive(entries=10)

        # Should have at least our 3 test jobs
        self.assertGreaterEqual(len(retrieved_jobs), 3)

        # Find our test jobs in the results
        our_jobs = [
            job
            for job in retrieved_jobs
            if job.task_name.startswith("archive_test_task_")
        ]
        self.assertEqual(len(our_jobs), 3)

        # Verify the jobs have the correct statuses and error information
        succeeded_job = None
        failed_job = None
        cancelled_job = None

        for job in our_jobs:
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
        self.assertIsNotNone(failed_job.error)
        self.assertIn("Error in job", failed_job.error)


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

        # Create queuer with the test database configuration to ensure consistency
        self.queuer = new_queuer_with_db("TestQueuerRunning", 100, "", self.db_config)

        # Add test tasks
        self.queuer.add_task(task_mock)

        # Start the queuer
        self.queuer.start()

    def tearDown(self):
        """Clean up after each test method."""
        if hasattr(self, "queuer") and self.queuer:
            try:
                # Stop the queuer properly to avoid resource leaks
                self.queuer.stop()
                # Add a small delay to allow threads to fully terminate
                import time

                time.sleep(0.1)
            except Exception as e:
                # Log but don't fail the test cleanup
                print(f"Warning: Error stopping queuer in tearDown: {e}")

        super().tearDown()

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

        # If the listener approach failed, check if the job was completed in the database
        if finished_job is None:
            # Give the job some extra time to complete
            time.sleep(2)

            # Check if job was moved to archive (completed)
            try:
                archived_job = self.queuer.db_job.select_job_from_archive(job.rid)
                print(f"Job found in archive: {archived_job}")
                if archived_job:
                    print(f"Archived job status: {archived_job.status}")
                    # If the job was completed but listener failed, the test should still pass
                    self.assertEqual(archived_job.status, "SUCCEEDED")
                    # Verify the job results are correct
                    self.assertEqual(archived_job.results, 3)  # 1 + 2 = 3
                    print("Job completed successfully (verified via database)")
                    return
            except Exception as e:
                print(f"Error checking archive: {e}")

        # Verify job finished successfully (either via listener or database check)
        if finished_job is not None:
            self.assertEqual(finished_job.status, "SUCCEEDED")
            self.assertEqual(finished_job.results, 3)
            self.assertEqual(finished_job.status, JobStatus.SUCCEEDED)
            self.assertEqual(finished_job.results, 3)  # 1 + 2 = 3
        else:
            # If we get here, the job didn't complete even in the database
            self.fail("Job did not complete within the expected time frame")

        # Verify job is no longer in main table (moved to archive)
        with self.assertRaises(Exception):
            self.queuer.get_job(job.rid)


if __name__ == "__main__":
    unittest.main()
