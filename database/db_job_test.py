"""
Test for Job Database Handler using testcontainers.
"""

import unittest
from datetime import datetime
from uuid import uuid4, UUID

from database.db_job import JobDBHandler
from helper.test_database import DatabaseTestMixin
from model.job import Job, JobStatus


class TestJobDBHandler(DatabaseTestMixin, unittest.TestCase):
    """Test JobDBHandler with real database using testcontainers."""

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
        self.job_handler = JobDBHandler(self.db, with_table_drop=True)

    def tearDown(self):
        """Clean up after each test method."""
        super().teardown_method()

    def test_create_job_handler(self):
        """Test creating a job handler."""
        self.assertIsNotNone(self.job_handler)
        self.assertEqual(self.job_handler.db, self.db)
        self.assertTrue(self.job_handler.check_tables_existence())

    def test_insert_and_select_job(self):
        """Test inserting and selecting a job."""
        # Create a job
        job = Job(
            task_name="test_task",
            parameters=["param1", "param2"],
            status=JobStatus.QUEUED,
        )

        # Insert the job
        inserted_job = self.job_handler.insert_job(job)
        self.assertIsNotNone(inserted_job)
        self.assertIsInstance(inserted_job.rid, UUID)

        # Select the job back
        retrieved_job = self.job_handler.select_job(inserted_job.rid)
        self.assertIsNotNone(retrieved_job)
        self.assertEqual(retrieved_job.rid, inserted_job.rid)
        self.assertEqual(retrieved_job.task_name, "test_task")
        self.assertEqual(retrieved_job.parameters, ["param1", "param2"])
        self.assertEqual(retrieved_job.status, JobStatus.QUEUED)
        self.assertIsNotNone(retrieved_job.created_at)

    def test_select_job_not_found(self):
        """Test selecting a non-existent job."""
        fake_id = uuid4()
        result = self.job_handler.select_job(fake_id)
        self.assertIsNone(result)

    def test_select_all_jobs(self):
        """Test selecting all jobs."""
        # Create multiple jobs
        jobs = []
        for i in range(3):
            job = Job(
                task_name=f"test_task_{i}",
                parameters=[f"param_{i}"],
                status=JobStatus.QUEUED,
            )
            inserted_job = self.job_handler.insert_job(job)
            jobs.append(inserted_job)

        # Get all jobs
        all_jobs = self.job_handler.select_all_jobs()
        self.assertGreaterEqual(len(all_jobs), 3)

        # Verify our jobs are in the results
        job_rids = {job.rid for job in all_jobs}
        for job in jobs:
            self.assertIn(job.rid, job_rids)

    def test_delete_job(self):
        """Test deleting a job."""
        # Create and insert a job
        job = Job(
            task_name="delete_test", parameters=["param"], status=JobStatus.QUEUED
        )

        inserted_job = self.job_handler.insert_job(job)
        self.assertIsNotNone(inserted_job)

        # Verify it exists
        retrieved_job = self.job_handler.select_job(inserted_job.rid)
        self.assertIsNotNone(retrieved_job)

        # Delete the job
        self.job_handler.delete_job(inserted_job.rid)

        # Verify it's deleted
        deleted_job = self.job_handler.select_job(inserted_job.rid)
        self.assertIsNone(deleted_job)

    def test_job_with_complex_parameters(self):
        """Test job with complex parameter types."""
        # Create a job with complex parameters
        complex_params = [
            "string_param",
            42,
            {"key": "value", "nested": {"inner": "data"}},
            [1, 2, 3, "mixed", True],
        ]

        job = Job(
            task_name="complex_task", parameters=complex_params, status=JobStatus.QUEUED
        )

        # Insert and retrieve
        inserted_job = self.job_handler.insert_job(job)
        retrieved_job = self.job_handler.select_job(inserted_job.rid)

        self.assertIsNotNone(retrieved_job)
        self.assertEqual(retrieved_job.parameters, complex_params)
        self.assertEqual(retrieved_job.task_name, "complex_task")

    def test_job_with_result(self):
        """Test job with result data."""
        # Create a job with result
        job = Job(
            task_name="result_task",
            parameters=["input"],
            status=JobStatus.SUCCEEDED,
            results=[{"output": "processed_data", "count": 42}],
        )

        # Insert and retrieve
        inserted_job = self.job_handler.insert_job(job)
        retrieved_job = self.job_handler.select_job(inserted_job.rid)

        self.assertIsNotNone(retrieved_job)
        self.assertEqual(
            retrieved_job.results, [{"output": "processed_data", "count": 42}]
        )
        self.assertEqual(retrieved_job.status, JobStatus.SUCCEEDED)

    def test_job_with_error(self):
        """Test job with error information."""
        # Create a job with error
        job = Job(
            task_name="error_task",
            parameters=["input"],
            status=JobStatus.FAILED,
            error="Task failed due to invalid input",
        )

        # Insert and retrieve
        inserted_job = self.job_handler.insert_job(job)
        retrieved_job = self.job_handler.select_job(inserted_job.rid)

        self.assertIsNotNone(retrieved_job)
        self.assertEqual(retrieved_job.error, "Task failed due to invalid input")
        self.assertEqual(retrieved_job.status, JobStatus.FAILED)

    def test_job_pagination(self):
        """Test job pagination functionality."""
        # Create multiple jobs for pagination testing
        jobs = []
        for i in range(5):
            job = Job(
                task_name=f"page_task_{i}",
                parameters=[f"page_param_{i}"],
                status=JobStatus.QUEUED,
            )
            inserted_job = self.job_handler.insert_job(job)
            jobs.append(inserted_job)

        # Test pagination
        page1 = self.job_handler.select_all_jobs(entries=2)
        self.assertGreaterEqual(len(page1), 2)

        if len(page1) >= 2:
            last_id = page1[-1].id
            page2 = self.job_handler.select_all_jobs(last_id=last_id, entries=2)

            # Verify no overlap between pages
            page1_ids = {job.id for job in page1}
            page2_ids = {job.id for job in page2}
            self.assertEqual(len(page1_ids.intersection(page2_ids)), 0)

    def test_job_with_worker_assignment(self):
        """Test job with worker assignment."""
        worker_rid = uuid4()

        # Create a job assigned to a worker
        job = Job(
            task_name="worker_task",
            parameters=["input"],
            status=JobStatus.RUNNING,
            worker_rid=worker_rid,
            started_at=datetime.now(),
        )

        # Insert and retrieve
        inserted_job = self.job_handler.insert_job(job)
        retrieved_job = self.job_handler.select_job(inserted_job.rid)

        self.assertIsNotNone(retrieved_job)
        self.assertEqual(retrieved_job.worker_rid, worker_rid)
        self.assertEqual(retrieved_job.status, JobStatus.RUNNING)
        self.assertIsNotNone(retrieved_job.started_at)

    def test_job_lifecycle(self):
        """Test complete job lifecycle."""
        # Create a job
        job = Job(
            task_name="lifecycle_task",
            parameters=["test_data"],
            status=JobStatus.QUEUED,
        )

        # Insert job
        inserted_job = self.job_handler.insert_job(job)

        # Verify queued state
        retrieved_job = self.job_handler.select_job(inserted_job.rid)
        self.assertEqual(retrieved_job.status, JobStatus.QUEUED)
        self.assertIsNotNone(retrieved_job.created_at)
        self.assertIsNone(retrieved_job.started_at)
        # Note: finished_at is not a field in the Job model

        # Note: The current API doesn't support job updates, only inserts
        # In a real system, you'd update the job status as it progresses
        # For now, we just verify the basic insert/select functionality


if __name__ == "__main__":
    unittest.main()
