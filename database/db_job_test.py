"""
Test for Job Database Handler using testcontainers.
"""

from typing import Any, List
import unittest
from datetime import datetime
from uuid import uuid4, UUID

from database.db_job import JobDBHandler
from database.db_worker import WorkerDBHandler
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
        self.worker_handler = WorkerDBHandler(self.db, with_table_drop=True)

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
        job = Job(
            task_name="test_task",
            parameters=["param1", "param2"],
            status=JobStatus.QUEUED,
        )
        inserted_job = self.job_handler.insert_job(job)
        self.assertIsNotNone(inserted_job)
        self.assertIsInstance(inserted_job.rid, UUID)

        # Select the job back
        retrieved_job = self.job_handler.select_job(inserted_job.rid)
        self.assertIsNotNone(retrieved_job)
        if retrieved_job:
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
        jobs: List[Job] = []
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
        complex_params: List[Any] = [
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
        if retrieved_job:
            self.assertEqual(retrieved_job.parameters, complex_params)
            self.assertEqual(retrieved_job.task_name, "complex_task")

    def test_job_with_result(self):
        """Test job with result data."""
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
        if retrieved_job:
            self.assertEqual(
                retrieved_job.results, [{"output": "processed_data", "count": 42}]
            )
            self.assertEqual(retrieved_job.status, JobStatus.SUCCEEDED)

    def test_job_with_error(self):
        """Test job with error information."""
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
        if retrieved_job:
            self.assertEqual(retrieved_job.error, "Task failed due to invalid input")
            self.assertEqual(retrieved_job.status, JobStatus.FAILED)

    def test_job_pagination(self):
        """Test job pagination functionality."""
        jobs: List[Job] = []
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
        if retrieved_job:
            self.assertEqual(retrieved_job.worker_rid, worker_rid)
            self.assertEqual(retrieved_job.status, JobStatus.RUNNING)
            self.assertIsNotNone(retrieved_job.started_at)

    def test_job_lifecycle(self):
        """Test complete job lifecycle."""
        job = Job(
            task_name="lifecycle_task",
            parameters=["test_data"],
            status=JobStatus.QUEUED,
        )
        inserted_job = self.job_handler.insert_job(job)

        # Verify queued state
        retrieved_job = self.job_handler.select_job(inserted_job.rid)
        if retrieved_job:
            self.assertEqual(retrieved_job.status, JobStatus.QUEUED)
            self.assertIsNotNone(retrieved_job.created_at)
            self.assertIsNone(retrieved_job.started_at)

    def test_update_job_final(self):
        """Test updating job with final status and archiving."""
        job = Job(
            task_name="test_final_task",
            parameters=["param1"],
            status=JobStatus.RUNNING,
        )
        inserted_job = self.job_handler.insert_job(job)
        inserted_job.status = "SUCCEEDED"
        inserted_job.results = [{"result": "success"}]
        inserted_job.error = ""

        updated_job = self.job_handler.update_job_final(inserted_job)

        self.assertIsNotNone(updated_job)
        self.assertEqual(updated_job.status, "SUCCEEDED")
        self.assertEqual(updated_job.results, [{"result": "success"}])

        # Verify job is no longer in main table
        main_job = self.job_handler.select_job(updated_job.rid)
        self.assertIsNone(main_job)

        # Verify job is in archive
        archived_job = self.job_handler.select_job_from_archive(updated_job.rid)
        self.assertIsNotNone(archived_job)
        if archived_job:
            self.assertEqual(archived_job.status, "SUCCEEDED")

    def test_update_job_final_with_error(self):
        """Test updating job with failed status and error message."""
        job = Job(
            task_name="test_failed_task",
            parameters=["param1"],
            status=JobStatus.RUNNING,
        )
        inserted_job = self.job_handler.insert_job(job)
        inserted_job.status = "FAILED"
        inserted_job.results = []
        inserted_job.error = "Task execution failed"

        updated_job = self.job_handler.update_job_final(inserted_job)

        self.assertIsNotNone(updated_job)
        self.assertEqual(updated_job.status, "FAILED")
        self.assertEqual(updated_job.error, "Task execution failed")

        # Verify job is in archive with error
        archived_job = self.job_handler.select_job_from_archive(updated_job.rid)
        self.assertIsNotNone(archived_job)
        if archived_job:
            self.assertEqual(archived_job.status, "FAILED")
            self.assertEqual(archived_job.error, "Task execution failed")

    def test_select_job_from_archive(self):
        """Test selecting a job from archive."""
        job = Job(
            task_name="archive_test_task",
            parameters=["test"],
            status=JobStatus.RUNNING,
        )
        inserted_job = self.job_handler.insert_job(job)
        inserted_job.status = "SUCCEEDED"
        inserted_job.results = [{"archived": True}]

        # Archive the job
        archived_job = self.job_handler.update_job_final(inserted_job)

        # Test selecting from archive
        retrieved_job = self.job_handler.select_job_from_archive(archived_job.rid)
        self.assertIsNotNone(retrieved_job)
        if retrieved_job:
            self.assertEqual(retrieved_job.rid, archived_job.rid)
            self.assertEqual(retrieved_job.task_name, "archive_test_task")
            self.assertEqual(retrieved_job.status, "SUCCEEDED")
            self.assertEqual(retrieved_job.results, [{"archived": True}])

    def test_select_all_jobs_from_archive(self):
        """Test selecting all jobs from archive with pagination."""
        archived_jobs: List[Job] = []
        for i in range(5):
            job = Job(
                task_name=f"archive_job_{i}",
                parameters=[f"param_{i}"],
                status=JobStatus.RUNNING,
            )
            inserted_job = self.job_handler.insert_job(job)
            inserted_job.status = "SUCCEEDED"

            archived_job = self.job_handler.update_job_final(inserted_job)
            archived_jobs.append(archived_job)

        # Test retrieving all archived jobs
        retrieved_jobs = self.job_handler.select_all_jobs_from_archive(entries=10)
        self.assertGreaterEqual(len(retrieved_jobs), 5)

        # Test pagination
        paginated_jobs = self.job_handler.select_all_jobs_from_archive(entries=3)
        self.assertEqual(len(paginated_jobs), 3)

    def test_select_all_jobs_by_search(self):
        """Test searching jobs by various criteria."""
        search_jobs: List[Job] = []
        for i in range(3):
            job = Job(
                task_name=f"SearchableTask_{i}",
                parameters=[f"param_{i}"],
                status=JobStatus.QUEUED,
            )
            inserted_job = self.job_handler.insert_job(job)
            search_jobs.append(inserted_job)

        # Create jobs with different names
        for i in range(2):
            job = Job(
                task_name=f"OtherTask_{i}",
                parameters=[f"param_{i}"],
                status=JobStatus.QUEUED,
            )
            self.job_handler.insert_job(job)

        # Test search functionality
        found_jobs = self.job_handler.select_all_jobs_by_search(
            "SearchableTask", entries=10
        )

        # Should find our 3 searchable jobs
        searchable_jobs = [
            job for job in found_jobs if "SearchableTask" in job.task_name
        ]
        self.assertEqual(len(searchable_jobs), 3)

    def test_select_all_jobs_from_archive_by_search(self):
        """Test searching archived jobs by various criteria."""
        for i in range(3):
            job = Job(
                task_name=f"ArchiveSearchTask_{i}",
                parameters=[f"param_{i}"],
                status=JobStatus.RUNNING,
            )

            inserted_job = self.job_handler.insert_job(job)
            inserted_job.status = "SUCCEEDED"
            self.job_handler.update_job_final(inserted_job)

        # Create other archived jobs
        for i in range(2):
            job = Job(
                task_name=f"OtherArchiveTask_{i}",
                parameters=[f"param_{i}"],
                status=JobStatus.RUNNING,
            )

            inserted_job = self.job_handler.insert_job(job)
            inserted_job.status = "SUCCEEDED"
            self.job_handler.update_job_final(inserted_job)

        # Test archive search functionality
        found_jobs = self.job_handler.select_all_jobs_from_archive_by_search(
            "ArchiveSearchTask", entries=10
        )

        # Should find our 3 searchable archived jobs
        searchable_jobs = [
            job for job in found_jobs if "ArchiveSearchTask" in job.task_name
        ]
        self.assertEqual(len(searchable_jobs), 3)

    def test_update_stale_jobs(self):
        """Test updating stale jobs functionality."""
        updated_count = self.job_handler.update_stale_jobs()

        # Should return a number (could be 0 if no stale jobs)
        self.assertIsInstance(updated_count, int)
        self.assertGreaterEqual(updated_count, 0)

    def test_nil_database_error(self):
        """Test error handling with None database connection."""
        with self.assertRaises(ValueError) as context:
            JobDBHandler(None)

        self.assertIn("Database connection is None", str(context.exception))


if __name__ == "__main__":
    unittest.main()
