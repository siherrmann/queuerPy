"""
Test for Job Database Handler using testcontainers.
Mirrors Go's database/dbJob_test.go structure exactly.
"""

import unittest
from datetime import datetime, timedelta
from typing import Any, List

from .db_job import JobDBHandler
from .db_worker import WorkerDBHandler
from ..helper.test_database import DatabaseTestMixin
from ..model.job import Job, JobStatus, new_job
from ..model.options import Options, Schedule
from ..model.options_on_error import OnError, RetryBackoff
from ..model.worker import new_worker


class TestJobDBHandler(DatabaseTestMixin, unittest.TestCase):
    """Test JobDBHandler matching Go's dbJob_test.go exactly."""

    @classmethod
    def setUpClass(cls):
        super().setup_class()

    @classmethod
    def tearDownClass(cls):
        super().teardown_class()

    def setUp(self):
        super().setup_method()

    # NewJobDBHandler
    def test_new_job_db_handler_valid_call(self):
        """Test valid call to create new JobDBHandler."""
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        self.assertIsNotNone(
            job_db_handler, "Expected JobDBHandler to return a non-nil instance"
        )
        self.assertIsNotNone(
            job_db_handler.db,
            "Expected JobDBHandler to have a non-nil database instance",
        )
        self.assertIsNotNone(
            job_db_handler.db.instance,
            "Expected JobDBHandler to have a non-nil database connection instance",
        )

        exists = job_db_handler.check_tables_existence()
        self.assertTrue(exists)

        job_db_handler.drop_tables()

    def test_new_job_db_handler_invalid_call_with_nil_database(self):
        """Test invalid call with None database."""
        with self.assertRaises(AttributeError) as context:
            JobDBHandler(None, with_table_drop=True)  # type: ignore
        # The error should be about accessing 'instance' attribute on NoneType
        self.assertIn(
            "'NoneType' object has no attribute 'instance'", str(context.exception)
        )

    # CheckTableExistance
    def test_check_table_existence(self):
        """Test checking job table existence."""
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        exists = job_db_handler.check_tables_existence()
        self.assertTrue(exists, "Expected job table to exist")

    # CreateTable
    def test_create_table(self):
        """Test creating job table."""
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)
        job_db_handler.create_table()

    # DropTable
    def test_drop_table(self):
        """Test dropping job table."""
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)
        job_db_handler.drop_tables()

    # InsertJob
    def test_insert_job(self):
        """Test inserting a job."""
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        job = new_job("TestTask", None)

        inserted_job = job_db_handler.insert_job(job)
        self.assertIsNotNone(inserted_job, "Expected InsertJob to return a non-nil job")
        self.assertEqual(
            job.task_name, inserted_job.task_name, "Expected task name to match"
        )
        self.assertEqual(
            JobStatus.QUEUED, inserted_job.status, "Expected job status to be QUEUED"
        )
        self.assertEqual(0, inserted_job.attempts, "Expected job attempts to be 0")
        self.assertIsNotNone(
            inserted_job.created_at, "Expected inserted job CreatedAt time to be set"
        )
        self.assertIsNotNone(
            inserted_job.updated_at, "Expected inserted job UpdatedAt time to be set"
        )

    # InsertJobTx
    def test_insert_job_tx(self):
        """Test inserting a job using transaction."""
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        job = new_job("TestTask", None)

        inserted_job = job_db_handler.insert_job_tx(job)
        self.assertIsNotNone(
            inserted_job, "Expected InsertJobTx to return a non-nil job"
        )
        self.assertEqual(
            job.task_name, inserted_job.task_name, "Expected task name to match"
        )
        self.assertEqual(
            JobStatus.QUEUED, inserted_job.status, "Expected job status to be QUEUED"
        )
        self.assertEqual(0, inserted_job.attempts, "Expected job attempts to be 0")
        self.assertIsNotNone(
            inserted_job.created_at, "Expected inserted job CreatedAt time to be set"
        )
        self.assertIsNotNone(
            inserted_job.updated_at, "Expected inserted job UpdatedAt time to be set"
        )

    # BatchInsertJobs (all 4 sub-tests)
    def test_batch_insert_jobs_successful(self):
        """Test successful batch insert jobs."""
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        job_count = 5
        jobs: List[Job] = []
        for _ in range(job_count):
            job = new_job("TestTask", None)
            jobs.append(job)

        job_db_handler.batch_insert_jobs(jobs)

    def test_batch_insert_jobs_with_error_options(self):
        """Test successful batch insert jobs with error options."""
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        job_count = 5
        jobs: List[Job] = []
        for _ in range(job_count):
            options = Options(
                on_error=OnError(
                    timeout=0.1,
                    retry_delay=1,
                    retry_backoff=RetryBackoff.EXPONENTIAL,
                    max_retries=3,
                )
            )
            job = new_job("TestTask", options)
            jobs.append(job)

        job_db_handler.batch_insert_jobs(jobs)

    def test_batch_insert_jobs_with_schedule_options(self):
        """Test successful batch insert jobs with schedule options."""
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        job_count = 5
        jobs: List[Job] = []
        for _ in range(job_count):
            start_time = datetime.now() + timedelta(seconds=10)
            options = Options(
                schedule=Schedule(
                    start=start_time,
                    interval=timedelta(seconds=5),
                    max_count=10,
                )
            )
            job = new_job("TestTask", options)
            jobs.append(job)

        job_db_handler.batch_insert_jobs(jobs)

    def test_batch_insert_jobs_with_parameters(self):
        """Test successful batch insert jobs with parameters."""
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        job_count = 5
        jobs: List[Job] = []
        for i in range(job_count):
            job = new_job("TestTask", None)
            job.parameters = [i, f"param-{i}"]
            jobs.append(job)

        job_db_handler.batch_insert_jobs(jobs)

    # UpdateJobsInitial
    def test_update_jobs_initial(self):
        """Test updating jobs initial."""
        # Prerequisite: Insert a worker for the job to be associated with
        worker_db_handler = WorkerDBHandler(self.db, with_table_drop=True)

        worker = new_worker("TestWorker", 1)
        inserted_worker = worker_db_handler.insert_worker(worker)
        self.assertIsNotNone(inserted_worker, "Expected worker to be inserted")

        inserted_worker.available_tasks = ["TestTask"]
        updated_worker = worker_db_handler.update_worker(inserted_worker)
        self.assertIsNotNone(updated_worker, "Expected worker to be updated")

        # Now we can proceed with the job insertion and update
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        job = new_job("TestTask", None)
        inserted_job = job_db_handler.insert_job(job)

        # Type assertion - we've already checked that updated_worker is not None
        assert updated_worker is not None
        updated_jobs = job_db_handler.update_jobs_initial(updated_worker)
        self.assertEqual(len(updated_jobs), 1, "Expected one job to be updated")
        self.assertEqual(
            inserted_job.id,
            updated_jobs[0].id,
            "Expected updated job ID to match inserted job ID",
        )
        self.assertEqual(
            updated_worker.id,
            updated_jobs[0].worker_id,
            "Expected updated job WorkerID to match new worker ID",
        )
        self.assertEqual(
            updated_jobs[0].worker_rid,
            updated_worker.rid,
            "Expected updated job WorkerRID to match new worker RID",
        )
        self.assertEqual(
            JobStatus.RUNNING,
            updated_jobs[0].status,
            "Expected job status to be RUNNING",
        )
        self.assertIsNotNone(
            updated_jobs[0].updated_at, "Expected updated job UpdatedAt time to be set"
        )

    # UpdateJobFinal
    def test_update_job_final(self):
        """Test updating job final."""
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        job = new_job("TestTask", None)
        inserted_job = job_db_handler.insert_job(job)

        # Update the job status to SUCCEEDED
        inserted_job.status = JobStatus.SUCCEEDED
        updated_job = job_db_handler.update_job_final(inserted_job)
        self.assertEqual(
            inserted_job.id,
            updated_job.id,
            "Expected updated job ID to match inserted job ID",
        )
        self.assertEqual(
            JobStatus.SUCCEEDED,
            updated_job.status,
            "Expected job status to be SUCCEEDED",
        )
        self.assertIsNotNone(
            updated_job.updated_at, "Expected updated job UpdatedAt time to be set"
        )

    # TestUpdateStaleJobs
    def test_update_stale_jobs(self):
        """Test updating stale jobs functionality."""
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        updated_count = job_db_handler.update_stale_jobs()

        # Should return a number (could be 0 if no stale jobs)
        self.assertIsInstance(updated_count, int)
        self.assertGreaterEqual(updated_count, 0)

    # DeleteJob
    def test_delete_job(self):
        """Test deleting a job."""
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        job = new_job("TestTask", None)
        inserted_job = job_db_handler.insert_job(job)

        job_db_handler.delete_job(inserted_job.rid)

        # Verify that the job no longer exists
        deleted_job = job_db_handler.select_job(inserted_job.rid)
        self.assertIsNone(deleted_job, "Expected deleted job to be None")

    # SelectJob
    def test_select_job(self):
        """Test selecting a job."""
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        job = new_job("TestTask", None)
        inserted_job = job_db_handler.insert_job(job)

        selected_job = job_db_handler.select_job(inserted_job.rid)
        self.assertIsNotNone(selected_job, "Expected SelectJob to return a non-nil job")
        # Type assertion - we've already checked that selected_job is not None
        assert selected_job is not None
        self.assertEqual(
            inserted_job.rid,
            selected_job.rid,
            "Expected selected job RID to match inserted job RID",
        )

    # SelectAllJobs
    def test_select_all_jobs(self):
        """Test selecting all jobs."""
        new_job_count = 5
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        for i in range(new_job_count):
            job = new_job(f"TestJob{i}", None)
            job_db_handler.insert_job(job)

        jobs = job_db_handler.select_all_jobs(0, 10)
        self.assertEqual(
            len(jobs), new_job_count, "Expected SelectAllJobs to return all jobs"
        )

        page_length = 3
        paginated_jobs = job_db_handler.select_all_jobs(0, page_length)
        self.assertEqual(
            len(paginated_jobs),
            page_length,
            "Expected SelectAllJobs to return paginated jobs",
        )

    # SelectAllJobsByWorkerRID
    def test_select_all_jobs_by_worker_rid(self):
        """Test selecting jobs by worker RID."""
        # This test requires worker functionality
        worker_db_handler = WorkerDBHandler(self.db, with_table_drop=True)
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        # Create a worker with available tasks
        worker = new_worker("TestWorker", 2)
        inserted_worker = worker_db_handler.insert_worker(worker)
        self.assertIsNotNone(inserted_worker)

        inserted_worker.available_tasks = ["TestTask"]
        updated_worker = worker_db_handler.update_worker(inserted_worker)
        self.assertIsNotNone(updated_worker)

        # Create jobs that will be assigned to this worker
        job1 = new_job("TestTask", None)
        job_db_handler.insert_job(job1)

        job2 = new_job("TestTask", None)
        job_db_handler.insert_job(job2)

        # Create a job with different task name (won't be assigned)
        job3 = new_job("OtherTask", None)
        job_db_handler.insert_job(job3)

        # Assign jobs to worker using update_jobs_initial
        assert updated_worker is not None
        assigned_jobs = job_db_handler.update_jobs_initial(updated_worker)
        self.assertEqual(
            len(assigned_jobs), 2, "Expected 2 jobs to be assigned to worker"
        )

        # Test selecting jobs by worker RID
        worker_jobs = job_db_handler.select_all_jobs_by_worker_rid(
            updated_worker.rid, 0, 10
        )
        self.assertEqual(len(worker_jobs), 2, "Expected to find 2 jobs for the worker")

    # SelectAllJobsBySearch
    def test_select_all_jobs_by_search(self):
        """Test selecting jobs by search term."""
        search_term = "TestTaskSearch"
        new_job_count_search = 5
        new_job_count_other = 3

        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        # Insert multiple jobs with different names
        for _ in range(new_job_count_search):
            job = new_job(search_term, None)
            job_db_handler.insert_job(job)

        for _ in range(new_job_count_other):
            job = new_job("TestTask", None)
            job_db_handler.insert_job(job)

        jobs_by_search = job_db_handler.select_all_jobs_by_search(search_term, 0, 10)
        self.assertEqual(
            len(jobs_by_search),
            new_job_count_search,
            "Expected SelectAllJobsBySearch to return all jobs matching the search term",
        )

        page_length = 3
        paginated_jobs_by_search = job_db_handler.select_all_jobs_by_search(
            search_term, 0, page_length
        )
        self.assertEqual(
            len(paginated_jobs_by_search),
            page_length,
            "Expected SelectAllJobsBySearch to return paginated jobs",
        )

    # AddRetentionArchive
    def test_add_retention_archive(self):
        """Test adding retention policy for archive cleanup"""
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        # Test adding retention policy with 30 days
        retention_days = 30
        job_db_handler.add_retention_archive(retention_days)
        self.assertTrue(True, "Expected add_retention_archive to execute without error")

    # RemoveRetentionArchive
    def test_remove_retention_archive(self):
        """Test removing retention policy for archive cleanup"""
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        # First add a retention policy
        job_db_handler.add_retention_archive(30)
        job_db_handler.remove_retention_archive()
        self.assertTrue(
            True, "Expected remove_retention_archive to execute without error"
        )

    # SelectJobFromArchive
    def test_select_job_from_archive(self):
        """Test selecting job from archive."""
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        job = new_job("TestTask", None)
        inserted_job = job_db_handler.insert_job(job)

        # Update the job status to SUCCEEDED
        inserted_job.status = JobStatus.SUCCEEDED
        updated_job = job_db_handler.update_job_final(inserted_job)

        # Now select the job from archive
        archived_job = job_db_handler.select_job_from_archive(updated_job.rid)
        self.assertIsNotNone(
            archived_job, "Expected SelectJobFromArchive to return a non-nil job"
        )
        # Type assertion - we've already checked that archived_job is not None
        assert archived_job is not None
        self.assertEqual(
            inserted_job.rid,
            archived_job.rid,
            "Expected archived job RID to match inserted job RID",
        )
        self.assertEqual(
            JobStatus.SUCCEEDED,
            archived_job.status,
            "Expected archived job status to be SUCCEEDED",
        )

    # SelectAllJobsFromArchive
    def test_select_all_jobs_from_archive(self):
        """Test selecting all jobs from archive."""
        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        new_job_count = 5
        for i in range(new_job_count):
            job = new_job(f"TestJob{i}", None)
            inserted_job = job_db_handler.insert_job(job)

            # Update the job status to SUCCEEDED
            inserted_job.status = JobStatus.SUCCEEDED
            job_db_handler.update_job_final(inserted_job)

        jobs_from_archive = job_db_handler.select_all_jobs_from_archive(0, 10)
        self.assertEqual(
            len(jobs_from_archive),
            new_job_count,
            "Expected SelectAllJobsFromArchive to return all archived jobs",
        )

        page_length = 3
        paginated_jobs_from_archive = job_db_handler.select_all_jobs_from_archive(
            0, page_length
        )
        self.assertEqual(
            len(paginated_jobs_from_archive),
            page_length,
            "Expected SelectAllJobsFromArchive to return paginated archived jobs",
        )

    # SelectAllJobsFromArchiveBySearch
    def test_select_all_jobs_from_archive_by_search(self):
        """Test selecting archived jobs by search term."""
        search_term = "TestTaskSearch"
        new_job_count_search = 5
        new_job_count_other = 3

        job_db_handler = JobDBHandler(self.db, with_table_drop=True)

        # Insert multiple jobs with different names
        for _ in range(new_job_count_search):
            job = new_job(search_term, None)
            inserted_job = job_db_handler.insert_job(job)

            # Update the job status to SUCCEEDED
            inserted_job.status = JobStatus.SUCCEEDED
            job_db_handler.update_job_final(inserted_job)

        for _ in range(new_job_count_other):
            job = new_job("TestTask", None)
            inserted_job = job_db_handler.insert_job(job)

            # Update the job status to SUCCEEDED
            inserted_job.status = JobStatus.SUCCEEDED
            job_db_handler.update_job_final(inserted_job)

        jobs_by_search_from_archive = (
            job_db_handler.select_all_jobs_from_archive_by_search(search_term, 0, 10)
        )
        self.assertEqual(
            len(jobs_by_search_from_archive),
            new_job_count_search,
            "Expected SelectAllJobsFromArchiveBySearch to return all archived jobs matching the search term",
        )

        page_length = 3
        paginated_jobs_by_search_from_archive = (
            job_db_handler.select_all_jobs_from_archive_by_search(
                search_term, 0, page_length
            )
        )
        self.assertEqual(
            len(paginated_jobs_by_search_from_archive),
            page_length,
            "Expected SelectAllJobsFromArchiveBySearch to return paginated archived jobs",
        )

    # UpdateJobFinalEncrypted
    def test_update_job_final_encrypted(self):
        """Test updating job final with encryption"""
        job_db_handler = JobDBHandler(
            self.db, with_table_drop=True, encryption_key="test-encryption-key"
        )

        task_name = "TestTask"
        job = new_job(task_name, None)
        inserted_job = job_db_handler.insert_job(job)
        self.assertIsNotNone(inserted_job)

        # Update job with encrypted results
        test_results: List[Any] = ["data", 42.0]
        inserted_job.status = JobStatus.SUCCEEDED
        inserted_job.results = test_results

        updated_job = job_db_handler.update_job_final(inserted_job)
        self.assertIsNotNone(updated_job)
        self.assertEqual(updated_job.status, JobStatus.SUCCEEDED)

        # Job should be archived after UpdateJobFinal
        archived_jobs = job_db_handler.select_all_jobs_from_archive(0, 10)
        self.assertEqual(len(archived_jobs), 1)
        self.assertEqual(
            archived_jobs[0].results, test_results
        )  # Should be decrypted when retrieved

    # SelectJobEncrypted
    def test_select_job_encrypted(self):
        """Test selecting job with encryption"""
        job_db_handler = JobDBHandler(
            self.db, with_table_drop=True, encryption_key="test-encryption-key"
        )

        task_name = "TestTask"
        job = new_job(task_name, None)
        inserted_job = job_db_handler.insert_job(job)
        self.assertIsNotNone(inserted_job)

        # Update job with encrypted results
        test_results: List[Any] = ["data", 42.0]
        inserted_job.status = JobStatus.SUCCEEDED
        inserted_job.results = test_results

        updated_job = job_db_handler.update_job_final(inserted_job)
        self.assertIsNotNone(updated_job)

        # Job is now archived, select it from archive
        archived_job = job_db_handler.select_job_from_archive(updated_job.rid)
        self.assertIsNotNone(archived_job)
        if archived_job:
            self.assertEqual(archived_job.task_name, task_name)
            self.assertEqual(archived_job.status, JobStatus.SUCCEEDED)
            self.assertEqual(
                archived_job.results, test_results
            )  # Should be decrypted when retrieved

    # SelectAllJobsEncrypted
    def test_select_all_jobs_encrypted(self):
        """Test selecting all jobs with encryption"""
        job_db_handler = JobDBHandler(
            self.db, with_table_drop=True, encryption_key="test-encryption-key"
        )

        new_job_count = 5
        expected_results_by_task_name: dict[str, List[Any]] = {}

        # Insert multiple jobs with encrypted results
        for i in range(new_job_count):
            task_name = f"TestJob{i}"
            job = new_job(task_name, None)
            inserted_job = job_db_handler.insert_job(job)
            self.assertIsNotNone(inserted_job)

            test_results: List[Any] = ["data", float(i)]
            expected_results_by_task_name[task_name] = test_results
            inserted_job.status = JobStatus.SUCCEEDED
            inserted_job.results = test_results

            updated_job = job_db_handler.update_job_final(inserted_job)
            self.assertIsNotNone(updated_job)

        # Jobs are archived after UpdateJobFinal, so test archive retrieval
        archived_jobs = job_db_handler.select_all_jobs_from_archive(0, 10)
        self.assertEqual(len(archived_jobs), new_job_count)

        # Test pagination
        page_length = 3
        paginated_archived_jobs = job_db_handler.select_all_jobs_from_archive(
            0, page_length
        )
        self.assertEqual(len(paginated_archived_jobs), page_length)

        # Verify all archived jobs have correct decrypted data
        for job in archived_jobs:
            self.assertIsNotNone(job.task_name)
            self.assertEqual(job.status, JobStatus.SUCCEEDED)

            expected_results = expected_results_by_task_name.get(job.task_name)
            self.assertIsNotNone(
                expected_results,
                f"Expected to find results for task name {job.task_name}",
            )
            self.assertEqual(
                job.results,
                expected_results,
                f"Expected job Results to match test results after decryption for task {job.task_name}",
            )


if __name__ == "__main__":
    unittest.main()
