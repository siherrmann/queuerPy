"""
Test cases for the queuer job functionality.
Mirrors Go's queuerJob_test.go with testcontainers for end-to-end testing.
"""

import pytest
import pytest_asyncio
import time
import asyncio
import unittest
from unittest.mock import Mock, patch
from uuid import uuid4, UUID

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queuer import new_queuer
from queuer_job import QueuerJobMixin
from model.job import Job, JobStatus, new_job
from model.options import Options
from model.options_on_error import OnError, RetryBackoff
from model.worker import Worker, WorkerStatus
from database.db_job import JobDBHandler
from database.db_worker import WorkerDBHandler
from helper.test_database import DatabaseTestMixin


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
        self.queuer = new_queuer("test_job_queuer", 2)
        self.queuer.db_job = JobDBHandler(self.db, with_table_drop=True)
        self.queuer.db_worker = WorkerDBHandler(self.db, with_table_drop=True)
        
        # Insert the worker into the database
        self.queuer.worker = self.queuer.db_worker.insert_worker(self.queuer.worker)
        
        # Add test tasks
        self.queuer.add_task(task_mock)
        mock_failer = MockFailer()
        self.queuer.add_task(mock_failer.task_mock_failing)
    
    def tearDown(self):
        """Clean up after each test method."""
        super().teardown_method()
    
    def test_add_job_basic(self):
        """Test adding a basic job with real database integration."""
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
    
    def test_add_job_with_options(self):
        """Test adding a job with specific options and real database integration."""
        options = {
            'on_error': OnError(
                max_retries=3,
                retry_backoff=RetryBackoff.LINEAR
            )
        }
        
        # Add job with options
        job = self.queuer.add_job_with_options(options, "task_mock", 1, "2")
        
        # Verify job was created with options
        self.assertIsNotNone(job)
        self.assertIsNotNone(job.options)
        self.assertEqual(job.options.on_error.max_retries, 3)
        self.assertEqual(job.options.on_error.retry_backoff, RetryBackoff.LINEAR)
        
        # Verify job exists in database
        retrieved_job = self.queuer.db_job.select_job(job.rid)
        self.assertIsNotNone(retrieved_job)
        self.assertEqual(retrieved_job.rid, job.rid)
    
    def test_cancel_job(self):
        """Test cancelling a job with real database integration."""
        # First add a job
        job = self.queuer.add_job(task_mock, 1, "2")
        self.assertEqual(job.status, JobStatus.QUEUED)
        
        # Cancel the job
        cancelled_job = self.queuer.cancel_job(job.rid)
        
        # Verify job was cancelled
        self.assertIsNotNone(cancelled_job)
        self.assertEqual(cancelled_job.status, JobStatus.CANCELLED)
        
        # Verify job status in database
        retrieved_job = self.queuer.db_job.select_job_from_archive(job.rid)
        if retrieved_job:  # Job might be moved to archive
            self.assertEqual(retrieved_job.status, JobStatus.CANCELLED)
    
    def test_cancel_job_not_found(self):
        """Test cancelling a job that doesn't exist."""
        job_rid = uuid4()
        
        # Try to cancel non-existent job
        with self.assertRaises(Exception) as context:
            self.queuer.cancel_job(job_rid)
        
        self.assertIn("Job not found", str(context.exception))
    
    def test_get_job(self):
        """Test retrieving a job by RID."""
        job_rid = uuid4()
        
        # Mock the database select
        self.queuer.db_job.select_job.return_value = self.mock_job
        
        result = self.queuer.get_job(job_rid)
        
        assert result == self.mock_job
        self.queuer.db_job.select_job.assert_called_once_with(job_rid)
    
    def test_get_job_not_found(self):
        """Test retrieving a job that doesn't exist."""
        job_rid = uuid4()
        
        # Mock the database select to return None
        self.queuer.db_job.select_job.return_value = None
        
        with pytest.raises(Exception, match="Job not found"):
            self.queuer.get_job(job_rid)
    
    def test_get_jobs(self):
        """Test retrieving all jobs."""
        mock_jobs = [self.mock_job, Mock()]
        
        # Mock the database select
        self.queuer.db_job.select_all_jobs.return_value = mock_jobs
        
        result = self.queuer.get_jobs(0, 100)
        
        assert result == mock_jobs
        self.queuer.db_job.select_all_jobs.assert_called_once_with(0, 100)
    
    def test_get_jobs_by_worker_rid(self):
        """Test retrieving jobs by worker RID."""
        worker_rid = uuid4()
        mock_jobs = [self.mock_job, Mock()]
        
        # Mock the database select
        self.queuer.db_job.select_all_jobs_by_worker_rid.return_value = mock_jobs
        
        result = self.queuer.get_jobs_by_worker_rid(worker_rid, 0, 100)
        
        assert result == mock_jobs
        self.queuer.db_job.select_all_jobs_by_worker_rid.assert_called_once_with(worker_rid, 0, 100)
    
    def test_merge_options_none(self):
        """Test merging options when both are None."""
        result = self.queuer._merge_options(None)
        assert result is None
    
    def test_merge_options_job_only(self):
        """Test merging options when only job options are provided."""
        job_options = Options(
            on_error=OnError(timeout=5.0, max_retries=2, retry_delay=1.0)
        )
        
        result = self.queuer._merge_options(job_options)
        assert result == job_options
    
    def test_merge_options_worker_only(self):
        """Test merging options when only worker options are provided."""
        worker_options = OnError(timeout=10.0, max_retries=3, retry_delay=2.0)
        self.queuer.worker.options = worker_options
        
        with patch('model.options.Options') as mock_options:
            mock_options.return_value = Mock()
            
            result = self.queuer._merge_options(None)
            
            mock_options.assert_called_once_with(on_error=worker_options)
    
    def test_cancel_job_internal(self):
        """Test internal job cancellation method."""
        job = Mock()
        job.status = JobStatus.RUNNING
        job.rid = uuid4()
        
        self.queuer.db_job.update_job_final.return_value = job
        
        self.queuer._cancel_job(job)
        
        assert job.status == JobStatus.CANCELLED
        self.queuer.db_job.update_job_final.assert_called_once_with(job)
    
    @pytest.mark.asyncio
    async def test_succeed_job(self):
        """Test succeeding a job."""
        job = Mock()
        job.rid = uuid4()
        job.worker_id = 1
        results = [42, "success"]
        
        self.queuer.db_job.update_job_final.return_value = job
        
        await self.queuer._succeed_job(job, results)
        
        assert job.status == JobStatus.SUCCEEDED
        assert job.results == results
        self.queuer.db_job.update_job_final.assert_called_once_with(job)
    
    @pytest.mark.asyncio
    async def test_fail_job(self):
        """Test failing a job."""
        job = Mock()
        job.rid = uuid4()
        job.worker_id = 1
        error = Exception("Test error")
        
        self.queuer.db_job.update_job_final.return_value = job
        
        await self.queuer._fail_job(job, error)
        
        assert job.status == JobStatus.FAILED
        assert job.error == "Test error"
        self.queuer.db_job.update_job_final.assert_called_once_with(job)