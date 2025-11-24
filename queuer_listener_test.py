"""
Tests for listener-related methods in the Python queuer implementation.
Mirrors Go's queuerListener_test.go functionality.
"""

import asyncio
import threading
import time
from typing import List
import unittest
from uuid import uuid4

from .core.runner import go_func
from .helper.logging import get_logger
from .helper.test_database import DatabaseTestMixin
from .queuer import new_queuer_with_db
from .model.job import Job

logger = get_logger(__name__)


class TestQueuerListener(DatabaseTestMixin, unittest.TestCase):
    """Test cases for queuer listener functionality."""

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
        self.queuer = new_queuer_with_db("test_no_hang", 5, "", self.db_config)

    def test_listen_for_job_insert_success(self):
        """Test successfully listening for job insert events."""
        self.queuer.start()

        jobs: List[Job] = []
        jobs_lock = threading.Lock()

        def on_job_inserted(job: Job):
            logger.debug(f"got job notification: {job.rid if job else 'None'}")
            with jobs_lock:
                jobs.append(job)

        runner = go_func(self.queuer.listen_for_job_insert, False, on_job_inserted)

        time.sleep(0.1)

        rid_a = uuid4()
        rid_b = uuid4()

        if self.queuer.job_insert_broadcaster:
            asyncio.run(self.queuer.job_insert_broadcaster.broadcast(Job(rid=rid_a)))
        if self.queuer.job_insert_broadcaster:
            asyncio.run(self.queuer.job_insert_broadcaster.broadcast(Job(rid=rid_b)))

        time.sleep(0.5)

        with jobs_lock:
            self.assertEqual(len(jobs), 2)
            self.assertEqual(jobs[0].rid, rid_a)
            self.assertEqual(jobs[1].rid, rid_b)

        runner.get_results()
        self.queuer.stop()

    def test_listen_for_job_update_success(self):
        """Test successfully listening for job update events."""
        self.queuer.start()

        jobs: List[Job] = []
        jobs_lock = threading.Lock()

        def on_job_updated(job: Job):
            logger.debug(f"got job update notification: {job.rid if job else 'None'}")
            with jobs_lock:
                jobs.append(job)

        runner = go_func(self.queuer.listen_for_job_update, False, on_job_updated)

        time.sleep(0.1)

        rid_a = uuid4()
        rid_b = uuid4()

        if self.queuer.job_update_broadcaster:
            asyncio.run(self.queuer.job_update_broadcaster.broadcast(Job(rid=rid_a)))
        if self.queuer.job_update_broadcaster:
            asyncio.run(self.queuer.job_update_broadcaster.broadcast(Job(rid=rid_b)))

        time.sleep(0.5)

        with jobs_lock:
            self.assertEqual(len(jobs), 2)
            self.assertEqual(jobs[0].rid, rid_a)
            self.assertEqual(jobs[1].rid, rid_b)

        runner.get_results()
        self.queuer.stop()

    def test_listen_for_job_delete_success(self):
        """Test successfully listening for job delete events."""
        self.queuer.start()

        jobs: List[Job] = []
        jobs_lock = threading.Lock()

        def on_job_deleted(job: Job):
            logger.debug(f"got job delete notification: {job.rid if job else 'None'}")
            with jobs_lock:
                jobs.append(job)

        runner = go_func(self.queuer.listen_for_job_delete, False, on_job_deleted)

        time.sleep(0.1)

        rid_a = uuid4()
        rid_b = uuid4()

        if self.queuer.job_delete_broadcaster:
            asyncio.run(self.queuer.job_delete_broadcaster.broadcast(Job(rid=rid_a)))
        if self.queuer.job_delete_broadcaster:
            asyncio.run(self.queuer.job_delete_broadcaster.broadcast(Job(rid=rid_b)))

        time.sleep(0.5)

        with jobs_lock:
            self.assertEqual(len(jobs), 2)
            self.assertEqual(jobs[0].rid, rid_a)
            self.assertEqual(jobs[1].rid, rid_b)

        runner.get_results()
        self.queuer.stop()

    def test_listen_for_job_insert_not_running_error(self):
        """Test that succeeds by expecting RuntimeError when queuer is not running."""

        def mock_notify_function(_: Job):
            pass

        with self.assertRaises(RuntimeError) as context:
            self.queuer.listen_for_job_insert(mock_notify_function)

        self.assertIn(
            "Cannot listen with not running Queuer",
            str(context.exception),
        )
