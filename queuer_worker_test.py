"""
Test cases for worker-related methods in Python queuer implementation.
Mirrors Go's queuerWorker_test.go.
"""

import time
import unittest
from datetime import timedelta
from uuid import uuid4

from .helper.test_database import DatabaseTestMixin
from .model.worker import Worker, WorkerStatus, new_worker
from .queuer import new_queuer_with_db
from .helper.error import QueuerError


class TestQueuerWorker(DatabaseTestMixin, unittest.TestCase):
    """Test cases for queuer worker functionality."""

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

    def test_stop_worker(self):
        """Test stopping a worker."""
        queuer = new_queuer_with_db("test_stop_worker", 10, "", self.db_config)

        try:
            # Set short heartbeat interval for testing
            queuer.worker_poll_interval = timedelta(seconds=0.5)
            queuer.start()

            # Get the worker RID
            worker_rid = queuer.worker.rid

            # Stop the worker
            queuer.stop_worker(worker_rid)

            # Verify the local worker object was updated immediately
            self.assertEqual(queuer.worker.status, WorkerStatus.STOPPED)

            # Verify the worker status is STOPPED in database
            worker = queuer.get_worker(worker_rid)
            self.assertIsNotNone(worker)
            assert worker is not None  # Type narrowing for type checker
            self.assertEqual(worker.status, WorkerStatus.STOPPED)

            # Wait for heartbeat to detect and stop the queuer (takes ~5s)
            time.sleep(6.0)

            # Queuer should have stopped automatically
            self.assertFalse(queuer.running)
        finally:
            # Only stop if still running (shouldn't be the case)
            if hasattr(queuer, "running") and queuer.running:
                queuer.stop()

    def test_stop_worker_gracefully(self):
        """Test stopping a worker gracefully."""
        queuer = new_queuer_with_db(
            "test_stop_worker_gracefully", 10, "", self.db_config
        )

        try:
            # Set short heartbeat interval for testing
            queuer.worker_poll_interval = timedelta(seconds=0.5)
            queuer.start()

            # Get the worker RID
            worker_rid = queuer.worker.rid

            # Stop the worker gracefully
            queuer.stop_worker_gracefully(worker_rid)

            # Verify the local worker object was updated immediately
            self.assertEqual(queuer.worker.status, WorkerStatus.STOPPING)

            # Verify the worker status is STOPPING in database
            worker = queuer.get_worker(worker_rid)
            self.assertIsNotNone(worker)
            assert worker is not None  # Type narrowing for type checker
            self.assertEqual(worker.status, WorkerStatus.STOPPING)

            # Wait for heartbeat to detect STOPPING status and eventually stop (takes ~6s)
            time.sleep(7.0)

            # Queuer should have stopped automatically
            self.assertFalse(queuer.running)
        finally:
            # Only stop if still running (shouldn't be the case)
            if hasattr(queuer, "running") and queuer.running:
                queuer.stop()

    def test_stop_worker_not_found(self):
        """Test stopping a worker that doesn't exist."""
        queuer = new_queuer_with_db(
            "test_stop_worker_not_found", 10, "", self.db_config
        )

        try:
            # Try to stop a non-existent worker
            with self.assertRaises(QueuerError):
                queuer.stop_worker(uuid4())
        finally:
            # Clean up the database connection
            if queuer.database:
                queuer.database.close()

    def test_get_worker(self):
        """Test getting a worker by RID."""
        queuer = new_queuer_with_db("test_get_worker", 10, "", self.db_config)

        try:
            # Get the worker RID
            worker_rid = queuer.worker.rid

            # Get the worker
            worker = queuer.get_worker(worker_rid)

            # Verify the worker was retrieved
            self.assertIsNotNone(worker)
            assert worker is not None  # Type narrowing for type checker
            self.assertEqual(worker.rid, worker_rid)
            self.assertEqual(worker.name, "test_get_worker")
        finally:
            # Clean up the database connection
            if queuer.database:
                queuer.database.close()

    def test_get_worker_not_found(self):
        """Test getting a worker that doesn't exist."""
        queuer = new_queuer_with_db("test_get_worker_not_found", 10, "", self.db_config)

        try:
            # Try to get a non-existent worker
            worker = queuer.get_worker(uuid4())

            # Verify the worker was not found
            self.assertIsNone(worker)
        finally:
            # Clean up the database connection
            if queuer.database:
                queuer.database.close()


if __name__ == "__main__":
    unittest.main()
