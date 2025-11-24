"""
Test for Worker Database Handler using testcontainers.
Mirrors Go's dbWorker_test.go test suite.
"""

from typing import List
import unittest
from datetime import datetime, timedelta, timezone

from .db_worker import WorkerDBHandler
from ..helper.test_database import DatabaseTestMixin
from ..model.worker import Worker, WorkerStatus


class TestWorkerDBHandler(DatabaseTestMixin, unittest.TestCase):
    """Test WorkerDBHandler with real database using testcontainers."""

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
        self.worker_handler = WorkerDBHandler(self.db, with_table_drop=True)

    def tearDown(self):
        """Clean up after each test method."""
        super().teardown_method()

    def test_new_worker_db_handler(self):
        """Test creating a worker handler. Mirrors Go's TestWorkerNewWorkerDBHandler."""
        self.assertIsNotNone(self.worker_handler)
        self.assertEqual(self.worker_handler.db, self.db)
        self.assertTrue(self.worker_handler.check_table_existance())

    def test_check_table_existance(self):
        """Test checking table existence. Mirrors Go's TestWorkerCheckTableExistance."""
        exists = self.worker_handler.check_table_existance()
        self.assertTrue(exists)

    def test_create_table(self):
        """Test table creation. Mirrors Go's TestWorkerCreateTable."""
        # Create a new handler without table drop to test explicit creation
        handler = WorkerDBHandler(self.db, with_table_drop=False)
        # Table should exist since constructor calls create_table
        self.assertTrue(handler.check_table_existance())

    def test_drop_table(self):
        """Test table dropping. Mirrors Go's TestWorkerDropTable."""
        # Verify table exists first
        self.assertTrue(self.worker_handler.check_table_existance())

        # Drop table
        self.worker_handler.drop_table()

        # Verify table no longer exists
        self.assertFalse(self.worker_handler.check_table_existance())

    def test_insert_worker(self):
        """Test inserting a worker. Mirrors Go's TestWorkerInsertWorker."""
        # Create a test worker (mirrors Go's model.NewWorker)
        worker = Worker(name="Worker", status=WorkerStatus.READY, max_concurrency=1)

        # Insert worker
        inserted_worker = self.worker_handler.insert_worker(worker)
        self.assertIsNotNone(inserted_worker)
        self.assertIsNotNone(inserted_worker.id)
        self.assertIsNotNone(inserted_worker.rid)
        self.assertNotEqual(worker.rid, inserted_worker.rid)  # RID should be generated
        self.assertEqual(worker.name, inserted_worker.name)
        self.assertEqual(worker.status, inserted_worker.status)
        self.assertIsNotNone(inserted_worker.created_at)
        self.assertIsNotNone(inserted_worker.updated_at)
        # Verify timestamps are recent (within 1 second)
        # Database timestamps are stored in UTC but returned timezone-naive
        # Compare with UTC time since database uses UTC
        from datetime import timezone

        utc_now = datetime.now(timezone.utc).replace(
            tzinfo=None
        )  # Make timezone-naive for comparison
        time_diff = abs((utc_now - inserted_worker.created_at).total_seconds())
        self.assertLess(time_diff, 1)

    def test_update_worker(self):
        """Test updating a worker. Mirrors Go's TestWorkerUpdateWorker."""
        # Create and insert a worker
        worker = Worker(name="Worker", status=WorkerStatus.READY, max_concurrency=1)

        inserted_worker = self.worker_handler.insert_worker(worker)
        self.assertIsNotNone(inserted_worker)

        # Update the worker's name and add tasks/intervals (mirrors Go test)
        inserted_worker.name = "UpdatedWorker"
        # Note: Options equivalent would need to be added to Worker model
        inserted_worker.available_tasks = ["task1", "task2"]
        inserted_worker.available_next_interval_funcs = ["interval1", "interval2"]

        updated_worker = self.worker_handler.update_worker(inserted_worker)
        if updated_worker is not None:
            self.assertIsNotNone(updated_worker)
            self.assertEqual(inserted_worker.name, updated_worker.name)
            self.assertEqual(
                inserted_worker.available_tasks, updated_worker.available_tasks
            )
            self.assertEqual(
                inserted_worker.available_next_interval_funcs,
                updated_worker.available_next_interval_funcs,
            )
            self.assertEqual(
                inserted_worker.max_concurrency, updated_worker.max_concurrency
            )

    def test_update_stale_workers(self):
        """Test updating stale workers. Mirrors Go's TestUpdateStaleWorkers."""
        if self.db.instance is None:
            self.fail("Database instance is not initialized.")

        statuses = [
            WorkerStatus.READY,
            WorkerStatus.RUNNING,
            WorkerStatus.STOPPED,
            WorkerStatus.READY,
        ]
        workers: List[Worker] = []

        for i, status in enumerate(statuses):
            worker = Worker(
                name=f"worker-{i}",
                status=WorkerStatus.READY,  # Start as READY, will update after insert
                max_concurrency=3,
            )

            inserted_worker = self.worker_handler.insert_worker(worker)
            self.assertIsNotNone(inserted_worker)

            # Update status after insertion
            inserted_worker.status = status
            updated_worker = self.worker_handler.update_worker(inserted_worker)
            if updated_worker is not None:
                workers.append(updated_worker)

        stale_time = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
            hours=1
        )
        with self.db.instance.cursor() as cur:
            for i in range(3):
                cur.execute(
                    "UPDATE worker SET updated_at = %s WHERE rid = %s",
                    (stale_time, workers[i].rid),
                )
            self.db.instance.commit()

        # Test UpdateStaleWorkers - should update only stale READY and RUNNING workers
        stale_threshold = timedelta(minutes=10)
        updated_count = self.worker_handler.update_stale_workers(stale_threshold)
        self.assertEqual(updated_count, 2)  # Should update READY and RUNNING workers

        # Verify results
        for i, worker in enumerate(workers):
            updated_worker = self.worker_handler.select_worker(worker.rid)
            self.assertIsNotNone(updated_worker)
            if updated_worker is not None:
                if i < 2:  # First two workers (READY, RUNNING) should be STOPPED
                    self.assertEqual(updated_worker.status, WorkerStatus.STOPPED)
                elif i == 2:  # STOPPED worker should remain STOPPED
                    self.assertEqual(updated_worker.status, WorkerStatus.STOPPED)
                else:  # Fresh worker should remain READY
                    self.assertEqual(updated_worker.status, WorkerStatus.READY)

        # Clean up
        for worker in workers:
            self.worker_handler.delete_worker(worker.rid)

    def test_delete_worker(self):
        """Test deleting a worker. Mirrors Go's TestWorkerDeleteWorker."""
        # Create and insert a worker
        worker = Worker(name="Worker", status=WorkerStatus.READY, max_concurrency=1)

        inserted_worker = self.worker_handler.insert_worker(worker)
        self.assertIsNotNone(inserted_worker)

        # Delete worker
        self.worker_handler.delete_worker(inserted_worker.rid)

        # Verify that the worker was deleted
        deleted_worker = self.worker_handler.select_worker(inserted_worker.rid)
        self.assertIsNone(deleted_worker)

    def test_select_worker(self):
        """Test selecting a worker. Mirrors Go's TestWorkerSelectWorker."""
        # Create and insert a worker
        worker = Worker(name="Worker", status=WorkerStatus.READY, max_concurrency=1)

        inserted_worker = self.worker_handler.insert_worker(worker)
        self.assertIsNotNone(inserted_worker)

        # Select the worker
        selected_worker = self.worker_handler.select_worker(inserted_worker.rid)
        self.assertIsNotNone(selected_worker)
        if selected_worker is not None:
            self.assertEqual(inserted_worker.rid, selected_worker.rid)
            self.assertEqual(inserted_worker.name, selected_worker.name)

    def test_select_all_workers(self):
        """Test selecting all workers. Mirrors Go's TestWorkerSelectAllWorkers."""
        # Insert multiple workers
        for i in range(5):
            worker = Worker(
                name=f"Worker{i}", status=WorkerStatus.READY, max_concurrency=1
            )
            self.worker_handler.insert_worker(worker)

        # Test selecting all workers
        all_workers = self.worker_handler.select_all_workers(0, 10)
        self.assertEqual(len(all_workers), 5)

        # Test pagination
        page_length = 3
        paginated_workers = self.worker_handler.select_all_workers(0, page_length)
        self.assertEqual(len(paginated_workers), page_length)

    def test_select_all_workers_by_search(self):
        """Test selecting workers by search. Mirrors Go's TestWorkerSelectAllWorkersBySearch."""
        # Insert multiple workers with different names
        for i in range(5):
            worker = Worker(
                name=f"Worker{i}", status=WorkerStatus.READY, max_concurrency=1
            )
            self.worker_handler.insert_worker(worker)

        # Search for specific worker
        search_term = "Worker1"
        found_workers = self.worker_handler.select_all_workers_by_search(
            search_term, 0, 10
        )
        self.assertEqual(len(found_workers), 1)
        self.assertEqual(found_workers[0].name, "Worker1")

    def test_select_all_connections(self):
        """Test selecting all connections. Mirrors Go's TestWorkerSelectAllConnections."""
        # There should already be an active connection from our test database
        all_connections = self.worker_handler.select_all_connections()
        self.assertGreaterEqual(len(all_connections), 1)

        # Verify connection structure
        for connection in all_connections:
            self.assertIsNotNone(connection.pid)
            self.assertIsNotNone(connection.database)
            self.assertIsNotNone(connection.username)


if __name__ == "__main__":
    unittest.main()
