"""
Test for Worker Database Handler using testcontainers.
"""

import unittest
from datetime import datetime, timedelta
from uuid import uuid4

from database.db_worker import WorkerDBHandler
from helper.test_database import DatabaseTestMixin
from model.worker import Worker, WorkerStatus


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
    
    def test_create_worker_handler(self):
        """Test creating a worker handler."""
        self.assertIsNotNone(self.worker_handler)
        self.assertEqual(self.worker_handler.db, self.db)
        self.assertTrue(self.worker_handler.check_table_existence())
    
    def test_insert_worker(self):
        """Test inserting a worker."""
        # Create a test worker
        worker = Worker(
            name="test_worker",
            status=WorkerStatus.READY,
            available_tasks=["task1", "task2"],
            max_concurrency=2
        )
        
        # Insert worker
        inserted_worker = self.worker_handler.insert_worker(worker)
        self.assertIsNotNone(inserted_worker)
        self.assertIsNotNone(inserted_worker.id)
        self.assertIsNotNone(inserted_worker.rid)
        self.assertEqual(inserted_worker.name, "test_worker")
        self.assertEqual(inserted_worker.status, WorkerStatus.READY)
        # New workers start with empty task lists - tasks are added via updates
        self.assertEqual(len(inserted_worker.available_tasks), 0)
        self.assertEqual(inserted_worker.max_concurrency, 2)
    
    def test_update_worker(self):
        """Test updating a worker."""
        # Create and insert a worker
        worker = Worker(
            name="update_test_worker",
            status=WorkerStatus.READY,
            available_tasks=["task1"],
            max_concurrency=1
        )
        
        inserted_worker = self.worker_handler.insert_worker(worker)
        self.assertIsNotNone(inserted_worker)
        
        # Update worker status and concurrency
        inserted_worker.status = WorkerStatus.RUNNING
        inserted_worker.max_concurrency = 3
        inserted_worker.available_tasks = ["task1", "task2", "task3"]
        
        updated_worker = self.worker_handler.update_worker(inserted_worker)
        self.assertIsNotNone(updated_worker)
        self.assertEqual(updated_worker.status, WorkerStatus.RUNNING)
        self.assertEqual(updated_worker.max_concurrency, 3)
        self.assertEqual(len(updated_worker.available_tasks), 3)

    def test_delete_worker(self):
        """Test retrieving all workers with pagination."""
        # Create multiple test workers
        workers = []
        for i in range(3):
            worker = Worker(
                name=f"worker_{i}",
                status=WorkerStatus.READY,
                available_tasks=[f"task_{i}"],
                max_concurrency=1
            )
            inserted_worker = self.worker_handler.insert_worker(worker)
            workers.append(inserted_worker)
        
        # Retrieve all workers
        all_workers = self.worker_handler.select_all_workers()
        self.assertGreaterEqual(len(all_workers), 3)
        
        # Check that our workers are in the results
        worker_names = [worker.name for worker in all_workers]
        for i in range(3):
            self.assertIn(f"worker_{i}", worker_names)
    
    def test_delete_worker(self):
        """Test deleting a worker."""
        # Create and insert a worker
        worker = Worker(
            name="delete_me",
            status=WorkerStatus.READY,
            available_tasks=["task1"],
            max_concurrency=1
        )
        
        inserted_worker = self.worker_handler.insert_worker(worker)
        self.assertIsNotNone(inserted_worker)
        
        # Delete worker
        deleted_count = self.worker_handler.delete_worker(inserted_worker.rid)
        self.assertEqual(deleted_count, 1)

    def test_worker_with_multiple_tasks(self):
        """Test worker with multiple available tasks."""
        tasks = ["task1", "task2", "task3", "complex_task", "async_task"]
        
        worker = Worker(
            name="multi_task_worker",
            status=WorkerStatus.READY,
            available_tasks=tasks,
            max_concurrency=5
        )
        
        # Insert worker  
        inserted_worker = self.worker_handler.insert_worker(worker)
        self.assertIsNotNone(inserted_worker)
        
        # Now update the worker with tasks (this is how tasks are actually added)
        inserted_worker.available_tasks = tasks
        updated_worker = self.worker_handler.update_worker(inserted_worker)
        
        # Verify the update worked
        self.assertIsNotNone(updated_worker)
        self.assertEqual(len(updated_worker.available_tasks), len(tasks))
        for task in tasks:
            self.assertIn(task, updated_worker.available_tasks)

    def test_worker_table_existence(self):
        """Test checking worker table existence."""
        exists = self.worker_handler.check_table_existence()
        self.assertTrue(exists)

    def test_update_stale_workers(self):
        """Test updating stale workers."""
        # This test would require workers with old timestamps
        # For now, just test that the method exists and runs
        stale_count = self.worker_handler.update_stale_workers()
        self.assertIsInstance(stale_count, int)
        self.assertGreaterEqual(stale_count, 0)


if __name__ == '__main__':
    unittest.main()