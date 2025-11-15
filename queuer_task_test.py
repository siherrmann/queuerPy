"""
Tests for task-related methods in the Python queuer implementation.
Mirrors Go's queuerTask_test.go functionality.
"""

import unittest
import pytest
from model.task import Task
from queuer import new_queuer_with_db
from helper.test_database import DatabaseTestMixin


def task_1_test(data):
    """Global test task function."""
    return f"Processed: {data}"


def task_2_test(data):
    """Global custom task function."""
    return f"Custom: {data}"


class TestQueuerTask(DatabaseTestMixin, unittest.TestCase):
    """Test cases for queuer task functionality."""

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

    def test_add_task_success(self):
        """Test successfully adding a task."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        task = queuer.add_task(task_1_test)

        self.assertIsNotNone(task)
        self.assertEqual(task.name, "task_1_test")
        self.assertIn("task_1_test", queuer.tasks)
        self.assertIn("task_1_test", queuer.worker.available_tasks)

        queuer.stop()

    def test_add_task_creation_error(self):
        """Test error during task creation."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)

        # Test with invalid function (None)
        with pytest.raises(Exception):
            queuer.add_task(None)

        queuer.stop()

    def test_add_task_already_exists(self):
        """Test adding a task that already exists."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)

        queuer.add_task(task_1_test)

        with pytest.raises(RuntimeError, match="Task already exists"):
            queuer.add_task(task_1_test)

        queuer.stop()

    def test_add_task_with_name_success(self):
        """Test successfully adding a task with a specific name."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        result: Task = queuer.add_task_with_name(task_1_test, "custom_name")

        self.assertIsNotNone(result)
        self.assertEqual(result.name, "custom_name")
        self.assertIn("custom_name", queuer.tasks)
        self.assertIn("custom_name", queuer.worker.available_tasks)

        queuer.stop()

    def test_add_task_with_name_creation_error(self):
        """Test error during task creation with name."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)

        # Test with invalid function (None)
        with pytest.raises(Exception):
            queuer.add_task_with_name(None, "custom_name")

        queuer.stop()

    def test_add_task_with_name_already_exists(self):
        """Test adding a task with name that already exists."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task_with_name(task_1_test, "custom_name")

        # Try adding another task with the same name
        with pytest.raises(RuntimeError, match="Task already exists"):
            queuer.add_task_with_name(task_1_test, "custom_name")

        queuer.stop()

    def test_add_multiple_tasks(self):
        """Test adding multiple different tasks."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_task(task_1_test)
        queuer.add_task(task_2_test)

        self.assertIn("task_1_test", queuer.tasks)
        self.assertIn("task_2_test", queuer.tasks)
        self.assertIn("task_1_test", queuer.worker.available_tasks)
        self.assertIn("task_2_test", queuer.worker.available_tasks)
        self.assertEqual(len(queuer.worker.available_tasks), 2)

        queuer.stop()
