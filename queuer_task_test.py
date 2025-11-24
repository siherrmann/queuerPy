"""
Tests for task-related methods in the Python queuer implementation.
Mirrors Go's queuerTask_test.go functionality.
"""

import unittest
import pytest
from model.task import Task
from queuer import new_queuer_with_db
from helper.test_database import DatabaseTestMixin


def task_1_test(data: str) -> str:
    """Global test task function."""
    return f"Processed: {data}"


def task_2_test(data: str) -> str:
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
        if task:
            self.assertEqual(task.name, "task_1_test")
            self.assertIn("task_1_test", queuer.tasks)
            self.assertIn("task_1_test", queuer.worker.available_tasks)

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

    def test_task_decorator_without_name(self):
        """Test the @queuer.task() decorator without specifying a name."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)

        @queuer.task()
        def decorated_task(data: str) -> str:
            """Test task added via decorator."""
            return f"Decorated: {data}"

        # Verify the task was registered with the function name
        self.assertIn("decorated_task", queuer.tasks)
        self.assertIn("decorated_task", queuer.worker.available_tasks)

        # Verify the original function is still callable
        result = decorated_task("test")
        self.assertEqual(result, "Decorated: test")

        # Verify the task object is correct
        task = queuer.tasks["decorated_task"]
        self.assertEqual(task.name, "decorated_task")
        self.assertEqual(task.task, decorated_task)

        queuer.stop()

    def test_task_decorator_with_name(self):
        """Test the @queuer.task_with_name(name="custom_name") decorator with a custom name."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)

        @queuer.task_with_name(name="my_custom_task")
        def some_function(data: str) -> str:
            """Test task with custom name via decorator."""
            return f"Custom: {data}"

        # Verify the task was registered with the custom name
        self.assertIn("my_custom_task", queuer.tasks)
        self.assertNotIn(
            "some_function", queuer.tasks
        )  # Original function name should not be used
        self.assertIn("my_custom_task", queuer.worker.available_tasks)

        # Verify the original function is still callable
        result = some_function("test")
        self.assertEqual(result, "Custom: test")

        # Verify the task object is correct
        task = queuer.tasks["my_custom_task"]
        self.assertEqual(task.name, "my_custom_task")
        self.assertEqual(task.task, some_function)

        queuer.stop()
