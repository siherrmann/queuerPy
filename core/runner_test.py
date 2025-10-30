"""
Test cases for the singleton Runner - mirrors Go tests.
"""

import unittest
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.runner import Runner, AsyncTask, go_func


class TestRunner(unittest.TestCase):
    """Test the singleton Runner implementation."""

    def test_successful_task(self):
        """Test running a successful task."""

        def add_task(a, b):
            return a + b

        runner = Runner()
        async_task = AsyncTask(add_task, [5, 3])

        # Run the task
        success = async_task.run()
        self.assertTrue(success, "Task should start successfully")

        # Get results
        result = async_task.get_results(timeout=2.0)
        self.assertEqual(result, 8, "Expected 5 + 3 = 8")

        # Check completion status
        self.assertTrue(async_task.is_done(), "Task should be completed")

    def test_failed_task(self):
        """Test running a task that fails."""

        def failing_task():
            raise ValueError("Test error")

        runner = Runner()
        async_task = AsyncTask(failing_task, [])

        # Run the task
        success = async_task.run()
        self.assertTrue(success, "Task should start successfully")

        # Get results (should raise the exception)
        with self.assertRaises(ValueError) as context:
            async_task.get_results(timeout=2.0)

        self.assertEqual(str(context.exception), "Test error", "Expected error message")

    def test_parameter_validation(self):
        """Test parameter validation."""

        def task_with_params(a, b, c):
            return a + b + c

        runner = Runner()

        # Should work with correct parameters
        async_task_correct = AsyncTask(task_with_params, [1, 2, 3])
        success = async_task_correct.run()
        self.assertTrue(success, "Task should start successfully")
        result = async_task_correct.get_results(timeout=2.0)
        self.assertEqual(result, 6, "Expected 1 + 2 + 3 = 6")

        # Should fail with wrong number of parameters
        async_task_wrong = AsyncTask(task_with_params, [1, 2])  # Missing one parameter
        success = async_task_wrong.run()
        self.assertTrue(success, "Task should start successfully")

        with self.assertRaises(TypeError):
            async_task_wrong.get_results(timeout=2.0)

    def test_non_callable_task(self):
        """Test that non-callable task raises error."""
        # AsyncTask doesn't validate callability at construction
        # But will fail when trying to execute
        async_task = AsyncTask("not a function", [])
        success = async_task.run()
        self.assertTrue(success, "Task should start")  # It starts but will fail

        # Should fail when getting results
        with self.assertRaises(TypeError):
            async_task.get_results(timeout=2.0)

    def test_go_func_pattern(self):
        """Test Go-like go_func pattern."""

        def multiply_task(a, b):
            return a * b

        # Test go_func convenience function
        task = go_func(multiply_task, 4, 6)
        result = task.get_results(timeout=2.0)
        self.assertEqual(result, 24, "Expected 4 * 6 = 24")

    def test_multiple_concurrent_tasks(self):
        """Test running multiple tasks concurrently."""

        def slow_task(value, delay):
            time.sleep(delay)
            return value * 2

        runner = Runner()

        # Create multiple tasks
        task1 = AsyncTask(slow_task, [10, 0.1])
        task2 = AsyncTask(slow_task, [20, 0.1])
        task3 = AsyncTask(slow_task, [30, 0.1])

        # Start all tasks
        start_time = time.time()
        self.assertTrue(task1.run(), "Task 1 should start")
        self.assertTrue(task2.run(), "Task 2 should start")
        self.assertTrue(task3.run(), "Task 3 should start")

        # Get results
        result1 = task1.get_results(timeout=2.0)
        result2 = task2.get_results(timeout=2.0)
        result3 = task3.get_results(timeout=2.0)
        end_time = time.time()

        # Check results
        self.assertEqual(result1, 20, "Expected 10 * 2 = 20")
        self.assertEqual(result2, 40, "Expected 20 * 2 = 40")
        self.assertEqual(result3, 60, "Expected 30 * 2 = 60")

        # Should complete faster than sequential execution (< 0.25s vs 0.3s)
        total_time = end_time - start_time
        self.assertLess(
            total_time,
            0.25,
            f"Concurrent execution took {total_time}s, should be < 0.25s",
        )

    def test_async_function(self):
        """Test running async functions."""
        import asyncio

        async def async_add(a, b):
            await asyncio.sleep(0.1)
            return a + b

        runner = Runner()
        async_task = AsyncTask(async_add, [7, 8])

        success = async_task.run()
        self.assertTrue(success, "Task should start successfully")
        result = async_task.get_results(timeout=2.0)

        self.assertEqual(result, 15, "Expected 7 + 8 = 15")


if __name__ == "__main__":
    unittest.main()
