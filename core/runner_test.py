"""Test cases for the multiprocessing Runner."""

import unittest
import time
import asyncio

from .runner import Runner, SmallRunner, go_func

# Module-level functions for multiprocessing compatibility


async def task_async(a, b):
    """Async task returning the sum and waiting for <a> seconds."""
    await asyncio.sleep(a)
    return a + b


def task_sync(a, b):
    """Sync task returning the sum and waiting for <a> seconds."""
    time.sleep(a)
    return a + b


async def failing_task_async():
    """Async task that fails."""
    raise ValueError("This task always fails")


def failing_task_sync():
    """Sync task that fails."""
    raise ValueError("This task always fails")


class TestRunner(unittest.TestCase):
    """Test the singleton Runner implementation."""

    def test_successful_task(self):
        """Test running a successful task."""
        # Test async task
        runner = Runner(task_async, (0.1, 3), {})
        runner.go()
        result = runner.get_results(timeout=2.0)
        self.assertEqual(result, 3.1, "Expected 0.1 + 3 = 3.1")

        # Test sync task
        runner2 = Runner(task_sync, (0.1, 2), {})
        runner2.go()
        result2 = runner2.get_results(timeout=2.0)
        self.assertEqual(result2, 2.1, "Expected 0.1 + 2 = 2.1")

    def test_failed_task(self):
        """Test running a task that fails."""
        # Test async failing task
        runner = Runner(failing_task_async, (), {})
        runner.go()

        # Test that Runner properly handles exceptions
        with self.assertRaises(ValueError) as context:
            runner.get_results(timeout=2.0)

        self.assertEqual(
            str(context.exception), "This task always fails", "Expected error message"
        )

    def test_parameter_validation(self):
        """Test parameter validation."""
        # Should work with correct parameters
        runner = Runner(task_async, (0.1, 5), {})
        runner.go()
        result = runner.get_results(timeout=2.0)
        self.assertEqual(result, 5.1, "Expected 0.1 + 5 = 5.1")

        # Should fail with wrong number of parameters
        runner2 = Runner(task_async, (1,), {})  # Missing one parameter
        runner2.go()
        with self.assertRaises(TypeError):
            runner2.get_results(timeout=2.0)

    def test_non_callable_task(self):
        """Test that non-callable task raises error."""
        # Try to run a non-callable object through Runner
        # This should fail during construction or execution
        with self.assertRaises((TypeError, AttributeError)):
            runner = Runner("not a function", (), {})
            runner.go()
            runner.get_results(timeout=2.0)

    def test_multiple_concurrent_tasks(self):
        """Test running multiple tasks concurrently."""
        # Start multiple runners concurrently
        start_time = time.time()
        runner1 = Runner(task_async, (0.1, 10), {})
        runner2 = Runner(task_async, (0.1, 20), {})
        runner3 = Runner(task_async, (0.1, 30), {})

        # Start all runners
        runner1.go()
        runner2.go()
        runner3.go()

        # Get results
        result1 = runner1.get_results(timeout=2.0)
        result2 = runner2.get_results(timeout=2.0)
        result3 = runner3.get_results(timeout=2.0)
        end_time = time.time()

        # Check results
        self.assertEqual(result1, 10.1, "Expected 0.1 + 10 = 10.1")
        self.assertEqual(result2, 20.1, "Expected 0.1 + 20 = 20.1")
        self.assertEqual(result3, 30.1, "Expected 0.1 + 30 = 30.1")

        # Should complete faster than sequential execution (< 1.0s vs 3*0.1=0.3s sequential)
        total_time = end_time - start_time
        self.assertLess(
            total_time,
            1.0,
            f"Concurrent execution took {total_time}s, should be < 1.0s (sequential would be ~0.3s)",
        )

        # Runners should be processes, not threads (have pid attribute)
        self.assertTrue(
            hasattr(runner1, "pid"), "Runner should have pid (it's a process)"
        )
        self.assertIsNotNone(runner1.pid, "Runner should have a process ID")

        # Threads should be finished
        self.assertFalse(runner1.is_alive(), "Runner 1 should have finished")
        self.assertFalse(runner2.is_alive(), "Runner 2 should have finished")
        self.assertFalse(runner3.is_alive(), "Runner 3 should have finished")


class TestSmallRunner(unittest.TestCase):
    """Test the threading-based SmallRunner implementation."""

    def test_successful_task(self):
        """Test running a successful task with SmallRunner."""
        # Test async task
        runner = SmallRunner(task_async, (0.1, 3), {})
        runner.go()
        result = runner.get_results(timeout=2.0)
        self.assertEqual(result, 3.1, "Expected 0.1 + 3 = 3.1")

        # Test sync task
        runner2 = SmallRunner(task_sync, (0.1, 2), {})
        runner2.go()
        result2 = runner2.get_results(timeout=2.0)
        self.assertEqual(result2, 2.1, "Expected 0.1 + 2 = 2.1")

    def test_failed_task(self):
        """Test running a task that fails with SmallRunner."""
        # Test async failing task
        runner = SmallRunner(failing_task_async, (), {})
        runner.go()

        # Test that SmallRunner properly handles exceptions
        with self.assertRaises(ValueError) as context:
            runner.get_results(timeout=2.0)

        self.assertEqual(
            str(context.exception), "This task always fails", "Expected error message"
        )

    def test_parameter_validation(self):
        """Test parameter validation with SmallRunner."""
        # Should work with correct parameters
        runner = SmallRunner(task_async, (0.1, 5), {})
        runner.go()
        result = runner.get_results(timeout=2.0)
        self.assertEqual(result, 5.1, "Expected 0.1 + 5 = 5.1")

        # Should fail with wrong number of parameters
        runner2 = SmallRunner(task_async, (1,), {})  # Missing one parameter
        runner2.go()
        with self.assertRaises(TypeError):
            runner2.get_results(timeout=2.0)

    def test_non_callable_task(self):
        """Test that non-callable task raises error in SmallRunner."""
        # Try to run a non-callable object through SmallRunner
        # This should fail during construction or execution
        with self.assertRaises((TypeError, AttributeError)):
            runner = SmallRunner("not a function", (), {})
            runner.go()
            runner.get_results(timeout=2.0)

    def test_multiple_concurrent_tasks(self):
        """Test running multiple SmallRunner tasks concurrently."""
        # Start multiple SmallRunners concurrently
        start_time = time.time()
        runner1 = SmallRunner(task_async, (0.1, 10), {})
        runner2 = SmallRunner(task_async, (0.1, 20), {})
        runner3 = SmallRunner(task_async, (0.1, 30), {})

        # Start all runners
        runner1.go()
        runner2.go()
        runner3.go()

        # Get results
        result1 = runner1.get_results(timeout=2.0)
        result2 = runner2.get_results(timeout=2.0)
        result3 = runner3.get_results(timeout=2.0)
        end_time = time.time()

        # Check results
        self.assertEqual(result1, 10.1, "Expected 0.1 + 10 = 10.1")
        self.assertEqual(result2, 20.1, "Expected 0.1 + 20 = 20.1")
        self.assertEqual(result3, 30.1, "Expected 0.1 + 30 = 30.1")

        # Should complete faster than sequential execution (threading has less overhead than multiprocessing)
        total_time = end_time - start_time
        self.assertLess(
            total_time,
            0.5,
            f"Concurrent threading execution took {total_time}s, should be < 0.5s",
        )

        # SmallRunners should be threads, not processes (no pid attribute)
        self.assertFalse(
            hasattr(runner1, "pid"), "SmallRunner should not have pid (it's a thread)"
        )
        self.assertTrue(
            hasattr(runner1, "ident"), "SmallRunner should have thread ident"
        )

        # Threads should be finished
        self.assertFalse(runner1.is_alive(), "SmallRunner 1 should have finished")
        self.assertFalse(runner2.is_alive(), "SmallRunner 2 should have finished")
        self.assertFalse(runner3.is_alive(), "SmallRunner 3 should have finished")


class TestGoFunc(unittest.TestCase):
    """Test the go_func convenience function."""

    def test_go_func_threading(self):
        """Test the go_func function with threading mode."""
        runner1 = go_func(task_sync, False, 0.2, 1)
        runner2 = go_func(task_sync, False, 0.2, 2)
        runner3 = go_func(task_sync, False, 0.2, 3)

        result1 = runner1.get_results(timeout=2.0)
        result2 = runner2.get_results(timeout=2.0)
        result3 = runner3.get_results(timeout=2.0)

        self.assertEqual(result1, 1.2, "Expected 0.2 + 1 = 1.2")
        self.assertEqual(result2, 2.2, "Expected 0.2 + 2 = 2.2")
        self.assertEqual(result3, 3.2, "Expected 0.2 + 3 = 3.2")

    def test_go_func_multiprocessing(self):
        """Test the go_func function with multiprocessing mode."""
        runner1 = go_func(task_sync, True, 0.2, 1)
        runner2 = go_func(task_sync, True, 0.2, 2)
        runner3 = go_func(task_sync, True, 0.2, 3)

        result1 = runner1.get_results(timeout=2.0)
        result2 = runner2.get_results(timeout=2.0)
        result3 = runner3.get_results(timeout=2.0)

        self.assertEqual(result1, 1.2, "Expected 0.2 + 1 = 1.2")
        self.assertEqual(result2, 2.2, "Expected 0.2 + 2 = 2.2")
        self.assertEqual(result3, 3.2, "Expected 0.2 + 3 = 3.2")


if __name__ == "__main__":
    unittest.main()
