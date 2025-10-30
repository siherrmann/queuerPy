"""
Tests for ProcessRunner - CPU-intensive task execution.
"""

import asyncio
import time
import unittest
import math
import threading
from concurrent.futures import TimeoutError
from unittest.mock import Mock

from .process_runner import (
    ProcessRunner,
    new_process_runner,
    new_process_runner_from_job,
    _execute_task,
)


class MockJob:
    """Mock job object for testing."""

    def __init__(self, parameters=None):
        self.parameters = parameters or []


def cpu_intensive_task(n: int) -> int:
    """A CPU-intensive task for testing."""

    # Calculate prime numbers up to n (CPU-intensive)
    def is_prime(num):
        if num < 2:
            return False
        for i in range(2, int(math.sqrt(num)) + 1):
            if num % i == 0:
                return False
        return True

    primes = [num for num in range(2, n) if is_prime(num)]
    return len(primes)


def simple_task(x: int, y: int) -> int:
    """A simple task for testing."""
    return x + y


def failing_task():
    """A task that always fails."""
    raise ValueError("This task always fails")


def slow_task():
    """A slow task for testing."""
    time.sleep(0.5)
    return 42


def get_value():
    """Simple function that returns a value."""
    return 100


async def async_task(x: int) -> int:
    """An async task for testing."""
    import asyncio

    await asyncio.sleep(0.1)
    return x * 2


def fibonacci(n):
    """Calculate fibonacci number (inefficiently for CPU load)."""
    if n <= 1:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)


class TestProcessRunner(unittest.TestCase):
    """Test ProcessRunner functionality."""

    def setUp(self):
        """Clean up pool before each test."""
        ProcessRunner.shutdown_pool()

    def tearDown(self):
        """Clean up pool after each test."""
        ProcessRunner.shutdown_pool()

    def test_simple_function(self):
        """Test simple function execution."""
        runner = ProcessRunner(simple_task, [2, 3])
        self.assertTrue(runner.run())
        result = runner.get_results(timeout=5)
        self.assertEqual(result, 5)

    def test_cpu_intensive_task(self):
        """Test CPU-intensive task execution."""
        runner = ProcessRunner(cpu_intensive_task, [100])
        self.assertTrue(runner.run())
        result = runner.get_results(timeout=30)
        self.assertEqual(result, 25)  # 25 primes less than 100

    def test_async_task_execution(self):
        """Test async task execution."""
        runner = ProcessRunner(async_task, [10])
        self.assertTrue(runner.run())
        result = runner.get_results(timeout=10)
        self.assertEqual(result, 20)

    def test_failing_task(self):
        """Test error handling."""
        runner = ProcessRunner(failing_task, [])
        self.assertTrue(runner.run())

        with self.assertRaises(ValueError) as context:
            runner.get_results(timeout=10)
        self.assertIn("This task always fails", str(context.exception))

    def test_double_run_fails(self):
        """Test that running twice fails."""
        runner = ProcessRunner(simple_task, [1, 2])
        self.assertTrue(runner.run())
        self.assertFalse(runner.run())  # Second run should fail
        runner.get_results(timeout=5)

    def test_is_running(self):
        """Test is_running status."""
        runner = ProcessRunner(slow_task, [])
        self.assertFalse(runner.is_running())

        self.assertTrue(runner.run())
        self.assertTrue(runner.is_running())

        result = runner.get_results(timeout=5)
        self.assertEqual(result, 42)
        self.assertFalse(runner.is_running())

    def test_cancel_task(self):
        """Test task cancellation."""
        runner = ProcessRunner(cpu_intensive_task, [10000])
        self.assertTrue(runner.run())
        cancelled = runner.cancel()
        # Cancel might work if task hasn't started yet
        self.assertIsInstance(cancelled, bool)

    def test_cancel_not_started(self):
        """Test cancelling task that wasn't started."""
        runner = ProcessRunner(simple_task, [1, 2])
        self.assertFalse(runner.cancel())

    def test_timeout(self):
        """Test timeout functionality."""
        runner = ProcessRunner(cpu_intensive_task, [50000])
        self.assertTrue(runner.run())

        with self.assertRaises(TimeoutError):
            runner.get_results(timeout=0.1)

    def test_get_results_not_started(self):
        """Test getting results when not started."""
        runner = ProcessRunner(simple_task, [1, 2])

        with self.assertRaises(RuntimeError) as context:
            runner.get_results()
        self.assertEqual(str(context.exception), "Task not started")

    def test_shared_pool(self):
        """Test shared pool functionality."""
        runner1 = ProcessRunner(simple_task, [1, 2])
        runner2 = ProcessRunner(simple_task, [3, 4])

        pool1 = runner1._get_pool()
        pool2 = runner2._get_pool()

        # Should be the same pool
        self.assertIs(pool1, pool2)

    def test_thread_safety(self):
        """Test thread safety of pool creation."""
        pools = []

        def create_runner():
            runner = ProcessRunner(simple_task, [1, 2])
            pools.append(runner._get_pool())

        threads = [threading.Thread(target=create_runner) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All should reference same pool
        self.assertTrue(all(pool is pools[0] for pool in pools))

    def test_factory_functions(self):
        """Test factory functions."""
        # Test new_process_runner
        runner1 = new_process_runner(simple_task, [8, 2])
        self.assertTrue(runner1.run())
        result1 = runner1.get_results(timeout=5)
        self.assertEqual(result1, 10)

        # Test new_process_runner_from_job
        job = Mock()
        job.parameters = [6, 4]
        runner2 = new_process_runner_from_job(simple_task, job)
        self.assertTrue(runner2.run())
        result2 = runner2.get_results(timeout=5)
        self.assertEqual(result2, 10)

    def test_factory_from_job_no_parameters(self):
        """Test factory with job that has no parameters."""
        job = Mock(spec=[])  # No attributes
        runner = new_process_runner_from_job(get_value, job)
        self.assertEqual(runner.parameters, [])
        self.assertTrue(runner.run())
        result = runner.get_results(timeout=5)
        self.assertEqual(result, 100)


class TestExecuteTask(unittest.TestCase):
    """Test _execute_task helper function."""

    def test_sync_function(self):
        """Test sync function execution."""
        result = _execute_task(simple_task, [5, 3])
        self.assertEqual(result, 8)

    def test_async_function(self):
        """Test async function execution."""
        result = _execute_task(async_task, [4])
        self.assertEqual(result, 8)  # 4 * 2

    def test_exception_handling(self):
        """Test exception handling in _execute_task."""
        result = _execute_task(failing_task, [])
        self.assertIsInstance(result, ValueError)
        self.assertEqual(str(result), "This task always fails")


class TestProcessRunnerIntegration(unittest.TestCase):
    """Integration tests for ProcessRunner."""

    def setUp(self):
        """Clean up pool."""
        ProcessRunner.shutdown_pool()

    def tearDown(self):
        """Clean up pool."""
        ProcessRunner.shutdown_pool()

    def test_multiple_concurrent_runners(self):
        """Test multiple ProcessRunners running concurrently."""
        runners = []

        # Create multiple runners
        for i in range(3):
            runner = ProcessRunner(simple_task, [i, i + 1])
            runners.append(runner)
            self.assertTrue(runner.run())

        # Get results from all
        results = []
        for runner in runners:
            result = runner.get_results(timeout=10)
            results.append(result)

        # Check results
        expected = [1, 3, 5]  # [0+1, 1+2, 2+3]
        self.assertEqual(results, expected)

    def test_fibonacci_computation(self):
        """Test with Fibonacci computation."""
        runner = ProcessRunner(fibonacci, [20])
        self.assertTrue(runner.run())
        result = runner.get_results(timeout=30)
        self.assertEqual(result, 6765)  # 20th Fibonacci number


if __name__ == "__main__":
    unittest.main()
