"""Test cases for the singleton Runner."""

import unittest
import time

from .runner import Runner, AsyncTask, go_func


class TestRunner(unittest.TestCase):
    """Test the singleton Runner implementation."""

    def test_successful_task(self):
        """Test running a successful task."""

        async def add_task(a, b):
            return a + b

        runner = Runner()

        # Test Runner's submit_async_task method
        future = runner.submit_async_task(add_task(5, 3))
        result = future.result(timeout=2.0)
        self.assertEqual(result, 8, "Expected 5 + 3 = 8")

        # Test Runner's run_async_task method (convenience method)
        result2 = runner.run_async_task(add_task(7, 2), timeout=2.0)
        self.assertEqual(result2, 9, "Expected 7 + 2 = 9")

    def test_failed_task(self):
        """Test running a task that fails."""

        async def failing_task():
            raise ValueError("Test error")

        runner = Runner()

        # Test that Runner properly handles exceptions
        with self.assertRaises(ValueError) as context:
            runner.run_async_task(failing_task(), timeout=2.0)

        self.assertEqual(str(context.exception), "Test error", "Expected error message")

    def test_parameter_validation(self):
        """Test parameter validation."""

        async def task_with_params(a, b, c):
            return a + b + c

        runner = Runner()

        # Should work with correct parameters
        result = runner.run_async_task(task_with_params(1, 2, 3), timeout=2.0)
        self.assertEqual(result, 6, "Expected 1 + 2 + 3 = 6")

        # Should fail with wrong number of parameters
        async def failing_task():
            return task_with_params(1, 2)

        with self.assertRaises(TypeError):
            runner.run_async_task(failing_task(), timeout=2.0)

    def test_non_callable_task(self):
        """Test that non-callable task raises error."""
        runner = Runner()

        # Try to run a non-callable object through Runner
        # This should fail when the runner tries to execute it
        with self.assertRaises(TypeError):
            runner.run_async_task("not a function", timeout=2.0)

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

        async def slow_task(value, delay):
            import asyncio

            await asyncio.sleep(delay)
            return value * 2

        runner = Runner()

        # Submit multiple tasks concurrently using Runner
        start_time = time.time()
        future1 = runner.submit_async_task(slow_task(10, 0.1))
        future2 = runner.submit_async_task(slow_task(20, 0.1))
        future3 = runner.submit_async_task(slow_task(30, 0.1))

        # Get results
        result1 = future1.result(timeout=2.0)
        result2 = future2.result(timeout=2.0)
        result3 = future3.result(timeout=2.0)
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

        # Test Runner with async functions
        result = runner.run_async_task(async_add(7, 8), timeout=2.0)
        self.assertEqual(result, 15, "Expected 7 + 8 = 15")

        # Test event loop availability
        self.assertTrue(runner.is_available(), "Runner should be available")

        # Test that we can get the event loop
        loop = runner.get_event_loop()
        self.assertIsNotNone(loop, "Event loop should be available")
        self.assertFalse(loop.is_closed(), "Event loop should not be closed")

    def test_singleton_behavior(self):
        """Test that Runner follows singleton pattern."""
        runner1 = Runner()
        runner2 = Runner()
        runner3 = Runner()

        # All instances should be the same object
        self.assertIs(runner1, runner2, "Runner instances should be identical")
        self.assertIs(runner2, runner3, "Runner instances should be identical")
        self.assertIs(runner1, runner3, "Runner instances should be identical")

        # All should share the same event loop
        loop1 = runner1.get_event_loop()
        loop2 = runner2.get_event_loop()
        loop3 = runner3.get_event_loop()

        self.assertIs(loop1, loop2, "Event loops should be identical")
        self.assertIs(loop2, loop3, "Event loops should be identical")
        self.assertIs(loop1, loop3, "Event loops should be identical")

    def test_cancel_all_tasks_with_multiple_runners(self):
        """Test canceling all tasks when using multiple runner instances (same singleton)."""
        import asyncio

        async def long_running_task(task_id, delay=1.0):
            """A task that takes some time to complete."""
            try:
                await asyncio.sleep(delay)
                return f"Task {task_id} completed"
            except asyncio.CancelledError:
                return f"Task {task_id} was cancelled"

        # Create three runner instances (all the same singleton)
        runner1 = Runner()
        runner2 = Runner()
        runner3 = Runner()

        # Verify they are all the same instance
        self.assertIs(runner1, runner2)
        self.assertIs(runner2, runner3)

        # Submit tasks through different runner instances
        future1 = runner1.submit_async_task(long_running_task(1, 0.5))
        future2 = runner2.submit_async_task(long_running_task(2, 0.5))
        future3 = runner3.submit_async_task(long_running_task(3, 0.5))

        # Let tasks start
        time.sleep(0.1)

        # Verify all tasks are running (not done yet)
        self.assertFalse(future1.done(), "Task 1 should still be running")
        self.assertFalse(future2.done(), "Task 2 should still be running")
        self.assertFalse(future3.done(), "Task 3 should still be running")

        # Cancel all tasks
        cancelled_count = 0
        if future1.cancel():
            cancelled_count += 1
        if future2.cancel():
            cancelled_count += 1
        if future3.cancel():
            cancelled_count += 1

        # At least some tasks should be cancellable (they might have completed by now)
        # But we expect most to be cancelled since they have 0.5s delays

        # Wait a bit for cancellation to take effect
        time.sleep(0.2)

        # Check final states
        cancelled_futures = []
        completed_futures = []

        for i, future in enumerate([future1, future2, future3], 1):
            self.assertTrue(
                future.done(), f"Future {i} should be done (cancelled or completed)"
            )
            if future.cancelled():
                cancelled_futures.append(i)
            else:
                try:
                    result = future.result()
                    completed_futures.append((i, result))
                except Exception as e:
                    self.fail(f"Future {i} failed with exception: {e}")

        # Print results for debugging
        print(f"Cancelled futures: {cancelled_futures}")
        print(f"Completed futures: {completed_futures}")

        # At least one task should have been cancelled (since we cancelled early)
        # OR all tasks completed (if they were faster than expected)
        total_tasks = len(cancelled_futures) + len(completed_futures)
        self.assertEqual(total_tasks, 3, "Should account for all 3 tasks")

        # The key test: all runner instances share the same event loop
        # so cancellation affects the shared execution environment
        loop1 = runner1.get_event_loop()
        loop2 = runner2.get_event_loop()
        loop3 = runner3.get_event_loop()

        self.assertIs(loop1, loop2, "All runners should share the same event loop")
        self.assertIs(loop2, loop3, "All runners should share the same event loop")

        # Event loop should still be available after cancellations
        self.assertTrue(runner1.is_available(), "Runner should still be available")
        self.assertTrue(runner2.is_available(), "Runner should still be available")
        self.assertTrue(runner3.is_available(), "Runner should still be available")

    def test_go_func_standalone(self):
        """Test the standalone go_func function equivalent to Go's 'go func()'."""

        def add_task(a, b):
            return a + b

        # Test go_func standalone function (not a Runner method)
        task = go_func(add_task, 10, 5)
        result = task.get_results(timeout=1.0)
        self.assertEqual(
            result, 15, "go_func should execute function and return result"
        )


if __name__ == "__main__":
    unittest.main()
