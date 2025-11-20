"""
Simple tests for the scheduler implementation.
"""

import unittest
import time
from datetime import datetime, timedelta
from core.scheduler import Scheduler


class TestScheduler(unittest.TestCase):
    """Test the simplified scheduler implementation."""

    def test_scheduler_immediate_execution(self):
        """Test scheduler with immediate execution (None start_time)."""
        executed: list[str] = []

        def test_task(message: str) -> None:
            executed.append(message)

        scheduler = Scheduler(test_task, None, "immediate")
        scheduler.go()

        # Wait a bit for execution
        time.sleep(0.2)

        self.assertEqual(len(executed), 1)
        self.assertEqual(executed[0], "immediate")

    def test_scheduler_delayed_execution(self):
        """Test scheduler with delayed execution."""
        executed: list[tuple[str, float]] = []
        start_time = time.time()

        def test_task(message: str) -> None:
            execution_time = time.time()
            executed.append((message, execution_time - start_time))

        # Schedule for 0.3 seconds from now
        future_time = datetime.now() + timedelta(seconds=0.3)
        scheduler = Scheduler(test_task, future_time, "delayed")
        scheduler.go()

        # Wait for execution
        time.sleep(0.5)

        self.assertEqual(len(executed), 1)
        message, elapsed = executed[0]
        self.assertEqual(message, "delayed")
        self.assertGreaterEqual(
            elapsed, 0.25
        )  # Should have waited at least 0.25 seconds
        self.assertLessEqual(
            elapsed, 0.5
        )  # Should not have waited more than 0.5 seconds

    def test_scheduler_past_time_execution(self):
        """Test scheduler with past time (should execute immediately)."""
        executed: list[str] = []

        def test_task(message: str) -> None:
            executed.append(message)

        # Use a time in the past
        past_time = datetime.now() - timedelta(seconds=10)
        scheduler = Scheduler(test_task, past_time, "past_time")
        scheduler.go()

        # Wait a bit for execution
        time.sleep(0.2)

        self.assertEqual(len(executed), 1)
        self.assertEqual(executed[0], "past_time")

    def test_scheduler_with_multiple_args(self):
        """Test scheduler with multiple positional arguments."""
        executed: list[int] = []

        def test_task(a: int, b: int, c: int) -> None:
            executed.append(a + b + c)

        scheduler = Scheduler(test_task, None, 10, 20, 30)
        scheduler.go()

        time.sleep(0.2)

        self.assertEqual(len(executed), 1)
        self.assertEqual(executed[0], 60)

    def test_scheduler_with_no_parameters(self):
        """Test scheduler with task that takes no parameters."""
        executed: list[str] = []

        def test_task() -> None:
            executed.append("no_params")

        scheduler = Scheduler(test_task, None)
        scheduler.go()

        time.sleep(0.2)

        self.assertEqual(len(executed), 1)
        self.assertEqual(executed[0], "no_params")

    def test_scheduler_invalid_task_validation(self):
        """Test that scheduler validates task and parameters."""

        def valid_task(_: str) -> None:
            pass

        # Test with wrong number of parameters
        with self.assertRaises(ValueError):
            Scheduler(valid_task, None)  # Missing required parameter

    def test_scheduler_properties(self):
        """Test scheduler properties are set correctly."""

        def test_task(_: int, __: int) -> None:
            pass

        start_time = datetime.now() + timedelta(seconds=1)
        scheduler = Scheduler(test_task, start_time, 1, 2)

        self.assertEqual(scheduler.task, test_task)
        self.assertEqual(scheduler.args, [1, 2])
        self.assertEqual(scheduler.kwargs, {})
        self.assertEqual(scheduler.start_time, start_time)


if __name__ == "__main__":
    unittest.main()
