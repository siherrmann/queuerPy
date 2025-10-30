"""
Test cases for the scheduler - mirrors Go implementation tests.
"""

import unittest
import time
import asyncio
import threading
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.scheduler import Scheduler
from datetime import datetime, timedelta


class TestScheduler(unittest.TestCase):
    """Test the scheduler implementation that mirrors Go's Scheduler."""

    def test_create_scheduler_directly(self):
        """Test creating a new scheduler using direct Scheduler instantiation."""
        executed = []

        def test_task(message):
            executed.append(message)

        # Test immediate execution (None start_time)
        scheduler = Scheduler(None, test_task, "immediate")
        self.assertIsNotNone(scheduler, "Expected non-None scheduler")
        self.assertEqual(scheduler.task, test_task, "Task should be stored correctly")
        self.assertEqual(
            scheduler.parameters, ["immediate"], "Parameters should be stored correctly"
        )
        self.assertIsNone(scheduler.start_time, "Start time should be None")

    def test_scheduler_immediate_execution(self):
        """Test scheduler with immediate execution (None start_time)."""
        executed = []

        def test_task(message):
            executed.append(message)

        scheduler = Scheduler(None, test_task, "immediate")
        scheduler.go()

        # Wait a bit for execution
        time.sleep(0.2)

        self.assertEqual(len(executed), 1, "Task should have been executed")
        self.assertEqual(executed[0], "immediate", "Expected correct message")

    def test_scheduler_delayed_execution(self):
        """Test scheduler with delayed execution."""
        executed = []
        start_time = time.time()

        def test_task(message):
            execution_time = time.time()
            executed.append((message, execution_time - start_time))

        # Schedule for 0.3 seconds from now
        future_time = datetime.now() + timedelta(seconds=0.3)
        scheduler = Scheduler(future_time, test_task, "delayed")
        scheduler.go()

        # Wait for execution
        time.sleep(0.5)

        self.assertEqual(len(executed), 1, "Task should have been executed")
        message, elapsed = executed[0]
        self.assertEqual(message, "delayed", "Expected correct message")
        self.assertGreaterEqual(
            elapsed, 0.25, "Should have waited at least 0.25 seconds"
        )
        self.assertLessEqual(
            elapsed, 0.5, "Should not have waited more than 0.5 seconds"
        )

    def test_scheduler_past_time_execution(self):
        """Test scheduler with past time (should execute immediately)."""
        executed = []

        def test_task(message):
            executed.append(message)

        # Use a time in the past
        past_time = datetime.now() - timedelta(seconds=10)
        scheduler = Scheduler(past_time, test_task, "past_time")
        scheduler.go()

        # Wait a bit for execution
        time.sleep(0.2)

        self.assertEqual(len(executed), 1, "Task should have been executed immediately")
        self.assertEqual(executed[0], "past_time", "Expected correct message")

    def test_scheduler_with_multiple_parameters(self):
        """Test scheduler with multiple parameters."""
        executed = []

        def test_task(a, b, c):
            executed.append(a + b + c)

        scheduler = Scheduler(None, test_task, 10, 20, 30)
        scheduler.go()

        time.sleep(0.2)

        self.assertEqual(len(executed), 1, "Task should have been executed")
        self.assertEqual(executed[0], 60, "Expected sum of parameters")

    def test_scheduler_without_event_loop(self):
        """Test scheduler when no event loop is running."""
        executed = []

        def test_task(message):
            executed.append(message)

        def run_in_thread():
            scheduler = Scheduler(None, test_task, "no_loop")
            scheduler.go()
            time.sleep(0.3)  # Give time for execution

        thread = threading.Thread(target=run_in_thread)
        thread.start()
        thread.join()

        self.assertEqual(len(executed), 1, "Task should have been executed in thread")
        self.assertEqual(executed[0], "no_loop", "Expected correct message")

    def test_scheduler_invalid_task_validation(self):
        """Test that scheduler validates task and parameters."""

        def valid_task(param):
            pass

        # Test with wrong number of parameters
        with self.assertRaises(ValueError):
            Scheduler(None, valid_task)  # Missing required parameter

        # Test with non-callable task
        with self.assertRaises(ValueError):
            Scheduler(None, "not_callable", "param")

    def test_scheduler_with_async_context(self):
        """Test scheduler in async context."""

        async def run_async_test():
            executed = []

            def test_task(value):
                executed.append(value * 2)

            # Schedule for 0.2 seconds from now
            future_time = datetime.now() + timedelta(seconds=0.2)
            scheduler = Scheduler(future_time, test_task, 42)
            scheduler.go()

            # Wait asynchronously
            await asyncio.sleep(0.4)

            self.assertEqual(len(executed), 1, "Task should have been executed")
            self.assertEqual(executed[0], 84, "Expected doubled value")

        # Run the async test
        asyncio.run(run_async_test())


if __name__ == "__main__":
    unittest.main()
