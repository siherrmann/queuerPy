"""
Test cases for the asyncio-based scheduler - mirrors Go tests.
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
    """Test the asyncio-based scheduler implementation."""

    def test_create_scheduler(self):
        """Test creating a new scheduler."""
        scheduler = Scheduler()
        self.assertIsNotNone(scheduler, "Expected non-None scheduler")
        self.assertFalse(scheduler.is_running(), "Should not be running initially")

    def test_start_stop_scheduler(self):
        """Test starting and stopping the scheduler."""
        scheduler = Scheduler()

        # Start scheduler
        scheduler.start()
        self.assertTrue(scheduler.is_running(), "Should be running after start")

        # Stop scheduler
        scheduler.stop()
        time.sleep(0.1)  # Give time for stop
        self.assertFalse(scheduler.is_running(), "Should not be running after stop")

    def test_schedule_task(self):
        """Test scheduling a task."""
        scheduler = Scheduler()
        scheduler.start()

        executed = []

        def test_task(value):
            executed.append(value)

        try:
            # Schedule task to run after 0.1 seconds
            scheduler.schedule(0.1, test_task, "test_value")

            # Wait for execution with cooperative yielding and longer timeout
            start_time = time.time()
            while len(executed) == 0 and time.time() - start_time < 0.5:
                time.sleep(0.01)  # Yield to allow asyncio tasks to execute

            # Check if task was executed
            self.assertEqual(len(executed), 1, "Task should have been executed")
            self.assertEqual(executed[0], "test_value", "Expected correct value")

        finally:
            scheduler.stop()

    def test_schedule_at_specific_time(self):
        """Test scheduling a task at a specific time."""
        scheduler = Scheduler()
        scheduler.start()

        executed = []

        def test_task(value):
            executed.append(value)

        try:
            # Schedule task to run 0.1 seconds from now
            execute_time = datetime.now() + timedelta(seconds=0.1)
            scheduler.schedule_at(execute_time, test_task, "scheduled_value")

            # Wait for execution with cooperative yielding and longer timeout
            start_time = time.time()
            while len(executed) == 0 and time.time() - start_time < 0.5:
                time.sleep(0.01)  # Yield to allow asyncio tasks to execute

            # Check if task was executed
            self.assertEqual(len(executed), 1, "Task should have been executed")
            self.assertEqual(executed[0], "scheduled_value", "Expected correct value")

        finally:
            scheduler.stop()

    def test_schedule_without_start_raises_error(self):
        """Test that scheduling without starting raises error."""
        scheduler = Scheduler()

        def test_task():
            pass

        with self.assertRaises(RuntimeError):
            scheduler.schedule(0.1, test_task)


if __name__ == "__main__":
    unittest.main()
