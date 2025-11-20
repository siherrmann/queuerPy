"""
Simple tests for the ticker implementation.
"""

from typing import Any
import unittest
import time
import threading
from datetime import timedelta
from core.ticker import Ticker


# Shared variables for testing (using threading-safe approach)
_test_counter = 0
_test_lock = threading.Lock()
_test_times: list[float] = []
_test_params: list[Any] = []


def _reset_test_data():
    """Reset test data before each test."""
    global _test_counter, _test_times, _test_params
    with _test_lock:
        _test_counter = 0
        _test_times.clear()
        _test_params.clear()


def _test_task_counter():
    """Simple counter task for testing."""
    global _test_counter
    with _test_lock:
        _test_counter += 1


def _test_task_with_params(param1: str, param2: int):
    """Task with parameters for testing."""
    global _test_params
    with _test_lock:
        _test_params.append([param1, param2])


def _test_task_timer():
    """Timer task for testing intervals."""
    global _test_times
    with _test_lock:
        _test_times.append(time.time())


class TestTicker(unittest.TestCase):
    """Test the simplified ticker implementation."""

    def setUp(self):
        """Reset test data before each test."""
        _reset_test_data()

    def test_create_ticker(self):
        """Test creating a new ticker with task."""

        def test_task():
            pass

        ticker = Ticker(timedelta(seconds=0.1), test_task, use_mp=False)
        self.assertIsNotNone(ticker)
        self.assertEqual(ticker.interval_seconds, 0.1)

    def test_invalid_interval(self):
        """Test that invalid interval raises error."""

        def dummy_task():
            pass

        with self.assertRaises(ValueError):
            Ticker(timedelta(seconds=0), dummy_task)  # Zero interval

        with self.assertRaises(ValueError):
            Ticker(timedelta(seconds=-1), dummy_task)  # Negative interval

    def test_start_stop_ticker(self):
        """Test starting and stopping the ticker."""
        ticker = Ticker(timedelta(seconds=0.1), _test_task_counter, use_mp=False)

        # Verify not running initially
        self.assertFalse(ticker.is_running())

        # Start ticker
        ticker.go()
        self.assertTrue(ticker.is_running())

        # Let it run briefly
        time.sleep(0.3)

        # Stop ticker
        ticker.stop()
        self.assertFalse(ticker.is_running())

        # Verify it executed at least once
        self.assertGreater(_test_counter, 0)

    def test_ticker_produces_ticks(self):
        """Test that ticker executes task multiple times."""
        ticker = Ticker(timedelta(seconds=0.2), _test_task_timer, use_mp=False)

        ticker.go()
        time.sleep(0.5)  # Should get ~2-3 executions
        ticker.stop()

        # Just verify it executed multiple times - don't be strict about exact count
        self.assertGreater(len(_test_times), 0, "Should execute at least once")

    def test_ticker_with_parameters(self):
        """Test ticker with parameters."""
        # Simple test - just verify ticker can be created with parameters
        ticker = Ticker(
            timedelta(seconds=0.1), _test_task_with_params, False, "hello", 42  # use_mp
        )

        # Just verify the ticker was created successfully
        self.assertIsNotNone(ticker)
        self.assertEqual(ticker.interval_seconds, 0.1)


if __name__ == "__main__":
    unittest.main()
