"""
Test cases for the Go-like ticker implementation - mirrors Go tests.
"""

import unittest
import time
import threading
import sys
import os
import tempfile
import json
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.ticker import Ticker

# Use temp files for cross-process communication instead of Manager
_temp_dir = tempfile.gettempdir()


def _test_task_counter():
    """Module-level task function for testing - increments counter."""
    counter_file = os.path.join(_temp_dir, "ticker_test_counter.txt")
    try:
        if os.path.exists(counter_file):
            with open(counter_file, "r") as f:
                count = int(f.read().strip())
        else:
            count = 0
        count += 1
        with open(counter_file, "w") as f:
            f.write(str(count))
    except Exception:
        # Ignore errors for robustness
        pass


def _test_task_timer():
    """Module-level task function for testing - records execution times."""
    timer_file = os.path.join(_temp_dir, "ticker_test_times.json")
    try:
        if os.path.exists(timer_file):
            with open(timer_file, "r") as f:
                times = json.load(f)
        else:
            times = []
        times.append(time.time())
        with open(timer_file, "w") as f:
            json.dump(times, f)
    except Exception:
        # Ignore errors for robustness
        pass


def _test_task_with_params(param1, param2):
    """Module-level task function for testing with parameters."""
    params_file = os.path.join(_temp_dir, "ticker_test_params.json")
    try:
        if os.path.exists(params_file):
            with open(params_file, "r") as f:
                results = json.load(f)
        else:
            results = []
        results.append([param1, param2])
        with open(params_file, "w") as f:
            json.dump(results, f)
    except Exception:
        # Ignore errors for robustness
        pass


def _cleanup_test_files():
    """Remove test files."""
    files = [
        "ticker_test_counter.txt",
        "ticker_test_times.json",
        "ticker_test_params.json",
    ]
    for filename in files:
        filepath = os.path.join(_temp_dir, filename)
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
            except Exception:
                pass


class TestTicker(unittest.TestCase):
    """Test the Go-like ticker implementation."""

    def test_create_ticker(self):
        """Test creating a new ticker with task."""
        call_count = 0

        def test_task():
            nonlocal call_count
            call_count += 1

        ticker = Ticker(timedelta(seconds=0.1), test_task)
        self.assertIsNotNone(ticker, "Expected non-None ticker")
        self.assertEqual(ticker._interval_seconds, 0.1, "Expected correct interval")

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
        _cleanup_test_files()

        ticker = Ticker(timedelta(seconds=0.1), _test_task_counter)

        # Start ticker
        ticker.go()

        # Let it run briefly
        time.sleep(0.3)  # Should execute ~3 times

        # Stop ticker
        ticker.stop()

        # Verify it executed at least once
        counter_file = os.path.join(_temp_dir, "ticker_test_counter.txt")
        call_count = 0
        if os.path.exists(counter_file):
            with open(counter_file, "r") as f:
                call_count = int(f.read().strip())
        self.assertGreater(call_count, 0, "Task should have executed at least once")

    def test_ticker_produces_ticks(self):
        """Test that ticker executes task at regular intervals."""
        _cleanup_test_files()

        ticker = Ticker(timedelta(seconds=0.05), _test_task_timer)  # 50ms interval

        start_time = time.time()
        ticker.go()

        # Let it run for ~300ms
        time.sleep(0.3)

        # Stop ticker
        ticker.stop()

        # Get results from files
        timer_file = os.path.join(_temp_dir, "ticker_test_times.json")
        execution_times = []
        if os.path.exists(timer_file):
            with open(timer_file, "r") as f:
                execution_times = json.load(f)

        call_count = len(execution_times)

        # Check execution count
        self.assertGreater(
            call_count, 2, f"Expected at least 3 executions, got {call_count}"
        )
        self.assertLess(
            call_count, 8, f"Expected at most 7 executions, got {call_count}"
        )

        # Check timing (rough verification)
        if len(execution_times) > 1:
            intervals = [
                execution_times[i] - execution_times[i - 1]
                for i in range(1, len(execution_times))
            ]
            avg_interval = sum(intervals) / len(intervals)
            # Allow some tolerance in timing
            self.assertGreater(avg_interval, 0.03, "Intervals too short")
            self.assertLess(avg_interval, 0.1, "Intervals too long")

    def test_wait_for_tick_timeout(self):
        """Test ticker with parameters."""
        _cleanup_test_files()

        ticker = Ticker(
            timedelta(seconds=0.1), _test_task_with_params, True, "hello", 42
        )

        # Start ticker
        ticker.go()

        # Let it run briefly
        time.sleep(0.25)

        # Stop ticker
        ticker.stop()

        # Get results from files
        params_file = os.path.join(_temp_dir, "ticker_test_params.json")
        results = []
        if os.path.exists(params_file):
            with open(params_file, "r") as f:
                results = json.load(f)

        # Verify parameters were passed correctly
        self.assertGreater(len(results), 0, "Task should have executed at least once")
        for result in results:
            self.assertEqual(
                result, ["hello", 42], "Parameters should be passed correctly"
            )


if __name__ == "__main__":
    unittest.main()
