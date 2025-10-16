"""
Test cases for the Go-like ticker implementation - mirrors Go tests.
"""

import unittest
import time
import threading
import sys
import os
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.ticker import Ticker, new_ticker


class TestTicker(unittest.TestCase):
    """Test the Go-like ticker implementation."""
    
    def test_create_ticker(self):
        """Test creating a new ticker with task."""
        call_count = 0
        
        def test_task():
            nonlocal call_count
            call_count += 1
        
        ticker = new_ticker(timedelta(seconds=0.1), test_task)
        self.assertIsNotNone(ticker, "Expected non-None ticker")
        self.assertEqual(ticker.interval, 0.1, "Expected correct interval")
    
    def test_invalid_interval(self):
        """Test that invalid interval raises error."""
        def dummy_task():
            pass
            
        with self.assertRaises(ValueError):
            new_ticker(timedelta(seconds=0), dummy_task)  # Zero interval
        
        with self.assertRaises(ValueError):
            new_ticker(timedelta(seconds=-1), dummy_task)  # Negative interval
    
    def test_start_stop_ticker(self):
        """Test starting and stopping the ticker."""
        call_count = 0
        
        def test_task():
            nonlocal call_count
            call_count += 1
        
        ticker = new_ticker(timedelta(seconds=0.1), test_task)
        
        # Create stop event
        stop_event = threading.Event()
        
        # Start ticker
        ticker.go(stop_event)
        
        # Let it run briefly
        time.sleep(0.3)  # Should execute ~3 times
        
        # Stop ticker
        stop_event.set()
        ticker.stop()
        
        # Verify it executed at least once
        self.assertGreater(call_count, 0, "Task should have executed at least once")
    
    def test_ticker_produces_ticks(self):
        """Test that ticker executes task at regular intervals."""
        call_count = 0
        execution_times = []
        
        def test_task():
            nonlocal call_count
            call_count += 1
            execution_times.append(time.time())
        
        ticker = new_ticker(timedelta(seconds=0.05), test_task)  # 50ms interval
        
        # Create stop event  
        stop_event = threading.Event()
        
        start_time = time.time()
        ticker.go(stop_event)
        
        # Let it run for a short time
        time.sleep(0.25)  # Should execute ~5 times
        
        stop_event.set()
        ticker.stop()
        
        # Check execution count
        self.assertGreater(call_count, 2, f"Expected at least 3 executions, got {call_count}")
        self.assertLess(call_count, 8, f"Expected at most 7 executions, got {call_count}")
        
        # Check timing (rough verification)
        if len(execution_times) > 1:
            intervals = [execution_times[i] - execution_times[i-1] for i in range(1, len(execution_times))]
            avg_interval = sum(intervals) / len(intervals)
            # Allow some tolerance in timing
            self.assertGreater(avg_interval, 0.03, "Intervals too short")
            self.assertLess(avg_interval, 0.1, "Intervals too long")
    
    def test_wait_for_tick_timeout(self):
        """Test ticker with parameters."""
        results = []
        
        def test_task_with_params(param1, param2):
            results.append((param1, param2))
        
        ticker = new_ticker(timedelta(seconds=0.1), test_task_with_params, "hello", 42)
        
        # Create stop event
        stop_event = threading.Event()
        
        # Start ticker
        ticker.go(stop_event)
        
        # Let it run briefly
        time.sleep(0.25)
        
        # Stop ticker
        stop_event.set()
        ticker.stop()
        
        # Verify parameters were passed correctly
        self.assertGreater(len(results), 0, "Task should have executed at least once")
        for result in results:
            self.assertEqual(result, ("hello", 42), "Parameters should be passed correctly")


if __name__ == '__main__':
    unittest.main()
    
