"""
Test cases for the goless-based runner - mirrors Go tests.
"""

import unittest
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.runner import Runner


class TestRunner(unittest.TestCase):
    """Test the goless-based runner implementation."""
    
    def test_successful_task(self):
        """Test running a successful task."""
        def add_task(a, b):
            return a + b
        
        runner = Runner(add_task, [5, 3])
        
        # Start the task
        self.assertTrue(runner.run(), "Should start successfully")
        
        # Get results
        result = runner.get_results(timeout=2.0)
        self.assertEqual(result, 8, "Expected 5 + 3 = 8")
        
        # Wait for completion
        self.assertTrue(runner.wait_done(timeout=2.0), "Should complete")
        self.assertFalse(runner.is_running(), "Should not be running after completion")
    
    def test_failed_task(self):
        """Test running a task that fails."""
        def failing_task():
            raise ValueError("Test error")
        
        runner = Runner(failing_task, [])
        
        # Start the task
        self.assertTrue(runner.run(), "Should start successfully")
        
        # Get results (should be the exception)
        result = runner.get_results(timeout=2.0)
        self.assertIsInstance(result, ValueError, "Expected ValueError")
        self.assertEqual(str(result), "Test error", "Expected error message")
        
        # Wait for completion
        self.assertTrue(runner.wait_done(timeout=2.0), "Should complete")
    
    def test_parameter_validation(self):
        """Test parameter validation."""
        def task_with_params(a, b, c):
            return a + b + c
        
        # Should fail with wrong number of parameters
        with self.assertRaises(ValueError):
            Runner(task_with_params, [1, 2])  # Missing one parameter
    
    def test_non_callable_task(self):
        """Test that non-callable task raises error."""
        with self.assertRaises(ValueError):
            Runner("not a function", [])


if __name__ == "__main__":
    unittest.main()