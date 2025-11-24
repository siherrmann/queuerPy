"""
Simple tests for error handling utilities.
"""

import unittest
from .error import QueuerError


class TestQueuerError(unittest.TestCase):
    """Test QueuerError functionality."""

    def test_queuer_error_with_regular_exception(self):
        """Test creating QueuerError with regular exception."""
        original = ValueError("original error")
        error = QueuerError("something went wrong", original)

        self.assertEqual(error.original, original)
        self.assertEqual(len(error.trace), 1)
        self.assertIn("something went wrong", error.trace[0])
        self.assertTrue(
            error.trace[0].startswith("test_queuer_error_with_regular_exception - ")
        )
        self.assertIn("original error | Trace:", str(error))

    def test_queuer_error_with_existing_queuer_error(self):
        """Test creating QueuerError with existing QueuerError (wrapping)."""
        # First create a QueuerError
        original_exception = ValueError("base error")
        first_error = QueuerError("first context", original_exception)

        # Then wrap it with another QueuerError
        second_error = QueuerError("second context", first_error)

        self.assertEqual(
            second_error.original, original_exception
        )  # Should keep original exception
        self.assertEqual(len(second_error.trace), 2)
        self.assertIn("first context", second_error.trace[0])
        self.assertIn("second context", second_error.trace[1])
        self.assertTrue(
            second_error.trace[1].startswith(
                "test_queuer_error_with_existing_queuer_error - "
            )
        )

    def test_error_string_representation(self):
        """Test string representation of QueuerError."""
        original = ValueError("base error")
        error = QueuerError("test context", original)

        error_str = str(error)
        self.assertIn("base error", error_str)
        self.assertIn("Trace:", error_str)
        self.assertIn("test context", error_str)

    def test_nested_error_wrapping(self):
        """Test multiple levels of error wrapping."""

        def level_3():
            raise ValueError("original error")

        def level_2():
            try:
                level_3()
            except Exception as e:
                raise QueuerError("level 2 context", e)

        def level_1():
            try:
                level_2()
            except Exception as e:
                raise QueuerError("level 1 context", e)

        with self.assertRaises(QueuerError) as cm:
            level_1()

        error = cm.exception
        self.assertIsInstance(error, QueuerError)
        self.assertIsInstance(error.original, ValueError)
        self.assertEqual(len(error.trace), 2)
        self.assertIn("level 2 context", error.trace[0])
        self.assertIn("level 1 context", error.trace[1])
        self.assertTrue(error.trace[0].startswith("level_2 - "))
        self.assertTrue(error.trace[1].startswith("level_1 - "))

    def test_trace_includes_function_name(self):
        """Test that trace includes the function name automatically."""

        def test_function():
            return QueuerError("custom message", ValueError("test"))

        error = test_function()
        self.assertEqual(len(error.trace), 1)
        self.assertTrue(error.trace[0].startswith("test_function - "))
        self.assertIn("custom message", error.trace[0])

    def test_multiple_wrapping_preserves_trace_order(self):
        """Test that multiple wrapping preserves correct trace order."""
        # Start with a regular exception
        original = ValueError("original problem")

        # Wrap it multiple times
        first_wrap = QueuerError("first wrap", original)
        second_wrap = QueuerError("second wrap", first_wrap)
        third_wrap = QueuerError("third wrap", second_wrap)

        # Check that the original exception is preserved
        self.assertEqual(third_wrap.original, original)

        # Check that traces are in the correct order (oldest first)
        self.assertEqual(len(third_wrap.trace), 3)
        self.assertIn("first wrap", third_wrap.trace[0])
        self.assertIn("second wrap", third_wrap.trace[1])
        self.assertIn("third wrap", third_wrap.trace[2])


if __name__ == "__main__":
    unittest.main()
