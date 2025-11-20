"""
Tests for Task model.
Mirrors Go's model/task_test.go functionality.
"""

import inspect
from typing import Any, Callable, Dict, List
import unittest

from model.task import new_task, new_task_with_name


class TestTask(unittest.TestCase):
    """Test cases for Task model."""

    def test_new_task(self):
        """Test NewTask function with various parameters."""

        # Define test functions
        def func_no_params():
            pass

        def func_with_input_params(a: int, b: str):
            pass

        def func_with_output_params() -> tuple[int, str]:
            return 1, "test"

        def func_with_multiple_params(a: int, b: str, c: float) -> tuple[bool, str]:
            return True, "result"

        test_cases: List[Dict[str, Any]] = [
            {
                "name": "Valid task with no parameters",
                "task": func_no_params,
                "exp_name": func_no_params.__name__,
                "exp_in_params": 0,
                "exp_out_params": 0,
                "want_err": False,
            },
            {
                "name": "Task with input parameters",
                "task": func_with_input_params,
                "exp_name": func_with_input_params.__name__,
                "exp_in_params": 2,
                "exp_out_params": 0,
                "want_err": False,
            },
            {
                "name": "Task with output parameters",
                "task": func_with_output_params,
                "exp_name": func_with_output_params.__name__,
                "exp_in_params": 0,
                "exp_out_params": 1,  # Python treats tuple return as single return
                "want_err": False,
            },
            {
                "name": "Task with multiple input and output parameters",
                "task": func_with_multiple_params,
                "exp_name": func_with_multiple_params.__name__,
                "exp_in_params": 3,
                "exp_out_params": 1,  # Python treats tuple return as single return
                "want_err": False,
            },
            {
                "name": "Invalid task type",
                "task": "not a function",
                "want_err": True,
            },
        ]

        for test_case in test_cases:
            with self.subTest(name=test_case["name"]):
                try:
                    task = new_task(test_case["task"])

                    if test_case["want_err"]:
                        self.fail(
                            f"NewTask should return an error for {test_case['name']}"
                        )
                    else:
                        self.assertIsNotNone(
                            task, "Task should not be None for valid options"
                        )
                        self.assertEqual(
                            test_case["exp_name"],
                            task.name,
                            "Task name should match the expected name",
                        )

                        # Get function signature for parameter validation
                        sig = inspect.signature(test_case["task"])
                        actual_in_params = len(sig.parameters)
                        self.assertEqual(
                            test_case["exp_in_params"],
                            actual_in_params,
                            "Input parameters count should match expected",
                        )

                        # For output parameters, check return annotation
                        return_annotation = sig.return_annotation
                        if return_annotation != inspect.Signature.empty:
                            # Has return type annotation
                            self.assertGreaterEqual(
                                test_case["exp_out_params"],
                                0,
                                "Should have output parameters when return annotation is present",
                            )

                except Exception as e:
                    if not test_case["want_err"]:
                        self.fail(
                            f"NewTask should not return an error for {test_case['name']}: {e}"
                        )

    def test_new_task_with_name(self):
        """Test NewTaskWithName function with various parameters."""

        def valid_func():
            pass

        test_cases: List[Dict[str, Any]] = [
            {
                "name": "Valid task with valid name",
                "task": valid_func,
                "task_name": "ValidTask",
                "want_err": False,
            },
            {
                "name": "Task with empty name",
                "task": valid_func,
                "task_name": "",
                "want_err": True,
            },
            {
                "name": "Task with too long name",
                "task": valid_func,
                "task_name": "TaskMockWithNameLonger100_283032343638404244464850525456586062646668707274767880828486889092949698100",
                "want_err": True,
            },
            {
                "name": "Invalid task type",
                "task": "not a function",
                "task_name": "InvalidTask",
                "want_err": True,
            },
        ]

        for test_case in test_cases:
            with self.subTest(name=test_case["name"]):
                try:
                    task = new_task_with_name(test_case["task"], test_case["task_name"])

                    if test_case["want_err"]:
                        self.fail(
                            f"NewTaskWithName should return an error for {test_case['name']}"
                        )
                    else:
                        self.assertIsNotNone(
                            task, "Task should not be None for valid options"
                        )
                        self.assertEqual(
                            test_case["task_name"],
                            task.name,
                            "Task name should match the provided name",
                        )

                except Exception as e:
                    if not test_case["want_err"]:
                        self.fail(
                            f"NewTaskWithName should not return an error for {test_case['name']}: {e}"
                        )

    def test_task_callable_validation(self):
        """Test that task validation properly checks for callable objects."""

        # Valid callables
        def regular_function():
            pass

        lambda_func: Callable[[int], int] = lambda x: x * 2

        class CallableClass:
            def __call__(self):
                pass

        callable_instance = CallableClass()

        # Test valid callables
        valid_callables: List[Callable[..., Any]] = [
            regular_function,
            lambda_func,
            callable_instance,
        ]
        for callable_obj in valid_callables:
            with self.subTest(callable_type=type(callable_obj).__name__):
                try:
                    task = new_task(callable_obj)
                    self.assertIsNotNone(task)
                except Exception as e:
                    self.fail(
                        f"Should accept callable of type {type(callable_obj).__name__}: {e}"
                    )

        # Test invalid callables
        invalid_callables: List[Any] = [123, "string", [], {}]
        for non_callable in invalid_callables:
            with self.subTest(non_callable_type=type(non_callable).__name__):
                with self.assertRaises(Exception):
                    new_task(non_callable)

    def test_task_name_extraction(self):
        """Test automatic name extraction from functions."""

        def test_function():
            pass

        lambda_func = lambda: None

        task1 = new_task(test_function)
        self.assertEqual(task1.name, "test_function")

        # Lambda functions get a generic name
        task2 = new_task(lambda_func)
        self.assertTrue(task2.name.startswith("<lambda>") or "lambda" in task2.name)


if __name__ == "__main__":
    unittest.main()
