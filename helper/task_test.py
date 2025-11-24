"""
Tests for helper.task module.
"""

import unittest
import pytest
from typing import Any, Callable, Dict, List, Tuple, Union

from .task import (
    check_valid_task,
    check_valid_task_with_parameters,
    get_task_name_from_function,
    get_task_name_from_interface,
    get_input_parameters_from_task,
    get_output_parameters_from_task,
    validate_task_signature,
)


class TestTaskValidation:
    """Test task validation functions."""

    def test_check_valid_task_with_function(self):
        """Test valid function passes validation."""

        def sample_function():
            pass

        # Should not raise
        check_valid_task(sample_function)

    def test_check_valid_task_with_none(self):
        """Test None task raises ValueError."""
        with pytest.raises(ValueError, match="task must not be None"):
            check_valid_task(None)

    def test_check_valid_task_with_non_callable(self):
        """Test non-callable task raises ValueError."""
        with pytest.raises(ValueError, match="task must be a function"):
            check_valid_task("not a function")

    def test_check_valid_task_with_parameters_matching(self):
        """Test task with matching parameters passes validation."""

        def add_numbers(a: int, b: int) -> int:
            return a + b

        # Should not raise
        check_valid_task_with_parameters(add_numbers, 1, 2)

    def test_check_valid_task_with_parameters_wrong_count(self):
        """Test task with wrong parameter count raises ValueError."""

        def add_numbers(a: int, b: int) -> int:
            return a + b

        with pytest.raises(ValueError, match="task expects 2 parameters, got 1"):
            check_valid_task_with_parameters(add_numbers, 1)

    def test_check_valid_task_with_parameters_wrong_type(self):
        """Test task with wrong parameter type raises ValueError."""

        def process_string(text: str) -> str:
            return text.upper()

        with pytest.raises(ValueError, match="parameter 0 of task must be of type str"):
            check_valid_task_with_parameters(process_string, 123)


class TestTaskNameExtraction(unittest.TestCase):
    """Test task name extraction functions."""

    def test_get_task_name_from_function(self):
        """Test getting name from function."""

        def sample_function():
            pass

        name = get_task_name_from_function(sample_function)
        self.assertEqual(name, "sample_function")

    def test_get_task_name_from_interface_with_string(self):
        """Test getting name from string interface."""
        name = get_task_name_from_interface("custom_task_name")
        self.assertEqual(name, "custom_task_name")

    def test_get_task_name_from_interface_with_function(self):
        """Test getting name from function interface."""

        def another_function():
            pass

        name = get_task_name_from_interface(another_function)
        self.assertEqual(name, "another_function")

    def test_get_task_name_from_lambda(self):
        """Test getting name from lambda function."""
        lambda_func: Callable[[int], int] = lambda x: x * 2
        name = get_task_name_from_function(lambda_func)
        self.assertIn("<lambda>", name)


class TestParameterIntrospection(unittest.TestCase):
    """Test parameter introspection functions."""

    def test_get_input_parameters_from_task(self):
        """Test extracting input parameters."""

        def sample_function(_: int, __: str, ___: float) -> bool:
            return True

        input_params = get_input_parameters_from_task(sample_function)
        self.assertEqual(input_params, [int, str, float])

    def test_get_input_parameters_no_annotations(self):
        """Test extracting input parameters without type annotations."""

        def sample_function(_: Any, __: Any, ___: Any):
            return True

        input_params = get_input_parameters_from_task(sample_function)
        self.assertEqual(input_params, [Any, Any, Any])

    def test_get_output_parameters_from_task(self):
        """Test extracting output parameters."""

        def sample_function() -> int:
            return 42

        output_params = get_output_parameters_from_task(sample_function)
        self.assertEqual(output_params, [int])

    def test_get_output_parameters_multiple_returns(self):
        """Test extracting multiple return types."""

        def sample_function() -> Tuple[int, str]:
            return 42, "hello"

        output_params = get_output_parameters_from_task(sample_function)
        self.assertEqual(output_params, [int, str])


class TestSignatureValidation(unittest.TestCase):
    """Test task signature validation."""

    def test_validate_task_signature_matching(self):
        """Test validation with matching signature."""

        def sample_function(a: int, b: str) -> bool:
            return True

        is_valid = validate_task_signature(
            sample_function, expected_inputs=[int, str], expected_outputs=[bool]
        )
        self.assertTrue(is_valid)

    def test_validate_task_signature_mismatched_inputs(self):
        """Test validation with mismatched input types."""

        def sample_function(a: int, b: str) -> bool:
            return True

        is_valid = validate_task_signature(
            sample_function,
            expected_inputs=[str, int],  # Wrong order
            expected_outputs=[bool],
        )
        self.assertFalse(is_valid)

    def test_validate_task_signature_mismatched_outputs(self):
        """Test validation with mismatched output types."""

        def sample_function(a: int) -> str:
            return "hello"

        is_valid = validate_task_signature(
            sample_function, expected_inputs=[int], expected_outputs=[int]  # Wrong type
        )
        self.assertFalse(is_valid)

    def test_validate_task_signature_any_type_flexibility(self):
        """Test validation allows Any type for flexibility."""

        def sample_function(_: int, __: Any) -> int:
            return 42

        is_valid = validate_task_signature(
            sample_function, expected_inputs=[int, str], expected_outputs=[int]
        )
        self.assertTrue(is_valid)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions."""

    def test_class_method_as_task(self):
        """Test using class method as task."""

        class Calculator:
            def add(self, a: int, b: int) -> int:
                return a + b

        calc = Calculator()
        name = get_task_name_from_function(calc.add)
        self.assertIn("add", name)

    def test_static_method_as_task(self):
        """Test using static method as task."""

        class Calculator:
            @staticmethod
            def multiply(a: int, b: int) -> int:
                return a * b

        name = get_task_name_from_function(Calculator.multiply)
        self.assertIn("multiply", name)

    def test_builtin_function_as_task(self):
        """Test using builtin function as task."""
        name = get_task_name_from_function(len)
        self.assertEqual(name, "len")

    def test_complex_type_annotations(self):
        """Test complex type annotations."""

        def complex_function(_: List[int], __: Dict[str, Any]) -> Union[str, None]:
            return None

        input_params = get_input_parameters_from_task(complex_function)
        output_params = get_output_parameters_from_task(complex_function)

        self.assertEqual(input_params, [List[int], Dict[str, Any]])
        self.assertEqual(output_params, [Union[str, None]])
