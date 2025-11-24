"""
Task helper functions for Python queuer implementation.
Mirrors Go's helper/task.go with Python types and reflection.
"""

import inspect
from typing import Any, Callable, List, Type


def check_valid_task(task: Any) -> None:
    """
    Check if the provided task is a valid function.
    Raises ValueError if the task is invalid.
    Mirrors Go's CheckValidTask function.

    :param task: The task function to validate.
    :raises ValueError: If the task is invalid.
    """
    if task is None:
        raise ValueError("task must not be None")

    if not callable(task):
        raise ValueError(f"task must be a function, got {type(task).__name__}")


def check_valid_task_with_parameters(
    task: Callable[..., Any], *parameters: Any
) -> None:
    """
    Check if the provided task and parameters are valid.
    Validates that the task is a valid function and parameters match input types.
    Mirrors Go's CheckValidTaskWithParameters function.

    :param task: The task function to validate.
    :param parameters: The parameters to validate against the task's input signature.
    :raises ValueError: If the task is invalid or parameters do not match.
    """
    check_valid_task(task)

    sig = inspect.signature(task)
    param_count = len(sig.parameters)

    if param_count != len(parameters):
        raise ValueError(
            f"task expects {param_count} parameters, got {len(parameters)}"
        )

    param_types: List[Type[Any]] = []
    for param in sig.parameters.values():
        param_types.append(param.annotation)

    for i, (param, expected_type) in enumerate(zip(parameters, param_types)):
        if expected_type != Any and not isinstance(param, expected_type):
            # Allow some flexibility for basic types
            param_type = type(param).__name__
            expected_type_name = getattr(expected_type, "__name__", str(expected_type))
            raise ValueError(
                f"parameter {i} of task must be of type {expected_type_name}, "
                f"got {param_type}"
            )


def get_task_name_from_function(task: Callable[..., Any]) -> str:
    """
    Get the name of the function from the provided task.
    Mirrors Go's GetTaskNameFromFunction function.

    :param task: The task function to inspect.
    :returns: The name of the task function as a string.
    """
    check_valid_task(task)

    if hasattr(task, "__name__"):
        return task.__name__
    elif hasattr(task, "__class__"):
        return task.__class__.__name__
    else:
        return str(task)


def get_task_name_from_interface(task: Any) -> str:
    """
    Get the name of the task from the provided interface.
    Handles both string and function inputs.
    Mirrors Go's GetTaskNameFromInterface function.

    :param task: The task, either as a string or a callable function.
    :returns: The name of the task as a string.
    """
    if isinstance(task, str):
        return task

    return get_task_name_from_function(task)


def get_input_parameters_from_task(task: Callable[..., Any]) -> List[Type[Any]]:
    """
    Get the input parameters of the provided task.
    Returns list of parameter types from function signature.
    Mirrors Go's GetInputParametersFromTask function.

    :param task: The task function to inspect.
    :returns: List of input parameter types.
    """
    check_valid_task(task)

    sig = inspect.signature(task)
    input_parameters: List[Type[Any]] = []

    for param in sig.parameters.values():
        input_parameters.append(param.annotation)

    return input_parameters


def get_output_parameters_from_task(task: Callable[..., Any]) -> List[Type[Any]]:
    """
    Get the output parameters of the provided task.
    Returns list of return types from function signature.
    Mirrors Go's GetOutputParametersFromTask function.

    :param task: The task function to inspect.
    :returns: List of output parameter types.
    """
    check_valid_task(task)

    sig = inspect.signature(task)
    output_parameters: List[Type[Any]] = []
    return_annotation = sig.return_annotation

    # Check if it's a tuple type (multiple returns)
    if (
        hasattr(return_annotation, "__origin__")
        and return_annotation.__origin__ is tuple
    ):
        output_parameters.extend(return_annotation.__args__)
    else:
        output_parameters.append(return_annotation)

    return output_parameters


def validate_task_signature(
    task: Callable[..., Any],
    expected_inputs: List[Type[Any]] = [],
    expected_outputs: List[Type[Any]] = [],
) -> bool:
    """
    Validate that a task has the expected input and output signature.
    Additional helper function for Python implementation.

    :param task: The task function to validate.
    :param expected_inputs: List of expected input parameter types.
    :param expected_outputs: List of expected output parameter types.
    :returns: True if the task matches the expected signature, False otherwise.
    """
    try:
        check_valid_task(task)

        actual_inputs = get_input_parameters_from_task(task)
        if len(actual_inputs) != len(expected_inputs):
            return False

        for actual, expected in zip(actual_inputs, expected_inputs):
            if actual != Any and expected != Any and actual != expected:
                return False

        actual_outputs = get_output_parameters_from_task(task)
        if len(actual_outputs) != len(expected_outputs):
            return False

        for actual, expected in zip(actual_outputs, expected_outputs):
            if actual != Any and expected != Any and actual != expected:
                return False

        return True
    except (ValueError, TypeError):
        return False


# Convenience function for task name extraction (used by Task model)
def get_task_name(task: Any) -> str:
    """
    Convenience function to get task name from any input.
    Used by the Task model for automatic name generation.

    :param task: The task, either as a string or a callable function.
    :returns: The name of the task as a string.
    """
    return get_task_name_from_interface(task)
