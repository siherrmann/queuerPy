"""
Task helper functions for Python queuer implementation.
Mirrors Go's helper/task.go with Python types and reflection.
"""

import inspect
from typing import Any, Callable, List, Type, get_type_hints


def check_valid_task(task: Any) -> None:
    """
    Check if the provided task is a valid function.
    Raises ValueError if the task is invalid.
    Mirrors Go's CheckValidTask function.
    """
    if task is None:
        raise ValueError("task must not be None")
    
    if not callable(task):
        raise ValueError(f"task must be a function, got {type(task).__name__}")


def check_valid_task_with_parameters(task: Callable, *parameters: Any) -> None:
    """
    Check if the provided task and parameters are valid.
    Validates that the task is a valid function and parameters match input types.
    Mirrors Go's CheckValidTaskWithParameters function.
    """
    check_valid_task(task)
    
    sig = inspect.signature(task)
    param_count = len(sig.parameters)
    
    if param_count != len(parameters):
        raise ValueError(f"task expects {param_count} parameters, got {len(parameters)}")
    
    # Get parameter types from signature
    param_types = []
    for param in sig.parameters.values():
        if param.annotation != inspect.Parameter.empty:
            param_types.append(param.annotation)
        else:
            param_types.append(Any)
    
    # Validate parameter types
    for i, (param, expected_type) in enumerate(zip(parameters, param_types)):
        if expected_type != Any and not isinstance(param, expected_type):
            # Allow some flexibility for basic types
            param_type = type(param).__name__
            expected_type_name = getattr(expected_type, '__name__', str(expected_type))
            raise ValueError(
                f"parameter {i} of task must be of type {expected_type_name}, "
                f"got {param_type}"
            )


def get_task_name_from_function(task: Callable) -> str:
    """
    Get the name of the function from the provided task.
    Mirrors Go's GetTaskNameFromFunction function.
    """
    check_valid_task(task)
    
    if hasattr(task, '__name__'):
        return task.__name__
    elif hasattr(task, '__class__'):
        return task.__class__.__name__
    else:
        return str(task)


def get_task_name_from_interface(task: Any) -> str:
    """
    Get the name of the task from the provided interface.
    Handles both string and function inputs.
    Mirrors Go's GetTaskNameFromInterface function.
    """
    if isinstance(task, str):
        return task
    
    return get_task_name_from_function(task)


def get_input_parameters_from_task(task: Callable) -> List[Type]:
    """
    Get the input parameters of the provided task.
    Returns list of parameter types from function signature.
    Mirrors Go's GetInputParametersFromTask function.
    """
    check_valid_task(task)
    
    sig = inspect.signature(task)
    input_parameters = []
    
    for param in sig.parameters.values():
        if param.annotation != inspect.Parameter.empty:
            input_parameters.append(param.annotation)
        else:
            input_parameters.append(Any)
    
    return input_parameters


def get_output_parameters_from_task(task: Callable) -> List[Type]:
    """
    Get the output parameters of the provided task.
    Returns list of return types from function signature.
    Mirrors Go's GetOutputParametersFromTask function.
    """
    check_valid_task(task)
    
    sig = inspect.signature(task)
    output_parameters = []
    
    if sig.return_annotation != inspect.Signature.empty:
        # Handle Union types and multiple return values
        return_annotation = sig.return_annotation
        
        # Check if it's a tuple type (multiple returns)
        if hasattr(return_annotation, '__origin__') and return_annotation.__origin__ is tuple:
            output_parameters.extend(return_annotation.__args__)
        else:
            output_parameters.append(return_annotation)
    else:
        output_parameters.append(Any)
    
    return output_parameters


def validate_task_signature(task: Callable, expected_inputs: List[Type] = None, 
                           expected_outputs: List[Type] = None) -> bool:
    """
    Validate that a task has the expected input and output signature.
    Additional helper function for Python implementation.
    """
    try:
        check_valid_task(task)
        
        if expected_inputs is not None:
            actual_inputs = get_input_parameters_from_task(task)
            if len(actual_inputs) != len(expected_inputs):
                return False
            
            for actual, expected in zip(actual_inputs, expected_inputs):
                if actual != Any and expected != Any and actual != expected:
                    return False
        
        if expected_outputs is not None:
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
    """
    return get_task_name_from_interface(task)