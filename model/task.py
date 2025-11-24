"""
Task model for Python queuer implementation.
Mirrors the Go Task struct with Python types.
"""

import inspect
from dataclasses import dataclass, field
from typing import Callable, List, Any

from ..helper.error import QueuerError


@dataclass
class Task:
    """
    Task represents a job task with its function, name, and parameters.
    Mirrors the Go Task struct for compatibility.
    """

    task: Callable[..., Any]
    name: str = ""
    input_parameters: List[Any] = field(default_factory=lambda: [])
    output_parameters: List[Any] = field(default_factory=lambda: [])

    def __post_init__(self):
        """Initialize parameter types from the function signature."""
        if callable(self.task):
            sig = inspect.signature(self.task)
            # Extract input parameter types
            self.input_parameters = [
                param.annotation if param.annotation != inspect.Parameter.empty else Any
                for param in sig.parameters.values()
            ]
            # Extract return type
            if sig.return_annotation != inspect.Signature.empty:
                self.output_parameters = [sig.return_annotation]
            else:
                self.output_parameters = [Any]


def get_task_name_from_function(task: Callable[..., Any]) -> str:
    """
    Get task name from function.
    Mirrors Go's helper.GetTaskNameFromFunction.
    """
    if not callable(task):
        raise ValueError("task must be callable")

    if hasattr(task, "__name__"):
        return task.__name__
    elif hasattr(task, "__class__"):
        return task.__class__.__name__
    else:
        return str(task)


def new_task(task: Callable[..., Any]) -> Task:
    """
    Create a new task from function.
    Mirrors Go's NewTask function.
    """
    try:
        task_name = get_task_name_from_function(task)
        return new_task_with_name(task, task_name)
    except Exception as e:
        raise QueuerError("creating task", e)


def new_task_with_name(task: Callable[..., Any], task_name: str) -> Task:
    """
    Create a new task with specified name.
    Mirrors Go's NewTaskWithName function.
    """
    if not task_name or len(task_name) > 100:
        raise ValueError("task_name must have a length between 1 and 100")

    if not callable(task):
        raise ValueError("task must be callable")

    return Task(task=task, name=task_name)
