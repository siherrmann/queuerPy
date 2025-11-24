"""
Task-related methods for the Python queuer implementation.
Mirrors Go's queuerTask.go functionality.
"""

import logging
from typing import Any, Callable, Optional, TypeVar

from .model.task import new_task, new_task_with_name
from .model.task import Task
from .queuer_global import QueuerGlobalMixin

logger = logging.getLogger(__name__)

# TypeVar for maintaining function type through decorator
F = TypeVar("F", bound=Callable[..., Any])


class QueuerTaskMixin(QueuerGlobalMixin):
    """
    Mixin class containing task-related methods for the Queuer.
    Mirrors Go's queuerTask.go functionality.
    """

    def __init__(self):
        super().__init__()

    def add_task(self, task: Callable[..., Any]) -> Optional[Task]:
        """
        Add a new task to the queuer.
        Creates a new task with the provided task function, adds it to the worker's available tasks,
        and updates the worker in the database.
        The task name is automatically generated based on the task's function name.

        :param task: The task function to add
        :return: The newly created task
        :raises RuntimeError: If task creation fails or task already exists
        """
        task_name = task.__name__
        if task_name in self.tasks:
            raise RuntimeError(f"Task already exists: {task_name}")

        try:
            new_task_obj = new_task(task)
        except Exception as e:
            raise RuntimeError(f"Error creating new task: {str(e)}")

        if new_task_obj.name in self.worker.available_tasks:
            raise RuntimeError(f"Task already exists: {new_task_obj.name}")

        self.tasks[new_task_obj.name] = new_task_obj
        self.worker.available_tasks.append(new_task_obj.name)

        # Update worker in DB
        try:
            self.db_worker.update_worker(self.worker)
        except Exception as e:
            raise RuntimeError(f"Error updating worker: {str(e)}")

        logger.info(f"Task added: {new_task_obj.name}")
        return new_task_obj

    def add_task_with_name(self, task: Callable[..., Any], name: str) -> "Task":
        """
        Add a new task with a specific name to the queuer.
        Creates a new task with the provided task function and name, adds it to the worker's available tasks,
        and updates the worker in the database.

        :param task: The task function to add
        :param name: The specific name for the task
        :return: The newly created task
        :raises RuntimeError: If task creation fails or task already exists
        """
        try:
            new_task_obj = new_task_with_name(task, name)
        except Exception as e:
            raise RuntimeError(f"Error creating new task: {str(e)}")

        if name in self.worker.available_tasks:
            raise RuntimeError(f"Task already exists: {name}")

        self.tasks[new_task_obj.name] = new_task_obj
        self.worker.available_tasks.append(new_task_obj.name)

        # Update worker in DB
        try:
            self.db_worker.update_worker(self.worker)
        except Exception as e:
            raise RuntimeError(f"Error updating worker: {str(e)}")

        logger.info(f"Task added: {new_task_obj.name}")
        return new_task_obj

    def task(self) -> Callable[[F], F]:
        """
        Decorator to register a task function with the queuer using the function name.
        This is equivalent to calling add_task().

        Usage:
            @queuer.task()
            def example_task(param1, param2):
                return param1 + param2

        :return: The decorator function that preserves the original function's type
        """

        def decorator(func: F) -> F:
            self.add_task(func)
            return func

        return decorator

    def task_with_name(self, name: str) -> Callable[[F], F]:
        """
        Decorator to register a task function with the queuer.
        This is equivalent to calling add_task() or add_task_with_name().

        Usage:
            @queuer.task_with_name(name="custom_task_name")
            def example_task(param):
                return param * 2

        :param name: Optional custom name for the task. If not provided, uses function name.
        :return: The decorator function that preserves the original function's type
        """

        def decorator(func: F) -> F:
            self.add_task_with_name(func, name)
            return func

        return decorator
