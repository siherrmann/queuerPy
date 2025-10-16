"""
Task-related methods for the Python queuer implementation.
Mirrors Go's queuerTask.go functionality.
"""

import logging
from typing import Callable, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from model.task import Task
    from model.worker import Worker

logger = logging.getLogger(__name__)


class QueuerTaskMixin:
    """
    Mixin class containing task-related methods for the Queuer.
    Mirrors Go's queuerTask.go functionality.
    """

    def add_task(self, task: Callable) -> 'Task':
        """
        Add a new task to the queuer.
        Creates a new task with the provided task function, adds it to the worker's available tasks,
        and updates the worker in the database.
        The task name is automatically generated based on the task's function name.

        Args:
            task: The task function to add

        Returns:
            The newly created task

        Raises:
            RuntimeError: If task creation fails or task already exists
        """
        from model.task import new_task

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

    def add_task_with_name(self, task: Callable, name: str) -> 'Task':
        """
        Add a new task with a specific name to the queuer.
        Creates a new task with the provided task function and name, adds it to the worker's available tasks,
        and updates the worker in the database.

        Args:
            task: The task function to add
            name: The specific name for the task

        Returns:
            The newly created task

        Raises:
            RuntimeError: If task creation fails or task already exists
        """
        from model.task import new_task_with_name

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