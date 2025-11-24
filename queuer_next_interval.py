"""
Next interval function-related methods for the Python queuer implementation.
Mirrors Go's queuerNextInterval.go functionality.
"""

import logging
from typing import Any, Callable

from .queuer_global import QueuerGlobalMixin
from .model.worker import Worker

logger = logging.getLogger(__name__)


class QueuerNextIntervalMixin(QueuerGlobalMixin):
    """
    Mixin class containing next interval function-related methods for the Queuer.
    Mirrors Go's queuerNextInterval.go functionality.
    """

    def __init__(self):
        super().__init__()

    def add_next_interval_func(self, nif: Callable[..., Any]) -> Worker:
        """
        Add a NextIntervalFunc to the worker's available next interval functions.
        Takes a NextIntervalFunc and adds it to the worker's available_next_interval_funcs.
        The function name is derived from the function using helper.get_task_name_from_interface.

        :param nif: The next interval function to add
        :return: The updated worker after adding the function
        :raises ValueError: If function is None
        :raises RuntimeError: If function already exists or database update fails
        """
        from .helper.task import get_task_name_from_interface

        try:
            nif_name = get_task_name_from_interface(nif)
        except Exception as e:
            raise RuntimeError(f"Error getting function name: {str(e)}")

        if nif_name in self.worker.available_next_interval_funcs:
            raise RuntimeError(f"NextIntervalFunc already exists: {nif_name}")

        self.next_interval_funcs[nif_name] = nif
        self.worker.available_next_interval_funcs.append(nif_name)

        try:
            worker = self.db_worker.update_worker(self.worker)
            if not worker:
                raise Exception("Worker could not be updated")

            logger.info(f"NextInterval function added: {nif_name}")
            return worker
        except Exception as e:
            raise RuntimeError(f"Error updating worker: {str(e)}")

    def add_next_interval_func_with_name(
        self, nif: Callable[..., Any], name: str
    ) -> Worker:
        """
        Add a NextIntervalFunc to the worker's available next interval functions with a specific name.
        Takes a NextIntervalFunc and a name, checks if the function is not None
        and doesn't already exist in the worker's available next interval functions,
        and adds it to the worker's available_next_interval_funcs.

        This function is useful when you want to add a NextIntervalFunc
        with a specific name that you control, rather than deriving it from the function itself.

        :param nif: The next interval function to add
        :param name: The specific name for the function
        :return: The updated worker after adding the function with the specified name
        :raises ValueError: If function is None
        :raises RuntimeError: If function already exists or database update fails
        """
        if nif is None:
            raise ValueError("NextIntervalFunc cannot be None")

        if name in self.worker.available_next_interval_funcs:
            raise RuntimeError(f"NextIntervalFunc with name already exists: {name}")

        self.next_interval_funcs[name] = nif
        self.worker.available_next_interval_funcs.append(name)

        try:
            worker = self.db_worker.update_worker(self.worker)
            if not worker:
                raise Exception("Worker could not be updated")

            logger.info(f"NextInterval function added: {name}")
            return worker
        except Exception as e:
            raise RuntimeError(f"Error updating worker: {str(e)}")
