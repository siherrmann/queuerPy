"""
Listener-related methods for the Python queuer implementation.
Mirrors Go's queuerListener.go functionality.
"""

import asyncio
import logging
from typing import Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from model.job import Job

logger = logging.getLogger(__name__)


class QueuerListenerMixin:
    """
    Mixin class containing listener-related methods for the Queuer.
    Mirrors Go's queuerListener.go functionality.
    """

    def listen_for_job_update(self, notify_function: Callable[["Job"], None]) -> None:
        """
        Listen for job update events and notify the provided function when a job is updated.

        :param notify_function: Function to call when a job update event occurs
        :raises: RuntimeError: If queuer is uninitialized or not running
        """
        if self is None or not hasattr(self, "_running") or not self._running:
            raise RuntimeError("Cannot listen with uninitialized or not running Queuer")

        if not hasattr(self, "job_update_listener") or self.job_update_listener is None:
            raise RuntimeError("Job update listener not initialized")

        try:
            # Start listening in the background
            self.job_update_listener.listen(notify_function=notify_function)
        except Exception as e:
            logger.error(f"Error listening for job updates: {str(e)}")
            raise RuntimeError(f"Failed to listen for job updates: {str(e)}")

    def listen_for_job_delete(self, notify_function: Callable[["Job"], None]) -> None:
        """
        Listen for job delete events and notify the provided function when a job is deleted.

        :param notify_function: Function to call when a job delete event occurs
        :raises: RuntimeError: If queuer is uninitialized or not running
        """
        if self is None or not hasattr(self, "_running") or not self._running:
            raise RuntimeError("Cannot listen with uninitialized or not running Queuer")

        if not hasattr(self, "job_delete_listener") or self.job_delete_listener is None:
            raise RuntimeError("Job delete listener not initialized")

        try:
            # Start listening in the background
            self.job_delete_listener.listen(notify_function=notify_function)
        except Exception as e:
            logger.error(f"Error listening for job deletes: {str(e)}")
            raise RuntimeError(f"Failed to listen for job deletes: {str(e)}")

    def listen_for_job_insert(self, notify_function: Callable[["Job"], None]) -> None:
        """
        Listen for job insert events and notify the provided function when a job is inserted.

        :param notify_function: Function to call when a job insert event occurs
        :raises: RuntimeError: If queuer is uninitialized or not running
        """
        if self is None or not hasattr(self, "_running") or not self._running:
            raise RuntimeError("Cannot listen with uninitialized or not running Queuer")

        if not hasattr(self, "job_insert_listener") or self.job_insert_listener is None:
            raise RuntimeError("Job insert listener not initialized")

        try:
            # Start listening in the background
            self.job_insert_listener.listen(notify_function=notify_function)
        except Exception as e:
            logger.error(f"Error listening for job inserts: {str(e)}")
            raise RuntimeError(f"Failed to listen for job inserts: {str(e)}")
