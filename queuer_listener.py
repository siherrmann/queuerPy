"""
Listener-related methods for the Python queuer implementation.
Mirrors Go's queuerListener.go functionality.
"""

import logging
from typing import Callable

from .queuer_global import QueuerGlobalMixin
from .model.job import Job

logger = logging.getLogger(__name__)


class QueuerListenerMixin(QueuerGlobalMixin):
    """
    Mixin class containing listener-related methods for the Queuer.
    Mirrors Go's queuerListener.go functionality.
    """

    def __init__(self):
        super().__init__()

    def listen_for_job_update(self, notify_function: Callable[[Job], None]) -> None:
        """
        Listen for job update events and notify the provided function when a job is updated.

        :param notify_function: Function to call when a job update event occurs
        :raises: RuntimeError: If queuer is not running
        """
        if not self.running:
            raise RuntimeError("Cannot listen with not running Queuer")

        try:
            # Start listening in the background
            self.job_update_listener.listen(notify_function=notify_function)
        except Exception as e:
            logger.error(f"Error listening for job updates: {str(e)}")
            raise RuntimeError(f"Failed to listen for job updates: {str(e)}")

    def listen_for_job_delete(self, notify_function: Callable[[Job], None]) -> None:
        """
        Listen for job delete events and notify the provided function when a job is deleted.

        :param notify_function: Function to call when a job delete event occurs
        :raises: RuntimeError: If queuer is not running
        """
        if not self.running:
            raise RuntimeError("Cannot listen with not running Queuer")

        try:
            # Start listening in the background
            self.job_delete_listener.listen(notify_function=notify_function)
        except Exception as e:
            logger.error(f"Error listening for job deletes: {str(e)}")
            raise RuntimeError(f"Failed to listen for job deletes: {str(e)}")

    def listen_for_job_insert(self, notify_function: Callable[[Job], None]) -> None:
        """
        Listen for job insert events and notify the provided function when a job is inserted.

        :param notify_function: Function to call when a job insert event occurs
        :raises: RuntimeError: If queuer is not running
        """
        if not self.running:
            raise RuntimeError("Cannot listen with not running Queuer")

        try:
            # Start listening in the background
            self.job_insert_listener.listen(notify_function=notify_function)
        except Exception as e:
            logger.error(f"Error listening for job inserts: {str(e)}")
            raise RuntimeError(f"Failed to listen for job inserts: {str(e)}")
