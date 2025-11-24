"""
Scheduler component using go_func for non-blocking delayed execution.
"""

import time
from datetime import datetime
from typing import Callable, Dict, Optional, Any, Tuple

from .runner import go_func
from ..helper.logging import get_logger
from ..helper.task import check_valid_task_with_parameters

logger = get_logger(__name__)


class Scheduler:
    """
    Scheduler manages tasks to be run after a specific delay (start time).
    It uses go_func with the SmallRunner (threading) for non-blocking execution.
    """

    def __init__(
        self,
        task: Callable[..., Any],
        start_time: Optional[datetime] = None,
        *args: Any,
        **kwargs: Any,
    ):
        """
        Initialize a new scheduler with task and parameters.

        :param task: The synchronous function to execute.
        :param args: Positional arguments for the task.
        :param kwargs: Keyword arguments for the task.
        :param start_time: The datetime when the task should be executed.
        :raises ValueError: If the task or parameters are invalid.
        """
        try:
            check_valid_task_with_parameters(task, *args, **kwargs)
        except Exception as e:
            raise ValueError(f"Invalid task or parameters: {e}")

        self.task = task
        self.args = list(args)
        self.kwargs = kwargs
        self.start_time = start_time

    def go(self) -> None:
        """
        Start the scheduler to run the task at the specified start time.
        Mirrors Go's Go(ctx) method exactly.
        """
        if self.start_time is not None:
            duration = (self.start_time - datetime.now()).total_seconds()
            if duration < 0:
                duration = 0
        else:
            duration = 0

        def scheduled_runner_task() -> None:
            """
            This synchronous function runs in the SmallRunner thread.
            It executes the delay and then runs the user's task.
            """
            if duration > 0:
                time.sleep(duration)
                logger.info(f"Task {self.task.__name__} is in {duration:.3f} seconds.")
            try:
                if self.args or self.kwargs:
                    self.task(*self.args, **self.kwargs)
                else:
                    self.task()
            except Exception as e:
                logger.error(f"Scheduled task failed: {e}")

        logger.info(f"Scheduling task to run in {duration:.3f} seconds...")
        go_func(scheduled_runner_task, use_mp=False)
