"""
Threadsafe Ticker - Mirrors Go time.Ticker.
"""

import multiprocessing
import time
import inspect
import asyncio
import logging
from datetime import timedelta
from typing import Callable, Any, Optional, Tuple, Union

from core.runner import Runner, SmallRunner, go_func

# Configure logging for better visibility in examples
logging.basicConfig(
    level=logging.INFO, format="[%(levelname)s] [%(processName)s] %(message)s"
)
logger = logging.getLogger(__name__)


class Ticker:
    """
    Manages the background execution of a task at a fixed interval.
    It delegates execution to a Runner instance which runs the scheduling loop.
    """

    def __init__(
        self, interval: timedelta, task: Callable, use_mp: bool = True, *args, **kwargs
    ):
        """
        Initializes the Ticker.

        :param interval: The time delta between consecutive task executions.
        :param task: The synchronous or asynchronous function to execute.
        :param use_mp: If True (default), uses multiprocessing Runner. If False, uses threading SmallRunner.
        :param args: Positional arguments to pass to the task function.
        :param kwargs: Keyword arguments to pass to the task function.
        """
        if not isinstance(interval, timedelta) or interval.total_seconds() <= 0:
            raise ValueError("Interval must be a positive timedelta.")

        self._interval_seconds = interval.total_seconds()
        self._task = task
        self._use_mp = use_mp
        self._args = args
        self._kwargs = kwargs
        self._runner: Optional[Union[Runner, SmallRunner]] = None

    def _ticker_function(self):
        """
        The continuous loop function executed by the Runner process.
        It handles all the fixed-rate scheduling logic for the Ticker.
        """
        logger.info(f"Ticker loop started with interval {self._interval_seconds}s.")

        # Determine if the user's task is async for correct execution inside the loop
        is_async = inspect.iscoroutinefunction(self._task)

        while True:
            start_time = time.monotonic()
            try:
                if is_async:
                    asyncio.run(self._task(*self._args, **self._kwargs))
                else:
                    self._task(*self._args, **self._kwargs)
            except Exception as e:
                logger.error(f"Scheduled task failed: {e.__class__.__name__}: {e}")

            elapsed_time = time.monotonic() - start_time
            sleep_duration = self._interval_seconds - elapsed_time

            if sleep_duration > 0:
                time.sleep(sleep_duration)
            else:
                logger.warning(
                    f"Scheduled task took too long ({elapsed_time:.3f}s). "
                    f"Interval {self._interval_seconds}s exceeded. Skipping sleep."
                )

    def go(self):
        """
        Starts the background runner process/thread, executing the internal scheduling loop.
        """
        if self.is_running():
            runner_type = "process" if self._use_mp else "thread"
            runner_id = getattr(
                self._runner, "pid", getattr(self._runner, "ident", "unknown")
            )
            logger.warning(
                f"Ticker for '{self._task.__name__}' is already running ({runner_type} {runner_id})."
            )
            return

        logger.info(
            f"Starting Ticker for '{self._task.__name__}' using {'multiprocessing' if self._use_mp else 'threading'}..."
        )

        # Use go_func to create the appropriate runner type
        self._runner = go_func(self._ticker_function, use_mp=self._use_mp)

        runner_type = "process" if self._use_mp else "thread"
        runner_id = getattr(
            self._runner, "pid", getattr(self._runner, "ident", "unknown")
        )
        logger.info(f"Ticker started in {runner_type} {runner_id}.")

    def stop(self):
        """
        Stops the background runner process/thread.
        """
        if self.is_running():
            runner_type = "process" if self._use_mp else "thread"
            runner_id = getattr(
                self._runner, "pid", getattr(self._runner, "ident", "unknown")
            )

            logger.info(
                f"Stopping Ticker for '{self._task.__name__}' ({runner_type} {runner_id})..."
            )

            if isinstance(self._runner, SmallRunner):
                # SmallRunner is a thread - termination is not possible, only graceful joining
                logger.info(
                    "SmallRunner detected - termination not possible, waiting for thread to complete..."
                )
                self._runner.join(timeout=5.0)  # Wait up to 5 seconds
                if self._runner.is_alive():
                    logger.warning(
                        "SmallRunner thread did not complete within timeout - may continue running"
                    )
            else:
                # Runner is a process - can be terminated
                self._runner.terminate()
                self._runner.join()

            self._runner = None
            logger.info(f"Ticker stopped.")
        else:
            logger.warning(f"Ticker for '{self._task.__name__}' is not running.")

    def is_running(self) -> bool:
        """
        Checks if the runner process/thread is currently active.
        """
        return self._runner is not None and self._runner.is_alive()
