"""
Threadsafe Ticker - Mirrors Go time.Ticker.
Simplified to rely on go_func for execution management.
"""

import multiprocessing
import threading
import time
from datetime import timedelta
from typing import Callable, Optional, Union

from core.runner import Runner, SmallRunner, go_func
from helper.logging import get_logger

logger = get_logger(__name__)


class Ticker:
    """
    Manages the background execution of a task at a fixed interval.
    It delegates execution to a Runner instance (via go_func) which runs
    the internal scheduling loop.
    """

    def __init__(
        self,
        interval: timedelta,
        task: Callable,
        use_mp: bool = True,
        *args,
        **kwargs,
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

        if use_mp:
            self._stop_event = multiprocessing.Event()
        else:
            self._stop_event = threading.Event()

        logger.debug(
            f"Ticker created: task={task.__name__}, interval={self._interval_seconds}s, use_mp={use_mp}, stop_event_created={id(self._stop_event)}"
        )

    def _ticker_function(self):
        """
        The continuous loop function executed by the Runner process/thread.
        It handles all the fixed-rate scheduling logic (sleep math).
        """
        logger.info(
            f"Ticker {self._task.__name__} started, interval {self._interval_seconds}s."
        )

        while not self._stop_event.is_set():
            start_time = time.monotonic()

            try:
                runner = go_func(
                    self._task, use_mp=self._use_mp, *self._args, **self._kwargs
                )
                runner.get_results()
            except Exception as e:
                logger.error(
                    f"Ticker {self._task.__name__}: Scheduled task failed: {e.__class__.__name__}: {e}"
                )

            elapsed_time = time.monotonic() - start_time
            sleep_duration = self._interval_seconds - elapsed_time

            if sleep_duration > 0:
                # Sleep in smaller chunks to check stop event more frequently
                sleep_start = time.monotonic()
                while (
                    not self._stop_event.is_set()
                    and (time.monotonic() - sleep_start) < sleep_duration
                ):
                    chunk_sleep = min(
                        0.1, sleep_duration - (time.monotonic() - sleep_start)
                    )
                    if chunk_sleep > 0:
                        time.sleep(chunk_sleep)
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
            return

        self._runner = go_func(
            self._ticker_function,
            use_mp=self._use_mp,
        )

    def stop(self):
        """
        Stops the background runner process/thread.
        """
        if not self.is_running():
            return

        self._stop_event.set()

        if isinstance(self._runner, Runner):
            self._runner.cancel()
        else:
            self._runner.join(timeout=5.0)

        self._runner = None
        logger.info(f"Ticker '{self._task.__name__}' stopped.")

    def is_running(self) -> bool:
        """
        Checks if the runner process/thread is currently active.
        """
        return self._runner is not None and self._runner.is_alive()
