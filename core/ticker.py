"""
Ticker implementation using threading - mirrors Go time.Ticker.
"""

import asyncio
import threading
import time
from typing import Callable, Any, List, Optional
from datetime import timedelta

from .runner import Runner, AsyncTask
from helper.task import check_valid_task_with_parameters


class Ticker:
    """
    A ticker that executes a task at regular intervals.
    Mirrors Go's Ticker behavior with a task and runner.
    """

    def __init__(self, interval: timedelta, task: Callable, *parameters: Any):
        """
        Create a new ticker with the given interval and task.

        Args:
            interval: Time interval between task executions
            task: Callable task to execute
            *parameters: Parameters to pass to the task

        Raises:
            ValueError: If interval is not positive or task is invalid
        """
        if interval.total_seconds() <= 0:
            raise ValueError("Ticker interval must be positive")

        # Validate task and parameters
        try:
            check_valid_task_with_parameters(task, *parameters)
        except Exception as e:
            raise ValueError(f"Invalid task or parameters: {e}")

        self.interval = interval.total_seconds()

        # Store task and parameters for execution
        self.task = task
        self.parameters = list(parameters)

        # Threading coordination
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def go(self, stop_event: Optional[threading.Event] = None) -> None:
        """
        Start the ticker. Runs the task at the specified interval.
        Mirrors Go's Go(ctx) method.

        Args:
            stop_event: Optional event to signal when to stop the ticker
        """
        if self._thread and self._thread.is_alive():
            return  # Already running

        # Use provided stop event or create our own
        if stop_event:
            self._stop_event = stop_event
        else:
            self._stop_event = threading.Event()

        def ticker_loop():
            """Main ticker loop."""
            # Run initial task
            try:
                self._run_task()
            except Exception as e:
                print(f"Error running initial task: {e}")

            # Start ticker loop
            while not self._stop_event.is_set():
                # Wait for interval or stop signal
                if self._stop_event.wait(timeout=self.interval):
                    # Stop event was set, exit
                    break

                # Run the task
                try:
                    self._run_task()
                except Exception as e:
                    print(f"Error running task: {e}")
                    # Continue running despite errors

        # Start ticker in separate thread
        self._thread = threading.Thread(target=ticker_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the ticker."""
        if self._stop_event:
            self._stop_event.set()

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)

    def _run_task(self) -> None:
        """Execute the task."""
        try:
            if asyncio.iscoroutinefunction(self.task):
                # For async tasks, use go_func from runner
                from .runner import go_func

                go_func(self.task, *self.parameters)
            else:
                # For sync tasks, just call directly
                self.task(*self.parameters)
        except Exception as e:
            print(f"Task execution error: {e}")
            raise


def new_ticker(interval: timedelta, task: Callable, *parameters: Any) -> Ticker:
    """
    Create a new ticker.
    Mirrors Go's NewTicker function.

    Args:
        interval: Time interval between task executions
        task: Callable task to execute
        *parameters: Parameters to pass to the task

    Returns:
        New Ticker instance

    Raises:
        ValueError: If interval is not positive or task is invalid
    """
    return Ticker(interval, task, *parameters)
