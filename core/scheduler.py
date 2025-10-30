"""
Scheduler component using asyncio - mirrors Go implementation.
"""

import asyncio
import logging
from typing import Callable, Optional, Any, List
from datetime import datetime, timedelta

from helper.task import check_valid_task_with_parameters


class Scheduler:
    """
    Scheduler manages scheduled tasks.
    Mirrors the Go Scheduler implementation exactly.
    """

    def __init__(self, start_time: Optional[datetime], task: Callable, *parameters):
        """Initialize a new scheduler with task and parameters."""
        # Validate task and parameters
        try:
            check_valid_task_with_parameters(task, *parameters)
        except Exception as e:
            raise ValueError(f"Invalid task or parameters: {e}")

        self.task = task
        self.parameters = list(parameters)
        self.start_time = start_time

    def go(self, context=None):
        """
        Start the scheduler to run the task at the specified start time.
        Mirrors Go's Go(ctx) method exactly.
        """
        # Calculate duration until start time
        if self.start_time is not None:
            duration = (self.start_time - datetime.now()).total_seconds()
            if duration < 0:
                duration = 0  # Run immediately if time has passed
        else:
            duration = 0  # Run immediately if no start time

        # Create a function that will be called after the delay
        def scheduled_task():
            """Execute the task with parameters."""
            try:
                if self.parameters:
                    self.task(*self.parameters)
                else:
                    self.task()
            except Exception as e:
                logging.error(f"Scheduled task failed: {e}")

        # Schedule the task using asyncio (similar to Go's time.AfterFunc + Runner)
        try:
            loop = asyncio.get_running_loop()

            async def run_after_delay():
                """Wait for the specified duration then run the task."""
                if duration > 0:
                    await asyncio.sleep(duration)
                # Run the task in a thread to avoid blocking the event loop
                await loop.run_in_executor(None, scheduled_task)

            # Create and start the task (equivalent to go runner.Run(ctx))
            asyncio.create_task(run_after_delay())

        except RuntimeError:
            # No event loop running, use threading approach
            import threading

            def run_in_thread():
                """Run the scheduler in a separate thread."""
                import time

                if duration > 0:
                    time.sleep(duration)
                scheduled_task()

            thread = threading.Thread(target=run_in_thread, daemon=True)
            thread.start()
