"""
Runner component using asyncio - mirrors Go implementation.
"""

import asyncio
import threading
import inspect
import sys
import os
import time
from typing import Any, List, Optional, Callable

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from model.options_on_error import OnError


class Runner:
    """
    Runner executes tasks with parameters and handles results/errors.
    Mirrors the Go Runner implementation using goless channels.
    """

    def __init__(
        self,
        task: Callable,
        parameters: Optional[List[Any]] = None,
        options: Optional[List[OnError]] = None,
    ):
        """Initialize a new runner with task and parameters."""
        if not callable(task):
            raise ValueError("Task must be callable")

        self.task = task
        self.parameters = parameters or []
        self.options = options or []

        # Asyncio queues for results and cancellation
        self.results_chan = asyncio.Queue()
        self.cancel_chan = asyncio.Queue()
        self.done_chan = asyncio.Queue()

        # Threading events for coordination
        self._running = threading.Event()
        self._cancelled = threading.Event()

        # Event loop for async operations
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # Validate parameters match task signature
        self._validate_parameters()

    def _validate_parameters(self):
        """Validate that parameters match the task's expected signature."""
        try:
            sig = inspect.signature(self.task)
            param_count = len(
                [
                    p
                    for p in sig.parameters.values()
                    if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
                ]
            )

            if len(self.parameters) != param_count:
                raise ValueError(
                    f"Task expects {param_count} parameters, got {len(self.parameters)}"
                )
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid task or parameters: {e}")

    def run(self) -> bool:
        """
        Run the task with parameters.
        Returns True if started successfully, False otherwise.
        Mirrors Go's Run() method.
        """
        if self._running.is_set():
            return False  # Already running

        # Get or create event loop for async operations
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            # No loop running, create one in a thread
            self._loop = None

        async def _async_execute():
            try:
                self._running.set()

                # Execute the task
                if asyncio.iscoroutinefunction(self.task):
                    result = await self.task(*self.parameters)
                else:
                    # Run sync function in thread pool
                    loop = asyncio.get_running_loop()
                    result = await loop.run_in_executor(
                        None, self.task, *self.parameters
                    )

                # Send result to channel
                await self.results_chan.put(result)

            except Exception as e:
                # Handle error with options
                handled = False
                for option in self.options:
                    if option and hasattr(option, "handle_error"):
                        if option.handle_error(e):
                            handled = True
                            break

                if not handled:
                    await self.results_chan.put(e)

            finally:
                self._running.clear()
                await self.done_chan.put(True)

        def _execute_in_thread():
            asyncio.run(_async_execute())

        if self._loop and not self._loop.is_closed():
            # Run in existing loop
            asyncio.create_task(_async_execute())
        else:
            # Run in new thread with new loop
            thread = threading.Thread(target=_execute_in_thread, daemon=True)
            thread.start()
        return True

    def cancel(self):
        """Cancel the running task. Mirrors Go's Cancel() method."""
        if not self._running.is_set():
            return  # Not running

        self._cancelled.set()

        # Send cancel signal via asyncio queue
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(
                lambda: asyncio.create_task(self.cancel_chan.put(True))
            )
        else:
            # If no loop, just try put_nowait
            try:
                self.cancel_chan.put_nowait(True)
            except:
                pass  # Queue might be full

    def get_results(self, timeout: Optional[float] = None) -> Any:
        """
        Get results from the task execution.
        Mirrors Go's GetResults() method.
        """

        async def _get_results_async():
            if timeout is None:
                # Blocking receive
                return await self.results_chan.get()
            else:
                # Non-blocking receive with timeout
                return await asyncio.wait_for(self.results_chan.get(), timeout=timeout)

        # Run the async function in a sync context
        try:
            if self._loop and not self._loop.is_closed():
                # Use existing loop via run_coroutine_threadsafe
                future = asyncio.run_coroutine_threadsafe(
                    _get_results_async(), self._loop
                )
                if timeout is None:
                    return future.result()
                else:
                    return future.result(timeout=timeout)
            else:
                # Create new loop
                return asyncio.run(_get_results_async())
        except asyncio.TimeoutError:
            raise TimeoutError("Timeout waiting for results")
        except Exception as e:
            raise e

    def is_running(self) -> bool:
        """Check if the task is currently running. Mirrors Go's IsRunning() method."""
        return self._running.is_set()

    def is_cancelled(self) -> bool:
        """Check if the task was cancelled. Mirrors Go's IsCancelled() method."""
        return self._cancelled.is_set()

    def wait_done(self, timeout: Optional[float] = None) -> bool:
        """
        Wait for the task to complete.
        Returns True if completed, False if timeout.
        Mirrors Go's WaitDone() method.
        """
        """
        Wait for task completion.
        Mirrors Go's WaitForCompletion() method.
        """

        async def _wait_async():
            if timeout is None:
                # Blocking wait
                await self.done_chan.get()
                return True
            else:
                # Wait with timeout
                try:
                    await asyncio.wait_for(self.done_chan.get(), timeout=timeout)
                    return True
                except asyncio.TimeoutError:
                    return False

        # Run the async function in a sync context
        try:
            if self._loop and not self._loop.is_closed():
                # Use existing loop via run_coroutine_threadsafe
                future = asyncio.run_coroutine_threadsafe(_wait_async(), self._loop)
                if timeout is None:
                    return future.result()
                else:
                    return future.result(timeout=timeout + 0.1)  # Add small buffer
            else:
                # Create new loop
                return asyncio.run(_wait_async())
        except Exception:
            return False


def new_runner_from_job(task: Callable, job) -> Runner:
    """
    Create a new runner from a task and job.
    Extracts parameters from the job and creates appropriate runner.
    """
    # Extract parameters from job
    parameters = job.parameters if hasattr(job, "parameters") else []
    options = []

    # Extract options if available
    if hasattr(job, "options") and job.options:
        options = [job.options]

    print(
        f"DEBUG: Creating runner with task={task.__name__ if hasattr(task, '__name__') else task}, parameters={parameters}"
    )

    return Runner(task, parameters, options)
