"""
Threadsafe Runner - Go-like async task execution with result channel.
"""

import multiprocessing
import threading
import inspect
import asyncio
import traceback
import queue
from typing import Callable, Any, Optional, Union

from ..helper.logging import get_logger

logger = get_logger(__name__)


class Runner:
    """
    The core execution environment. Manages the execution of a single task
    in a multiprocessing pool for efficient process reuse.

    Supports cancellation with automatic periodic checking:
    - Async tasks are monitored every 0.1s and can be cancelled mid-execution
    - Sync tasks can only be cancelled before they start (Python limitation)

    :param task: The synchronous or asynchronous function to execute.
    :param args: Arguments to pass to the task function.
    :param pool: Multiprocessing.Pool for process reuse (required).
    :param kwargs: Keyword arguments to pass to the task function.
    """

    def __init__(
        self,
        task: Callable[..., Any],
        *args: Any,
        pool: Any,
        **kwargs: Any,
    ):
        """
        Initializes the Runner.

        :param task: The synchronous or asynchronous function to execute.
        :param args: Arguments to pass to the task function.
        :param pool: Multiprocessing.Pool for process reuse (required).
        :param kwargs: Keyword arguments to pass to the task function.
        """
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self.is_async = inspect.iscoroutinefunction(task)
        self.name = task.__name__
        self._pool = pool
        self._async_result: Optional[Any] = None
        # Use Manager Event for cross-process sharing (pickleable)
        manager = multiprocessing.Manager()
        self._cancel_event = manager.Event()

    @property
    def pid(self) -> Optional[int]:
        """Process ID (not available in pool mode)."""
        return None

    @property
    def daemon(self) -> bool:
        """Daemon status (always True for pool workers)."""
        return True

    @property
    def exitcode(self) -> Optional[int]:
        """Exit code (not available in pool mode)."""
        return None

    @staticmethod
    def _execute_in_pool_worker(
        task: Callable[..., Any],
        args: tuple,
        kwargs: dict,
        is_async: bool,
        cancel_event: Any,
    ) -> Any:
        """
        Static method to execute tasks in pool workers.
        Handles both sync and async tasks, returns result directly.
        Checks for cancellation before and during execution.
        """
        # Check if task was cancelled before execution
        if cancel_event.is_set():
            raise asyncio.CancelledError("Task was cancelled before execution")

        if is_async:
            # For async tasks, monitor cancellation during execution
            result = asyncio.run(
                Runner._run_async_with_cancellation(task, args, kwargs, cancel_event)
            )
        else:
            # For sync tasks, just execute (can't interrupt mid-execution easily)
            result = task(*args, **kwargs)
        return result

    @staticmethod
    async def _run_async_with_cancellation(
        task: Callable[..., Any],
        args: tuple,
        kwargs: dict,
        cancel_event: Any,
    ) -> Any:
        """
        Run async task with periodic cancellation checking.
        Monitors cancel_event and cancels the task if it's set.
        """
        # Create the task
        task_coro = task(*args, **kwargs)
        task_obj = asyncio.create_task(task_coro)

        # Monitor for cancellation every second
        async def monitor_cancellation():
            while not task_obj.done():
                if cancel_event.is_set():
                    task_obj.cancel()
                    break
                await asyncio.sleep(1)

        monitor = asyncio.create_task(monitor_cancellation())
        try:
            result = await task_obj
            monitor.cancel()
            return result
        except asyncio.CancelledError:
            monitor.cancel()
            raise asyncio.CancelledError("Task was cancelled during execution")
        finally:
            try:
                await monitor
            except asyncio.CancelledError:
                pass

    def go(self) -> None:
        """Starts the Runner in the background using the pool."""
        self._async_result = self._pool.apply_async(
            Runner._execute_in_pool_worker,
            (self.task, self.args, self.kwargs, self.is_async, self._cancel_event),
        )

    def get_results(self, timeout: Optional[float] = None) -> Any:
        """
        Waits for the runner to complete and returns the result.

        :param timeout: Time in seconds to wait for the result. If None, waits indefinitely.
        :returns: The result returned by the executed task function.
        :raises TimeoutError: If the result is not available within the specified timeout.
        :raises asyncio.CancelledError: If the task was cancelled.
        :raises Exception: If the task execution failed.
        """
        logger.debug(f"Runner.get_results() called for {self.name}, timeout={timeout}")
        assert (
            self._async_result is not None
        ), "go() must be called before get_results()"
        try:
            result = self._async_result.get(timeout=timeout)
            return result
        except multiprocessing.TimeoutError:
            raise TimeoutError(f"runner timed out after {timeout} seconds.")
        except asyncio.CancelledError:
            logger.debug(f"Runner {self.name} was cancelled")
            raise

    def cancel(self) -> bool:
        """
        Attempts to cancel the running task by setting the cancellation event.

        Cancellation behavior:
        - Async tasks: Checked every second during execution, task will be cancelled
        - Sync tasks: Only checked before execution starts (cannot interrupt mid-execution)
        - Queued tasks: Cancelled before they begin executing

        :returns: True if cancellation signal was sent successfully.
        """
        logger.debug(f"Cancelling runner {self.name}")
        self._cancel_event.set()
        return True


class SmallRunner(threading.Thread):
    """
    A lightweight, thread-based runner suitable for tasks needing
    direct access to the parent process's shared state (like heartbeats).

    :param task: The synchronous or asynchronous function to execute.
    :param args: Arguments to pass to the task function.
    :param name_prefix: Prefix for the thread name (e.g., "Job-SmallRunner").
    """

    def __init__(
        self,
        task: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ):
        """
        Initializes the SmallRunner thread.

        :param task: The synchronous or asynchronous function to execute.
        :param args: Arguments to pass to the task function.
        """
        super().__init__(name=task.__name__, daemon=True)
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self.is_async = inspect.iscoroutinefunction(task)
        self._result_queue: queue.Queue[Any] = queue.Queue(maxsize=1)

    def go(self) -> None:
        """Starts the SmallRunner thread in the background."""
        self.start()

    def run(self) -> None:
        """
        The main execution method for the thread.
        """
        loop_created = False
        loop = None

        try:
            if self.is_async:
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop_created = True

                result = loop.run_until_complete(self.task(*self.args, **self.kwargs))
            else:
                result = self.task(*self.args, **self.kwargs)

            self._result_queue.put(result)

        except Exception as e:
            logger.error(f"SmallRunner {self.name} task failed: {e}")
            self._result_queue.put(e)
        finally:
            if loop_created and loop and not loop.is_closed():
                try:
                    pending_tasks = asyncio.all_tasks(loop)
                    for task in pending_tasks:
                        task.cancel()
                    if pending_tasks:
                        loop.run_until_complete(
                            asyncio.gather(*pending_tasks, return_exceptions=True)
                        )
                    loop.close()
                except Exception as cleanup_error:
                    logger.debug(f"error cleaning SmallRunner loop {cleanup_error}")

    def get_results(self, timeout: Optional[float] = None) -> Any:
        """
        Waits for the thread to complete and returns the result.

        :param timeout: Time in seconds to wait for the result. If None, waits indefinitely.
        :returns: The result returned by the executed task function.
        :raises TimeoutError: If the result is not available within the specified timeout.
        :raises Exception: If the task execution in the thread failed.
        """
        try:
            self.join(timeout=timeout)

            if self.is_alive():
                raise TimeoutError(f"small runner timed out after {timeout} seconds")

            try:
                result = self._result_queue.get(block=False)
            except queue.Empty:
                # If queue is empty and thread is done, return None
                return None
            except Exception as e:
                raise

            if isinstance(result, Exception):
                raise result

            return result
        except Exception as e:
            raise e

    def cancel(self) -> bool:
        """
        Attempts to cancel the running thread.
        Note: Python threads cannot be forcefully terminated, so this is a no-op
        that provides interface consistency with Runner.

        :returns: True if the thread is not alive, False if still running.
        """
        try:
            if not self.is_alive():
                logger.debug(f"SmallRunner {self.name}: Already finished")
                return True

            logger.debug(
                f"SmallRunner {self.name}: Cannot force terminate thread, waiting for natural completion"
            )
            # We can't actually cancel a thread in Python, just check if it's done
            return not self.is_alive()

        except Exception as e:
            logger.warning(f"Error checking SmallRunner {self.name}: {e}")
            raise e


def go_func(
    func: Callable[..., Any],
    use_mp: bool = True,
    *args: Any,
    pool: Any = None,
    **kwargs: Any,
) -> Union[Runner, SmallRunner]:
    """
    Go-like async function execution factory. Selects the appropriate runner.

    :param func: The function to execute.
    :param use_mp: If True (default), uses the multiprocessing Runner (isolated).
                   If False, uses the threading SmallRunner (shared state).
    :param args: Positional arguments for the function.
    :param pool: Multiprocessing.Pool for process reuse (required if use_mp=True).
    :param kwargs: Keyword arguments for the function.
    :returns: A running Runner or SmallRunner instance.
    """
    if use_mp:
        if pool is None:
            raise ValueError("pool is required when use_mp=True")
        runner = Runner(func, *args, pool=pool, **kwargs)
    else:
        runner = SmallRunner(func, *args, **kwargs)

    runner.go()
    return runner
