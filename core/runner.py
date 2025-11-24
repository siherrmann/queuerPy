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


class Runner(multiprocessing.Process):
    """
    The core execution environment. Manages the execution of a single task
    in a separate, reliably killable multiprocessing context, and retrieves its result.

    :param task: The synchronous or asynchronous function to execute.
    :param args: Arguments to pass to the task function.
    :param name_prefix: Prefix for the process name (e.g., "Job-Runner").
    """

    def __init__(
        self,
        task: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ):
        """
        Initializes the Runner process.

        :param task: The synchronous or asynchronous function to execute.
        :param args: Arguments to pass to the task function.
        """
        super().__init__(name=task.__name__, daemon=True)
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self.is_async = inspect.iscoroutinefunction(task)
        self._result_queue: multiprocessing.Queue[Any] = multiprocessing.Queue(1)

    def go(self) -> None:
        """Starts the Runner process in the background."""
        self.start()

    def run(self) -> None:
        """
        The main execution method, executed in the child process.
        It executes the assigned task, sends the result to the queue, and exits.
        """
        try:
            if self.is_async:
                result = asyncio.run(self.task(*self.args, **self.kwargs))
            else:
                result = self.task(*self.args, **self.kwargs)

            try:
                self._result_queue.put(result)
            except BrokenPipeError as e:
                pass
        except Exception as e:
            logger.debug(traceback.format_exc())
            try:
                self._result_queue.put(e)
            except BrokenPipeError as e:
                pass

    def get_results(self, timeout: Optional[float] = None) -> Any:
        """
        Waits for the runner process to complete and returns the result.

        :param timeout: Time in seconds to wait for the result. If None, waits indefinitely.
        :returns: The result returned by the executed task function.
        :raises TimeoutError: If the result is not available within the specified timeout.
        :raises Exception: If the task execution in the runner process failed.
        """
        logger.debug(f"Runner.get_results() called for {self.name}, timeout={timeout}")
        try:
            self.join(timeout=timeout)

            if self.is_alive():
                raise TimeoutError(f"runner timed out after {timeout} seconds.")

            if self._result_queue.empty():
                if self.exitcode == 0:
                    return None
                else:
                    raise Exception(f"runner failed with code {self.exitcode}")

            try:
                result = self._result_queue.get_nowait()
            except Exception as e:
                raise

            if isinstance(result, Exception):
                raise result

            return result
        except Exception as e:
            raise e

    def cancel(self) -> bool:
        """
        Attempts to cancel the running process.

        :returns: True if the process was successfully terminated, False otherwise.
        :raises: Exception if an error occurs during termination.
        """
        try:
            if not self.is_alive():
                logger.debug(f"Runner {self.name}: Already finished")
                return True

            logger.debug(f"Cancelling runner {self.name}")
            self.terminate()
            self.join(timeout=5.0)  # Wait up to 5 seconds for clean termination
            result = not self.is_alive()
            return result
        except Exception as e:
            logger.warning(f"Error cancelling runner {self.name}: {e}")
            return False


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
    func: Callable[..., Any], use_mp: bool = True, *args: Any, **kwargs: Any
) -> Union[Runner, SmallRunner]:
    """
    Go-like async function execution factory. Selects the appropriate runner.

    :param func: The function to execute.
    :param use_mp: If True (default), uses the multiprocessing Runner (isolated).
                   If False, uses the threading SmallRunner (shared state).
    :param args: Positional arguments for the function.
    :param kwargs: Keyword arguments for the function.
    :returns: A running Runner or SmallRunner instance.
    """
    if use_mp:
        runner = Runner(func, *args, **kwargs)
    else:
        runner = SmallRunner(func, *args, **kwargs)

    runner.go()
    return runner
