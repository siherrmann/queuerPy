"""
Threadsafe Runner - Go-like async task execution with result channel.
"""

import multiprocessing
import threading
import time
import inspect
import asyncio
import logging
import traceback
from datetime import timedelta
from typing import Callable, Any, Dict, Optional, Tuple, Union

# Configure logging for better visibility in examples
logging.basicConfig(
    level=logging.INFO, format="[%(levelname)s] [%(processName)s] %(message)s"
)
logger = logging.getLogger(__name__)


class Runner(multiprocessing.Process):
    """
    The core execution environment. Manages the execution of a single task
    in a separate, reliably killable multiprocessing context, and retrieves its result.
    """

    def __init__(
        self,
        task: Callable,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        name_prefix: str = "Runner",
    ):
        """
        Initializes the Runner process.

        :param task: The synchronous or asynchronous function to execute.
        :param args: Arguments to pass to the task function.
        :param name_prefix: Prefix for the process name (e.g., "Job-Runner").
        """
        super().__init__(name=f"{name_prefix}-{task.__name__}", daemon=True)
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self.is_async = inspect.iscoroutinefunction(task)
        self._result_queue: multiprocessing.Queue = multiprocessing.Queue(1)

    def go(self):
        """Starts the Runner process in the background."""
        self.start()

    def run(self):
        """
        The main execution method, executed in the child process.
        It executes the assigned task, sends the result to the queue, and exits.
        """
        try:
            if self.is_async:
                result = asyncio.run(self.task(*self.args, **self.kwargs))
            else:
                result = self.task(*self.args, **self.kwargs)
            self._result_queue.put(result)
        except Exception as e:
            logger.error(f"Task execution failed: {e.__class__.__name__}: {e}")
            logger.debug(traceback.format_exc())
            self._result_queue.put(e)

    def get_results(self, timeout: Optional[float] = None) -> Any:
        """
        Waits for the runner process to complete and returns the result.

        :param timeout: Time in seconds to wait for the result. If None, waits indefinitely.
        :returns: The result returned by the executed task function.
        :raises TimeoutError: If the result is not available within the specified timeout.
        :raises Exception: If the task execution in the runner process failed.
        """
        try:
            self.join(timeout=timeout)
            if self.is_alive():
                raise TimeoutError(f"runner timed out after {timeout} seconds.")

            if self._result_queue.empty():
                if self.exitcode == 0:
                    return None
                else:
                    raise Exception(f"runner failed with code {self.exitcode}")

            result = self._result_queue.get_nowait()

            if isinstance(result, Exception):
                raise result

            return result
        except Exception as e:
            raise e

    def cancel(self) -> bool:
        """
        Attempts to cancel the running process.

        :returns: True if the process was successfully terminated, False otherwise.
        """
        try:
            if self.is_alive():
                self.terminate()
                self.join(timeout=5.0)  # Wait up to 5 seconds for clean termination
                return not self.is_alive()
            return True  # Already finished
        except Exception as e:
            logger.error(f"Error cancelling process: {e}")
            return False


class SmallRunner(threading.Thread):
    """
    A lightweight, thread-based runner suitable for tasks needing
    direct access to the parent process's shared state (like heartbeats).
    """

    def __init__(
        self,
        task: Callable,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        name_prefix: str = "SmallRunner",
    ):
        super().__init__(name=f"{name_prefix}-{task.__name__}", daemon=True)
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self.is_async = inspect.iscoroutinefunction(task)
        self._result_queue: multiprocessing.Queue = multiprocessing.Queue(1)

    def go(self):
        """Starts the SmallRunner thread in the background."""
        self.start()

    def run(self):
        """
        The main execution method for the thread.
        """
        try:
            if self.is_async:
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                result = loop.run_until_complete(self.task(*self.args, **self.kwargs))
            else:
                result = self.task(*self.args, **self.kwargs)

            self._result_queue.put(result)

        except Exception as e:
            logger.error(f"SmallRunner task failed: {e.__class__.__name__}: {e}")
            logger.debug(traceback.format_exc())
            self._result_queue.put(e)

    def get_results(self, timeout: Optional[float] = None) -> Any:
        """
        Waits for the thread to complete and returns the result.
        """
        try:
            self.join(timeout=timeout)
            if self.is_alive():
                raise TimeoutError(f"small runner timed out after {timeout} seconds")

            result = self._result_queue.get(timeout=0.1)

            if isinstance(result, Exception):
                raise result

            return result
        except Exception as e:
            raise e


def go_func(
    func: Callable, use_mp: bool = True, *args, **kwargs
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
        runner = Runner(func, args, kwargs)
    else:
        runner = SmallRunner(func, args, kwargs)
    runner.go()
    return runner
