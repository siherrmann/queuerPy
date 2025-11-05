"""
ProcessRunner for better process control and cancellation using multiprocessing.
"""

import multiprocessing as mp
import asyncio
import time
from typing import Any, List, Optional, Callable


def _execute_task(task: Callable, parameters: List[Any]) -> Any:
    """Execute task in separate process."""
    try:
        if asyncio.iscoroutinefunction(task):
            return asyncio.run(task(*parameters))
        else:
            return task(*parameters)
    except Exception as e:
        # Return the exception instead of raising it for compatibility with tests
        return e


class ProcessRunner:
    """Task runner using multiprocessing.Process for better cancellation control."""

    def __init__(self, task: Callable, parameters: List[Any] = None):
        self.task = task
        self.parameters = parameters or []
        self.process: Optional[mp.Process] = None
        self._result_queue: Optional[mp.Queue] = None
        self._exception_queue: Optional[mp.Queue] = None

    def run(self) -> bool:
        """Start task execution in separate process."""
        if self.process is not None:
            return False

        try:
            # Create queues for result and exception passing
            self._result_queue = mp.Queue()
            self._exception_queue = mp.Queue()

            # Create and start the process
            self.process = mp.Process(
                target=self._run_task_in_process,
                args=(
                    self.task,
                    self.parameters,
                    self._result_queue,
                    self._exception_queue,
                ),
            )
            self.process.start()
            return True

        except Exception as e:
            print(f"Failed to start process: {e}")
            return False

    def get_results(self, timeout: float = None) -> Any:
        """Get task results."""
        if self.process is None:
            raise RuntimeError("Task not started")

        try:
            # Wait for process to complete
            self.process.join(timeout=timeout)

            if self.process.is_alive():
                # Process is still running after timeout
                self.cancel()
                raise TimeoutError("Task timed out")

            # Check for exceptions first
            if not self._exception_queue.empty():
                exception = self._exception_queue.get()
                raise exception

            # Get the result
            if not self._result_queue.empty():
                return self._result_queue.get()
            else:
                # Process completed but no result - might have been cancelled
                if self.process.exitcode != 0:
                    raise Exception(f"Process exited with code {self.process.exitcode}")
                else:
                    raise Exception("No result available")

        except TimeoutError:
            raise
        except Exception as e:
            raise e

    def cancel(self) -> bool:
        """Cancel the running process."""
        if self.process is None or not self.process.is_alive():
            return False

        try:
            # Terminate the process
            self.process.terminate()

            # Wait a bit for graceful shutdown
            self.process.join(timeout=2.0)

            # If still alive, force kill
            if self.process.is_alive():
                self.process.kill()
                self.process.join()

            return True

        except Exception as e:
            print(f"Error cancelling process: {e}")
            return False

    def is_running(self) -> bool:
        """Check if the process is running."""
        if self.process is None:
            return False
        return self.process.is_alive()

    def _get_pool(self):
        """Compatibility method for tests - returns None since we don't use a shared pool."""
        return None

    @staticmethod
    def _run_task_in_process(
        task: Callable,
        parameters: List[Any],
        result_queue: mp.Queue,
        exception_queue: mp.Queue,
    ):
        """Run the task in a separate process and put results in queue."""
        try:
            result = _execute_task(task, parameters)
            # Check if the result is an exception
            if isinstance(result, Exception):
                exception_queue.put(result)
            else:
                result_queue.put(result)
        except Exception as e:
            # This should rarely happen now since _execute_task returns exceptions
            exception_queue.put(e)

    @classmethod
    def shutdown_pool(cls):
        """Compatibility method - no-op for process runner."""
        pass


# Factory functions
def new_process_runner(task: Callable, parameters: List[Any] = None) -> ProcessRunner:
    """Create ProcessRunner."""
    return ProcessRunner(task, parameters)


def new_process_runner_from_job(task: Callable, job) -> ProcessRunner:
    """Create ProcessRunner from job."""
    parameters = getattr(job, "parameters", [])
    return ProcessRunner(task, parameters)
