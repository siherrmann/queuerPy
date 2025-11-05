"""
Slim ProcessRunner for CPU-intensive tasks.
"""

import asyncio
import multiprocessing as mp
import threading
from concurrent.futures import ProcessPoolExecutor, Future
from typing import Any, List, Optional, Callable


class ProcessRunner:
    """CPU-intensive task runner using process pool."""

    _pool: Optional[ProcessPoolExecutor] = None
    _lock = threading.Lock()

    def __init__(self, task: Callable, parameters: List[Any] = None):
        self.task = task
        self.parameters = parameters or []
        self._future: Optional[Future] = None

    @classmethod
    def _get_pool(cls) -> ProcessPoolExecutor:
        """Get shared process pool."""
        if cls._pool is None:
            with cls._lock:
                if cls._pool is None:
                    cls._pool = ProcessPoolExecutor(
                        max_workers=min(8, mp.cpu_count() + 2),
                        mp_context=mp.get_context("spawn"),
                    )
        return cls._pool

    def run(self) -> bool:
        """Start task execution."""
        if self._future is not None:
            return False
        try:
            self._future = self._get_pool().submit(
                _execute_task, self.task, self.parameters
            )
            return True
        except Exception:
            return False

    def get_results(self, timeout: float = None) -> Any:
        """Get task results."""
        if self._future is None:
            raise RuntimeError("Task not started")
        result = self._future.result(timeout=timeout)
        if isinstance(result, Exception):
            raise result
        return result

    def cancel(self) -> bool:
        """Cancel task."""
        return self._future.cancel() if self._future else False

    def is_running(self) -> bool:
        """Check if running."""
        return self._future is not None and not self._future.done()

    @classmethod
    def shutdown_pool(cls):
        """Shutdown process pool."""
        with cls._lock:
            if cls._pool:
                cls._pool.shutdown(wait=True)
                cls._pool = None


def _execute_task(task: Callable, parameters: List[Any]) -> Any:
    """Execute task in process."""
    try:
        if asyncio.iscoroutinefunction(task):
            return asyncio.run(task(*parameters))
        else:
            return task(*parameters)
    except Exception as e:
        return e  # Return exception to be raised in main process


# Factory functions
def new_process_runner(task: Callable, parameters: List[Any] = None) -> ProcessRunner:
    """Create ProcessRunner."""
    return ProcessRunner(task, parameters)


def new_process_runner_from_job(task: Callable, job) -> ProcessRunner:
    """Create ProcessRunner from job."""
    parameters = getattr(job, "parameters", [])
    return ProcessRunner(task, parameters)
