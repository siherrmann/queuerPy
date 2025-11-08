"""
Slim Runner - Go-like async task execution with singleton event loop.
"""

import asyncio
import threading
import logging
import time
from typing import Any, List, Optional, Callable

from model.options_on_error import OnError


class Runner:
    """
    Singleton Runner with shared event loop for all async tasks.
    Usage: go_func(my_function, arg1, arg2) or Runner().submit_async_task(coro)
    """

    _instance: Optional["Runner"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "Runner":
        if cls._instance is None:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized") and self._initialized:
            return

        self._initialized = True
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._setup_event_loop()

    def _setup_event_loop(self) -> None:
        """Set up the shared event loop."""
        # Always create a new loop in background thread to avoid conflicts
        # with the main event loop from asyncio.run()
        loop = asyncio.new_event_loop()

        def run_loop():
            asyncio.set_event_loop(loop)
            loop.run_forever()

        thread = threading.Thread(target=run_loop, daemon=True, name="RunnerEventLoop")
        thread.start()
        time.sleep(0.2)  # Give loop more time to start
        self._event_loop = loop

    def get_event_loop(self) -> asyncio.AbstractEventLoop:
        """Get the shared event loop."""
        if self._event_loop is None or self._event_loop.is_closed():
            raise RuntimeError("Event loop not available")
        return self._event_loop

    def submit_async_task(self, coro: Callable) -> asyncio.Future:
        """Submit async task to shared event loop."""
        return asyncio.run_coroutine_threadsafe(coro, self.get_event_loop())

    def run_async_task(self, coro: Callable, timeout: float = None) -> Any:
        """Run async task and wait for result."""
        future = self.submit_async_task(coro)
        return future.result(timeout=timeout)

    def run_in_event_loop(self, coro: Callable) -> asyncio.Future:
        """Alias for submit_async_task."""
        return self.submit_async_task(coro)

    def is_available(self) -> bool:
        """Check if event loop is available."""
        try:
            return not self.get_event_loop().is_closed()
        except:
            return False


class AsyncTask:
    """Individual async task that runs in the shared Runner."""

    def __init__(
        self,
        task: Callable,
        parameters: List[Any] = None,
        options: List[OnError] = None,
    ):
        self.task = task
        self.parameters = parameters or []
        self.options = options or []
        self._running = threading.Event()
        self._done = threading.Event()
        self._result = None
        self._exception = None
        self._future = None

    def run(self) -> bool:
        """Start the task."""
        if self._running.is_set():
            return False

        try:
            runner = Runner()
            self._future = runner.submit_async_task(self._execute())

            def on_done(fut):
                try:
                    self._result = fut.result() if not fut.exception() else None
                    self._exception = fut.exception()
                except Exception as e:
                    self._exception = e
                finally:
                    self._running.clear()
                    self._done.set()

            self._future.add_done_callback(on_done)
            self._running.set()
            return True
        except Exception:
            return False

    async def _execute(self) -> Any:
        """Execute the task."""
        if asyncio.iscoroutinefunction(self.task):
            return await self.task(*self.parameters)
        else:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, self.task, *self.parameters)

    def get_results(self, timeout: float = None) -> Any:
        """Get task results."""
        if not self._done.wait(timeout=timeout):
            raise TimeoutError("Task timed out")
        if self._exception:
            raise self._exception
        return self._result

    def is_running(self) -> bool:
        return self._running.is_set() and not self._done.is_set()

    def is_done(self) -> bool:
        return self._done.is_set()


def go_func(func: Callable, *args, **kwargs) -> AsyncTask:
    """Go-like async function execution."""
    if kwargs:
        wrapper = lambda: func(*args, **kwargs)
        task = AsyncTask(wrapper)
    else:
        task = AsyncTask(func, list(args))
    task.run()
    return task
