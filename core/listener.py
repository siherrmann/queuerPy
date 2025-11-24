"""
Listener using asyncio for fully async-aware notifications, cleaned up using a
SmallRunner (threading.Thread) abstraction for non-blocking execution of callbacks.

This implementation uses asyncio throughout for consistent async behavior.
"""

import asyncio
import threading
from typing import TypeVar, Generic, Callable, Optional, Any

from .broadcaster import Broadcaster
from .runner import go_func

T = TypeVar("T")


class Listener(Generic[T]):
    """A listener that receives broadcasts and calls notification functions."""

    def __init__(self, broadcaster: Broadcaster[T]):
        """Initialize listener."""
        self.broadcaster: Broadcaster[T] = broadcaster
        self._stop_event = None
        self._listening = False
        self._active_notifications = 0
        self._notification_lock = threading.Lock()
        self._completion_event = threading.Event()

    def listen(
        self,
        stop_event: Optional[threading.Event] = None,
        ready_event: Optional[threading.Event] = None,
        notify_function: Optional[Callable[[T], Any]] = None,
    ) -> None:
        """
        Starts listening for broadcasts and calls the notify function when received.

        The core asyncio loop is launched in a separate thread (via SmallRunner).
        """
        if notify_function is None:
            if ready_event is not None:
                ready_event.set()
            return

        # Create internal stop event if not provided
        if stop_event is None:
            stop_event = threading.Event()

        self._stop_event = stop_event

        # Define the main coroutine that handles subscription and message reception
        async def _listen_async() -> None:
            ch = await self.broadcaster.subscribe()
            if ready_event is not None:
                ready_event.set()

            if self._stop_event is None:
                self._stop_event = threading.Event()

            try:
                while not self._stop_event.is_set():
                    try:
                        msg = await asyncio.wait_for(ch.get(), timeout=0.1)

                        with self._notification_lock:
                            self._active_notifications += 1
                            self._completion_event.clear()

                        def process_notification() -> None:
                            """Executes the user's callback in a separate thread."""
                            try:
                                notify_function(msg)
                            finally:
                                with self._notification_lock:
                                    self._active_notifications -= 1
                                    if self._active_notifications == 0:
                                        self._completion_event.set()

                        go_func(process_notification, use_mp=False)
                    except asyncio.TimeoutError:
                        # Timeout is expected; loop checks stop event and continues
                        continue
                    except Exception as e:
                        print(f"Error receiving message: {e}")
                        break

            finally:
                await self.broadcaster.unsubscribe(ch)

        go_func(_listen_async, use_mp=False)
        self._listening = True

    def notify(self, data: T) -> None:
        """
        Sends notification directly by broadcasting the data in a separate thread.
        """

        async def _notify() -> None:
            await self.broadcaster.broadcast(data)

        go_func(_notify, use_mp=False)

    def wait_for_notifications_processed(self, timeout: float = 5.0) -> bool:
        """Wait for all notifications to be processed."""
        return self._completion_event.wait(timeout=timeout)

    def stop(self) -> None:
        """Stop listening."""
        if self._stop_event:
            self._stop_event.set()
        self._listening = False

    def is_listening(self) -> bool:
        """Check if currently listening."""
        return self._listening
