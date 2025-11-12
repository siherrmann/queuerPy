"""
Listener using asyncio for fully async-aware notifications.

This implementation uses asyncio throughout for consistent async behavior.
"""

import asyncio
import threading
from typing import TypeVar, Generic, Callable, Optional, Any

from core.broadcaster import Broadcaster

T = TypeVar("T")


class Listener(Generic[T]):
    """A listener that receives broadcasts and calls notification functions."""

    def __init__(self, broadcaster: Broadcaster[T]):
        """Initialize listener."""
        if broadcaster is None:
            raise ValueError("broadcaster cannot be None")

        self.broadcaster: Broadcaster[T] = broadcaster
        self._stop_event = None
        self._listening = False
        self._listen_task = None
        self._active_notifications = 0
        self._notification_lock = threading.Lock()
        self._completion_event = threading.Event()

    def listen(
        self,
        stop_event: threading.Event,
        ready_event: Optional[threading.Event],
        notify_function: Optional[Callable[[T], Any]],
    ) -> None:
        """Start listening for broadcasts and call notify function when received."""
        if notify_function is None:
            if ready_event is not None:
                ready_event.set()
            return

        self._stop_event = stop_event

        async def _listen_async():
            ch = await self.broadcaster.subscribe()
            if ready_event is not None:
                ready_event.set()

            try:
                while not self._stop_event.is_set():
                    try:
                        msg = await asyncio.wait_for(ch.get(), timeout=0.1)

                        # Track notification
                        with self._notification_lock:
                            self._active_notifications += 1
                            self._completion_event.clear()

                        # Call notification function
                        def process_notification():
                            try:
                                notify_function(msg)
                            finally:
                                with self._notification_lock:
                                    self._active_notifications -= 1
                                    if self._active_notifications == 0:
                                        self._completion_event.set()

                        # Process in thread to avoid blocking
                        thread = threading.Thread(
                            target=process_notification, daemon=True
                        )
                        thread.start()

                    except asyncio.TimeoutError:
                        # Check stop event and continue
                        continue
                    except Exception as e:
                        print(f"Error receiving message: {e}")
                        break

            finally:
                await self.broadcaster.unsubscribe(ch)

        # Start listening task
        def run_listen():
            asyncio.run(_listen_async())

        thread = threading.Thread(target=run_listen, daemon=True)
        thread.start()
        self._listening = True

    def notify(self, data: T) -> None:
        """Send notification directly."""

        # Run broadcast in asyncio context
        async def _notify():
            await self.broadcaster.broadcast(data)

        def run_notify():
            asyncio.run(_notify())

        thread = threading.Thread(target=run_notify, daemon=True)
        thread.start()

    def wait_for_notifications_processed(self, timeout: float = 5.0) -> bool:
        """Wait for all notifications to be processed."""
        return self._completion_event.wait(timeout=timeout)

    def stop(self):
        """Stop listening."""
        if self._stop_event:
            self._stop_event.set()
        self._listening = False

    def is_listening(self) -> bool:
        """Check if currently listening."""
        return self._listening
