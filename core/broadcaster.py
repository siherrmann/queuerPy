"""
Broadcaster using asyncio for fully async-aware notifications.

This implementation uses asyncio.Queue for consistent async behavior and follows
the singleton broadcaster pattern for sharing broadcasters across the system.
"""

import asyncio
from typing import Any, TypeVar, Generic, Dict
import uuid

T = TypeVar("T")

# Global registry for singleton broadcasters
broadcaster_registry: Dict[str, "Broadcaster[Any]"] = {}


class BroadcasterQueue(asyncio.Queue[T]):
    """An asyncio.Queue with a broadcaster ID for tracking."""

    def __init__(self):
        super().__init__()
        self.broadcaster_id: str = ""


class Broadcaster(Generic[T]):
    """A broadcaster that manages async listeners and broadcasts messages."""

    def __new__(cls, name: str) -> "Broadcaster[T]":
        """Singleton pattern: return existing instance if it exists."""
        if name not in broadcaster_registry:
            instance = super().__new__(cls)
            broadcaster_registry[name] = instance
            return instance
        return broadcaster_registry[name]

    def __init__(self, name: str):
        """Initialize a new broadcaster (only called once per unique name)."""
        if not hasattr(self, "name"):
            self.name = name
            self.listeners: Dict[str, asyncio.Queue[T]] = {}
            self._lock = asyncio.Lock()
            self.broadcaster_id: str = ""

    async def subscribe(self) -> BroadcasterQueue[T]:
        """Subscribe to broadcasts and return a queue for receiving messages."""
        queue = BroadcasterQueue[T]()
        queue.broadcaster_id = str(uuid.uuid4())

        async with self._lock:
            self.listeners[queue.broadcaster_id] = queue

        return queue

    async def unsubscribe(self, queue: BroadcasterQueue[T]) -> None:
        """Unsubscribe a queue from broadcasts."""
        queue_id = queue.broadcaster_id
        async with self._lock:
            if queue_id in self.listeners:
                del self.listeners[queue_id]

    async def broadcast(self, message: T) -> None:
        """Broadcast a message to all subscribers."""
        async with self._lock:
            current_listeners = list(self.listeners.values())

        # Send to all listeners without holding the lock
        for queue in current_listeners:
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                pass


def new_broadcaster(name: str) -> Broadcaster[Any]:
    """Get or create a singleton broadcaster with the given name."""
    return Broadcaster(name)
