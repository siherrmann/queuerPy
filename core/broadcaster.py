"""
Broadcaster using asyncio for fully async-aware messaging.

This implementation uses asyncio throughout for consistent async behavior.
Uses singleton pattern to ensure all queuer instances share the same broadcaster.
"""

import asyncio
from typing import TypeVar, Generic, Dict, ClassVar

T = TypeVar("T")

# Global registry of broadcasters by name
_broadcasters: Dict[str, "Broadcaster"] = {}
_broadcaster_lock = asyncio.Lock()


async def new_broadcaster(name: str) -> "Broadcaster[T]":
    """
    Create a new broadcaster or return existing one.
    Mirrors Go's NewBroadcaster function but with singleton behavior.
    """
    async with _broadcaster_lock:
        if name not in _broadcasters:
            _broadcasters[name] = Broadcaster[T](name)
        return _broadcasters[name]


def new_broadcaster_sync(name: str) -> "Broadcaster[T]":
    """
    Synchronous version for use in non-async contexts.
    """
    # For backwards compatibility, create directly if no event loop
    try:
        loop = asyncio.get_running_loop()
        # We can't use async here, so check if it exists and create if not
        if name not in _broadcasters:
            _broadcasters[name] = Broadcaster[T](name)
        return _broadcasters[name]
    except RuntimeError:
        # No event loop running, create directly
        if name not in _broadcasters:
            _broadcasters[name] = Broadcaster[T](name)
        return _broadcasters[name]


class Broadcaster(Generic[T]):
    """A broadcaster using asyncio queues for fully async messaging."""

    def __init__(self, name: str):
        """Initialize broadcaster."""
        self.name: str = name
        self.listeners: Dict[asyncio.Queue, bool] = {}
        self.mutex: asyncio.Lock = asyncio.Lock()

    async def subscribe(self) -> asyncio.Queue:
        """Subscribe and get an asyncio queue."""
        ch = asyncio.Queue(maxsize=100)  # Large buffer to avoid blocking

        async with self.mutex:
            self.listeners[ch] = True

        return ch

    async def unsubscribe(self, ch: asyncio.Queue) -> None:
        """Unsubscribe a channel."""
        async with self.mutex:
            if ch in self.listeners:
                del self.listeners[ch]

    async def broadcast(self, msg: T) -> None:
        """Broadcast message to all channels."""
        async with self.mutex:
            listeners = list(self.listeners.keys())

        for ch in listeners:
            try:
                # Non-blocking send to buffered channel
                ch.put_nowait(msg)
            except asyncio.QueueFull:
                # Remove failed channels
                async with self.mutex:
                    if ch in self.listeners:
                        del self.listeners[ch]
