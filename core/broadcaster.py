"""
Broadcaster using asyncio for fully async-aware messaging.

This implementation uses asyncio throughout for consistent async behavior.
"""

import asyncio
from typing import TypeVar, Generic, Dict

T = TypeVar("T")


def new_broadcaster(name: str) -> "Broadcaster[T]":
    """Create a new broadcaster. Mirrors Go's NewBroadcaster function."""
    return Broadcaster[T](name)


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
