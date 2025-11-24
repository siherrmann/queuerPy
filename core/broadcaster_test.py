"""
Test cases for the asyncio-based broadcaster - mirrors Go tests.
"""

import unittest
import asyncio

from .broadcaster import Broadcaster, broadcaster_registry


class TestBroadcaster(unittest.IsolatedAsyncioTestCase):
    """Test the asyncio-based broadcaster implementation."""

    def setUp(self):
        """Clear broadcaster registry before each test."""
        broadcaster_registry.clear()

    async def test_new_broadcaster(self):
        """Test creating a new broadcaster."""
        b = Broadcaster[int]("testBroadcaster")
        self.assertIsNotNone(b, "Broadcaster should not be None")
        self.assertEqual(
            len(b.listeners),
            0,
            "NewBroadcaster should initialize with an empty listeners map",
        )

    async def test_subscribe(self):
        """Test subscribing to a broadcaster."""
        b = Broadcaster[int]("testBroadcaster")
        ch = await b.subscribe()
        self.assertIsNotNone(ch, "Subscribe should return a valid channel")
        self.assertEqual(
            len(b.listeners), 1, "Should have one listener after subscribe"
        )

    async def test_unsubscribe(self):
        """Test unsubscribing from a broadcaster."""
        b: Broadcaster[int] = Broadcaster[int]("testBroadcaster")
        ch = await b.subscribe()
        self.assertEqual(
            len(b.listeners), 1, "Should have one listener after subscribe"
        )

        await b.unsubscribe(ch)
        self.assertEqual(
            len(b.listeners), 0, "Should have no listeners after unsubscribe"
        )

    async def test_broadcast(self):
        """Test broadcasting to multiple channels."""
        b = Broadcaster[int]("testBroadcaster")
        ch1 = await b.subscribe()
        ch2 = await b.subscribe()

        # Test broadcasting like Go test
        num_messages = 5
        for i in range(num_messages):
            await b.broadcast(i)

        messages_received = 0

        # Receive from both channels
        for _ in range(num_messages * 2):
            try:
                # Try ch1 first
                try:
                    msg = ch1.get_nowait()
                    self.assertLess(
                        msg, num_messages, "Channel 1 should receive valid message"
                    )
                    messages_received += 1
                    continue
                except asyncio.QueueEmpty:
                    pass

                # Try ch2
                try:
                    msg = ch2.get_nowait()
                    self.assertLess(
                        msg, num_messages, "Channel 2 should receive valid message"
                    )
                    messages_received += 1
                except asyncio.QueueEmpty:
                    pass

            except Exception:
                break

        # Should have received some messages
        self.assertGreater(
            messages_received,
            0,
            f"Should have received some messages, got {messages_received}",
        )


if __name__ == "__main__":
    unittest.main()


if __name__ == "__main__":
    unittest.main()
