"""
Test cases for the goless-based listener - mirrors Go tests.
"""

from typing import List
import unittest
import threading
import time

from .broadcaster import Broadcaster
from .listener import Listener


class TestListener(unittest.TestCase):
    """Test the goless-based listener implementation."""

    def test_new_listener(self):
        """Test creating a new listener."""
        broadcaster = Broadcaster[str]("testBroadcaster")
        listener = Listener(broadcaster)
        self.assertIsNotNone(listener, "Expected non-None listener")

    def test_listen_and_notify(self):
        """Test listening and notification - mirrors Go test."""
        broadcaster = Broadcaster[str]("testBroadcaster")
        listener = Listener(broadcaster)

        data = "test data"
        received_data: List[str] = []

        def notify_function(d: str):
            received_data.append(d)

        for i in range(2):
            stop_event = threading.Event()
            ready_event = threading.Event()

            listener.listen(stop_event, ready_event, notify_function)
            self.assertTrue(
                ready_event.wait(timeout=2.0),
                f"Listener should be ready on iteration {i}",
            )

            # Send notification
            listener.notify(data)

            # Give time for processing
            time.sleep(0.1)

            success = listener.wait_for_notifications_processed(timeout=2.0)
            self.assertTrue(success, f"Processing should complete on iteration {i}")

            stop_event.set()
            time.sleep(0.01)

        # Check if data was received
        self.assertGreater(len(received_data), 0, "Should have received some data")
        for received in received_data:
            self.assertEqual(data, received, "Expected to receive the same data")

    def test_listen_none_notify_function(self):
        """Test listening with None notify function."""
        broadcaster = Broadcaster[str]("testBroadcaster")
        listener = Listener(broadcaster)

        stop_event = threading.Event()
        ready_event = threading.Event()

        # This should return immediately and signal ready
        listener.listen(stop_event, ready_event, None)

        # Should be ready immediately
        self.assertTrue(
            ready_event.wait(timeout=1.0), "Should signal ready immediately"
        )

    def test_wait_for_notifications_processed(self):
        """Test waiting for notifications to be processed."""
        broadcaster = Broadcaster[str]("testBroadcaster")
        listener = Listener(broadcaster)

        processed_count: List[str] = []
        processing_started = threading.Event()

        def slow_notify_function(data: str):
            processing_started.set()
            time.sleep(0.05)  # Simulate processing time
            processed_count.append(data)

        stop_event = threading.Event()
        ready_event = threading.Event()

        # Start listening
        listener.listen(stop_event, ready_event, slow_notify_function)
        self.assertTrue(ready_event.wait(timeout=2.0), "Listener should be ready")

        listener.notify("test_message")
        self.assertTrue(processing_started.wait(timeout=2.0), "Processing should start")

        # Wait for all notifications to be processed
        start_time = time.time()
        success = listener.wait_for_notifications_processed(timeout=3.0)
        end_time = time.time()
        self.assertTrue(success, "Processing should complete")

        # Should have waited for processing to complete
        processing_time = end_time - start_time
        self.assertGreaterEqual(
            processing_time, 0.03, "Should have waited for processing"
        )
        self.assertEqual(len(processed_count), 1, "Should have processed one message")
        self.assertEqual(processed_count[0], "test_message")

        stop_event.set()


if __name__ == "__main__":
    unittest.main()
