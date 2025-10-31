"""
Test module for async QueuerListener with real PostgreSQL testcontainers.

Tests the async implementation without any threading.
"""

import asyncio
import pytest

from database.db_listener import QueuerListener, new_queuer_db_listener
from helper.database import DatabaseConfiguration
from helper.test_database import DatabaseTestMixin
from core.runner import Runner


class TestAsyncQueuerListenerWithContainer(DatabaseTestMixin):
    """Test class for async QueuerListener functionality using real PostgreSQL containers."""

    def setup_method(self):
        """Set up test environment with real PostgreSQL container."""
        super().setup_method()
        self._test_listeners = []

    def teardown_method(self, method=None):
        """Clean up test environment."""
        if hasattr(self, "_test_listeners"):

            async def stop_listeners():
                for listener in self._test_listeners:
                    try:
                        await listener.stop()
                    except Exception as e:
                        print(f"Error stopping listener: {e}")

            # Use the Runner to handle async cleanup
            try:
                runner = Runner()
                runner.run_async_task(stop_listeners())
            except Exception as e:
                print(f"Error during async cleanup: {e}")

        super().teardown_method(method)

    @pytest.mark.asyncio
    async def test_new_queuer_db_listener_real_connection(self):
        """Test creating async listener with real database connection."""
        config = self.db_config

        listener = new_queuer_db_listener(config, "test_channel")
        self._test_listeners.append(listener)

        assert listener is not None
        assert isinstance(listener, QueuerListener)
        assert not listener._stop_event.is_set()

    @pytest.mark.asyncio
    async def test_async_listen_for_real_notifications(self):
        """Test that async listener receives real PostgreSQL notifications."""
        received_notifications = []

        async def callback(payload):
            received_notifications.append(payload)

        config = self.db_config

        # Create listener
        listener = new_queuer_db_listener(config, "test_channel")
        self._test_listeners.append(listener)

        # Start listening with callback
        listen_task = asyncio.create_task(listener.listen(callback))

        # Give listener time to start
        await asyncio.sleep(0.1)

        # Send notification using separate connection
        import psycopg

        conn_str = f"host={config.host} port={config.port} user={config.username} password={config.password} dbname={config.database}"

        async with await psycopg.AsyncConnection.connect(conn_str) as conn:
            async with conn.cursor() as cur:
                await cur.execute("NOTIFY test_channel, 'async_test_payload'")

        # Wait for notification to be processed
        await asyncio.sleep(0.2)

        # Stop the listener
        await listener.stop()

        # Verify notification was received
        assert len(received_notifications) == 1
        assert received_notifications[0] == "async_test_payload"
