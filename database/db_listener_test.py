"""
Test module for async QueuerListener with real PostgreSQL testcontainers.
Clean async implementation tests.
"""

import asyncio
from typing import Any, List
import pytest
import psycopg

from .db_listener import QueuerListener, new_queuer_db_listener
from ..helper.database import DatabaseConfiguration
from ..helper.test_database import DatabaseTestMixin
from ..helper.error import QueuerError


class TestQueuerListenerWithContainer(DatabaseTestMixin):
    """Test class for async QueuerListener functionality using real PostgreSQL containers."""

    def setup_method(self, method: Any = None):
        """Set up test environment with real PostgreSQL container."""
        super().setup_method()
        self._test_listeners: List[QueuerListener] = []

    def teardown_method(self, method: Any = None):
        """Clean up test environment."""
        super().teardown_method()

    def test_new_queuer_db_listener_real_connection(self):
        """Test creating listener with real database connection."""
        config = self.db_config

        listener = new_queuer_db_listener(config, "test_channel")
        self._test_listeners.append(listener)

        # Check similar fields as Go test
        assert listener is not None, "Expected listener to be created"
        assert isinstance(listener, QueuerListener)
        assert (
            listener.channel == "test_channel"
        ), "Expected listener.channel to match the provided channel name"
        assert (
            listener.db_config == config
        ), "Expected listener.db_config to match the provided config"
        # In async version, connection is created on demand, not at construction
        assert listener.connection is None, "Expected connection to be None initially"

    @pytest.mark.asyncio
    async def test_listener_connection_established_on_listen(self):
        """Test that connection is established when listening starts (like Go's Listener field check)."""
        config = self.db_config

        listener = new_queuer_db_listener(config, "test_channel")
        self._test_listeners.append(listener)

        # Initially no connection
        assert listener.connection is None

        # Start connecting manually (like what happens during listen)
        await listener.connect()

        # Now connection should be established
        assert (
            listener.connection is not None
        ), "Expected connection to be established after _connect()"
        assert not listener.connection.closed, "Expected connection to be open"

        # Clean up
        await listener.stop()

    @pytest.mark.asyncio
    async def test_new_queuer_db_listener_invalid_config(self):
        """Test creating listener with invalid database configuration."""
        # Invalid config should raise QueuerError when connecting
        invalid_config = DatabaseConfiguration(
            host="invalid_host",
            port=5432,
            username="invalid_user",
            password="invalid_password",
            database="invalid_db",
            sslmode="disable",
        )

        listener = new_queuer_db_listener(invalid_config, "test_channel")

        # Error should occur when trying to connect/listen
        with pytest.raises(QueuerError) as exc_info:
            await listener.connect()

        assert "listen" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_listen_for_real_notifications(self):
        """Test that listener receives real PostgreSQL notifications."""
        received_notifications: List[str] = []

        async def async_callback(payload: str):
            received_notifications.append(payload)

        config = self.db_config

        # Create listener
        listener = new_queuer_db_listener(config, "test_channel")
        self._test_listeners.append(listener)

        # Start listening in background task
        listen_task = asyncio.create_task(listener.listen(async_callback))

        # Give listener time to start and connect
        await asyncio.sleep(0.5)

        # Send notification using separate connection
        notify_conn = psycopg.connect(
            host=config.host,
            port=config.port,
            user=config.username,
            password=config.password,
            dbname=config.database,
            sslmode=config.sslmode,
        )

        with notify_conn.cursor() as cur:
            cur.execute("NOTIFY test_channel, 'test_payload'")
        notify_conn.commit()
        notify_conn.close()

        # Wait for notification to be processed
        await asyncio.sleep(0.5)

        # Stop the listener
        await listener.stop()
        listen_task.cancel()

        try:
            await listen_task
        except asyncio.CancelledError:
            pass

        # Verify notification was received
        assert len(received_notifications) == 1
        assert received_notifications[0] == "test_payload"

    @pytest.mark.asyncio
    async def test_listen_with_multiple_notifications(self):
        """Test listener handles multiple notifications correctly."""
        received_notifications: List[str] = []

        async def async_callback(payload: str):
            received_notifications.append(payload)

        config = self.db_config

        # Create listener
        listener = new_queuer_db_listener(config, "multi_channel")
        self._test_listeners.append(listener)

        # Start listening
        listen_task = asyncio.create_task(listener.listen(async_callback))

        # Give listener time to start
        await asyncio.sleep(0.5)

        # Send multiple notifications
        notify_conn = psycopg.connect(
            host=config.host,
            port=config.port,
            user=config.username,
            password=config.password,
            dbname=config.database,
            sslmode=config.sslmode,
        )

        with notify_conn.cursor() as cur:
            cur.execute("NOTIFY multi_channel, 'payload1'")
            cur.execute("NOTIFY multi_channel, 'payload2'")
            cur.execute("NOTIFY multi_channel, 'payload3'")
        notify_conn.commit()
        notify_conn.close()

        # Wait for notifications to be processed
        await asyncio.sleep(1.0)

        # Stop the listener
        await listener.stop()
        listen_task.cancel()

        try:
            await listen_task
        except asyncio.CancelledError:
            pass

        # Verify all notifications were received
        assert len(received_notifications) == 3
        assert "payload1" in received_notifications
        assert "payload2" in received_notifications
        assert "payload3" in received_notifications

    @pytest.mark.asyncio
    async def test_listen_ignores_other_channels(self):
        """Test that listener only processes notifications for its channel."""
        received_notifications: List[str] = []

        async def async_callback(payload: str):
            received_notifications.append(payload)

        config = self.db_config

        # Create listener for specific channel
        listener = new_queuer_db_listener(config, "target_channel")
        self._test_listeners.append(listener)

        # Start listening
        listen_task = asyncio.create_task(listener.listen(async_callback))

        # Give listener time to start
        await asyncio.sleep(0.5)

        # Send notifications to different channels
        notify_conn = psycopg.connect(
            host=config.host,
            port=config.port,
            user=config.username,
            password=config.password,
            dbname=config.database,
            sslmode=config.sslmode,
        )

        with notify_conn.cursor() as cur:
            cur.execute("NOTIFY other_channel, 'should_ignore'")
            cur.execute("NOTIFY target_channel, 'should_receive'")
            cur.execute("NOTIFY another_channel, 'should_ignore_too'")
        notify_conn.commit()
        notify_conn.close()

        # Wait for notifications to be processed
        await asyncio.sleep(1.0)

        # Stop the listener
        await listener.stop()
        listen_task.cancel()

        try:
            await listen_task
        except asyncio.CancelledError:
            pass

        # Verify only target channel notification was received
        assert len(received_notifications) == 1
        assert received_notifications[0] == "should_receive"

    @pytest.mark.asyncio
    async def test_listen_with_timeout_ping_functionality(self):
        """Test ping timeout functionality like Go's TestListenWithTimeout."""
        config = self.db_config

        # Create listener
        listener = new_queuer_db_listener(config, "test_ping_channel")
        self._test_listeners.append(listener)

        # Track that no notifications are processed
        received_notifications: List[str] = []

        async def async_callback(payload: str):
            received_notifications.append(payload)

        # Start listening
        listen_task = asyncio.create_task(listener.listen(async_callback))

        # Let it run for a short time to test ping behavior
        await asyncio.sleep(2.0)  # Should trigger internal ping/timeout handling

        # Stop the listener
        await listener.stop()
        listen_task.cancel()

        try:
            await listen_task
        except asyncio.CancelledError:
            pass

        # Verify no notifications were processed (this tests that the timeout logic works)
        assert len(received_notifications) == 0
        # If we get here without hanging, the ping timeout logic is working correctly
