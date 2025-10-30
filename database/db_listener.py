"""
Database listener handler for Python queuer implementation.
Async version using psycopg AsyncConnection for PostgreSQL LISTEN/NOTIFY.
"""

import asyncio
import logging
from typing import Optional, Callable, Awaitable
import psycopg
from psycopg import AsyncConnection
from psycopg.rows import dict_row

from helper.database import DatabaseConfiguration


class QueuerListener:
    """
    Async database listener for PostgreSQL LISTEN/NOTIFY.
    Completely async implementation without threading.
    """

    def __init__(self, db_config: DatabaseConfiguration, channel: str):
        """Initialize async database listener."""
        self.db_config = db_config
        self.channel = channel
        self.connection: Optional[AsyncConnection] = None
        self.logger = logging.getLogger(__name__)
        self._listening = False
        self._listen_task: Optional[asyncio.Task] = None
        self._notify_function: Optional[Callable[[str], Awaitable[None]]] = None
        self._ping_timeout = 5.0
        self._stop_event = asyncio.Event()

    async def _connect(self) -> None:
        """Establish async database connection."""
        try:
            connection_string = self.db_config.connection_string()
            self.connection = await psycopg.AsyncConnection.connect(
                connection_string,
                row_factory=dict_row,
                autocommit=True,  # Required for LISTEN/NOTIFY
            )

            # Start listening to the channel
            async with self.connection.cursor() as cur:
                await cur.execute(f"LISTEN {self.channel};")

            self.logger.info(
                f"Async database listener connected to channel: {self.channel}"
            )
        except Exception as e:
            self.logger.error(f"Failed to connect async database listener: {e}")
            self.connection = None
            raise

    async def _reconnect(self) -> bool:
        """Attempt to reconnect to the database."""
        # Close existing connection if any
        if self.connection:
            try:
                await self.connection.close()
            except Exception:
                pass  # Ignore errors when closing
            self.connection = None

        # Attempt to reconnect
        try:
            await self._connect()
            return True
        except Exception as e:
            self.logger.error(f"Failed to reconnect async database listener: {e}")
            return False

    async def listen(self, notify_function: Callable[[str], Awaitable[None]]) -> None:
        """
        Listen for events on the specified channel and process them.
        Async version of the listen method.
        """
        await self.listen_with_timeout(notify_function, 90.0)

    async def listen_with_timeout(
        self, notify_function: Callable[[str], Awaitable[None]], ping_timeout: float
    ) -> None:
        """
        Listen with timeout - async version.
        """
        self._notify_function = notify_function
        self._ping_timeout = ping_timeout

        if not self._listening:
            self._listening = True
            # Connect first if not already connected
            if not self.connection:
                await self._connect()

            # Start the listening loop
            self._listen_task = asyncio.create_task(self._listen_loop())

    async def _listen_loop(self) -> None:
        """
        Main async listening loop for processing notifications.
        Uses asyncio instead of threading for better performance.
        """
        retry_count = 0
        max_retries = 3

        while self._listening and not self._stop_event.is_set():
            try:
                await self._handle_listen_iteration()
                retry_count = 0  # Reset on successful iteration
            except Exception as e:
                retry_count += 1
                self.logger.error(
                    f"Error in listen loop (attempt {retry_count}/{max_retries}): {e}"
                )

                if retry_count >= max_retries:
                    self.logger.error("Max retries exceeded, stopping listener")
                    break

                # Wait before retrying
                await asyncio.sleep(min(retry_count * 2, 10))

        self._listening = False
        self.logger.info(f"Async listener stopped for channel: {self.channel}")

    async def _handle_listen_iteration(self) -> None:
        """Handle a single iteration of the listen loop."""
        # Ensure connection is available
        if not self.connection:
            self.logger.warning("No connection, attempting to reconnect...")
            if not await self._reconnect():
                self.logger.error("Failed to reconnect, stopping listener")
                raise ConnectionError("Failed to reconnect to database")

        # Check for notifications with timeout
        await self._check_and_process_notifications()

        # Check if we should stop (with short timeout for responsiveness)
        await self._check_stop_condition()

    async def _check_and_process_notifications(self) -> None:
        """Check for and process notifications."""
        try:
            # Use asyncio.wait_for to add timeout protection
            notifies = await asyncio.wait_for(
                self._check_notifications(), timeout=self._ping_timeout
            )

            if notifies:
                self.logger.info(
                    f"Received {len(notifies)} notifications on channel {self.channel}"
                )

            # Process notifications concurrently
            tasks = []
            for notify in notifies:
                if notify and self._notify_function:
                    self.logger.info(
                        f"Processing notification: channel={notify.channel}, payload={notify.payload[:100] if notify.payload else 'None'}..."
                    )
                    # Create async task for notification processing
                    task = asyncio.create_task(
                        self._notify_function(notify.payload or "")
                    )
                    tasks.append(task)
                else:
                    self.logger.warning(
                        f"Skipping notification: notify={notify}, function={self._notify_function}"
                    )

            # Wait for all notification tasks to complete
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        except asyncio.TimeoutError:
            # Timeout is normal - just continue listening
            pass
        except Exception as notify_error:
            self.logger.error(f"Error checking notifications: {notify_error}")
            # If we can't check notifications, the connection might be bad
            self.connection = None
            raise

    async def _check_stop_condition(self) -> None:
        """Check if the listener should stop."""
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=0.5)
            # If we get here without timeout, we should stop
            self._listening = False
        except asyncio.TimeoutError:
            # Timeout is expected - continue loop
            pass

    async def _check_notifications(self):
        """Check for notifications on the connection."""
        if not self.connection:
            return []

        # Get notifications (this should be non-blocking in async mode)
        notifies = []

        # Use a generator to get notifications without blocking
        gen = self.connection.notifies()
        try:
            notify = await gen.__anext__()
            notifies.append(notify)
        except StopAsyncIteration:
            # No notifications available
            pass
        except Exception as e:
            self.logger.debug(f"No notifications available: {e}")

        return notifies

    async def stop(self) -> None:
        """
        Stop the async listener.
        """
        if not self._listening:
            return

        self._listening = False
        self._stop_event.set()

        # Cancel the listen task
        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass

        # Close the connection
        if self.connection:
            try:
                await self.connection.close()
            except Exception as e:
                self.logger.error(f"Error closing connection: {e}")
            self.connection = None

        self.logger.info(f"Async database listener stopped for channel: {self.channel}")


def new_queuer_db_listener(
    db_config: DatabaseConfiguration, channel: str
) -> QueuerListener:
    """
    Create a new QueuerListener instance.
    Mirrors Go's NewQueuerDBListener function.
    """
    return QueuerListener(db_config, channel)
