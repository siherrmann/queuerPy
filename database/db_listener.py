"""
Database listener for PostgreSQL LISTEN/NOTIFY functionality.
Simplified async implementation that maintains compatibility with existing queuer architecture.
"""

import asyncio
import logging
from typing import Optional, Callable, Awaitable
import psycopg
from psycopg import AsyncConnection

from ..helper.database import DatabaseConfiguration
from ..helper.error import QueuerError


class QueuerListener:
    """
    Async database listener for PostgreSQL LISTEN/NOTIFY.
    Simplified version that maintains the working interface.
    """

    def __init__(self, db_config: DatabaseConfiguration, channel: str):
        """
        Initialize async database listener.

        Args:
            db_config: Database configuration
            channel: Channel name to listen on
        """
        self.db_config = db_config
        self.channel = channel
        self.connection: Optional[AsyncConnection] = None
        self.logger = logging.getLogger(f"queuer_db_listener_{channel}")
        self.listening = False
        self._listen_task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()

    async def connect(self) -> None:
        """Establish async database connection."""
        try:
            self.connection = await psycopg.AsyncConnection.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                user=self.db_config.username,
                password=self.db_config.password,
                dbname=self.db_config.database,
                sslmode=self.db_config.sslmode,
                autocommit=True,  # Required for LISTEN/NOTIFY
            )

            # Start listening to the channel
            async with self.connection.cursor() as cur:
                await cur.execute(f"LISTEN {self.channel}".encode("utf-8"))

            self.logger.info(f"Connected and listening on channel: {self.channel}")
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            self.connection = None
            raise QueuerError("listen", e)

    async def listen(self, notify_function: Callable[[str], Awaitable[None]]) -> None:
        """
        Start listening for notifications.
        Maintains the simple async interface expected by queuer.py

        Args:
            notify_function: Async function to call when a notification is received
        """
        if self.listening:
            return

        self.listening = True
        self._stop_event.clear()

        # Connect if needed
        if not self.connection:
            await self.connect()

        # Verify connection was established
        if not self.connection:
            raise Exception("No database connection established.")

        self.logger.info(f"Starting listener for channel: {self.channel}")

        try:
            # Main listening loop
            while self.listening and not self._stop_event.is_set():
                try:
                    # Check for notifications with timeout
                    gen = self.connection.notifies()
                    notifications_processed = False

                    # Process all available notifications
                    while True:
                        try:
                            notify = await asyncio.wait_for(
                                gen.__anext__(), timeout=0.1
                            )
                            if notify.channel == self.channel:
                                try:
                                    await notify_function(notify.payload or "")
                                    notifications_processed = True
                                except Exception as e:
                                    self.logger.error(
                                        f"Error processing notification: {e}"
                                    )
                        except (StopAsyncIteration, asyncio.TimeoutError):
                            # No more notifications available
                            break

                    # If no notifications were processed, sleep briefly to prevent tight loop
                    if not notifications_processed:
                        await asyncio.sleep(0.1)

                except Exception as e:
                    self.logger.error(f"Error in listen loop: {e}")
                    # Try to reconnect
                    try:
                        await self.connect()
                    except:
                        self.logger.error("Failed to reconnect, stopping listener")
                        break

        except asyncio.CancelledError:
            self.logger.info("Listener cancelled")
        finally:
            self.listening = False
            self.logger.info(f"Listener stopped for channel: {self.channel}")

    async def stop(self) -> None:
        """Stop the listener."""
        if not self.listening:
            return

        self.listening = False
        self._stop_event.set()

        # Close the connection
        if self.connection:
            try:
                await self.connection.close()
            except Exception as e:
                self.logger.debug(f"Error closing connection: {e}")
            self.connection = None

        self.logger.info(f"Listener stopped for channel: {self.channel}")


def new_queuer_db_listener(
    db_config: DatabaseConfiguration, channel: str
) -> QueuerListener:
    """
    Create a new QueuerListener instance.
    """
    return QueuerListener(db_config, channel)
