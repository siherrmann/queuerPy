"""
Database listener handler for Python queuer implementation.
Mirrors Go's database/dbListener.go with psycopg3 connection.
"""

import logging
import threading
import time
from typing import Optional, Callable
import psycopg
from psycopg import Connection
from psycopg.rows import dict_row

from helper.database import DatabaseConfiguration


class QueuerListener:
    """
    Database listener for PostgreSQL LISTEN/NOTIFY.
    Mirrors Go's QueuerListener struct.
    """
    
    def __init__(self, db_config: DatabaseConfiguration, channel: str):
        """Initialize database listener."""
        self.db_config = db_config
        self.channel = channel
        self.connection: Optional[Connection] = None
        self.logger = logging.getLogger(__name__)
        self._listening = False
        self._stop_event = threading.Event()
        self._listen_thread: Optional[threading.Thread] = None
        self._notify_function: Optional[Callable[[str], None]] = None
        self._ping_timeout = 90.0
        
        # Initialize connection and start listening
        self._connect()
    
    def _connect(self) -> None:
        """Establish database connection with error handling."""
        try:
            connection_string = self.db_config.connection_string()
            self.connection = psycopg.connect(
                connection_string,
                row_factory=dict_row,
                autocommit=True  # Required for LISTEN/NOTIFY
            )
            
            # Start listening to the channel
            with self.connection.cursor() as cur:
                cur.execute(f"LISTEN {self.channel};")
            
            self.logger.info(f"Database listener connected to channel: {self.channel}")
        except Exception as e:
            self.logger.error(f"Failed to connect database listener: {e}")
            raise
    
    def listen(self, notify_function: Callable[[str], None]) -> None:
        """
        Listen for events on the specified channel and process them.
        Mirrors Go's Listen method.
        """
        self.listen_with_timeout(notify_function, 90.0)
    
    def listen_with_timeout(self, notify_function: Callable[[str], None], ping_timeout: float) -> None:
        """
        Listen with timeout - similar to Go's ListenWithTimeout.
        This is primarily used for testing to avoid waiting the full 90 seconds.
        """
        self._notify_function = notify_function
        self._ping_timeout = ping_timeout
        
        if not self._listening:
            self._listening = True
            self._listen_thread = threading.Thread(target=self._listen_loop, daemon=True)
            self._listen_thread.start()
    
    def _listen_loop(self) -> None:
        """
        Main listening loop for processing notifications.
        Mirrors Go's select loop with timeout.
        """
        if not self.connection:
            return
        
        while self._listening and not self._stop_event.is_set():
            try:
                # Check for notifications
                notifies = self.connection.notifies()
                while notifies:
                    notify = notifies.popleft()
                    if notify and self._notify_function:
                        # Call notify function in separate thread to avoid blocking
                        threading.Thread(
                            target=self._notify_function,
                            args=(notify.payload or "",),
                            daemon=True
                        ).start()
                
                # Check if we should stop
                if self._stop_event.wait(self._ping_timeout):
                    break
                
                # Ping connection to check health (like Go's time.After)
                try:
                    with self.connection.cursor() as cur:
                        cur.execute("SELECT 1;")
                except Exception as e:
                    self.logger.error(f"Error pinging listener: {e}")
                    break
                    
            except Exception as e:
                self.logger.error(f"Error in listen loop: {e}")
                break
    
    def stop(self) -> None:
        """Stop listening and close connection."""
        self._listening = False
        self._stop_event.set()
        
        if self._listen_thread and self._listen_thread.is_alive():
            self._listen_thread.join(timeout=5.0)
        
        if self.connection:
            try:
                self.connection.close()
            except Exception as e:
                self.logger.error(f"Error closing connection: {e}")
            self.connection = None
        
        self.logger.info(f"Database listener stopped for channel: {self.channel}")


def new_queuer_db_listener(db_config: DatabaseConfiguration, channel: str) -> QueuerListener:
    """
    Create a new QueuerListener instance.
    Mirrors Go's NewQueuerDBListener function.
    """
    return QueuerListener(db_config, channel)
