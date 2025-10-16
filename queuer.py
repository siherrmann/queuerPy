"""
Main Queuer class for Python implementation.
Mirrors the Go Queuer struct with Python async patterns.
"""

import asyncio
import threading
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Callable, Any
from uuid import UUID
import os

# Import database components
from database.db_job import JobDBHandler
from database.db_worker import WorkerDBHandler  
from database.db_listener import QueuerListener

# Import core components
from core.broadcaster import Broadcaster, new_broadcaster
from core.listener import Listener, new_listener
from core.ticker import Ticker, new_ticker
from core.scheduler import Scheduler
from core.runner import Runner

# Import model classes
from model.job import Job, new_job, JobStatus
from model.task import Task, new_task, new_task_with_name
from model.worker import Worker, new_worker, new_worker_with_options, WorkerStatus
from model.options import Options
from model.options_on_error import OnError

# Import helper components
from helper.database import Database, new_database, DatabaseConfiguration

# Import mixins
from queuer_job import QueuerJobMixin
from queuer_task import QueuerTaskMixin
from queuer_next_interval import QueuerNextIntervalMixin
from queuer_listener import QueuerListenerMixin


class Queuer(QueuerJobMixin, QueuerTaskMixin, QueuerNextIntervalMixin, QueuerListenerMixin):
    """
    Main queuing system class.
    Mirrors the Go Queuer struct with Python async compatibility.
    """

    def __init__(
        self,
        name: str,
        max_concurrency: int,
        encryption_key: str = "",
        db_config: Optional[DatabaseConfiguration] = None,
        options: Optional[OnError] = None):
        """
        __init__ is the inner initialization method that's being used for new_queuer and new_queuer_with_db.
        It initializes the database connection and worker.
        If options are provided, it creates a worker with those options.

        It takes the db configuration from environment variables if dbConfig is nil.
        - QUEUER_DB_HOST (required)
        - QUEUER_DB_PORT (required)
        - QUEUER_DB_DATABASE (required)
        - QUEUER_DB_USERNAME (required)
        - QUEUER_DB_PASSWORD (required)
        - QUEUER_DB_SCHEMA (required)
        - QUEUER_DB_SSLMODE (optional, defaults to "require")

        If the encryption key is empty, it defaults to unencrypted results.
        
        If any error occurs during initialization, it logs a panic error and exits the program.
        It returns a pointer to the newly created Queuer instance.
        """
        # Store name and concurrency for access
        self.name = name
        self.max_concurrency = max_concurrency
        
        # Logger
        self.log = logging.getLogger(f"queuer.{name}")
        
        # Context management
        self.ctx = None
        self.cancel_func = None
        self._running = False
        self._stopped = threading.Event()
        self._event_loop = None  # Store reference to the main event loop
        
        # Database configuration
        if db_config is not None:
            self.db_config = db_config
        else:
            self.db_config = DatabaseConfiguration.from_env()
        
        # Database connection
        self.database = new_database("queuer", self.db_config, self.log)
        self.DB = self.database.instance
        
        # Database handlers
        self.db_job = JobDBHandler(self.database, self.db_config.with_table_drop, encryption_key)
        self.db_worker = WorkerDBHandler(self.database, self.db_config.with_table_drop)
        
        # Configuration
        self.job_poll_interval = timedelta(minutes=1)
        self.retention_archive = timedelta(days=30)
        
        # Create and insert worker
        if options:
            new_worker_obj = new_worker_with_options(name, max_concurrency, options)
        else:
            new_worker_obj = new_worker(name, max_concurrency)
        
        self.worker = self.db_worker.insert_worker(new_worker_obj)
        self.worker_mutex = threading.RLock()
        
        self.log.info(f"Queuer with worker created: {new_worker_obj.name} (RID: {self.worker.rid})")
        
        # Active runners (jobs currently executing)
        self.active_runners: Dict[UUID, Runner] = {}
        
        # Tasks storage and next interval functions
        self.tasks: Dict[str, Task] = {}
        self.next_interval_funcs: Dict[str, Callable] = {}
        
        # Database listeners (will be initialized in start())
        self.job_db_listener: Optional[QueuerListener] = None
        self.job_archive_db_listener: Optional[QueuerListener] = None
        
        # Job broadcasters and listeners
        self.job_insert_broadcaster = new_broadcaster("job.INSERT")
        self.job_update_broadcaster = new_broadcaster("job.UPDATE") 
        self.job_delete_broadcaster = new_broadcaster("job.DELETE")
        
        self.job_insert_listener = new_listener(self.job_insert_broadcaster)
        self.job_update_listener = new_listener(self.job_update_broadcaster)
        self.job_delete_listener = new_listener(self.job_delete_broadcaster)

    def start(self, cancel_func: Optional[Callable] = None) -> None:
        """
        Start the queuer.
        Mirrors Go's Start(ctx, cancel, masterSettings) method.
        """
        if self._running:
            raise RuntimeError("Queuer is already running")
        
        # Set up context
        self.cancel_func = cancel_func
        self._running = True
        self._stopped.clear()  # Clear the stopped flag
        
        # Try to capture the current event loop
        try:
            self._event_loop = asyncio.get_running_loop()
            self.log.debug("Captured running event loop")
        except RuntimeError:
            # No running loop, create a new one if needed
            # In modern asyncio, we should avoid get_event_loop() when no loop is running
            self.log.debug("No running event loop found, will create one when needed")
            self._event_loop = None
        
        # Set up database listeners
        try:
            self.job_db_listener = QueuerListener(self.db_config, "job")
            self.log.info("Added listener for channel: job")
            self.job_archive_db_listener = QueuerListener(self.db_config, "job_archive")
            self.log.info("Added listener for channel: job_archive")
        except Exception as e:
            self.log.error(f"Error setting up database listeners: {e}")
            return

        # Broadcasters for job updates and deletes
        try:
            self.job_insert_broadcaster = new_broadcaster("job.INSERT")
            self.job_update_broadcaster = new_broadcaster("job.UPDATE")
            self.job_delete_broadcaster = new_broadcaster("job.DELETE")
        except Exception as e:
            self.log.error(f"Error setting up job broadcasters: {e}")
            return

        # Update worker status to running
        try:
            with self.worker_mutex:
                self.worker.status = WorkerStatus.RUNNING
                self.worker = self.db_worker.update_worker(self.worker)
        except Exception as e:
            self.log.error(f"Error updating worker status: {e}")
            return

        # Start background tasks
        self._start_listeners()
        self._start_heartbeat_ticker()
        self._start_poll_job_ticker()

        self.log.info(f"Queuer '{self.worker.name}' started with max concurrency {self.worker.max_concurrency}")

    def stop(self) -> None:
        """
        Stop the queuer.
        Mirrors Go's Stop() method.
        """
        if not self._running:
            return
        
        self._running = False
        self._stopped.set()  # Signal that the queuer is stopped
        
        # Cancel all active runners
        for runner_id, runner in list(self.active_runners.items()):
            runner.cancel()
            if runner_id in self.active_runners:
                del self.active_runners[runner_id]
        
        # Update worker status
        with self.worker_mutex:
            self.worker.status = WorkerStatus.STOPPED
            self.worker = self.db_worker.update_worker(self.worker)
        
        # Stop listeners
        if self.job_db_listener:
            self.job_db_listener.stop()
        if self.job_archive_db_listener:
            self.job_archive_db_listener.stop()
        
        # Signal cancellation
        if self.cancel_func:
            self.cancel_func()
        
        self.log.info(f"Queuer '{self.worker.name}' stopped")

    def _start_listeners(self) -> None:
        """Start database listeners."""
        def handle_job_notification(notification):
            """Handle job database notifications."""
            try:
                # Run job when notified
                self._run_job_initial()
            except Exception as e:
                self.log.error(f"Error handling job notification: {e}")
        
        # Start job listener
        if self.job_db_listener:
            self.job_db_listener.listen(handle_job_notification)

    def _start_heartbeat_ticker(self) -> None:
        """Start heartbeat ticker."""
        def heartbeat_func():
            """Send periodic heartbeats."""
            try:
                with self.worker_mutex:
                    self.worker.updated_at = datetime.now()
                    self.worker = self.db_worker.update_worker(self.worker)
            except Exception as e:
                self.log.error(f"Heartbeat error: {e}")
        
        ticker = new_ticker(timedelta(seconds=30), heartbeat_func)
        self.log.info("Starting heartbeat ticker...")
        ticker.go()

    def _start_poll_job_ticker(self) -> None:
        """Start job polling ticker."""
        def poll_func():
            """Poll for jobs to execute."""
            self.log.info("Polling jobs...")
            try:
                self._run_job_initial()
            except Exception as e:
                self.log.error(f"Error running job: {e}")
        
        # Create and start ticker - mirrors Go implementation
        ticker = new_ticker(self.job_poll_interval, poll_func)
        self.log.info("Starting job poll ticker...")
        ticker.go()


# Factory functions to match Go patterns
def new_queuer(name: str, max_concurrency: int, options: Optional[OnError] = None) -> Queuer:
    """
    new_queuer creates a new Queuer instance with the given name and max concurrency.
    It wraps new_queuer_with_db to initialize the queuer without an external db config and encryption key.
    The encryption key for the database is taken from an environment variable (QUEUER_ENCRYPTION_KEY),
    if not provided, it defaults to unencrypted results.
    """
    encryption_key = os.getenv("QUEUER_ENCRYPTION_KEY", "")
    return Queuer(name, max_concurrency, encryption_key, None, options)


def new_queuer_with_db(
        name: str,
        max_concurrency: int,
        encryption_key: str,
        db_config: Optional[DatabaseConfiguration],
        options: Optional[OnError] = None) -> Queuer:
    """
    new_queuer_with_db creates a new Queuer instance with the given name and max concurrency.
    It initializes the database connection and worker.
    If options are provided, it creates a worker with those options.

    It takes the db configuration from environment variables if dbConfig is nil.
    - QUEUER_DB_HOST (required)
    - QUEUER_DB_PORT (required)
    - QUEUER_DB_DATABASE (required)
    - QUEUER_DB_USERNAME (required)
    - QUEUER_DB_PASSWORD (required)
    - QUEUER_DB_SCHEMA (required)
    - QUEUER_DB_SSLMODE (optional, defaults to "require")
    If the encryption key is empty, it defaults to unencrypted results.

    If any error occurs during initialization, it logs a panic error and exits the program.
    It returns a pointer to the newly created Queuer instance.
    """
    return Queuer(name, max_concurrency, encryption_key, db_config, options)
