"""
Main Queuer class for Python implementation.
Mirrors the Go Queuer struct with Python async patterns.
"""

# Standard library imports
import asyncio
import json
import logging
import os
import threading
import traceback
from datetime import datetime, timedelta
from typing import Optional, Dict, Callable, Any
from uuid import UUID


# Local imports - database components
from database.db_job import JobDBHandler
from database.db_worker import WorkerDBHandler
from database.db_listener import QueuerListener, new_queuer_db_listener

# Local imports - core components
from core.broadcaster import Broadcaster, new_broadcaster
from core.listener import Listener
from core.runner import Runner

from core.runner import Runner, go_func

# Local imports - model classes
from model.job import Job, JobStatus
from model.task import Task
from model.worker import Worker, new_worker, new_worker_with_options, WorkerStatus
from model.options_on_error import OnError

# Local imports - helper components
from helper.database import Database, new_database, DatabaseConfiguration

# Local imports - mixins
from queuer_job import QueuerJobMixin
from queuer_task import QueuerTaskMixin
from queuer_next_interval import QueuerNextIntervalMixin
from queuer_listener import QueuerListenerMixin


def new_queuer(name: str, max_concurrency: int, *options: OnError) -> "Queuer":
    """
    Create a new Queuer instance with the given name and max concurrency.
    The encryption key for the database is taken from environment variable QUEUER_ENCRYPTION_KEY.
    """
    encryption_key = os.getenv("QUEUER_ENCRYPTION_KEY", "")
    first_option = options[0] if len(options) > 0 else None
    return Queuer(name, max_concurrency, encryption_key, None, first_option)


def new_queuer_with_db(
    name: str,
    max_concurrency: int,
    encryption_key: Optional[str] = None,
    db_config: Optional[DatabaseConfiguration] = None,
    *options: OnError,
) -> "Queuer":
    """
    Create a new Queuer instance with database configuration.
    If dbConfig is None, uses environment variables for database connection.

    If the encryption key is empty, it defaults to unencrypted results.

    If any error occurs during initialization, it logs a panic error and exits the program.
    It returns a pointer to the newly created Queuer instance.
    """
    first_option = options[0] if len(options) > 0 else None
    return Queuer(name, max_concurrency, encryption_key, db_config, first_option)


class Queuer(
    QueuerJobMixin, QueuerTaskMixin, QueuerNextIntervalMixin, QueuerListenerMixin
):
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
        options: Optional[OnError] = None,
    ):
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
        self.name: str = name
        self.max_concurrency: int = max_concurrency

        # Logger
        self.log: logging.Logger = logging.getLogger(f"queuer.{name}")

        # Context management - will be set in start()
        self._running: bool = False
        self._stopped: threading.Event = threading.Event()
        self._cancel_event: Optional[asyncio.Event] = None

        # Database configuration
        if db_config is not None:
            self.db_config: DatabaseConfiguration = db_config
        else:
            self.db_config: DatabaseConfiguration = DatabaseConfiguration.from_env()

        # Database connection
        self.database: Database = new_database("queuer", self.db_config, self.log)
        self.DB = self.database.instance

        # Database handlers
        self.db_job: JobDBHandler = JobDBHandler(
            self.database, self.db_config.with_table_drop, encryption_key
        )
        self.db_worker: WorkerDBHandler = WorkerDBHandler(
            self.database, self.db_config.with_table_drop
        )

        # Configuration
        self.job_poll_interval: timedelta = timedelta(
            minutes=5
        )  # Backup polling - listeners handle real-time processing
        self.retention_archive: timedelta = timedelta(days=30)

        # Create and insert worker
        if options:
            new_worker_obj: Worker = new_worker_with_options(
                name, max_concurrency, options
            )
        else:
            new_worker_obj: Worker = new_worker(name, max_concurrency)

        self.worker: Worker = self.db_worker.insert_worker(new_worker_obj)
        self.worker_mutex: threading.RLock = threading.RLock()

        self.log.info(
            f"Queuer with worker created: {new_worker_obj.name} (RID: {self.worker.rid})"
        )

        # Active runners (jobs currently executing)
        self.active_runners: Dict[UUID, Runner] = {}

        # Tasks storage and next interval functions
        self.tasks: Dict[str, Task] = {}
        self.next_interval_funcs: Dict[str, Callable] = {}

        # Database listeners (will be initialized in start())
        self.job_db_listener: Optional[QueuerListener] = None
        self.job_archive_db_listener: Optional[QueuerListener] = None

        # Job broadcasters and listeners (will be initialized in start())
        self.job_insert_broadcaster: Optional[Broadcaster] = None
        self.job_update_broadcaster: Optional[Broadcaster] = None
        self.job_delete_broadcaster: Optional[Broadcaster] = None

        self.job_insert_listener: Optional[Listener] = None
        self.job_update_listener: Optional[Listener] = None
        self.job_delete_listener: Optional[Listener] = None

        # Tickers (will be initialized in start())
        self._heartbeat_ticker: Optional[Any] = None
        self._poll_job_ticker: Optional[Any] = None

    def start(self) -> None:
        """Start the queuer."""
        if self._running:
            raise RuntimeError("Queuer is already running")

        # No need to verify event loop with multiprocessing-based Runner

        # Set up context
        self._running = True
        self._stopped.clear()  # Clear the stopped flag
        self._cancel_event = asyncio.Event()

        # Set up database listeners
        try:
            self.job_db_listener = new_queuer_db_listener(self.db_config, "job")
            self.log.info("Added listener for channel: job")
            self.job_archive_db_listener = new_queuer_db_listener(
                self.db_config, "job_archive"
            )
            self.log.info("Added listener for channel: job_archive")
        except Exception as e:
            self.log.error(f"Error creating database listeners: {e}")
            raise RuntimeError(f"Error creating database listeners: {e}")

        # Broadcasters for job updates and deletes
        # Use singleton broadcasters so all queuer instances share the same broadcasters
        try:
            self.job_insert_broadcaster = new_broadcaster("job.INSERT")
            self.job_insert_listener = Listener[Job](self.job_insert_broadcaster)

            self.job_update_broadcaster = new_broadcaster("job.UPDATE")
            self.job_update_listener = Listener[Job](self.job_update_broadcaster)

            self.job_delete_broadcaster = new_broadcaster("job.DELETE")
            self.job_delete_listener = Listener[Job](self.job_delete_broadcaster)
        except Exception as e:
            self.log.error(f"Error creating job broadcasters: {e}")
            raise RuntimeError(f"Error creating job broadcasters: {e}")

        # Update worker status to running
        try:
            with self.worker_mutex:
                self.worker.status = WorkerStatus.RUNNING
                self.worker = self.db_worker.update_worker(self.worker)
        except Exception as e:
            self.log.error(f"Error updating worker status to running: {e}")
            raise RuntimeError(f"Error updating worker status to running: {e}")

        # Start background tasks
        self._start_listeners()
        self._start_heartbeat_ticker()
        self._start_poll_job_ticker()  # Backup polling every 5 minutes

        self.log.info("Queuer started")

    def stop(self) -> None:
        """Stop the queuer."""
        if not self._running:
            return

        # Stop tickers first to prevent new work from being queued
        if self._heartbeat_ticker:
            try:
                self._heartbeat_ticker.stop()
                self.log.info("Stopped heartbeat ticker")
            except Exception as e:
                self.log.warning(f"Error stopping heartbeat ticker: {e}")

        if self._poll_job_ticker:
            try:
                self._poll_job_ticker.stop()
                self.log.info("Stopped poll job ticker")
            except Exception as e:
                self.log.warning(f"Error stopping poll job ticker: {e}")

        # Close database listeners
        if self.job_db_listener:
            try:
                # Use go_func but wait for completion
                task = go_func(self.job_db_listener.stop, use_mp=False)
                task.get_results(timeout=3.0)  # Wait up to 3 seconds total
            except Exception as e:
                # Only log if it's not already closed
                if "closed" not in str(e).lower():
                    self.log.warning(f"Error stopping job listener: {e}")

        if self.job_archive_db_listener:
            try:
                task = go_func(self.job_archive_db_listener.stop, use_mp=False)
                task.get_results(timeout=3.0)  # Wait up to 3 seconds total
            except Exception as e:
                if "closed" not in str(e).lower():
                    self.log.warning(f"Error stopping job archive listener: {e}")

        # Update worker status to stopped
        worker_rid = None
        try:
            with self.worker_mutex:
                self.worker.status = WorkerStatus.STOPPED
                self.worker = self.db_worker.update_worker(self.worker)
                worker_rid = self.worker.rid
        except Exception as e:
            self.log.error(f"Error updating worker status to stopped: {e}")

        # Cancel all queued and running jobs
        if worker_rid:
            try:
                self.cancel_all_jobs_by_worker(worker_rid, 100)
            except Exception as e:
                self.log.error(f"Error cancelling all jobs by worker: {e}")

        # Cancel the context equivalent - set cancellation event
        if self._cancel_event:
            try:
                self._cancel_event.set()
            except Exception as e:
                self.log.warning(f"Error setting cancel event: {e}")

        # Signal that we're stopping
        self._running = False
        self._stopped.set()

        # Cancel all active runners - similar to Go's job cancellation
        for runner_id, runner in list(self.active_runners.items()):
            try:
                runner.terminate()
                runner.join()
                if runner_id in self.active_runners:
                    del self.active_runners[runner_id]
            except Exception as e:
                self.log.warning(f"Error cancelling runner {runner_id}: {e}")

        # Shutdown process pool for CPU-intensive tasks
        try:
            # No pool shutdown needed for new Runner system
            self.log.info("Process pool shut down")
        except Exception as e:
            self.log.warning(f"Error shutting down process pool: {e}")

        # Close database connection
        if hasattr(self, "DB") and self.DB:
            try:
                self.log.info("Closing database connection")
                if hasattr(self.DB, "close"):
                    self.DB.close()
            except Exception as e:
                self.log.error(f"Error closing database connection: {e}")

        self.log.info(f"Queuer '{self.worker.name}' stopped")

    async def _handle_job_notification(self, notification):
        """Handle job database notifications."""
        try:
            self.log.info(
                f"[{self.name}] Received notification: {notification[:100]}..."
            )
            job_data = json.loads(notification)
            status = job_data.get("status")
            job_rid = job_data.get("rid")

            if status in ["QUEUED", "SCHEDULED"]:
                if self._running:
                    job_rid_uuid = UUID(job_data.get("rid"))
                    self.log.info(
                        f"[{self.name}] Processing {status} job: {job_rid_uuid}"
                    )
                    go_func(self._run_job_initial, use_mp=False)
            else:
                self.log.info(f"Job update detected with status: {status}")
                if (
                    hasattr(self, "job_update_broadcaster")
                    and self.job_update_broadcaster
                ):
                    job = Job.from_dict(job_data)
                    await self.job_update_broadcaster.broadcast(job)

        except Exception as e:
            self.log.error(f"Error handling job notification: {e}")
            import traceback

            traceback.print_exc()

    async def _handle_job_archive_notification(self, notification):
        """Handle job archive database notifications."""
        try:
            job_data = json.loads(notification)
            job = Job.from_dict(job_data)

            if job.status == "CANCELLED":
                if job.rid in self.active_runners:
                    self.log.info(f"Canceling running job: {job.rid}")
                    runner: Runner = self.active_runners[job.rid]
                    runner.terminate()
                    runner.join()
                    del self.active_runners[job.rid]
            else:
                self.log.info(f"Job completed with status: {job.status}")
                if (
                    hasattr(self, "job_delete_broadcaster")
                    and self.job_delete_broadcaster
                ):
                    await self.job_delete_broadcaster.broadcast(job)
                    self.log.info(f"Broadcasted job deletion completion for {job.rid}")

        except Exception as e:
            self.log.error(f"Error handling job archive notification: {e}")
            traceback.print_exc()

    def _start_listeners(self) -> None:
        """Start database listeners."""
        # Start job listener
        if self.job_db_listener:
            try:
                go_func(self._start_job_listener, False, self._handle_job_notification)
                self.log.info("Started job database listener")
            except Exception as e:
                self.log.error(f"Error starting job listener: {e}")

        # Start job archive listener
        if self.job_archive_db_listener:
            try:
                go_func(
                    self._start_job_archive_listener,
                    False,
                    self._handle_job_archive_notification,
                )
                self.log.info("Started job archive database listener")
            except Exception as e:
                self.log.error(f"Error starting job archive listener: {e}")

    def _heartbeat_func(self):
        """Send periodic heartbeats - only updates database, not queuer state."""
        try:
            # Get current worker from database
            current_worker = self.db_worker.select_worker(self.worker.rid)
            if current_worker:
                # Update timestamp and save to database
                current_worker.updated_at = datetime.now()
                self.db_worker.update_worker(current_worker)
                self.log.debug(
                    f"Updated worker heartbeat timestamp: {current_worker.updated_at}"
                )
        except Exception as e:
            self.log.error(f"Heartbeat error: {e}")

    def _start_heartbeat_ticker(self) -> None:
        """Start heartbeat ticker using threading."""
        from core.ticker import Ticker

        self._heartbeat_ticker = Ticker(
            timedelta(seconds=30), self._heartbeat_func, use_mp=False
        )
        self.log.info("Starting heartbeat ticker...")
        self._heartbeat_ticker.go()

    def _poll_jobs_func(self):
        """Poll for jobs as backup to notification system."""
        try:
            self.log.debug("Running backup job polling")
            self._run_job_initial()
        except Exception as e:
            self.log.warning(f"Backup job polling error: {e}")

    async def _start_job_listener(self, handle_job_notification):
        """Start job database listener."""
        await self.job_db_listener.listen(handle_job_notification)

    async def _start_job_archive_listener(self, handle_job_archive_notification):
        """Start job archive database listener."""
        await self.job_archive_db_listener.listen(handle_job_archive_notification)

    def _start_poll_job_ticker(self) -> None:
        """
        Start job polling ticker as backup mechanism.
        This provides a safety net in case notification-based processing fails.
        """
        from core.ticker import Ticker

        self._poll_job_ticker = Ticker(
            self.job_poll_interval, self._poll_jobs_func, use_mp=False
        )
        self.log.info(
            f"Starting backup job polling ticker (interval: {self.job_poll_interval})..."
        )
        self._poll_job_ticker.go()

    async def _async_run_job_initial(self) -> None:
        """
        Async wrapper for _run_job_initial to avoid database transaction conflicts.
        This ensures job processing runs in a separate context from the notification handler.
        """
        try:
            # Use a small delay to ensure the notification transaction has completed
            await asyncio.sleep(0.1)

            # Run job processing in the main thread context
            # Since _run_job_initial is synchronous, we need to run it properly
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._run_job_initial)
        except Exception as e:
            self.log.error(f"Error in async job processing: {e}")
