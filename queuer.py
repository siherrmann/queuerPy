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
from datetime import datetime, timedelta
from typing import Optional, Dict, Callable, Any
from uuid import UUID

# Local imports - database components
from database.db_job import JobDBHandler
from database.db_worker import WorkerDBHandler
from database.db_listener import QueuerListener, new_queuer_db_listener

# Local imports - core components
from core.broadcaster import Broadcaster, new_broadcaster
from core.listener import Listener, new_listener
from core.ticker import Ticker, new_ticker
from core.runner import Runner

# Local imports - model classes
from model.job import Job, JobStatus
from model.task import Task
from model.worker import Worker, new_worker, new_worker_with_options, WorkerStatus
from model.options import Options
from model.options_on_error import OnError

# Local imports - helper components
from helper.database import Database, new_database, DatabaseConfiguration

# Local imports - shared event loop
from core.runner import Runner

# Local imports - mixins
from queuer_job import QueuerJobMixin
from queuer_task import QueuerTaskMixin
from queuer_next_interval import QueuerNextIntervalMixin
from queuer_listener import QueuerListenerMixin


def new_queuer(name: str, max_concurrency: int, *options: OnError) -> "Queuer":
    """
    Create a new Queuer instance with the given name and max concurrency.
    Mirrors Go's NewQueuer function.
    It wraps new_queuer_with_db to initialize the queuer without an external db config and encryption key.
    The encryption key for the database is taken from an environment variable (QUEUER_ENCRYPTION_KEY),
    if not provided, it defaults to unencrypted results.
    """
    encryption_key = os.getenv("QUEUER_ENCRYPTION_KEY", "")
    first_option = options[0] if len(options) > 0 else None
    return Queuer(name, max_concurrency, encryption_key, None, first_option)


def new_queuer_with_db(
    name: str,
    max_concurrency: int,
    encryption_key: str,
    db_config: Optional[DatabaseConfiguration],
    *options: OnError,
) -> "Queuer":
    """
    Create a new Queuer instance with the given name and max concurrency.
    Mirrors Go's NewQueuerWithDB function.
    It initializes the database connection and worker.
    If options are provided, it creates a worker with those options.

    It takes the db configuration from environment variables if dbConfig is None.
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

    def start(self) -> None:
        """
        Start the queuer.
        Mirrors Go's Start method but uses asyncio patterns instead of context.Context.
        """
        if self._running:
            raise RuntimeError("Queuer is already running")

        # Verify event loop is still available
        try:
            Runner().get_event_loop()
        except RuntimeError as e:
            self.log.error(f"Cannot start queuer: {e}")
            raise

        # Set up context - mirrors Go's ctx and cancel assignment
        self._running = True
        self._stopped.clear()  # Clear the stopped flag
        self._cancel_event = asyncio.Event()

        # Set up database listeners - mirrors Go's NewQueuerDBListener calls
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

        # Broadcasters for job updates and deletes - mirrors Go's broadcaster creation
        try:
            self.job_insert_broadcaster = new_broadcaster("job.INSERT")
            self.job_insert_listener = new_listener(self.job_insert_broadcaster)

            self.job_update_broadcaster = new_broadcaster("job.UPDATE")
            self.job_update_listener = new_listener(self.job_update_broadcaster)

            self.job_delete_broadcaster = new_broadcaster("job.DELETE")
            self.job_delete_listener = new_listener(self.job_delete_broadcaster)
        except Exception as e:
            self.log.error(f"Error creating job broadcasters: {e}")
            raise RuntimeError(f"Error creating job broadcasters: {e}")

        # Update worker status to running - mirrors Go's worker status update
        try:
            with self.worker_mutex:
                self.worker.status = WorkerStatus.RUNNING
                self.worker = self.db_worker.update_worker(self.worker)
        except Exception as e:
            self.log.error(f"Error updating worker status to running: {e}")
            raise RuntimeError(f"Error updating worker status to running: {e}")

        # Initialize process pool for CPU-intensive tasks
        try:
            from core.process_runner import ProcessRunner

            # Process pool will be created lazily when first needed
            self.log.info(f"Process pool enabled for CPU-intensive job execution")
        except Exception as e:
            self.log.error(f"Failed to initialize process pool: {e}")
            raise RuntimeError(f"Failed to initialize process pool: {e}")

        # Start background tasks - mirrors Go's goroutine approach
        self._start_listeners()
        self._start_heartbeat_ticker()
        self._start_poll_job_ticker()  # Backup polling every 5 minutes

        self.log.info("Queuer started")

    def stop(self) -> None:
        """
        Stop the queuer.
        Mirrors Go's Stop() method closely.
        """
        if not self._running:
            return

        # Close database listeners - mirrors Go's listener cleanup
        if self.job_db_listener:
            try:
                future = Runner().run_in_event_loop(self.job_db_listener.stop())
                future.result(timeout=2.0)  # Wait up to 2 seconds
            except Exception as e:
                # Only log if it's not already closed
                if "closed" not in str(e).lower():
                    self.log.warning(f"Error stopping job listener: {e}")

        if self.job_archive_db_listener:
            try:
                future = Runner().run_in_event_loop(self.job_archive_db_listener.stop())
                future.result(timeout=2.0)  # Wait up to 2 seconds
            except Exception as e:
                # Only log if it's not already closed
                if "closed" not in str(e).lower():
                    self.log.warning(f"Error stopping job archive listener: {e}")

        # Update worker status to stopped - mirrors Go's worker status update
        worker_rid = None
        try:
            with self.worker_mutex:
                self.worker.status = WorkerStatus.STOPPED
                self.worker = self.db_worker.update_worker(self.worker)
                worker_rid = self.worker.rid
        except Exception as e:
            self.log.error(f"Error updating worker status to stopped: {e}")

        # Cancel all queued and running jobs - mirrors Go's CancelAllJobsByWorker
        if worker_rid:
            try:
                self.cancel_all_jobs_by_worker(worker_rid, 100)
            except Exception as e:
                self.log.error(f"Error cancelling all jobs by worker: {e}")

        # Cancel the context equivalent - set cancellation event
        if self._cancel_event:
            try:
                loop = Runner().get_event_loop()
                loop.call_soon_threadsafe(self._cancel_event.set)
            except Exception as e:
                self.log.warning(f"Error setting cancel event: {e}")

        # Signal that we're stopping
        self._running = False
        self._stopped.set()

        # Cancel all active runners - similar to Go's job cancellation
        for runner_id, runner in list(self.active_runners.items()):
            try:
                runner.cancel()
                if runner_id in self.active_runners:
                    del self.active_runners[runner_id]
            except Exception as e:
                self.log.warning(f"Error cancelling runner {runner_id}: {e}")

        # Wait a moment for background tasks to finish gracefully - mirrors Go's sleep
        import time

        time.sleep(0.1)

        # Shutdown process pool for CPU-intensive tasks
        try:
            from core.process_runner import ProcessRunner

            ProcessRunner.shutdown_pool()
            self.log.info("Process pool shut down")
        except Exception as e:
            self.log.warning(f"Error shutting down process pool: {e}")

        # Note: Event loop management is now handled by the singleton
        # No need to stop it here as it may be shared with other queuer instances

        # Close database connection - mirrors Go's DB.Close()
        if hasattr(self, "DB") and self.DB:
            try:
                self.log.info("Closing database connection")
                # For psycopg, we need to close the connection properly
                if hasattr(self.DB, "close"):
                    self.DB.close()
            except Exception as e:
                self.log.error(f"Error closing database connection: {e}")

        self.log.info(f"Queuer '{self.worker.name}' stopped")

    def _start_listeners(self) -> None:
        """Start database listeners - mirrors Go implementation."""

        async def handle_job_notification(notification):
            """Handle job database notifications."""
            try:
                self.log.info(f"Job notification received: {notification}")
                job_data = json.loads(notification)
                self.log.info(f"Parsed job data: {job_data}")

                # Check job status to determine action
                status = job_data.get("status")
                self.log.info(f"Job status: {status}")

                if status in ["QUEUED", "SCHEDULED"]:
                    # New job - run job processing immediately (only if queuer is running)
                    if self._running:
                        self.log.info("New job detected - processing immediately")
                        asyncio.create_task(self._async_run_job_initial())
                    else:
                        self.log.debug(
                            "New job detected but queuer not running - skipping immediate processing"
                        )
                else:
                    # Job update - notify update listener
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

        async def handle_job_archive_notification(notification):
            """Handle job archive database notifications."""
            try:
                self.log.info(f"Job archive notification received: {notification}")
                job_data = json.loads(notification)
                self.log.info(f"Parsed archive job data: {job_data}")

                status = job_data.get("status")
                if status == "CANCELLED":
                    # Cancel running job if it exists
                    from uuid import UUID

                    job_rid = UUID(job_data.get("rid"))
                    if job_rid in self.active_runners:
                        self.log.info(f"Canceling running job: {job_rid}")
                        runner = self.active_runners[job_rid]
                        runner.cancel()
                        del self.active_runners[job_rid]
                else:
                    # Job completed - notify delete listeners (job moved from main table to archive)
                    self.log.info(f"Job completed with status: {status}")
                    if (
                        hasattr(self, "job_delete_broadcaster")
                        and self.job_delete_broadcaster
                    ):
                        job = Job.from_dict(job_data)
                        await self.job_delete_broadcaster.broadcast(job)
                        self.log.info(
                            f"Broadcasted job deletion completion for {job.rid}"
                        )

            except Exception as e:
                self.log.error(f"Error handling job archive notification: {e}")
                import traceback

                traceback.print_exc()

        # Start job listener
        if self.job_db_listener:
            try:

                async def start_job_listener():
                    await self.job_db_listener.listen(handle_job_notification)

                Runner().run_in_event_loop(start_job_listener())

                # Wait for listener to establish connection
                import time

                time.sleep(1.0)  # Give more time for connection to establish

                self.log.info("Started job database listener")
            except Exception as e:
                self.log.error(f"Error starting job listener: {e}")

        # Start job archive listener
        if self.job_archive_db_listener:
            try:

                async def start_archive_listener():
                    await self.job_archive_db_listener.listen(
                        handle_job_archive_notification
                    )

                Runner().run_in_event_loop(start_archive_listener())

                # Wait for listener to establish connection
                time.sleep(1.0)  # Give more time for connection to establish

                self.log.info("Started job archive database listener")
            except Exception as e:
                self.log.error(f"Error starting job archive listener: {e}")

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

        ticker: Ticker = new_ticker(timedelta(seconds=30), heartbeat_func)
        self.log.info("Starting heartbeat ticker...")
        ticker.go()

    def _start_poll_job_ticker(self) -> None:
        """
        Start job polling ticker as backup mechanism.
        This provides a safety net in case notification-based processing fails.
        """

        def poll_jobs_func():
            """Poll for jobs as backup to notification system."""
            try:
                self.log.debug("Running backup job polling")
                self._run_job_initial()
            except Exception as e:
                self.log.warning(f"Backup job polling error: {e}")

        ticker: Ticker = new_ticker(self.job_poll_interval, poll_jobs_func)
        self.log.info(
            f"Starting backup job polling ticker (interval: {self.job_poll_interval})..."
        )
        ticker.go()

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
