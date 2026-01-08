"""
Main Queuer class for Python implementation.
Mirrors the Go Queuer struct with Python async patterns.
"""

# Standard library imports
import asyncio
import json
import os
import threading
import time
import traceback
from datetime import datetime, timedelta
from typing import Optional, Dict, Callable, Any
from uuid import UUID

from .core.ticker import Ticker
from .core.runner import Runner, SmallRunner, go_func
from .database.db_listener import QueuerListener, new_queuer_db_listener
from .helper.logging import get_logger
from .helper.database import DatabaseConfiguration
from .model.job import Job, JobStatus
from .model.worker import Worker, new_worker, new_worker_with_options, WorkerStatus
from .model.master import MasterSettings
from .model.options_on_error import OnError
from .queuer_job import QueuerJobMixin
from .queuer_task import QueuerTaskMixin
from .queuer_next_interval import QueuerNextIntervalMixin
from .queuer_listener import QueuerListenerMixin
from .queuer_master import QueuerMasterMixin
from .queuer_worker import QueuerWorkerMixin

logger = get_logger(__name__)


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
    encryption_key: str = "",
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
    QueuerJobMixin,
    QueuerTaskMixin,
    QueuerNextIntervalMixin,
    QueuerListenerMixin,
    QueuerMasterMixin,
    QueuerWorkerMixin,
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
        super().__init__()

        # Store name and concurrency for access
        self.name: str = name
        self.max_concurrency: int = max_concurrency

        # Context management - will be set in start()
        self._stopped: threading.Event = threading.Event()
        self._cancel_event: Optional[asyncio.Event] = None

        # Configuration
        self.job_poll_interval: timedelta = timedelta(minutes=5)
        self.worker_poll_interval: timedelta = timedelta(seconds=30)
        self.retention_archive: timedelta = timedelta(days=30)

        # Active runners (jobs currently executing)
        self.active_runners: Dict[UUID, Runner] = {}

        # Database listeners (will be initialized in start())
        self.job_db_listener: Optional[QueuerListener] = None
        self.job_archive_db_listener: Optional[QueuerListener] = None

        # Database listener runners (track go_func runners for cleanup)
        self.job_db_listener_runner: Optional[SmallRunner] = None
        self.job_archive_db_listener_runner: Optional[SmallRunner] = None

        # Database configuration
        if db_config is not None:
            self.db_config: DatabaseConfiguration = db_config
        else:
            self.db_config: DatabaseConfiguration = DatabaseConfiguration.from_env()

        super().initialise(
            db_config=self.db_config,
            encryption_key=encryption_key,
        )

        # Create and insert worker
        if options:
            new_worker_obj: Worker = new_worker_with_options(
                name, max_concurrency, options
            )
        else:
            new_worker_obj: Worker = new_worker(name, max_concurrency)

        try:
            self.worker: Worker = self.db_worker.insert_worker(new_worker_obj)
        except Exception as e:
            logger.error(f"Error inserting worker into database: {e}")
            raise RuntimeError(f"Error inserting worker into database: {e}")

        logger.info(
            f"Queuer with worker created: {new_worker_obj.name} (RID: {self.worker.rid})"
        )

    def start(self, master_settings: Optional[MasterSettings] = None) -> None:
        """Start the queuer."""
        if self.running:
            raise RuntimeError("Queuer is already running")

        # No need to verify event loop with multiprocessing-based Runner

        # Set up context
        self.running = True
        self._stopped.clear()
        self._cancel_event = asyncio.Event()

        # Set up database listeners
        try:
            self.job_db_listener = new_queuer_db_listener(
                self.db_config,
                "job",
            )
            logger.info("Added listener for channel: job")
            self.job_archive_db_listener = new_queuer_db_listener(
                self.db_config,
                "job_archive",
            )
            logger.info("Added listener for channel: job_archive")
        except Exception as e:
            logger.error(f"Error creating database listeners: {e}")
            raise RuntimeError(f"Error creating database listeners: {e}")

        # Ensure database is connected (reconnect if it was closed by a previous stop())
        try:
            if not self.database.instance:
                logger.debug("Database connection was closed, reconnecting...")
                self.database.connect_to_database()
        except Exception as e:
            logger.error(f"Error reconnecting to database: {e}")
            raise RuntimeError(f"Error reconnecting to database: {e}")

        # Update worker status to running
        try:
            with self.worker_mutex:
                self.worker.status = WorkerStatus.RUNNING
                worker_updated = self.db_worker.update_worker(self.worker)
                if not worker_updated:
                    raise Exception("Could not update worker to RUNNING")

                self.worker = worker_updated
        except Exception as e:
            logger.error(f"Error updating worker status to running: {e}")
            raise RuntimeError(f"Error updating worker status to running: {e}")

        # Start background tasks
        self._start_listeners()
        self._wait_for_listeners_ready()

        self._start_heartbeat_ticker()
        self._start_poll_job_ticker()  # Backup polling every 5 minutes

        # Start master polling if master settings provided
        if master_settings:
            self.poll_master_ticker(master_settings)

        logger.info("Queuer started")

    def stop(self) -> None:
        """
        Stop stops the queuer by closing the job listeners, cancelling all queued and running jobs,
        and cancelling the context to stop the queuer.
        """
        # Check if already stopped (database is None means we've already cleaned up)
        if self.database is None:
            return  # Already stopped

        # Mark as not running immediately to prevent re-entrant calls
        was_running = self.running
        self.running = False

        # Stop tickers first to prevent them from calling stop() recursively
        if hasattr(self, "heartbeat_ticker") and self.heartbeat_ticker:
            try:
                self.heartbeat_ticker.stop()
            except Exception as e:
                logger.warning(f"Error stopping heartbeat ticker: {e}")

        if hasattr(self, "poll_job_ticker") and self.poll_job_ticker:
            try:
                self.poll_job_ticker.stop()
            except Exception as e:
                logger.warning(f"Error stopping poll job ticker: {e}")

        # Close db listeners
        if self.job_db_listener:
            try:
                task = go_func(self.job_db_listener.stop, use_mp=False)
                task.get_results(timeout=3.0)
            except Exception as e:
                # Only log if it's not already closed
                if "Listener has been closed" not in str(e):
                    logger.error(f"Error closing job insert listener: {e}")

        if self.job_archive_db_listener:
            try:
                task = go_func(self.job_archive_db_listener.stop, use_mp=False)
                task.get_results(timeout=3.0)
            except Exception as e:
                if "Listener has been closed" not in str(e):
                    logger.error(f"Error closing job archive listener: {e}")

        # Update worker status to stopped
        err = None
        worker_rid = None
        if self.database and self.database.instance:
            with self.worker_mutex:
                if self.worker:
                    self.worker.status = WorkerStatus.STOPPED
                    worker_updated = self.db_worker.update_worker(self.worker)
                    if worker_updated:
                        self.worker = worker_updated
                        worker_rid = self.worker.rid
                    else:
                        err = Exception("Failed to update worker")

        if err:
            logger.error(f"Error updating worker status to stopped: {err}")
            return

        # Cancel all queued and running jobs (only if we have a valid worker RID)
        if worker_rid:
            try:
                self.cancel_all_jobs_by_worker(worker_rid, 100)
            except Exception as e:
                logger.error(f"Error cancelling all jobs by worker: {e}")
                return

        # Cancel all active runners
        for runner_id, runner in list(self.active_runners.items()):
            runner.cancel()
            del self.active_runners[runner_id]

        # Cancel the context to stop the queuer
        if self._cancel_event:
            self._cancel_event.set()

        # Wait a moment for background goroutines to finish gracefully
        time.sleep(0.1)

        # Close database connection
        if self.database:
            logger.info("Closing database connection")
            try:
                self.database.close()
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")

        logger.info("Queuer stopped")

    # Job notification listeners
    async def _handle_job_notification(self, notification: str) -> None:
        """
        Handle job database notifications.

        :param notification: The notification payload as a string
        """
        if not self.running:
            logger.warning(f"Queuer not running, ignoring job notification")
            return

        if not (
            hasattr(self, "job_insert_broadcaster") and self.job_insert_broadcaster
        ):
            logger.debug(f"[{self.name}] No job_insert_broadcaster available")
            return

        if not (
            hasattr(self, "job_update_broadcaster") and self.job_update_broadcaster
        ):
            logger.debug(f"[{self.name}] No job_update_broadcaster available")
            return

        try:
            job = Job.from_dict(json.loads(notification))

            logger.debug(f"Job added: {job.rid}")

            if job.status in [JobStatus.QUEUED, JobStatus.SCHEDULED]:
                go_func(self._run_job_initial, use_mp=False)
                await self.job_insert_broadcaster.broadcast(job)
            else:
                await self.job_update_broadcaster.broadcast(job)
        except Exception as e:
            logger.error(f"Error handling job notification: {e}")

    async def _handle_job_archive_notification(self, notification: str) -> None:
        """
        Handle job archive database notifications.

        :param notification: The notification payload as a string
        """
        try:
            job_data = json.loads(notification)
            job = Job.from_dict(job_data)

            if job.status == "CANCELLED":
                if job.rid in self.active_runners:
                    logger.info(f"Canceling running job: {job.rid}")
                    runner: Runner = self.active_runners[job.rid]
                    runner.cancel()
                    del self.active_runners[job.rid]
            else:
                logger.info(f"Job {job.rid} completed with status: {job.status}")
                if (
                    hasattr(self, "job_delete_broadcaster")
                    and self.job_delete_broadcaster
                ):
                    await self.job_delete_broadcaster.broadcast(job)

        except Exception as e:
            logger.error(f"Error handling job archive notification: {e}")
            traceback.print_exc()

    def _start_listeners(self) -> None:
        """Start database listeners."""
        # Start job listener
        if self.job_db_listener:
            try:
                logger.debug("Starting job listeners")
                job_runner = go_func(
                    self._start_job_listener,
                    False,
                    self._handle_job_notification,
                )
                if job_runner and isinstance(job_runner, SmallRunner):
                    self.job_db_listener_runner = job_runner
            except Exception as e:
                logger.error(f"Error starting job listener: {e}")

        # Start job archive listener
        if self.job_archive_db_listener:
            try:
                logger.debug("Starting job archive listeners")
                archive_runner = go_func(
                    self._start_job_archive_listener,
                    False,
                    self._handle_job_archive_notification,
                )
                if archive_runner and isinstance(archive_runner, SmallRunner):
                    self.job_archive_db_listener_runner = archive_runner
            except Exception as e:
                logger.error(f"Error starting job archive listener: {e}")

    def _wait_for_listeners_ready(self, timeout_seconds: float = 3.0) -> None:
        """
        Wait for both database listeners to be ready by checking their connection status.

        :param timeout_seconds: Maximum time to wait for listeners to be ready
        :raises RuntimeError: If listeners don't become ready within timeout
        """
        logger.debug("Waiting for database listeners to be ready...")
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            job_ready = (
                self.job_db_listener
                and hasattr(self.job_db_listener, "connection")
                and self.job_db_listener.connection is not None
                and hasattr(self.job_db_listener, "listening")
                and self.job_db_listener.listening
            )

            archive_ready = (
                self.job_archive_db_listener
                and hasattr(self.job_archive_db_listener, "connection")
                and self.job_archive_db_listener.connection is not None
                and hasattr(self.job_archive_db_listener, "listening")
                and self.job_archive_db_listener.listening
            )

            if job_ready and archive_ready:
                logger.info("Database listeners are ready")
                return

            time.sleep(0.1)  # Small delay between checks

        raise RuntimeError(
            f"Database listeners failed to become ready within {timeout_seconds} seconds"
        )

    # Tickers
    def _heartbeat_func(self) -> None:
        """Send periodic heartbeats and handle worker status changes."""
        try:
            logger.debug("Sending worker heartbeat...")

            # Get current worker with read lock
            with self.worker_mutex:
                worker = self.worker

            if worker is None:
                return

            # Select worker from database for heartbeat
            worker_from_db = self.db_worker.select_worker(worker.rid)
            if not worker_from_db:
                logger.error("Error selecting worker for heartbeat")
                return

            # Handle worker status
            if worker_from_db.status == WorkerStatus.STOPPED:
                logger.info(
                    f"Stopping worker... (worker_status: {worker_from_db.status})"
                )
                try:
                    self.stop()
                except Exception as e:
                    logger.error(f"Error stopping queuer: {e}")
                return

            elif worker_from_db.status == WorkerStatus.STOPPING:
                if worker_from_db.max_concurrency != 0:
                    logger.info(
                        f"Gracefully stopping worker... (worker_status: {worker_from_db.status})"
                    )
                    worker_from_db.max_concurrency = 0
                    worker_from_db = self.db_worker.update_worker(worker_from_db)
                    if not worker_from_db:
                        logger.error("Error updating worker concurrency")
                        return
                elif len(self.active_runners) == 0:
                    logger.info(
                        f"All running jobs finished, stopping worker... (worker_status: {worker_from_db.status})"
                    )
                    worker_from_db.status = WorkerStatus.STOPPED
                    worker_from_db = self.db_worker.update_worker(worker_from_db)
                    if not worker_from_db:
                        logger.error("Error updating worker status to stopped")
                        return
                    try:
                        self.stop()
                    except Exception as e:
                        logger.error(f"Error stopping queuer: {e}")
                    return

            else:
                # Default case: update worker heartbeat
                worker_from_db = self.db_worker.update_worker(worker)
                if not worker_from_db:
                    logger.error("Error updating worker heartbeat")
                    return

            # Update local worker with write lock
            with self.worker_mutex:
                self.worker = worker_from_db

        except Exception as e:
            logger.error(f"Heartbeat error: {e}")

    def _start_heartbeat_ticker(self) -> None:
        """Start heartbeat ticker using threading."""
        self.heartbeat_ticker = Ticker(
            self.worker_poll_interval,
            self._heartbeat_func,
            use_mp=False,
        )
        self.heartbeat_ticker.go()

    def _poll_jobs_func(self) -> None:
        """Poll for jobs as backup to notification system."""
        try:
            logger.debug("Running backup job polling")
            self._run_job_initial()
        except Exception as e:
            logger.warning(f"Backup job polling error: {e}")

    def _start_poll_job_ticker(self) -> None:
        """
        Start job polling ticker as backup mechanism.
        This provides a safety net in case notification-based processing fails.
        """
        self.poll_job_ticker = Ticker(
            self.job_poll_interval,
            self._poll_jobs_func,
            use_mp=False,
        )
        self.poll_job_ticker.go()

    # Database listeners
    async def _start_job_listener(
        self, handle_job_notification: Callable[[str], Any]
    ) -> None:
        """
        Start job database listener.

        :param handle_job_notification: The callback to handle job notifications
        """
        if self.job_db_listener:
            await self.job_db_listener.listen(handle_job_notification)

    async def _start_job_archive_listener(
        self, handle_job_archive_notification: Callable[[str], Any]
    ):
        """
        Start job archive database listener.

        :param handle_job_archive_notification: The callback to handle job archive notifications
        """
        if self.job_archive_db_listener:
            await self.job_archive_db_listener.listen(handle_job_archive_notification)
