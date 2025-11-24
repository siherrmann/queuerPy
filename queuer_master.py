"""
Master functionality for Python queuer implementation.
Mirrors the Go queuerMaster.go with Python async patterns.
"""

from datetime import timedelta
from typing import Optional

from .core.runner import go_func
from .core.ticker import Ticker
from .helper.logging import get_logger
from .helper.error import QueuerError
from .model.master import Master, MasterSettings
from .queuer_global import QueuerGlobalMixin

logger = get_logger(__name__)


class QueuerMasterMixin(QueuerGlobalMixin):
    """
    Master functionality mixin for Queuer class.
    Mirrors Go's queuerMaster.go functionality.
    """

    async def master_ticker(
        self, old_master: Optional[Master], master_settings: MasterSettings
    ) -> None:
        """
        Master ticker function that manages retention archive and periodic tasks.
        Mirrors Go's masterTicker function.

        :param old_master: The previous master configuration
        :param master_settings: Current master settings
        :raises QueuerError: If old master is None or operations fail
        """
        if old_master is None:
            raise QueuerError("old master check", Exception("old master is None"))

        # Handle retention archive configuration
        try:
            if old_master.settings.retention_archive == 0:
                self.db_job.add_retention_archive(master_settings.retention_archive)
            elif (
                old_master.settings.retention_archive
                != master_settings.retention_archive
            ):
                self.db_job.remove_retention_archive()
                self.db_job.add_retention_archive(master_settings.retention_archive)
        except Exception as e:
            raise QueuerError("managing retention archive", e)

        try:

            def ticker_task() -> None:
                """Periodic task executed by the master ticker."""
                try:
                    try:
                        self.db_master.update_master(self.worker, master_settings)
                    except Exception:
                        try:
                            self.poll_master_ticker(master_settings)
                        except Exception as restart_err:
                            logger.error(
                                f"Error restarting poll master ticker: {restart_err}"
                            )

                    # Check for stale workers
                    try:
                        self.check_stale_workers()
                    except Exception as e:
                        logger.error(f"Error checking for stale workers: {e}")

                    # Check for stale jobs
                    try:
                        self.check_stale_jobs()
                    except Exception as e:
                        logger.error(f"Error checking for stale jobs: {e}")

                    # Additional periodic logic can be added here
                    # This could include database cleanup, monitoring, etc.

                except Exception as e:
                    logger.error(f"Error in master ticker task: {e}")

            ticker = Ticker(
                task=ticker_task,
                name="_master_ticker",
                interval=timedelta(seconds=master_settings.master_poll_interval),
                func=ticker_task,
            )
            logger.info("Starting master ticker...")
            ticker.go()

        except Exception as e:
            raise QueuerError("creating ticker", e)

    def check_stale_workers(self) -> None:
        """
        Check for and update stale workers.
        Mirrors Go's checkStaleWorkers function.
        """
        stale_threshold = timedelta(minutes=2)
        try:
            stale_count = self.db_worker.update_stale_workers(stale_threshold)
            if stale_count > 0:
                logger.info(f"Updated stale workers: {stale_count}")
        except Exception as e:
            raise QueuerError("updating stale workers", e)

    def check_stale_jobs(self) -> None:
        """
        Check for and update stale jobs.
        Mirrors Go's checkStaleJobs function.
        """
        try:
            stale_count = self.db_job.update_stale_jobs()
            if stale_count > 0:
                logger.info(f"Updated stale jobs: {stale_count}")
        except Exception as e:
            raise QueuerError("updating stale jobs", e)

    def poll_master_ticker(self, master_settings: MasterSettings) -> None:
        """
        Poll master ticker function that checks for master role and starts master ticker.
        Mirrors Go's pollMasterTicker function.

        :param master_settings: Current master settings
        :raises QueuerError: If ticker creation fails
        """
        try:

            def ticker_task() -> None:
                """Periodic task that polls for master role."""
                try:
                    logger.info("Polling master...")
                    with self.worker_mutex:
                        worker = self.worker
                        worker_rid = self.worker.rid

                    master = self.db_master.update_master(worker, master_settings)
                    if master:
                        logger.debug(f"New master: {worker_rid}")
                        try:
                            go_func(self.master_ticker, False, master, master_settings)
                        except Exception as e:
                            logger.error(f"Error starting master ticker: {e}")

                except Exception as e:
                    logger.error(f"Error in master poll task: {e}")

            # Create and start the polling ticker
            ticker = Ticker(
                task=ticker_task,
                name="_poll_master_ticker",
                interval=timedelta(seconds=master_settings.master_poll_interval),
                func=ticker_task,
            )

            logger.info("Starting master poll ticker...")
            ticker.go()

        except Exception as e:
            raise QueuerError("creating ticker", e)
