"""
Job-related methods for the Python queuer implementation.
Mirrors Go's queuerJob.go functionality.
"""

import asyncio
import logging
import threading
import time
from datetime import datetime
from typing import List, Optional, Any, Union, Callable, TYPE_CHECKING
from uuid import UUID
from psycopg.rows import dict_row

from core.runner import Runner, SmallRunner
from core.retryer import Retryer
from core.scheduler import Scheduler
from helper.task import get_task_name_from_interface
from model.job import Job, JobStatus, new_job as create_job
from model.options import Options

if TYPE_CHECKING:
    from model.batch_job import BatchJob

# Set up logger
logger = logging.getLogger(__name__)


class QueuerJobMixin:
    """
    Mixin class containing job-related methods for the Queuer.
    This mirrors the job methods from Go's queuerJob.go.
    """

    def add_job(self, task: Union[Callable, str], *parameters: Any) -> "Job":
        """
        Add a job to the queue with the given task and parameters.

        Args:
            task: Either a function or a string with the task name
            *parameters: Parameters to pass to the task

        Returns:
            The created job

        Raises:
            Exception: If something goes wrong
        """
        options: Optional[Options] = self._merge_options(None)
        job: Job = self._add_job(task, options, *parameters)

        logger.info(f"Job added: {job.rid}")

        return job

    def add_job_with_options(
        self, options: dict, task: Union[Callable, str], *parameters
    ):
        """Add a new job with specific options."""
        # Merge default options with provided options
        options: Optional[Options] = self._merge_options(options)

        try:
            # Create new job
            new_job: Job = create_job(task, options, *parameters)
            job: Job = self.db_job.insert_job(new_job)

            logger.info(f"Job with options added: {job.rid}")
            return job

        except Exception as e:
            logger.error(f"Error adding job with options: {str(e)}")
            raise Exception(f"Adding job: {str(e)}")

    def add_jobs(self, batch_jobs: List["BatchJob"]) -> None:
        """
        Add a batch of jobs to the queue.

        Args:
            batch_jobs: List of BatchJob objects containing task, options, and parameters

        Raises:
            Exception: If something goes wrong during the process
        """
        jobs: List[Job] = []
        for batch_job in batch_jobs:
            options: Optional[Options] = self._merge_options(batch_job.options)
            task_name: str = get_task_name_from_interface(batch_job.task)
            job: Job = create_job(task_name, batch_job.parameters, options)
            jobs.append(job)

        self.db_job.batch_insert_jobs(jobs)
        logger.info(f"Jobs added: {len(jobs)}")

    def wait_for_job_added(self, timeout_seconds: float = 30.0) -> Optional["Job"]:
        """
        Wait for any job to start and return the job.
        Uses the shared Runner event loop.
        """
        runner = Runner(
            task=self._wait_for_job_added_inner,
            args=(timeout_seconds,),
            kwargs={},
        )
        runner.go()
        return runner.get_results(timeout=timeout_seconds)

    async def _wait_for_job_added_inner(
        self, timeout_seconds: float = 30.0
    ) -> Optional["Job"]:
        """
        Wait for any job to start and return the job.
        Uses the broadcaster approach.

        Args:
            timeout_seconds: Maximum time to wait for a job to be added

        Returns:
            The job that was added, or None if cancelled or timeout
        """
        if (
            not hasattr(self, "job_insert_broadcaster")
            or self.job_insert_broadcaster is None
        ):
            logger.warning("Job insert broadcaster not available")
            return None

        # Subscribe to the broadcaster
        ch = await self.job_insert_broadcaster.subscribe()

        try:
            # Wait for job insertion with timeout
            job = await asyncio.wait_for(ch.get(), timeout=timeout_seconds)
            return job
        except asyncio.TimeoutError:
            logger.debug(
                f"Timeout waiting for job to be added after {timeout_seconds} seconds"
            )
            return None
        except Exception as e:
            logger.error(f"Error waiting for job added: {e}")
            return None
        finally:
            # Unsubscribe from broadcaster
            await self.job_insert_broadcaster.unsubscribe(ch)

    def wait_for_job_finished(
        self, job_rid: UUID, timeout_seconds: float = 30.0
    ) -> Optional["Job"]:
        """
        Wait for a job to finish and return the job.
        Uses SmallRunner for shared state access.
        """
        # Add buffer to runner timeout to allow async method final check to complete
        runner_timeout = timeout_seconds + 1.0
        runner = SmallRunner(
            task=self._wait_with_listener,
            args=(job_rid, timeout_seconds),
            kwargs={},
        )
        runner.go()
        return runner.get_results(timeout=runner_timeout)

    async def _wait_with_listener(
        self, job_rid: UUID, timeout_seconds: float
    ) -> Optional["Job"]:
        """Wait for job completion using pure database notifications."""

        # Check if job is already completed in archive
        def check_archive():
            with self.db_job.db.instance.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    "SELECT * FROM job_archive WHERE rid = %s ORDER BY updated_at DESC LIMIT 1",
                    (job_rid,),
                )
                row = cur.fetchone()
                if row:
                    from model.job import Job

                    return Job.from_row(row)
                return None

        # First check for immediate completion
        completed_job = check_archive()
        if completed_job:
            return completed_job

        # Job not in archive yet, wait for notification
        if (
            not hasattr(self, "job_delete_broadcaster")
            or self.job_delete_broadcaster is None
        ):
            return None

        channel = None
        try:
            # Subscribe to job delete notifications (job completion)
            channel = await self.job_delete_broadcaster.subscribe()

            # Check archive again in case job completed during subscription
            completed_job = check_archive()
            if completed_job:
                return completed_job

            # Wait for the specific job notification
            while True:
                completed_job = await asyncio.wait_for(
                    channel.get(), timeout=timeout_seconds
                )

                # Check if this is the job we're waiting for
                if completed_job.rid == job_rid:
                    return completed_job
                # Continue waiting for our specific job if it's a different one

        except asyncio.TimeoutError:
            # Final check before giving up
            return check_archive()
        except Exception:
            return None
        finally:
            # Cleanup subscription
            if channel:
                try:
                    await self.job_delete_broadcaster.unsubscribe(channel)
                except Exception:
                    pass

    def cancel_job(self, job_rid: UUID) -> "Job":
        """
        Cancel a job with the given job RID.

        Args:
            job_rid: The RID of the job to cancel

        Returns:
            The cancelled job

        Raises:
            Exception: If the job is not found or already cancelled
        """
        job = self.db_job.select_job(job_rid)
        if not job:
            raise Exception(f"Job not found: {job_rid}")

        self._cancel_job(job)
        return job

    def cancel_all_jobs_by_worker(self, worker_rid: UUID, entries: int = 100) -> None:
        """
        Cancel all jobs assigned to a specific worker by its RID.

        Args:
            worker_rid: The RID of the worker
            entries: Maximum number of jobs to cancel

        Raises:
            Exception: If something goes wrong during the process
        """
        jobs = self.db_job.select_all_jobs_by_worker_rid(worker_rid, 0, entries)
        for job in jobs:
            self._cancel_job(job)

    def readd_job_from_archive(self, job_rid: UUID) -> "Job":
        """
        Readd a job from the archive back to the queue.

        Args:
            job_rid: The RID of the job to readd

        Returns:
            The new job created from the archived job

        Raises:
            Exception: If the job is not found in archive
        """
        job = self.db_job.select_job_from_archive(job_rid)
        if not job:
            raise Exception(f"Job not found in archive: {job_rid}")

        # Readd the job to the queue
        new_job = self.add_job_with_options(job.options, job.task_name, *job.parameters)
        logger.info(f"Job readded: {new_job.rid}")
        return new_job

    def get_job(self, job_rid: UUID) -> "Job":
        """
        Retrieve a job by its RID.

        Args:
            job_rid: The RID of the job

        Returns:
            The job

        Raises:
            Exception: If the job is not found
        """
        job = self.db_job.select_job(job_rid)
        if not job:
            raise Exception(f"Job not found: {job_rid}")
        return job

    def get_jobs(self, last_id: int = 0, entries: int = 100) -> List["Job"]:
        """
        Retrieve all jobs in the queue.

        Args:
            last_id: Last job ID for pagination
            entries: Number of entries to retrieve

        Returns:
            List of jobs
        """
        return self.db_job.select_all_jobs(last_id, entries)

    def get_job_ended(self, job_rid: UUID) -> "Job":
        """
        Retrieve a job that has ended (succeeded, cancelled or failed) by its RID.

        Args:
            job_rid: The RID of the job

        Returns:
            The ended job

        Raises:
            Exception: If the job is not found
        """
        job = self.db_job.select_job_from_archive(job_rid)
        if not job:
            raise Exception(f"Ended job not found: {job_rid}")
        return job

    def get_jobs_ended(self, last_id: int = 0, entries: int = 100) -> List["Job"]:
        """
        Retrieve all jobs that have ended (succeeded, cancelled or failed).

        Args:
            last_id: Last job ID for pagination
            entries: Number of entries to retrieve

        Returns:
            List of ended jobs
        """
        return self.db_job.select_all_jobs_from_archive(last_id, entries)

    def get_jobs_by_worker_rid(
        self, worker_rid: UUID, last_id: int = 0, entries: int = 100
    ) -> List["Job"]:
        """
        Retrieve jobs assigned to a specific worker by its RID.

        Args:
            worker_rid: The RID of the worker
            last_id: Last job ID for pagination
            entries: Number of entries to retrieve

        Returns:
            List of jobs assigned to the worker
        """
        return self.db_job.select_all_jobs_by_worker_rid(worker_rid, last_id, entries)

    # Internal/Private methods

    def _merge_options(self, options: Optional["Options"]) -> Optional["Options"]:
        """
        Merge the worker options with optional job options.

        Args:
            options: Job-specific options (can be None)

        Returns:
            Merged options
        """
        worker_options = self.worker.options if self.worker else None

        if options is not None and options.on_error is None:
            options.on_error = worker_options
        elif options is None and worker_options is not None:
            options = Options(on_error=worker_options)

        return options

    def _add_job(
        self, task: Union[Callable, str], options: Optional["Options"], *parameters: Any
    ) -> "Job":
        """
        Add a job to the queue with all necessary parameters.

        Args:
            task: The task to execute
            options: Job options
            *parameters: Task parameters

        Returns:
            The created job
        """
        try:
            # Create new job with proper parameter order
            new_job = create_job(task, options, *parameters)
            job = self.db_job.insert_job(new_job)

            # IMMEDIATE PROCESSING - NO POLLING: Process jobs immediately after insertion
            # This ensures pure notification-driven operation without waiting for notifications
            logger.debug("Triggering immediate job processing after insertion")
            try:
                self._run_job_initial()
            except Exception as e:
                logger.warning(f"Error in immediate job processing: {e}")
                # Don't fail job creation if processing fails

            return job

        except Exception as e:
            raise Exception(f"Creating or inserting job: {str(e)}")

    def _run_job_initial(self) -> None:
        """
        Called to run the next job in the queue.
        Updates job status to running with worker.
        """
        if not self.worker:
            logger.debug("No worker available, skipping job run")
            return

        # Check if database connection is available
        if not self.db_job or not self.db_job.db or not self.db_job.db.instance:
            logger.debug("Database connection unavailable, skipping job run")
            return

        logger.debug(f"Worker available_tasks: {self.worker.available_tasks}")
        logger.debug(f"Worker max_concurrency: {self.worker.max_concurrency}")

        jobs = self.db_job.update_jobs_initial(self.worker)
        if not jobs:
            logger.debug("No jobs found to run")
            return

        logger.info(f"Found {len(jobs)} jobs to run")

        for job in jobs:
            logger.info(f"Processing job: {job.rid}")
            if (
                job.options
                and job.options.schedule
                and job.options.schedule.start
                and job.options.schedule.start > datetime.now()
            ):
                # Schedule the job for later using Scheduler - mirrors Go implementation
                logger.info(
                    f"Scheduling job: {job.rid} for {job.options.schedule.start}"
                )

                try:
                    scheduler = Scheduler(
                        job.options.schedule.start, self._run_job_sync, job
                    )
                    scheduler.go()
                    logger.info(f"Job {job.rid} scheduled successfully using Scheduler")
                except Exception as e:
                    logger.error(f"Failed to schedule job with Scheduler: {e}")
            else:
                # Run the job immediately using SmallRunner for shared state access
                logger.info(f"Running job immediately: {job.rid}")
                runner = SmallRunner(task=self._run_job, args=(job,), kwargs={})
                runner.go()

    async def _wait_for_job(
        self, job: "Job"
    ) -> tuple[List[Any], bool, Optional[Exception]]:
        """
        Execute the job and return the results or an error.

        Args:
            job: The job to execute

        Returns:
            Tuple of (results, cancelled, error)
        """
        # Get the task function
        task_obj = self.tasks.get(job.task_name)
        if not task_obj:
            return None, False, Exception(f"Task not found: {job.task_name}")

        # Extract the actual callable function from the Task object
        task = task_obj.task if hasattr(task_obj, "task") else task_obj
        if not callable(task):
            return None, False, Exception(f"Task is not callable: {job.task_name}")

        # Use ProcessRunner for better cancellation control
        try:
            logger.info(
                f"Starting ProcessRunner for task {job.task_name} with parameters {job.parameters}"
            )

            # Use Runner for all job execution
            parameters = getattr(job, "parameters", [])
            runner = Runner(task, tuple(parameters), {})

            logger.info(f"Created runner for job {job.rid}")

            # Store the runner for potential cancellation
            if not hasattr(self, "active_runners"):
                self.active_runners = {}
            self.active_runners[job.rid] = runner

            try:
                # Start the runner
                runner.go()

                # Get results directly since Runner handles execution
                try:
                    results = runner.get_results(
                        timeout=300
                    )  # 5 minute timeout for CPU work
                except Exception as e:
                    logger.error(f"Runner execution error: {e}")
                    raise

                logger.info(
                    f"Runner task {job.task_name} completed with results: {results}"
                )
                return results, False, None

            except asyncio.CancelledError:
                # Handle cancellation
                if runner:
                    runner.terminate()
                    runner.join()
                return None, True, None
            except Exception as e:
                logger.error(
                    f"Process runner task {job.task_name} failed with error: {e}"
                )
                return None, False, e
            finally:
                # Clean up runner reference
                self.active_runners.pop(job.rid, None)

        except Exception as e:
            logger.error(f"Error setting up runner for task {job.task_name}: {e}")
            return None, False, e

    async def _retry_job(self, job: "Job", job_error: Exception) -> None:
        """
        Retry the job with the given job error.

        Args:
            job: The job to retry
            job_error: The error that occurred
        """
        if (
            not job.options
            or not job.options.on_error
            or job.options.on_error.max_retries <= 0
        ):
            await self._fail_job(job, job_error)
            return

        # Create a function that will retry the job
        async def retry_function():
            logger.debug(f"Trying/retrying job: {job.rid}")
            results, cancelled, err = await self._wait_for_job(job)
            if err:
                raise Exception(f"Retrying job failed: {err}")
            return results

        # Create retryer and attempt retry
        retryer = Retryer(retry_function, job.options.on_error)
        error = await retryer.retry()

        if error:
            await self._fail_job(
                job,
                Exception(f"Retrying job failed: {error}, original error: {job_error}"),
            )
        else:
            # If retry succeeded, we need to get the results
            results, _, _ = await self._wait_for_job(job)
            await self._succeed_job(job, results)

    async def _run_job(self, job: "Job") -> None:
        """
        Run the job.

        Args:
            job: The job to run
        """
        logger.info(f"Running job: {job.rid}")

        results, cancelled, error = await self._wait_for_job(job)

        if cancelled:
            return
        elif error:
            await self._retry_job(job, error)
        else:
            await self._succeed_job(job, results)

    def _run_job_sync(self, job: "Job") -> None:
        """
        Synchronous wrapper for _run_job that can be called by the Scheduler.
        Mirrors the Go pattern where scheduler calls the function directly.
        """
        try:
            # Create a Runner process for the async job
            runner = Runner(task=self._run_job, args=(job,), kwargs={})
            runner.go()
            logger.info(f"Scheduled job {job.rid} started in process {runner.pid}")

        except Exception as e:
            logger.error(f"Error executing scheduled job {job.rid}: {e}")

    def _cancel_job(self, job: "Job") -> None:
        """
        Cancel a job.

        Args:
            job: The job to cancel
        """
        if job.status in [JobStatus.RUNNING, JobStatus.SCHEDULED, JobStatus.QUEUED]:
            # Cancel the running process if it exists
            if hasattr(self, "active_runners") and job.rid in self.active_runners:
                runner = self.active_runners[job.rid]
                try:
                    runner.terminate()
                    runner.join(timeout=3.0)  # Wait up to 3 seconds for clean shutdown
                    logger.info(f"Cancelled running process for job: {job.rid}")
                except Exception as e:
                    logger.warning(
                        f"Failed to cancel running process for job: {job.rid}: {e}"
                    )

            job.status = JobStatus.CANCELLED
            try:
                self.db_job.update_job_final(job)
                logger.info(f"Job cancelled: {job.rid}")
            except Exception as e:
                logger.error(f"Error updating job status to cancelled: {e}")

    async def _succeed_job(self, job: "Job", results: List[Any]) -> None:
        """
        Update the job status to succeeded and run the next job if available.

        Args:
            job: The job that succeeded
            results: The results from the job
        """
        job.status = JobStatus.SUCCEEDED
        job.results = results
        await self._end_job(job)

    async def _fail_job(self, job: "Job", job_error: Exception) -> None:
        """
        Update the job status to failed.

        Args:
            job: The job that failed
            job_error: The error that occurred
        """
        job.status = JobStatus.FAILED
        job.error = str(job_error)
        await self._end_job(job)

    async def _end_job(self, job: "Job") -> None:
        """
        End a job and potentially schedule it again if it's a recurring job.

        Args:
            job: The job to end
        """
        if not self.worker or job.worker_id != self.worker.id:
            return

        # Check if database connection is available
        if not self.db_job or not self.db_job.db or not self.db_job.db.instance:
            logger.error(f"Database connection unavailable for job {job.rid}")
            return

        try:
            ended_job = self.db_job.update_job_final(job)
            logger.debug(f"Job ended: {ended_job.status} - {ended_job.rid}")

            # Handle scheduled jobs (recurring jobs)
            if (
                ended_job.options
                and ended_job.options.schedule
                and ended_job.schedule_count < ended_job.options.schedule.max_count
            ):
                if ended_job.options.schedule.next_interval:
                    # Use next interval function if available
                    next_interval_func = self.next_interval_funcs.get(
                        ended_job.options.schedule.next_interval
                    )
                    if not next_interval_func:
                        logger.error(
                            f"NextIntervalFunc not found: {ended_job.options.schedule.next_interval} for job {ended_job.rid}"
                        )
                        return
                    new_scheduled_at = next_interval_func(
                        ended_job.scheduled_at, ended_job.schedule_count
                    )
                else:
                    # Use regular interval
                    new_scheduled_at = ended_job.scheduled_at + (
                        ended_job.schedule_count * ended_job.options.schedule.interval
                    )

                ended_job.scheduled_at = new_scheduled_at
                ended_job.status = JobStatus.SCHEDULED

                new_job = self.db_job.insert_job(ended_job)
                logger.info(f"Job added for next iteration to the queue: {new_job.rid}")

        except Exception as e:
            logger.error(f"Error updating finished job: {e}")

        # Try to run the next job in the queue
        try:
            # Only run next job if we have valid database connection
            if self.db_job and self.db_job.db and self.db_job.db.instance:
                self._run_job_initial()
        except Exception as e:
            logger.error(f"Error running next job: {e}")
