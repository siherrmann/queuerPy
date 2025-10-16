"""
Job-related methods for the Python queuer implementation.
Mirrors Go's queuerJob.go functionality.
"""

import asyncio
import logging
import threading
from typing import List, Optional, Any, Union, Callable, TYPE_CHECKING
from uuid import UUID
import time

from core.runner import Runner, new_runner_from_job
from core.retryer import Retryer
from helper.task import get_task_name_from_interface
from model.job import Job, JobStatus, new_job as create_job
from model.options import Options

# Set up logger
logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from model.job import Job
    from model.options import Options
    from model.batch_job import BatchJob

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
        options = self._merge_options(None)
        job = self._add_job(task, options, *parameters)

        logger.info(f"Job added: {job.rid}")

        return job

    def add_job_with_options(self, options: dict, task: str, *parameters):
        """Add a new job with specific options."""
        # Merge default options with provided options
        options = self._merge_options(options)

        try:
            # Create new job
            new_job = create_job(task, options, *parameters)
            job = self.db_job.insert_job(new_job)

            logger.info(f"Job with options added: {job.rid}")
            return job

        except Exception as e:
            logger.error(f"Error adding job with options: {str(e)}")
            raise Exception(f"Adding job: {str(e)}")

    def _add_job(self, task: str, options: dict, *parameters):
        """Internal method to add a job."""
        try:
            # Create new job
            new_job = create_job(task, options, *parameters)
            job = self.db_job.insert_job(new_job)
            return job

        except Exception as e:
            raise Exception(f"Creating or inserting job: {str(e)}")

    def add_jobs(self, batch_jobs: List["BatchJob"]) -> None:
        """
        Add a batch of jobs to the queue.

        Args:
            batch_jobs: List of BatchJob objects containing task, options, and parameters

        Raises:
            Exception: If something goes wrong during the process
        """
        jobs = []
        for batch_job in batch_jobs:
            options = self._merge_options(batch_job.options)
            task_name = get_task_name_from_interface(batch_job.task)
            job = create_job(task_name, batch_job.parameters, options)
            jobs.append(job)

        self.db_job.batch_insert_jobs(jobs)
        logger.info(f"Jobs added: {len(jobs)}")

    def wait_for_job_added(self, timeout_seconds: float = 30.0) -> Optional["Job"]:
        job_added = asyncio.run(self._wait_for_job_added_inner(timeout_seconds))
        return job_added

    async def _wait_for_job_added_inner(
        self, timeout_seconds: float = 30.0
    ) -> Optional["Job"]:
        """
        Wait for any job to start and return the job.
        Listens for job insert events and returns the job when it is added to the queue.
        Mirrors Go's WaitForJobAdded method.

        Args:
            timeout_seconds: Maximum time to wait for a job to be added

        Returns:
            The job that was added, or None if cancelled or timeout
        """
        if not hasattr(self, "job_insert_listener") or self.job_insert_listener is None:
            logger.warning("Job insert listener not available")
            return None

        job_added = asyncio.Event()
        received_job = None

        def on_job_added(job):
            nonlocal received_job
            received_job = job
            job_added.set()

        # Create threading events for the listener
        stop_event = threading.Event()
        ready_event = threading.Event()

        try:
            # Start listening in a separate thread
            def listen_thread():
                try:
                    self.job_insert_listener.listen(
                        stop_event, ready_event, on_job_added
                    )
                except Exception as e:
                    logger.error(f"Error listening for job inserts: {e}")

            listener_thread = threading.Thread(target=listen_thread)
            listener_thread.start()

            # Wait for listener to be ready
            ready_event.wait(timeout=5.0)  # 5 second timeout for listener ready

            # Wait for job to be added or timeout
            try:
                await asyncio.wait_for(job_added.wait(), timeout=timeout_seconds)
                return received_job
            except asyncio.TimeoutError:
                logger.debug(
                    f"Timeout waiting for job to be added after {timeout_seconds} seconds"
                )
                return None
            finally:
                # Stop the listener
                stop_event.set()
                listener_thread.join(timeout=1.0)

        except Exception as e:
            logger.error(f"Error waiting for job added: {e}")
            return None

    def wait_for_job_finished(
        self, job_rid: UUID, timeout_seconds: float = 30.0
    ) -> Optional["Job"]:
        finished_job = asyncio.run(
            self._wait_for_job_finished_inner(job_rid, timeout_seconds)
        )
        return finished_job

    async def _wait_for_job_finished_inner(
        self, job_rid: UUID, timeout_seconds: float = 30.0
    ) -> Optional["Job"]:
        """
        Wait for a job to finish and return the job.
        Listens for job delete events and returns the job when it is finished.
        Mirrors Go's WaitForJobFinished method.

        Args:
            job_rid: The RID of the job to wait for
            timeout_seconds: Maximum time to wait for job to finish

        Returns:
            The finished job, or None if cancelled or timeout
        """
        if not hasattr(self, "job_delete_listener") or self.job_delete_listener is None:
            logger.warning("Job delete listener not available")
            return None

        job_finished = asyncio.Event()
        finished_job = None

        def on_job_deleted(job):
            nonlocal finished_job
            if job.rid == job_rid:
                finished_job = job
                job_finished.set()

        # Create threading events for the listener
        stop_event = threading.Event()
        ready_event = threading.Event()

        try:
            # Start listening in a separate thread
            def listen_thread():
                try:
                    self.job_delete_listener.listen(
                        stop_event, ready_event, on_job_deleted
                    )
                except Exception as e:
                    logger.error(f"Error listening for job deletes: {e}")

            listener_thread = threading.Thread(target=listen_thread)
            listener_thread.start()

            # Wait for listener to be ready
            ready_event.wait(timeout=5.0)  # 5 second timeout for listener ready

            # Wait for specific job to be deleted/finished or timeout
            try:
                await asyncio.wait_for(job_finished.wait(), timeout=timeout_seconds)
                return finished_job
            except asyncio.TimeoutError:
                logger.debug(
                    f"Timeout waiting for job {job_rid} to finish after {timeout_seconds} seconds"
                )
                return None
            finally:
                # Stop the listener
                stop_event.set()
                listener_thread.join(timeout=1.0)

        except Exception as e:
            logger.error(f"Error waiting for job finished: {e}")
            return None

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

        jobs = self.db_job.update_jobs_initial(self.worker)
        if not jobs:
            logger.debug("No jobs found to run")
            return

        logger.info(f"Found {len(jobs)} jobs to run")

        # Get the event loop stored during start() or current running loop
        event_loop = getattr(self, "_event_loop", None)
        if not event_loop:
            try:
                event_loop = asyncio.get_running_loop()
                logger.debug("Using current running event loop for scheduled jobs")
            except RuntimeError:
                logger.error(
                    "No event loop available for running async jobs - queuer not properly started"
                )
                raise RuntimeError(
                    "Queuer not properly started: no event loop available"
                )

        for job in jobs:
            logger.info(f"Processing job: {job.rid}")
            if (
                job.options
                and job.options.schedule
                and job.options.schedule.start
                and job.options.schedule.start > time.time()
            ):
                # Schedule the job for later
                logger.info(
                    f"Scheduling job: {job.rid} for {job.options.schedule.start}"
                )

                async def run_scheduled_job():
                    await asyncio.sleep(job.options.schedule.start - time.time())
                    await self._run_job(job)

                # Submit the coroutine to the main event loop
                try:
                    future = asyncio.run_coroutine_threadsafe(
                        run_scheduled_job(), event_loop
                    )

                    # Add error handling for the scheduled task
                    def handle_schedule_done(future_ref):
                        try:
                            exc = future_ref.exception()
                            if exc is not None:
                                logger.error(f"Scheduled task failed: {exc}")
                        except Exception as e:
                            logger.error(f"Error in schedule done callback: {e}")

                    future.add_done_callback(handle_schedule_done)
                except Exception as e:
                    logger.error(f"Failed to schedule job: {e}")
            else:
                # Run the job immediately
                logger.info(f"Running job immediately: {job.rid}")
                try:
                    future = asyncio.run_coroutine_threadsafe(
                        self._run_job(job), event_loop
                    )

                    # Add error handling for the task to prevent unawaited coroutine warnings
                    def handle_task_done(future_ref):
                        try:
                            result = (
                                future_ref.result()
                            )  # Get result to check for exceptions
                            logger.debug(f"Job {job.rid} completed successfully")
                        except Exception as exc:
                            logger.error(f"Background job task failed: {exc}")

                    future.add_done_callback(handle_task_done)
                    logger.debug(f"Submitted job {job.rid} to event loop")
                except Exception as e:
                    logger.error(f"Failed to run job: {e}")

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

        # Use Runner architecture with proper event loop coordination
        try:
            logger.info(
                f"Starting Runner for task {job.task_name} with parameters {job.parameters}"
            )

            # Get the event loop stored during start() or current running loop
            event_loop = getattr(self, "_event_loop", None)
            if not event_loop:
                try:
                    event_loop = asyncio.get_running_loop()
                    logger.debug(
                        "Using current running event loop for Runner coordination"
                    )
                except RuntimeError:
                    logger.warning(
                        "No event loop available, falling back to direct execution"
                    )
                    # Fallback to direct execution
                    if asyncio.iscoroutinefunction(task):
                        results = await task(*job.parameters)
                    else:
                        loop = asyncio.get_running_loop()
                        results = await loop.run_in_executor(
                            None, task, *job.parameters
                        )
                    logger.info(
                        f"Task {job.task_name} completed with results: {results}"
                    )
                    return results, False, None

            # Use Runner with event loop coordination
            runner = new_runner_from_job(task, job)
            logger.info(f"Created runner for job {job.rid}")

            # Store the runner for potential cancellation
            if not hasattr(self, "active_runners"):
                self.active_runners = {}
            self.active_runners[job.rid] = runner

            try:
                # Start the runner
                if not runner.run():
                    return None, False, Exception("Failed to start runner")

                # Create an async wrapper to get results from the runner
                async def get_runner_results():
                    # Submit the get_results call to the event loop from a thread
                    def _get_results():
                        try:
                            return runner.get_results(timeout=30)  # 30 second timeout
                        except Exception as e:
                            logger.error(f"Runner execution error: {e}")
                            raise

                    # Run the sync get_results in the thread pool to avoid blocking
                    loop = asyncio.get_running_loop()
                    return await loop.run_in_executor(None, _get_results)

                # Wait for results
                results = await get_runner_results()
                logger.info(
                    f"Runner task {job.task_name} completed with results: {results}"
                )
                return results, False, None

            except asyncio.CancelledError:
                # Handle cancellation
                if runner:
                    runner.cancel()
                return None, True, None
            except Exception as e:
                logger.error(f"Runner task {job.task_name} failed with error: {e}")
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
        error = retryer.retry()

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

    def _cancel_job(self, job: "Job") -> None:
        """
        Cancel a job.

        Args:
            job: The job to cancel
        """
        if job.status in [JobStatus.RUNNING, JobStatus.SCHEDULED, JobStatus.QUEUED]:
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
            self._run_job_initial()
        except Exception as e:
            logger.error(f"Error running next job: {e}")
