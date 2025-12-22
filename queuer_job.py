"""
Job-related methods for the Python queuer implementation.
Mirrors Go's queuerJob.go functionality.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple, Union, Callable
from uuid import UUID

from .core.broadcaster import BroadcasterQueue
from .core.runner import Runner, SmallRunner
from .core.retryer import Retryer
from .core.scheduler import Scheduler
from .helper.error import QueuerError
from .helper.task import get_task_name_from_interface
from .model.batch_job import BatchJob
from .model.job import Job, JobStatus, new_job as create_job
from .model.options import Options
from .model.worker import Worker
from .queuer_global import QueuerGlobalMixin

# Set up logger
logger = logging.getLogger(__name__)


class QueuerJobMixin(QueuerGlobalMixin):
    """
    Mixin class containing job-related methods for the Queuer.
    This mirrors the job methods from Go's queuerJob.go.
    """

    def __init__(self):
        super().__init__()

    def add_job(
        self,
        task: Union[Callable[..., Any], str],
        *parameters: Any,
        **parameters_keyed: Any,
    ) -> Job:
        """
        Add a job to the queue with the given task and parameters.

        :param task: Either a function or a string with the task name
        :param parameters: Positional parameters to pass to the task
        :param parameters_keyed: Keyword parameters to pass to the task
        :returns: The created job
        :raises Exception: If something goes wrong
        """
        options: Optional[Options] = self._merge_options(None)
        job: Job = self._add_job(task, options, *parameters, **parameters_keyed)

        logger.info(f"Job added: {job.rid}")

        return job

    def add_job_with_options(
        self,
        options: Optional[Options],
        task: Union[Callable[..., Any], str],
        *parameters: Any,
        **parameters_keyed: Any,
    ) -> Job:
        """Add a new job with specific options."""
        # Merge default options with provided options
        options_merged: Optional[Options] = self._merge_options(options)

        try:
            # Create new job
            new_job: Job = create_job(
                task, options_merged, *parameters, **parameters_keyed
            )
            job: Job = self.db_job.insert_job(new_job)

            logger.info(f"Job with options added: {job.rid}")
            return job
        except Exception as e:
            logger.error(f"Error adding job with options: {str(e)}")
            raise Exception(f"Adding job: {str(e)}")

    def add_jobs(self, batch_jobs: List[BatchJob]) -> None:
        """
        Add a batch of jobs to the queue.

        :param batch_jobs: List of BatchJob objects to add
        :raises Exception: If something goes wrong during the process
        """
        jobs: List[Job] = []
        for batch_job in batch_jobs:
            options: Optional[Options] = self._merge_options(batch_job.options)
            task_name: str = get_task_name_from_interface(batch_job.task)
            job: Job = create_job(
                task_name, options, *batch_job.parameters, **batch_job.parameters_keyed
            )
            jobs.append(job)

        self.db_job.batch_insert_jobs(jobs)
        logger.info(f"Jobs added: {len(jobs)}")

    def wait_for_job_added(self, timeout_seconds: float = 30.0) -> Optional[Job]:
        """
        Wait for any job to start and return the job.
        Uses the shared Runner event loop.

        :param timeout_seconds: Maximum time to wait for a job to be added
        :returns: The job that was added, or None if cancelled or timeout
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
    ) -> Optional[Job]:
        """
        Wait for any job to start and return the job.
        Uses the broadcaster approach.

        :param timeout_seconds: Maximum time to wait for a job to be added
        :returns: The job that was added, or None if cancelled or timeout
        :raises Exception: If broadcaster is not available
        """
        channel: BroadcasterQueue[Job] = await self.job_insert_broadcaster.subscribe()
        try:
            job = await asyncio.wait_for(channel.get(), timeout=timeout_seconds)
            job_added = self.get_job(job.rid)
            if not job_added:
                raise Exception(f"Job added not found: {job.rid}")

            return job_added
        except asyncio.TimeoutError:
            logger.debug(f"Timed out waiting for job added ({timeout_seconds}s)")
            return None
        except Exception as e:
            logger.error(f"Error waiting for job added: {e}")
            return None
        finally:
            # Unsubscribe from broadcaster
            await self.job_insert_broadcaster.unsubscribe(channel)

    def wait_for_job_finished(
        self, job_rid: UUID, timeout_seconds: float = 30.0
    ) -> Optional[Job]:
        """
        Wait for a job to finish and return the job.
        Uses SmallRunner for shared state access.

        :param job_rid: The RID of the job to wait for
        :param timeout_seconds: Maximum time to wait for job completion
        :returns: The completed job, or None if not found or timeout
        :raises Exception: If broadcaster is not available
        """
        # Add buffer to runner timeout to allow async method final check to complete
        runner_timeout = timeout_seconds + 1.0
        runner = SmallRunner(self._wait_with_listener, job_rid, timeout_seconds)
        runner.go()
        return runner.get_results(timeout=runner_timeout)

    async def _wait_with_listener(
        self, job_rid: UUID, timeout_seconds: float
    ) -> Optional[Job]:
        """
        Wait for job completion using pure database notifications.

        :param job_rid: The RID of the job to wait for
        :param timeout_seconds: Maximum time to wait for job completion
        :returns: The completed job, or None if not found or timeout
        :raises Exception: If broadcaster is not available
        """
        try:
            job_ended: Job = self.get_job_ended(job_rid)
            if job_ended:
                return job_ended
        except Exception:
            pass

        channel: BroadcasterQueue[Job] = await self.job_delete_broadcaster.subscribe()
        try:
            while True:
                job = await asyncio.wait_for(channel.get(), timeout=timeout_seconds)
                if job.rid == job_rid:
                    complete_job = self.get_job_ended(job_rid)
                    if not complete_job:
                        raise Exception(f"Ended job not found: {job_rid}")

                    return complete_job
        except asyncio.TimeoutError:
            logger.debug(f"Timed out waiting for job ended ({timeout_seconds}s)")
            try:
                job_ended: Job = self.get_job_ended(job_rid)
                if job_ended:
                    return job_ended
            except Exception as e:
                raise QueuerError("get job ended", e)
        except Exception as e:
            raise QueuerError("waiting for job", e)
        finally:
            await self.job_delete_broadcaster.unsubscribe(channel)

    def cancel_job(self, job_rid: UUID) -> Job:
        """
         Cancel a job with the given job RID.

        :param job_rid: The RID of the job to cancel
        :returns: The cancelled job
        :raises Exception: If the job is not found or already cancelled
        """
        job = self.db_job.select_job(job_rid)
        if not job:
            raise Exception(f"Job not found: {job_rid}")

        self._cancel_job(job)
        return job

    def cancel_all_jobs_by_worker(self, worker_rid: UUID, entries: int = 100) -> None:
        """
        Cancel all jobs assigned to a specific worker by its RID.

        :param worker_rid: The RID of the worker
        :param entries: Maximum number of jobs to cancel
        :raises Exception: If something goes wrong during the process
        """
        jobs = self.db_job.select_all_jobs_by_worker_rid(worker_rid, 0, entries)
        for job in jobs:
            self._cancel_job(job)

    def readd_job_from_archive(self, job_rid: UUID) -> Job:
        """
        Readd a job from the archive back to the queue.

        :param job_rid: The RID of the job to readd
        :returns: The new job created from the archived job
        :raises Exception: If the job is not found in archive
        """
        job = self.db_job.select_job_from_archive(job_rid)
        if not job:
            raise Exception(f"Job not found in archive: {job_rid}")

        # Readd the job to the queue
        new_job = self.add_job_with_options(
            job.options, job.task_name, *job.parameters, **job.parameters_keyed
        )
        logger.info(f"Job readded: {new_job.rid}")
        return new_job

    def get_job(self, job_rid: UUID) -> Job:
        """
        Retrieve a job by its RID.

        :param job_rid: The RID of the job
        :returns: The job
        :raises Exception: If the job is not found
        """
        job = self.db_job.select_job(job_rid)
        if not job:
            raise Exception(f"Job not found: {job_rid}")
        return job

    def get_jobs(self, last_id: int = 0, entries: int = 100) -> List[Job]:
        """
        Retrieve all jobs in the queue.

        :param last_id: Last job ID for pagination
        :param entries: Number of entries to retrieve
        :returns: List of jobs
        """
        return self.db_job.select_all_jobs(last_id, entries)

    def get_job_ended(self, job_rid: UUID) -> Job:
        """
        Retrieve a job that has ended (succeeded, cancelled or failed) by its RID.

        :param job_rid: The RID of the job
        :returns: The ended job
        :raises Exception: If the job is not found
        """
        job = self.db_job.select_job_from_archive(job_rid)
        if not job:
            raise Exception(f"Ended job not found: {job_rid}")
        return job

    def get_jobs_ended(self, last_id: int = 0, entries: int = 100) -> List[Job]:
        """
        Retrieve all jobs that have ended (succeeded, cancelled or failed).

        :param last_id: Last job ID for pagination
        :param entries: Number of entries to retrieve
        :returns: List of ended jobs
        """
        return self.db_job.select_all_jobs_from_archive(last_id, entries)

    def get_jobs_by_worker_rid(
        self, worker_rid: UUID, last_id: int = 0, entries: int = 100
    ) -> List[Job]:
        """
        Retrieve jobs assigned to a specific worker by its RID.

        :param worker_rid: The RID of the worker
        :param last_id: Last job ID for pagination
        :param entries: Number of entries to retrieve
        :returns: List of jobs assigned to the worker
        """
        return self.db_job.select_all_jobs_by_worker_rid(worker_rid, last_id, entries)

    # Internal/Private methods

    def _merge_options(self, options: Optional[Options]) -> Optional[Options]:
        """
        Merge the worker options with optional job options.

        :param options: Job-specific options (can be None)
        :returns: Merged options
        """
        worker_options = self.worker.options if self.worker else None
        if options is not None and options.on_error is None:
            options.on_error = worker_options
        elif options is None and worker_options is not None:
            options = Options(on_error=worker_options)

        return options

    def _add_job(
        self,
        task: Union[Callable[..., Any], str],
        options: Optional["Options"],
        *parameters: Any,
        **parameters_keyed: Any,
    ) -> Job:
        """
        Add a job to the queue with all necessary parameters.

        :param task: Either a function or a string with the task name
        :param options: Job-specific options (can be None)
        :param parameters: Positional parameters to pass to the task
        :param parameters_keyed: Keyword parameters to pass to the task
        :returns: The created job
        :raises Exception: If something goes wrong
        """
        try:
            new_job = create_job(task, options, *parameters, **parameters_keyed)
            job = self.db_job.insert_job(new_job)

            return job

        except Exception as e:
            raise Exception(f"Creating or inserting job: {str(e)}")

    def _run_job_initial(self) -> None:
        """
        Called to run the next job in the queue.
        Updates job status to running with worker.
        """
        if not hasattr(self, "worker") or not getattr(self, "worker"):
            logger.debug("No worker available, skipping job run")
            return

        # Check if database connection is available
        if not self.db_job or not self.db_job.db or not self.db_job.db.instance:
            logger.debug("Database connection unavailable, skipping job run")
            return

        worker: Worker = getattr(self, "worker")

        logger.debug(
            f"Worker available_tasks: {worker.available_tasks}, max_concurrency: {worker.max_concurrency}"
        )

        jobs = self.db_job.update_jobs_initial(worker)
        if not jobs:
            logger.debug("No jobs found to run")
            return

        logger.info(f"Found {len(jobs)} jobs to run")

        for job in jobs:
            if (
                job.options
                and job.options.schedule
                and job.options.schedule.start
                and job.options.schedule.start > datetime.now()
            ):
                try:
                    scheduler = Scheduler(
                        self._run_job_sync,
                        job.options.schedule.start,
                        job,
                    )
                    scheduler.go()
                except Exception as e:
                    logger.error(f"Failed to schedule job with Scheduler: {e}")

                logger.info(f"Scheduled job {job.rid} for {job.options.schedule.start}")
            else:
                runner = SmallRunner(self._run_job, job)
                runner.go()

    async def _wait_for_job(
        self, job: Job
    ) -> Tuple[List[Any], bool, Optional[Exception]]:
        """
        Execute the job and return the results or an error.

        :param job: The job to execute
        :returns: A tuple containing the results, a cancellation flag, and an error if any
        """
        task_obj = self.tasks.get(job.task_name)
        if not task_obj:
            return [], False, Exception(f"Task not found: {job.task_name}")

        # Extract the actual callable function from the Task object
        task = task_obj.task if hasattr(task_obj, "task") else task_obj
        if not callable(task):
            return [], False, Exception(f"Task is not callable: {job.task_name}")

        try:
            parameters = getattr(job, "parameters", [])
            parameters_keyed = getattr(job, "parameters_keyed", {})
            runner = Runner(task, *parameters, **parameters_keyed)

            logger.info(f"Created runner for job {job.rid}")

            if not hasattr(self, "active_runners"):
                self.active_runners: Dict[UUID, Runner] = {}
            self.active_runners[job.rid] = runner

            try:
                runner.go()
                results = runner.get_results(timeout=None)
                logger.debug(f"Runner task {job.task_name} completed: {results}")
                return [results], False, None

            except asyncio.CancelledError:
                if runner:
                    runner.cancel()
                return [], True, None
            except Exception as e:
                logger.error(f"Process runner task {job.task_name} failed: {e}")
                return [], False, e
            finally:
                self.active_runners.pop(job.rid, None)

        except Exception as e:
            logger.error(f"Error setting up runner for task {job.task_name}: {e}")
            return [], False, e

    async def _retry_job(self, job: Job, job_error: Exception) -> None:
        """
        Retry the job with the given job error.

        :param job: The job to retry
        :param job_error: The error that occurred during the job execution
        """
        if (
            not job.options
            or not job.options.on_error
            or job.options.on_error.max_retries <= 0
        ):
            await self._fail_job(job, job_error)
            return

        async def retry_function() -> Tuple[List[Job], bool]:
            logger.debug(f"Trying/retrying job: {job.rid}")
            results, cancelled, e = await self._wait_for_job(job)
            if e:
                raise QueuerError("retrying job", e)
            return results, cancelled

        # Create retryer and attempt retry
        retryer = Retryer(retry_function, job.options.on_error)
        error = await retryer.retry()
        if error:
            await self._fail_job(
                job, QueuerError(f"Retrying job failed: {error}", job_error)
            )
        else:
            results, _, _ = await self._wait_for_job(job)
            await self._succeed_job(job, results)

    async def _run_job(self, job: Job) -> None:
        """
        Run the job.

        :param job: The job to run
        """
        logger.info(f"Running job: {job.rid}")

        results, cancelled, error = await self._wait_for_job(job)
        if cancelled:
            return
        elif error:
            await self._retry_job(job, error)
        else:
            await self._succeed_job(job, results)

    def _run_job_sync(self, job: Job) -> None:
        """
        Synchronous wrapper for _run_job that can be called by the Scheduler.
        Mirrors the Go pattern where scheduler calls the function directly.

        :param job: The job to run
        """
        try:
            # Create a Runner process for the async job
            runner = Runner(self._run_job, job)
            runner.go()
            logger.info(f"Scheduled job {job.rid} started in process {runner.pid}")

        except Exception as e:
            logger.error(f"Error executing scheduled job {job.rid}: {e}")

    def _cancel_job(self, job: Job) -> None:
        """
        Cancel a job.

        :param job: The job to cancel
        """
        if job.status in [JobStatus.RUNNING, JobStatus.SCHEDULED, JobStatus.QUEUED]:
            # Cancel the running process if it exists
            if hasattr(self, "active_runners") and job.rid in self.active_runners:
                runner = self.active_runners[job.rid]
                try:
                    runner.cancel()
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

    async def _succeed_job(self, job: Job, results: List[Any]) -> None:
        """
        Update the job status to succeeded and run the next job if available.

        :param job: The job that succeeded
        :param results: The results from the job
        """
        job.status = JobStatus.SUCCEEDED
        job.results = results
        logger.debug(f"_succeed_job: Setting job {job.rid} results to {results}")
        await self._end_job(job)

    async def _fail_job(self, job: Job, job_error: Exception) -> None:
        """
        Update the job status to failed.

        :param job: The job that failed
        :param job_error: The error that occurred
        """
        job.status = JobStatus.FAILED
        job.error = str(job_error)
        await self._end_job(job)

    async def _end_job(self, job: Job) -> None:
        """
        End a job and potentially schedule it again if it's a recurring job.

        :param job: The job to end
        """
        if not self.db_job or not self.db_job.db or not self.db_job.db.instance:
            logger.error(f"Database connection unavailable for job {job.rid}")
            return

        if job.worker_id != self.worker.id:
            return

        try:
            ended_job = self.db_job.update_job_final(job)
            logger.debug(
                f"Job ended: {ended_job.status} - {ended_job.rid}, results: {ended_job.results}"
            )

            if (
                ended_job.options
                and ended_job.scheduled_at
                and ended_job.options.schedule
                and ended_job.options.schedule.interval
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

        # Try to run the next job in the queue, so if one job is finished another can directly start
        try:
            if self.db_job and self.db_job.db and self.db_job.db.instance:
                self._run_job_initial()
        except Exception as e:
            logger.error(f"Error running next job: {e}")
