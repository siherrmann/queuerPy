"""
Job-related methods for the Python queuer implementation.
Mirrors Go's queuerJob.go functionality.
"""

import asyncio
import logging
from typing import List, Optional, Any, Union, Callable, TYPE_CHECKING
from uuid import UUID
import time

from model.job import Job, new_job as create_job

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
    
    def add_job(self, task: Union[Callable, str], *parameters: Any) -> 'Job':
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
        from model.job import new_job as create_job
        
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
        from model.job import new_job as create_job
        
        try:
            # Create new job  
            new_job = create_job(task, options, *parameters)
            job = self.db_job.insert_job(new_job)
            return job
            
        except Exception as e:
            raise Exception(f"Creating or inserting job: {str(e)}")


    def add_jobs(self, batch_jobs: List['BatchJob']) -> None:
        """
        Add a batch of jobs to the queue.
        
        Args:
            batch_jobs: List of BatchJob objects containing task, options, and parameters
            
        Raises:
            Exception: If something goes wrong during the process
        """
        from model.job import Job, new_job as create_job
        from helper.task import get_task_name_from_interface
        
        jobs = []
        for batch_job in batch_jobs:
            options = self._merge_options(batch_job.options)
            task_name = get_task_name_from_interface(batch_job.task)
            job = create_job(task_name, batch_job.parameters, options)
            jobs.append(job)
        
        self.db_job.batch_insert_jobs(jobs)
        logger.info(f"Jobs added: {len(jobs)}")


    async def wait_for_job_added(self) -> Optional['Job']:
        """
        Wait for any job to start and return the job.
        Listens for job insert events and returns the job when it is added to the queue.
        
        Returns:
            The job that was added, or None if cancelled
        """
        # Implementation would depend on having listeners set up
        # This is a simplified version
        try:
            while not self._stopped.is_set():
                await asyncio.sleep(0.1)
                # In a real implementation, this would listen to database events
                # For now, return None to indicate no job detected
            return None
        except asyncio.CancelledError:
            return None


    async def wait_for_job_finished(self, job_rid: UUID) -> Optional['Job']:
        """
        Wait for a job to finish and return the job.
        Listens for job delete events and returns the job when it is finished.
        
        Args:
            job_rid: The RID of the job to wait for
            
        Returns:
            The finished job, or None if cancelled
        """
        try:
            while not self._stopped.is_set():
                await asyncio.sleep(0.5)
                # Check if job is in archive (finished)
                try:
                    job = self.db_job.select_job_from_archive(job_rid)
                    if job:
                        return job
                except:
                    pass  # Job not finished yet
            return None
        except asyncio.CancelledError:
            return None


    def cancel_job(self, job_rid: UUID) -> 'Job':
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


    def readd_job_from_archive(self, job_rid: UUID) -> 'Job':
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


    def get_job(self, job_rid: UUID) -> 'Job':
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


    def get_jobs(self, last_id: int = 0, entries: int = 100) -> List['Job']:
        """
        Retrieve all jobs in the queue.
        
        Args:
            last_id: Last job ID for pagination
            entries: Number of entries to retrieve
            
        Returns:
            List of jobs
        """
        return self.db_job.select_all_jobs(last_id, entries)


    def get_job_ended(self, job_rid: UUID) -> 'Job':
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


    def get_jobs_ended(self, last_id: int = 0, entries: int = 100) -> List['Job']:
        """
        Retrieve all jobs that have ended (succeeded, cancelled or failed).
        
        Args:
            last_id: Last job ID for pagination
            entries: Number of entries to retrieve
            
        Returns:
            List of ended jobs
        """
        return self.db_job.select_all_jobs_from_archive(last_id, entries)


    def get_jobs_by_worker_rid(self, worker_rid: UUID, last_id: int = 0, entries: int = 100) -> List['Job']:
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

    def _merge_options(self, options: Optional['Options']) -> Optional['Options']:
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
            from model.options import Options
            options = Options(on_error=worker_options)
        
        return options


    def _add_job(self, task: Union[Callable, str], options: Optional['Options'], *parameters: Any) -> 'Job':
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
            return
        
        jobs = self.db_job.update_jobs_initial(self.worker)
        if not jobs:
            return
        
        for job in jobs:
            if (job.options and job.options.schedule and 
                job.options.schedule.start and 
                job.options.schedule.start > time.time()):
                # Schedule the job for later
                logger.info(f"Scheduling job: {job.rid} for {job.options.schedule.start}")
                # Use asyncio directly for scheduling since current Scheduler doesn't match Go interface
                async def run_scheduled_job():
                    await asyncio.sleep(job.options.schedule.start - time.time())
                    await self._run_job(job)
                
                schedule_task = asyncio.create_task(run_scheduled_job())
                # Add error handling for the scheduled task
                def handle_schedule_done(task_ref):
                    try:
                        exc = task_ref.exception()
                        if exc is not None:
                            logger.error(f"Scheduled task failed: {exc}")
                    except asyncio.CancelledError:
                        pass
                    except Exception as e:
                        logger.error(f"Error in schedule done callback: {e}")
                
                schedule_task.add_done_callback(handle_schedule_done)
            else:
                # Run the job immediately
                task = asyncio.create_task(self._run_job(job))
                # Add error handling for the task to prevent unawaited coroutine warnings
                def handle_task_done(task_ref):
                    try:
                        exc = task_ref.exception()
                        if exc is not None:
                            logger.error(f"Background job task failed: {exc}")
                    except asyncio.CancelledError:
                        pass
                    except Exception as e:
                        logger.error(f"Error in task done callback: {e}")
                
                task.add_done_callback(handle_task_done)


    async def _wait_for_job(self, job: 'Job') -> tuple[List[Any], bool, Optional[Exception]]:
        """
        Execute the job and return the results or an error.
        
        Args:
            job: The job to execute
            
        Returns:
            Tuple of (results, cancelled, error)
        """
        # Get the task function
        task = self.tasks.get(job.task_name)
        if not task:
            return None, False, Exception(f"Task not found: {job.task_name}")
        
        from core.runner import Runner
        
        runner = Runner.new_runner_from_job(task, job)
        
        # Store the runner for potential cancellation
        if not hasattr(self, 'active_runners'):
            self.active_runners = {}
        self.active_runners[job.rid] = runner
        
        try:
            # Run the task
            results = await runner.run()
            return results, False, None
        except asyncio.CancelledError:
            return None, True, None
        except Exception as e:
            return None, False, e
        finally:
            # Remove runner from active runners
            self.active_runners.pop(job.rid, None)


    async def _retry_job(self, job: 'Job', job_error: Exception) -> None:
        """
        Retry the job with the given job error.
        
        Args:
            job: The job to retry
            job_error: The error that occurred
        """
        if (not job.options or not job.options.on_error or 
            job.options.on_error.max_retries <= 0):
            await self._fail_job(job, job_error)
            return
        
        from core.retryer import Retryer
        
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
            await self._fail_job(job, Exception(f"Retrying job failed: {error}, original error: {job_error}"))
        else:
            # If retry succeeded, we need to get the results
            results, _, _ = await self._wait_for_job(job)
            await self._succeed_job(job, results)


    async def _run_job(self, job: 'Job') -> None:
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


    def _cancel_job(self, job: 'Job') -> None:
        """
        Cancel a job.
        
        Args:
            job: The job to cancel
        """
        from model.job import JobStatus
        
        if job.status in [JobStatus.RUNNING, JobStatus.SCHEDULED, JobStatus.QUEUED]:
            job.status = JobStatus.CANCELLED
            try:
                self.db_job.update_job_final(job)
                logger.info(f"Job cancelled: {job.rid}")
            except Exception as e:
                logger.error(f"Error updating job status to cancelled: {e}")


    async def _succeed_job(self, job: 'Job', results: List[Any]) -> None:
        """
        Update the job status to succeeded and run the next job if available.
        
        Args:
            job: The job that succeeded
            results: The results from the job
        """
        from model.job import JobStatus
        
        job.status = JobStatus.SUCCEEDED
        job.results = results
        await self._end_job(job)


    async def _fail_job(self, job: 'Job', job_error: Exception) -> None:
        """
        Update the job status to failed.
        
        Args:
            job: The job that failed
            job_error: The error that occurred
        """
        from model.job import JobStatus
        
        job.status = JobStatus.FAILED
        job.error = str(job_error)
        await self._end_job(job)


    async def _end_job(self, job: 'Job') -> None:
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
            if (ended_job.options and ended_job.options.schedule and 
                ended_job.schedule_count < ended_job.options.schedule.max_count):
                
                import time
                
                if ended_job.options.schedule.next_interval:
                    # Use next interval function if available
                    next_interval_func = self.next_interval_funcs.get(ended_job.options.schedule.next_interval)
                    if not next_interval_func:
                        logger.error(f"NextIntervalFunc not found: {ended_job.options.schedule.next_interval} for job {ended_job.rid}")
                        return
                    new_scheduled_at = next_interval_func(ended_job.scheduled_at, ended_job.schedule_count)
                else:
                    # Use regular interval
                    new_scheduled_at = ended_job.scheduled_at + (ended_job.schedule_count * ended_job.options.schedule.interval)
                
                from model.job import JobStatus
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