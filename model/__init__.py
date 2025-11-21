"""
Model package for queuerPy.

Contains data models and domain objects for the queuer system.
"""

from .job import Job, JobStatus, new_job
from .worker import Worker, WorkerStatus, new_worker, new_worker_with_options
from .task import Task, new_task, new_task_with_name
from .connection import Connection
from .options import Options, Schedule, new_options
from .options_on_error import OnError, RetryBackoff
from .batch_job import BatchJob

__all__ = [
    # Job related
    "Job",
    "JobStatus",
    "new_job",
    # Worker related
    "Worker",
    "WorkerStatus",
    "new_worker",
    "new_worker_with_options",
    # Task related
    "Task",
    "new_task",
    "new_task_with_name",
    # Connection
    "Connection",
    # Options
    "Options",
    "Schedule",
    "new_options",
    "OnError",
    "RetryBackoff",
    # Batch
    "BatchJob",
]
