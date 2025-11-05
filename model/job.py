"""
Job model for Python queuer implementation.
Mirrors the Go Job struct with Python types and async compatibility.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Any, Optional, Callable, Dict, Union
from uuid import UUID, uuid4

from helper.task import get_task_name_from_interface
from .options import Options


# Job status constants to match Go
class JobStatus:
    QUEUED = "QUEUED"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    SUCCEEDED = "SUCCEEDED"
    CANCELLED = "CANCELLED"


@dataclass
class Job:
    """
    Job represents a job in the queuing system.
    Mirrors the Go Job struct for compatibility.
    """

    # Core identifiers - set automatically
    id: int = 0
    rid: UUID = field(default_factory=uuid4)

    # Worker association
    worker_id: int = 0
    worker_rid: Optional[UUID] = None

    # Job definition
    task_name: str = ""
    parameters: List[Any] = field(default_factory=list)
    options: Optional[Options] = None

    # Job state
    status: str = JobStatus.QUEUED
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    schedule_count: int = 0
    attempts: int = 0
    results: List[Any] = field(default_factory=list)
    error: Optional[str] = None

    # Timestamps - set automatically
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict:
        """Convert job to dictionary for serialization."""
        return {
            "id": self.id,
            "rid": str(self.rid),
            "worker_id": self.worker_id,
            "worker_rid": str(self.worker_rid),
            "task_name": self.task_name,
            "parameters": self.parameters,
            "options": self.options.to_dict() if self.options else None,
            "status": self.status,
            "scheduled_at": (
                self.scheduled_at.isoformat() if self.scheduled_at else None
            ),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "schedule_count": self.schedule_count,
            "attempts": self.attempts,
            "results": self.results,
            "error": self.error,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Job":
        """Create job from dictionary."""
        job: Job = cls()
        job.id = data.get("id", 0)
        if "rid" in data:
            job.rid = UUID(data["rid"])
        job.worker_id = data.get("worker_id", 0)
        if "worker_rid" in data:
            job.worker_rid = UUID(data["worker_rid"])
        job.task_name = data.get("task_name", "")
        job.parameters = data.get("parameters", [])
        if data.get("options"):
            job.options = Options.from_dict(data["options"])
        job.status = data.get("status", JobStatus.QUEUED)
        if data.get("scheduled_at"):
            job.scheduled_at = datetime.fromisoformat(data["scheduled_at"])
        if data.get("started_at"):
            job.started_at = datetime.fromisoformat(data["started_at"])
        job.schedule_count = data.get("schedule_count", 0)
        job.attempts = data.get("attempts", 0)
        job.results = data.get("results", [])
        job.error = data.get("error", "")
        if data.get("created_at"):
            job.created_at = datetime.fromisoformat(data["created_at"])
        if data.get("updated_at"):
            job.updated_at = datetime.fromisoformat(data["updated_at"])
        return job

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "Job":
        """Create job from database row."""
        job = cls()
        job.id = row.get("id", 0)

        # Handle UUID fields - they may come as UUID objects or strings
        if row.get("rid"):
            job.rid = row["rid"] if isinstance(row["rid"], UUID) else UUID(row["rid"])

        job.worker_id = row.get("worker_id", 0)

        if row.get("worker_rid"):
            job.worker_rid = (
                row["worker_rid"]
                if isinstance(row["worker_rid"], UUID)
                else UUID(row["worker_rid"])
            )

        job.task_name = row.get("task_name", "")
        job.status = row.get("status", JobStatus.QUEUED)
        job.scheduled_at = row.get("scheduled_at")
        job.started_at = row.get("started_at")
        job.schedule_count = row.get("schedule_count", 0)
        job.attempts = row.get("attempts", 0)
        job.error = row.get("error", "")
        job.created_at = row.get("created_at", datetime.now())
        job.updated_at = row.get("updated_at", datetime.now())

        # Parse JSON fields
        if row.get("parameters"):
            job.parameters = (
                json.loads(row["parameters"])
                if isinstance(row["parameters"], str)
                else row["parameters"]
            )

        if row.get("results") is not None:
            job.results = (
                json.loads(row["results"])
                if isinstance(row["results"], str)
                else row["results"]
            )

        if row.get("options"):
            options_data = (
                json.loads(row["options"])
                if isinstance(row["options"], str)
                else row["options"]
            )
            job.options = Options.from_dict(options_data)

        return job


def new_job(
    task: Union[Callable, str], options: Optional[Options] = None, *parameters
) -> Job:
    """
    Create a new job from a task function or task name.
    Mirrors Go's NewJob function.

    Args:
        task: The task function to execute or task name string
        options: Optional job options (OnError, Schedule, etc.)
        *parameters: Variable number of parameters to pass to the task

    Returns:
        Job: New job instance

    Raises:
        ValueError: If task is invalid or task name is too long
        TypeError: If task is not callable and not a string
    """
    # Handle both callable tasks and string task names
    if callable(task):
        task_name: str = get_task_name_from_interface(task)
    elif isinstance(task, str):
        task_name: str = task
    else:
        raise TypeError("task must be callable or a string task name")

    if not task_name or len(task_name) > 100:
        raise ValueError("task name must have a length between 1 and 100")

    # Validate options if provided
    if options is not None:
        if options.on_error is not None and not options.on_error.is_valid():
            raise ValueError("OnError options are invalid")
        if options.schedule is not None and not options.schedule.is_valid():
            raise ValueError("Schedule options are invalid")

    # Determine initial status and scheduled time
    status: str = JobStatus.QUEUED
    scheduled_at: Optional[datetime] = None
    if options is not None and options.schedule is not None:
        status = JobStatus.SCHEDULED
        scheduled_at = options.schedule.start

    job: Job = Job()
    job.task_name = task_name
    job.parameters = list(parameters)
    job.options = options
    job.status = status
    job.scheduled_at = scheduled_at

    return job
