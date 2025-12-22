"""
Job model for Python queuer implementation.
Mirrors the Go Job struct with Python types and async compatibility.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Any, Optional, Callable, Dict, Union
from uuid import UUID, uuid4

from ..helper.task import get_task_name_from_interface
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
    rid: UUID = uuid4()

    # Worker association
    worker_id: int = 0
    worker_rid: Optional[UUID] = None

    # Job definition
    task_name: str = ""
    parameters: List[Any] = field(default_factory=list)
    parameters_keyed: Dict[str, Any] = field(default_factory=dict)
    options: Optional[Options] = None

    # Job state
    status: str = JobStatus.QUEUED
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    schedule_count: int = 0
    attempts: int = 0
    results: List[Any] = field(default_factory=lambda: [])
    error: Optional[str] = None

    # Timestamps - set automatically
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert job to dictionary for serialization."""
        return {
            "id": self.id,
            "rid": str(self.rid),
            "worker_id": self.worker_id,
            "worker_rid": str(self.worker_rid),
            "task_name": self.task_name,
            "parameters": self.parameters,
            "parameters_keyed": self.parameters_keyed,
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

    @staticmethod
    def _parse_datetime(datetime_str: str) -> datetime:
        """Parse datetime string from PostgreSQL format, handling microseconds correctly."""
        try:
            return datetime.fromisoformat(datetime_str)
        except ValueError:
            # Handle PostgreSQL microseconds format (can have 1-6 digits)
            if "." in datetime_str and "T" in datetime_str:
                # Split datetime and microseconds
                date_part, time_part = datetime_str.split("T")
                if "." in time_part:
                    time_base, microseconds = time_part.split(".")
                    # Pad or truncate microseconds to 6 digits
                    microseconds = microseconds.ljust(6, "0")[:6]
                    formatted_str = f"{date_part}T{time_base}.{microseconds}"
                    return datetime.fromisoformat(formatted_str)
            # Fallback: try without microseconds
            return datetime.fromisoformat(datetime_str.split(".")[0])

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Job":
        """Create job from dictionary."""
        job: Job = cls()
        job.id = data.get("id", 0)
        if "rid" in data and data["rid"]:
            job.rid = UUID(data["rid"])
        job.worker_id = data.get("worker_id", 0)
        if "worker_rid" in data and data["worker_rid"]:
            job.worker_rid = UUID(data["worker_rid"])
        job.task_name = data.get("task_name", "")
        job.parameters = data.get("parameters", [])
        job.parameters_keyed = data.get("parameters_keyed", {})
        if data.get("options"):
            job.options = Options.from_dict(data["options"])
        job.status = data.get("status", JobStatus.QUEUED)
        if data.get("scheduled_at"):
            job.scheduled_at = Job._parse_datetime(data["scheduled_at"])
        if data.get("started_at"):
            job.started_at = Job._parse_datetime(data["started_at"])
        job.schedule_count = data.get("schedule_count", 0)
        job.attempts = data.get("attempts", 0)
        job.results = data.get("results", [])
        job.error = data.get("error", "")
        if data.get("created_at"):
            job.created_at = Job._parse_datetime(data["created_at"])
        if data.get("updated_at"):
            job.updated_at = Job._parse_datetime(data["updated_at"])
        return job

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "Job":
        """Create job from database row. Supports both output_* and raw column names."""
        job = cls()
        job.id = row.get("output_id", row.get("id", 0))

        # Handle UUID fields - they may come as UUID objects or strings
        rid_value = row.get("output_rid", row.get("rid"))
        if rid_value:
            job.rid = rid_value if isinstance(rid_value, UUID) else UUID(rid_value)

        job.worker_id = row.get("output_worker_id", row.get("worker_id", 0))

        worker_rid_value = row.get("output_worker_rid", row.get("worker_rid"))
        if worker_rid_value:
            job.worker_rid = (
                worker_rid_value
                if isinstance(worker_rid_value, UUID)
                else UUID(worker_rid_value)
            )

        job.task_name = row.get("output_task_name", row.get("task_name", ""))
        job.status = row.get("output_status", row.get("status", JobStatus.QUEUED))
        job.scheduled_at = row.get("output_scheduled_at", row.get("scheduled_at"))
        job.started_at = row.get("output_started_at", row.get("started_at"))
        job.schedule_count = row.get(
            "output_schedule_count", row.get("schedule_count", 0)
        )
        job.attempts = row.get("output_attempts", row.get("attempts", 0))
        job.error = row.get("output_error", row.get("error", ""))
        job.created_at = row.get(
            "output_created_at", row.get("created_at", datetime.now())
        )
        job.updated_at = row.get(
            "output_updated_at", row.get("updated_at", datetime.now())
        )

        # Parse JSON fields - support both naming conventions
        parameters_value = row.get("output_parameters", row.get("parameters"))
        if parameters_value:
            job.parameters = (
                json.loads(parameters_value)
                if isinstance(parameters_value, str)
                else parameters_value
            )

        parameters_keyed_value = row.get(
            "output_parameters_keyed", row.get("parameters_keyed")
        )
        if parameters_keyed_value:
            job.parameters_keyed = (
                json.loads(parameters_keyed_value)
                if isinstance(parameters_keyed_value, str)
                else parameters_keyed_value
            )

        results_value = row.get("output_results", row.get("results"))
        if results_value is not None:
            # JSONB columns return Python objects directly, no JSON parsing needed
            job.results = results_value

        options_value = row.get("output_options", row.get("options"))
        if options_value:
            options_data = (
                json.loads(options_value)
                if isinstance(options_value, str)
                else options_value
            )
            job.options = Options.from_dict(options_data)

        return job


def new_job(
    task: Union[Callable[..., Any], str],
    options: Optional[Options] = None,
    *parameters: Any,
    **parameters_keyed: Any,
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
    else:
        task_name: str = task

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
    job.parameters_keyed = parameters_keyed
    job.options = options
    job.status = status
    job.scheduled_at = scheduled_at

    return job
