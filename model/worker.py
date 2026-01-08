"""
Worker model for Python queuer implementation.
Mirrors the Go Worker struct with Python types.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any
from uuid import UUID, uuid4

from .options_on_error import OnError


# Worker status constants to match Go
class WorkerStatus:
    READY = "READY"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"


@dataclass
class Worker:
    """
    Worker represents a worker that can execute tasks.
    Mirrors the Go Worker struct for compatibility.
    """

    # Core identifiers - set automatically
    id: int = 0
    rid: UUID = uuid4()

    # Worker configuration
    name: str = ""
    options: Optional[OnError] = None
    max_concurrency: int = 1
    available_tasks: List[str] = field(default_factory=lambda: [])
    available_next_interval_funcs: List[str] = field(default_factory=lambda: [])

    # Worker state
    status: str = WorkerStatus.READY

    # Timestamps - set automatically
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert worker to dictionary for serialization."""
        return {
            "id": self.id,
            "rid": str(self.rid),
            "name": self.name,
            "options": self.options.to_dict() if self.options else None,
            "max_concurrency": self.max_concurrency,
            "available_tasks": self.available_tasks,
            "available_next_interval_funcs": self.available_next_interval_funcs,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Worker":
        """Create worker from dictionary."""
        worker = cls()
        worker.id = data.get("id", 0)
        if "rid" in data:
            worker.rid = UUID(data["rid"])
        worker.name = data.get("name", "")
        if data.get("options"):
            worker.options = OnError.from_dict(data["options"])
        worker.max_concurrency = data.get("max_concurrency", 1)
        worker.available_tasks = data.get("available_tasks", [])
        worker.available_next_interval_funcs = data.get(
            "available_next_interval_funcs", []
        )
        worker.status = data.get("status", WorkerStatus.READY)
        if data.get("created_at"):
            worker.created_at = datetime.fromisoformat(data["created_at"])
        if data.get("updated_at"):
            worker.updated_at = datetime.fromisoformat(data["updated_at"])
        return worker

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "Worker":
        """Create worker from database row."""
        worker = cls()
        worker.id = row.get("output_id", 0)

        # Handle UUID fields
        if row.get("output_rid"):
            worker.rid = (
                row["output_rid"]
                if isinstance(row["output_rid"], UUID)
                else UUID(row["output_rid"])
            )

        worker.name = row.get("output_name", "")
        worker.max_concurrency = row.get("output_max_concurrency", 1)
        worker.available_tasks = row.get("output_available_tasks", [])
        worker.available_next_interval_funcs = row.get(
            "output_available_next_interval", []
        )
        worker.status = row.get("output_status", WorkerStatus.READY)
        worker.created_at = row.get("output_created_at", datetime.now())
        worker.updated_at = row.get("output_updated_at", datetime.now())

        # Parse options if present
        if row.get("output_options"):
            options_data = (
                json.loads(row["output_options"])
                if isinstance(row["output_options"], str)
                else row["output_options"]
            )
            worker.options = OnError.from_dict(options_data)

        return worker


def new_worker(name: str, max_concurrency: int) -> Worker:
    """
    Create a new worker.
    Mirrors Go's NewWorker function.
    """
    if not name or len(name) > 100:
        raise ValueError("name must have a length between 1 and 100")

    if max_concurrency <= 0:
        raise ValueError("max_concurrency must be greater than 0")

    worker = Worker()
    worker.name = name
    worker.max_concurrency = max_concurrency
    worker.status = WorkerStatus.READY
    return worker


def new_worker_with_options(
    name: str, max_concurrency: int, options: OnError
) -> Worker:
    """
    Create a new worker with options.
    Mirrors Go's NewWorkerWithOptions function.
    """
    worker = new_worker(name, max_concurrency)
    worker.options = options
    return worker
