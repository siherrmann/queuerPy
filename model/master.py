"""
Master model for Python queuer implementation.
Mirrors the Go Master and MasterSettings structs.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID


@dataclass
class MasterSettings:
    """
    Master settings configuration.
    Mirrors Go's MasterSettings struct.
    """

    retention_archive: int = 30  # Days to retain archived jobs
    master_poll_interval: float = 30.0  # Seconds between master polls
    lock_timeout_minutes: int = 5  # Minutes before master lock expires

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MasterSettings":
        """
        Create MasterSettings instance from dictionary.

        :param data: Dictionary containing settings data
        :return: MasterSettings instance
        """
        return cls(
            retention_archive=data.get("retention_archive", 30),
            master_poll_interval=data.get("master_poll_interval", 30.0),
            lock_timeout_minutes=data.get("lock_timeout_minutes", 5),
        )


@dataclass
class Master:
    """
    Master model representing the master worker in the system.
    Mirrors Go's Master struct.
    """

    id: int = 1
    worker_id: int = 0
    worker_rid: Optional[UUID] = None
    settings: MasterSettings = field(default_factory=MasterSettings)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "Master":
        """
        Create Master instance from database row.
        Supports both output_* prefixed columns (from SQL functions) and raw column names.

        :param row: Database row as dictionary
        :return: Master instance
        """
        master = cls()

        master.id = row.get("output_id", 1)
        master.worker_id = row.get("output_worker_id", 0)

        if row.get("output_worker_rid"):
            master.worker_rid = (
                row["output_worker_rid"]
                if isinstance(row["output_worker_rid"], UUID)
                else UUID(row["output_worker_rid"])
            )

        # Handle settings
        if row.get("output_settings"):
            settings_data = (
                json.loads(row["output_settings"])
                if isinstance(row["output_settings"], str)
                else row["output_settings"]
            )
            master.settings = MasterSettings.from_dict(settings_data)

        # Handle timestamps
        master.created_at = row.get("output_created_at")
        master.updated_at = row.get("output_updated_at")

        return master

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert Master instance to dictionary for JSON serialization.

        :return: Dictionary representation
        """
        return {
            "id": self.id,
            "worker_id": self.worker_id,
            "worker_rid": str(self.worker_rid) if self.worker_rid else None,
            "settings": {
                "retention_archive": self.settings.retention_archive,
                "master_poll_interval": self.settings.master_poll_interval,
                "lock_timeout_minutes": self.settings.lock_timeout_minutes,
            },
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
