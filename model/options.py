"""
Options model for Python queuer implementation.
Mirrors the Go Options struct with Python types.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from .options_on_error import OnError


@dataclass
class Schedule:
    """
    Schedule options for job execution.
    Mirrors the Go Schedule struct.
    """

    start: Optional[datetime] = None
    max_count: int = 1
    interval: Optional[timedelta] = None
    next_interval: Optional[str] = None

    def is_valid(self) -> bool:
        """
        Validate the schedule options.

        :return: True if valid, False otherwise.
        """
        if self.max_count < 0:
            return False
        if (
            self.max_count > 1
            and (self.interval is None or self.interval.total_seconds() <= 0)
            and not self.next_interval
        ):
            return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for JSON serialization.

        :return: Dictionary representation of the Schedule.
        """
        return {
            "start": self.start.isoformat() if self.start else None,
            "max_count": self.max_count,
            "interval": self.interval.total_seconds() if self.interval else None,
            "next_interval": self.next_interval,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Schedule":
        """
        Create Schedule from dictionary.

        :param data: Dictionary containing schedule data.
        :return: Schedule instance.
        """
        schedule = cls()
        if data.get("start"):
            schedule.start = datetime.fromisoformat(data["start"])
        schedule.max_count = data.get("max_count", 1)
        if data.get("interval"):
            schedule.interval = timedelta(seconds=data["interval"])
        schedule.next_interval = data.get("next_interval")
        return schedule


@dataclass
class Options:
    """
    Options for job execution.
    Mirrors the Go Options struct for compatibility.
    """

    on_error: Optional[OnError] = None
    schedule: Optional[Schedule] = None

    def is_valid(self) -> bool:
        """
        Validate the options.

        :return: True if valid, False otherwise.
        """
        if self.on_error is not None and not self.on_error.is_valid():
            return False
        if self.schedule is not None and not self.schedule.is_valid():
            return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert options to dictionary for serialization.

        :return: Dictionary representation of the Options.
        """
        return {
            "on_error": self.on_error.to_dict() if self.on_error else None,
            "schedule": self.schedule.to_dict() if self.schedule else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Options":
        """
        Create options from dictionary.

        :param data: Dictionary containing options data.
        :return: Options instance.
        """
        options = cls()
        if data.get("on_error"):
            options.on_error = OnError.from_dict(data["on_error"])
        if data.get("schedule"):
            options.schedule = Schedule.from_dict(data["schedule"])
        return options


def new_options(
    on_error: Optional[OnError] = None, schedule: Optional[Schedule] = None
) -> Options:
    """
    Create new options. Mirrors Go's pattern.

    :param on_error: OnError options.
    :param schedule: Schedule options.
    :return: Options instance.
    """
    return Options(on_error=on_error, schedule=schedule)
