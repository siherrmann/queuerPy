"""
Model definitions for the goless-based queuer implementation.

This module contains data structures and enums that mirror the Go model package.
"""

from enum import Enum
from typing import Dict, Any
import json


class RetryBackoff(str, Enum):
    """Retry backoff strategies."""

    NONE = "none"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"


class OnError:
    """Options for handling errors during job execution.

    This mirrors the Go OnError struct with validation.
    """

    def __init__(
        self,
        timeout: float = 30.0,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        retry_backoff: str = RetryBackoff.NONE,
    ):
        """Initialize OnError options.

        Args:
            timeout: Maximum time in seconds to wait for completion
            max_retries: Maximum number of retry attempts
            retry_delay: Delay in seconds before retrying
            retry_backoff: Backoff strategy (none, linear, exponential)
        """
        # Validate values
        if timeout < 0:
            raise ValueError("timeout cannot be negative")
        if max_retries < 0:
            raise ValueError("max retries cannot be negative")
        if retry_delay < 0:
            raise ValueError("retry delay cannot be negative")
        if retry_backoff not in [
            RetryBackoff.NONE,
            RetryBackoff.LINEAR,
            RetryBackoff.EXPONENTIAL,
        ]:
            raise ValueError("invalid retry backoff")

        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry_backoff = retry_backoff

    def is_valid(self) -> bool:
        """
        Check if the OnError options are valid.
        Mirrors Go's IsValid() method behavior.
        """
        if self.timeout < 0:
            return False
        if self.max_retries < 0:
            return False
        if self.retry_delay < 0:
            return False
        if self.retry_backoff not in [
            RetryBackoff.NONE,
            RetryBackoff.LINEAR,
            RetryBackoff.EXPONENTIAL,
        ]:
            return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "retry_delay": self.retry_delay,
            "retry_backoff": self.retry_backoff,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OnError":
        """Create OnError from dictionary."""
        return cls(
            timeout=data.get("timeout", 30.0),
            max_retries=data.get("max_retries", 3),
            retry_delay=data.get("retry_delay", 1.0),
            retry_backoff=data.get("retry_backoff", RetryBackoff.NONE),
        )

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> "OnError":
        """Create OnError from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)


# Constants for retry backoff strategies (matching Go constants)
RETRY_BACKOFF_NONE = RetryBackoff.NONE
RETRY_BACKOFF_LINEAR = RetryBackoff.LINEAR
RETRY_BACKOFF_EXPONENTIAL = RetryBackoff.EXPONENTIAL
