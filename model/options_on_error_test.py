"""
Test cases for the OnError model.

These tests mirror the Go model tests.
"""

from typing import Any, Dict
import pytest
import json

from .options_on_error import OnError, RetryBackoff


class TestOnError:
    """Test cases for OnError options."""

    def test_default_options(self):
        """Test default OnError options."""
        opts = OnError()
        assert opts.timeout == 30.0
        assert opts.max_retries == 3
        assert opts.retry_delay == 1.0
        assert opts.retry_backoff == RetryBackoff.NONE
        assert opts.is_valid()

    def test_custom_options(self):
        """Test custom OnError options."""
        opts = OnError(
            timeout=60.0,
            max_retries=5,
            retry_delay=2.0,
            retry_backoff=RetryBackoff.EXPONENTIAL,
        )
        assert opts.timeout == 60.0
        assert opts.max_retries == 5
        assert opts.retry_delay == 2.0
        assert opts.retry_backoff == RetryBackoff.EXPONENTIAL
        assert opts.is_valid()

    def test_validation_negative_timeout(self):
        """Test validation fails for negative timeout."""
        with pytest.raises(ValueError, match="timeout cannot be negative"):
            OnError(timeout=-1.0)

    def test_validation_negative_max_retries(self):
        """Test validation fails for negative max retries."""
        with pytest.raises(ValueError, match="max retries cannot be negative"):
            OnError(max_retries=-1)

    def test_validation_negative_retry_delay(self):
        """Test validation fails for negative retry delay."""
        with pytest.raises(ValueError, match="retry delay cannot be negative"):
            OnError(retry_delay=-1.0)

    def test_validation_invalid_backoff(self):
        """Test validation fails for invalid backoff strategy."""
        with pytest.raises(ValueError, match="invalid retry backoff"):
            OnError(retry_backoff="invalid")

    def test_is_valid_returns_false_for_invalid(self):
        """Test is_valid returns False for invalid options."""
        # Create valid options first
        opts = OnError()
        # Manually set invalid value to bypass constructor validation
        opts.timeout = -1.0
        assert not opts.is_valid()

    def test_to_dict(self):
        """Test converting OnError to dictionary."""
        opts = OnError(
            timeout=45.0,
            max_retries=2,
            retry_delay=1.5,
            retry_backoff=RetryBackoff.LINEAR,
        )
        expected: Dict[str, Any] = {
            "timeout": 45.0,
            "max_retries": 2,
            "retry_delay": 1.5,
            "retry_backoff": "linear",
        }
        assert opts.to_dict() == expected

    def test_from_dict(self):
        """Test creating OnError from dictionary."""
        data: Dict[str, Any] = {
            "timeout": 45.0,
            "max_retries": 2,
            "retry_delay": 1.5,
            "retry_backoff": "linear",
        }
        opts = OnError.from_dict(data)
        assert opts.timeout == 45.0
        assert opts.max_retries == 2
        assert opts.retry_delay == 1.5
        assert opts.retry_backoff == RetryBackoff.LINEAR

    def test_from_dict_with_defaults(self):
        """Test creating OnError from dictionary with missing values."""
        data = {"timeout": 60.0}
        opts = OnError.from_dict(data)
        assert opts.timeout == 60.0
        assert opts.max_retries == 3  # default
        assert opts.retry_delay == 1.0  # default
        assert opts.retry_backoff == RetryBackoff.NONE  # default

    def test_to_json(self):
        """Test converting OnError to JSON."""
        opts = OnError(
            timeout=30.0,
            max_retries=3,
            retry_delay=1.0,
            retry_backoff=RetryBackoff.EXPONENTIAL,
        )
        json_str = opts.to_json()
        data = json.loads(json_str)
        expected: Dict[str, Any] = {
            "timeout": 30.0,
            "max_retries": 3,
            "retry_delay": 1.0,
            "retry_backoff": "exponential",
        }
        assert data == expected

    def test_from_json(self):
        """Test creating OnError from JSON."""
        json_str = '{"timeout": 60.0, "max_retries": 5, "retry_delay": 2.0, "retry_backoff": "linear"}'
        opts = OnError.from_json(json_str)
        assert opts.timeout == 60.0
        assert opts.max_retries == 5
        assert opts.retry_delay == 2.0
        assert opts.retry_backoff == RetryBackoff.LINEAR

    def test_retry_backoff_enum(self):
        """Test RetryBackoff enum values."""
        assert RetryBackoff.NONE == "none"
        assert RetryBackoff.LINEAR == "linear"
        assert RetryBackoff.EXPONENTIAL == "exponential"
