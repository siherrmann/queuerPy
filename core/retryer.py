"""
Retryer component for the queuer system.

This module provides retry functionality mirroring the Go implementation
for reliable task execution with various backoff strategies.
"""

import logging
import asyncio
import inspect
from typing import Any, Callable, Optional

from ..model.options_on_error import OnError, RetryBackoff


# Configure logging
logger = logging.getLogger(__name__)


class Retryer:
    """Simple retryer mirroring Go's Retryer implementation."""

    def __init__(self, function: Callable[..., Any], options: Optional[OnError]):
        """Initialize the retryer.

        :param function: The function to execute with retries (can be sync or async)
        :param options: OnError options for retry behavior
        :raises ValueError: If options are invalid
        """
        if options is None or options.max_retries <= 0 or options.retry_delay < 0:
            raise ValueError("No valid retry options provided")

        self.function = function
        self.options = options
        self.sleep_duration = options.retry_delay
        self.is_async = inspect.iscoroutinefunction(function)

    async def retry(self) -> Optional[Exception]:
        """Attempt to execute the function up to MaxRetries times.

        The retry behavior is determined by the RetryBackoff option.
        If the function raises an exception, it will retry according to the specified backoff strategy.
        If all retries fail, it returns the last exception encountered.

        The backoff strategies are:
        - RETRY_BACKOFF_NONE: No backoff, retries immediately.
        - RETRY_BACKOFF_LINEAR: Increases the sleep duration linearly by the initial delay.
        - RETRY_BACKOFF_EXPONENTIAL: Doubles the sleep duration after each retry.

        :returns: The last exception if all retries fail, otherwise None on success.
        """
        last_error: Optional[Exception] = None

        for i in range(self.options.max_retries):
            try:
                if self.is_async:
                    await self.function()
                else:
                    self.function()
                return None  # Success
            except Exception as err:
                last_error = err
                logger.warning(
                    f"Retry attempt {i + 1}/{self.options.max_retries} failed: {err}"
                )

                # Sleep between retries (except for the last attempt)
                if i < self.options.max_retries - 1:
                    await asyncio.sleep(self.sleep_duration)

                    # Apply backoff strategy
                    if self.options.retry_backoff == RetryBackoff.NONE:
                        continue
                    elif self.options.retry_backoff == RetryBackoff.LINEAR:
                        self.sleep_duration += self.options.retry_delay
                        continue
                    elif self.options.retry_backoff == RetryBackoff.EXPONENTIAL:
                        self.sleep_duration *= 2
                        continue

        return last_error
