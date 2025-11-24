"""
Error handling utilities for Python queuer implementation.
Mirrors Go's helper/error.go with Python exception handling.
"""

import inspect
from types import FrameType
from typing import Optional


class QueuerError(Exception):
    """
    Enhanced error class with trace information.
    Mirrors Go's Error struct for better error tracking.
    """

    def __init__(self, trace: str, original: Exception):
        """Initialize QueuerError with original error and trace."""
        traceWithFunction = trace
        current_frame = inspect.currentframe()

        frame: Optional[FrameType] = None
        if current_frame is not None:
            frame = current_frame.f_back

        if frame:
            traceWithFunction = f"{frame.f_code.co_name} - {trace}"

        if isinstance(original, QueuerError):
            self.original = original.original
            self.trace = original.trace + [traceWithFunction]
        else:
            self.original = original
            self.trace = [traceWithFunction]

        super().__init__(str(self.original))

    def __str__(self) -> str:
        """Return formatted error message with trace."""
        return f"{str(self.original)} | Trace: {', '.join(self.trace)}"
