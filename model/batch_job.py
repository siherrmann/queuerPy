"""
BatchJob model for Python queuer implementation.
Mirrors Go's model.BatchJob struct.
"""

from typing import Any, List, Optional, Union, Callable, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    from model.options import Options


@dataclass
class BatchJob:
    """
    Represents a batch job with task, parameters, and options.
    """
    task: Union[Callable, str]
    parameters: List[Any]
    options: Optional['Options'] = None

    def __post_init__(self):
        """Validate the batch job after initialization."""
        if self.parameters is None:
            self.parameters = []