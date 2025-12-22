"""
BatchJob model for Python queuer implementation.
Mirrors Go's model.BatchJob struct.
"""

from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass, field

from .options import Options


@dataclass
class BatchJob:
    """
    Represents a batch job with task, parameters, and options.
    """

    task: Union[Callable[..., Any], str]
    parameters: List[Any] = field(default_factory=list)
    parameters_keyed: Dict[str, Any] = field(default_factory=dict)
    options: Optional[Options] = None
