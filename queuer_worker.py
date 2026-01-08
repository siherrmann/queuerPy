"""
Worker-related methods for the Python queuer implementation.
Mirrors Go's queuerWorker.go functionality.
"""

import logging
from typing import Optional
from uuid import UUID

from .helper.error import QueuerError
from .model.worker import Worker, WorkerStatus
from .queuer_global import QueuerGlobalMixin

# Set up logger
logger = logging.getLogger(__name__)


class QueuerWorkerMixin(QueuerGlobalMixin):
    """
    Mixin class containing worker-related methods for the Queuer.
    This mirrors the worker methods from Go's queuerWorker.go.
    """

    def __init__(self):
        super().__init__()

    def stop_worker(self, worker_rid: UUID) -> None:
        """
        StopWorkerGracefully sets the status of the specified worker to 'STOPPED'
        to cancel running jobs when stopping.

        :param worker_rid: The RID of the worker to stop
        :raises QueuerError: If getting or updating the worker fails
        """
        try:
            worker = self.get_worker(worker_rid)
            if worker is None:
                raise ValueError(f"Worker {worker_rid} not found")

            worker.status = WorkerStatus.STOPPED

            worker_updated = self.db_worker.update_worker(worker)
            if not worker_updated:
                raise ValueError(
                    f"Failed to update worker {worker_rid} status to stopped"
                )

            # Update local worker object if this is the current queuer's worker
            if self.worker is not None and self.worker.rid == worker_rid:
                with self.worker_mutex:
                    self.worker = worker_updated

        except Exception as e:
            raise QueuerError("stopping worker", e)

    def stop_worker_gracefully(self, worker_rid: UUID) -> None:
        """
        StopWorkerGracefully sets the worker's status to STOPPING
        to allow it to finish current tasks before stopping.

        :param worker_rid: The RID of the worker to stop gracefully
        :raises QueuerError: If getting or updating the worker fails
        """
        try:
            worker = self.get_worker(worker_rid)
            if worker is None:
                raise ValueError(f"Worker {worker_rid} not found")

            worker.status = WorkerStatus.STOPPING

            worker_updated = self.db_worker.update_worker(worker)
            if not worker_updated:
                raise ValueError(
                    f"Failed to update worker {worker_rid} status to stopping"
                )

            # Update local worker object if this is the current queuer's worker
            if self.worker is not None and self.worker.rid == worker_rid:
                with self.worker_mutex:
                    self.worker = worker_updated

        except Exception as e:
            raise QueuerError("stopping worker gracefully", e)

    def get_worker(self, worker_rid: UUID) -> Optional[Worker]:
        """
        Get a worker by its RID.

        :param worker_rid: The RID of the worker to get
        :returns: The worker if found, None otherwise
        :raises QueuerError: If the database query fails
        """
        try:
            return self.db_worker.select_worker(worker_rid)
        except Exception as e:
            raise QueuerError("getting worker", e)
