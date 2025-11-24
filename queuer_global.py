import threading
from typing import Any, Callable, Dict, Optional

from .core.broadcaster import new_broadcaster
from .core.listener import Listener
from .database.db_job import JobDBHandler
from .database.db_master import MasterDBHandler
from .database.db_worker import WorkerDBHandler
from .helper.database import Database, DatabaseConfiguration, new_database
from .helper.logging import get_logger
from .model.job import Job
from .model.task import Task
from .model.worker import Worker

logger = get_logger()


class QueuerGlobalMixin:
    def __init__(self):
        self.running: bool = False

        # Worker mutex for thread-safe access
        self.worker_mutex: threading.RLock = threading.RLock()

        # DB Handlers
        self.db_job: JobDBHandler
        self.db_worker: WorkerDBHandler
        self.db_master: MasterDBHandler

        # Broadcaster
        self.job_insert_broadcaster = new_broadcaster("job.INSERT")
        self.job_update_broadcaster = new_broadcaster("job.UPDATE")
        self.job_delete_broadcaster = new_broadcaster("job.DELETE")

        # Listener
        self.job_insert_listener = Listener[Job](self.job_insert_broadcaster)
        self.job_update_listener = Listener[Job](self.job_update_broadcaster)
        self.job_delete_listener = Listener[Job](self.job_delete_broadcaster)

        # Ticker
        self.heartbeat_ticker: Optional[Any] = None
        self.poll_job_ticker: Optional[Any] = None

        self.worker: Worker
        self.tasks: Dict[str, Task] = {}
        self.next_interval_funcs: Dict[str, Callable[..., Any]] = {}

    def initialise(
        self,
        db_config: DatabaseConfiguration,
        encryption_key: str = "",
    ):
        # Database connection
        self.database: Database = new_database("queuer", db_config, logger)
        self.DB = self.database.instance

        # Database handlers
        self.db_job: JobDBHandler = JobDBHandler(
            self.database, db_config.with_table_drop, encryption_key
        )
        self.db_worker: WorkerDBHandler = WorkerDBHandler(
            self.database, db_config.with_table_drop
        )
        self.db_master: MasterDBHandler = MasterDBHandler(
            self.database, db_config.with_table_drop
        )
