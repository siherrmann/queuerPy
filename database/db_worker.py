"""
Worker database handler for Python queuer implementation.
Mirrors Go's database/dbWorker.go with psycopg3 and references same SQL functions.
"""

import json
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from uuid import UUID
from psycopg.rows import dict_row

from ..helper.database import Database
from ..helper.sql import SQLLoader, run_ddl
from ..model.worker import Worker, WorkerStatus
from ..model.connection import Connection as ConnectionModel


class WorkerDBHandler:
    """
    Worker database handler.
    Mirrors Go's WorkerDBHandler with psycopg3.
    """

    def __init__(self, db_connection: Database, with_table_drop: bool = False):
        """
        Initialize worker database handler.
        Mirrors Go's NewWorkerDBHandler method.
        """
        self.db: Database = db_connection

        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        sql_loader: SQLLoader = SQLLoader()
        sql_loader.load_worker_sql(self.db.instance, force=with_table_drop)

        if with_table_drop:
            self.drop_table()

        self.create_table()

    def check_table_existance(self) -> bool:
        """Check if worker table exists. Mirrors Go's CheckTableExistance method."""
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = current_schema() 
                    AND table_name = 'worker'
                );
            """
            )
            result = cur.fetchone()
            return result[0] if result else False

    def create_table(self) -> None:
        """Create worker table using SQL init function. Mirrors Go's CreateTable method."""
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        run_ddl(self.db.instance, "SELECT init_worker();")

    def drop_table(self) -> None:
        """Drop worker table with DDL deadlock protection. Mirrors Go's DropTable method."""
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        run_ddl(self.db.instance, "DROP TABLE IF EXISTS worker CASCADE;")

    def insert_worker(self, worker: Worker) -> Worker:
        """
        Insert a worker into the database using the SQL function.
        Mirrors Go's InsertWorker method.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT * FROM insert_worker(%s, %s, %s);
            """,
                (
                    worker.name,
                    json.dumps(worker.options.to_dict()) if worker.options else None,
                    worker.max_concurrency,
                ),
            )
            row = cur.fetchone()
            if row:
                self.db.instance.commit()  # Commit the transaction
                return Worker.from_row(row)

            raise RuntimeError("Failed to insert worker")

    def update_worker(self, worker: Worker) -> Optional[Worker]:
        """
        Update a worker in the database.
        Mirrors Go's UpdateWorker method.
        Falls back to direct SQL if update_worker function is not available.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            try:
                # Try using the SQL function first (preferred method)
                cur.execute(
                    """
                    SELECT * FROM update_worker(%s, %s, %s, %s, %s, %s, %s);
                """,
                    (
                        worker.name,
                        (
                            json.dumps(worker.options.to_dict())
                            if worker.options
                            else None
                        ),
                        worker.available_tasks,
                        worker.available_next_interval_funcs,
                        worker.max_concurrency,
                        worker.status,
                        worker.rid,
                    ),
                )
                row = cur.fetchone()
                if row:
                    self.db.instance.commit()  # Commit the transaction
                    return Worker.from_row(row)
            except Exception:
                raise RuntimeError("Failed to update worker using SQL function")

    def update_stale_workers(self, stale_threshold: timedelta) -> int:
        """
        Update stale workers to STOPPED status based on the provided threshold.
        Mirrors Go's UpdateStaleWorkers method exactly.
        Workers are considered stale if they have READY or RUNNING status and their updated_at
        timestamp is older than the threshold.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        cutoff_time = datetime.now(timezone.utc).replace(tzinfo=None) - stale_threshold

        with self.db.instance.cursor() as cur:
            cur.execute(
                "SELECT update_stale_workers(%s, %s, %s, %s);",
                (
                    WorkerStatus.STOPPED,
                    WorkerStatus.READY,
                    WorkerStatus.RUNNING,
                    cutoff_time,
                ),
            )
            result = cur.fetchone()
            if result and result[0] > 0:
                self.db.instance.commit()  # Commit if workers were updated
            return result[0] if result else 0

    def delete_worker(self, rid: UUID) -> None:
        """
        Delete a worker by RID.
        Mirrors Go's DeleteWorker method using SQL function.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor() as cur:
            cur.execute("SELECT delete_worker(%s);", (rid,))
        self.db.instance.commit()

    def select_worker(self, rid: UUID) -> Optional[Worker]:
        """
        Select a worker by RID using SQL function.
        Mirrors Go's SelectWorker method.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT * FROM select_worker(%s);", (rid,))

            row = cur.fetchone()
            return Worker.from_row(row) if row else None

    def select_all_workers(self, last_id: int = 0, entries: int = 100) -> List[Worker]:
        """
        Select all workers with pagination.
        Mirrors Go's SelectAllWorkers method.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT * FROM select_all_workers(%s, %s);", (last_id, entries))

            workers: List[Worker] = []
            for row in cur.fetchall():
                workers.append(Worker.from_row(row))

            return workers

    def select_all_workers_by_search(
        self, search: str, last_id: int = 0, entries: int = 100
    ) -> List[Worker]:
        """
        Select all workers filtered by search string.
        Mirrors Go's SelectAllWorkersBySearch method.
        Searches across 'name', 'available_tasks', and 'status' fields.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT * FROM select_all_workers_by_search(%s, %s, %s);",
                (search, last_id, entries),
            )

            workers: List[Worker] = []
            for row in cur.fetchall():
                workers.append(Worker.from_row(row))

            return workers

    def select_all_connections(self) -> List[ConnectionModel]:
        """
        Select all active connections from the database.
        Mirrors Go's SelectAllConnections method.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT * FROM select_all_connections();")

            connections: List[ConnectionModel] = []
            for row in cur.fetchall():
                connection = ConnectionModel(
                    pid=row["output_pid"],
                    database=row["output_datname"],
                    username=row["output_usename"],
                    application_name=row["output_application_name"],
                    query=row["output_query"],
                    state=row["output_state"],
                )
                connections.append(connection)

            return connections
