"""
Worker database handler for Python queuer implementation.
Mirrors Go's database/dbWorker.go with psycopg3 and references same SQL functions.
"""

import json
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from uuid import UUID
import psycopg
from psycopg import Connection
from psycopg.rows import dict_row

from helper.database import Database
from helper.sql import SQLLoader
from model.worker import Worker, WorkerStatus


class WorkerDBHandler:
    """
    Worker database handler.
    Mirrors Go's WorkerDBHandler with psycopg3.
    """

    def __init__(self, db_connection: Database, with_table_drop: bool = False):
        """Initialize worker database handler."""
        if db_connection is None:
            raise ValueError("database connection is None")

        self.db: Database = db_connection

        # Load SQL functions using helper.sql
        connection: Connection = self.db.instance
        sql_loader: SQLLoader = SQLLoader()
        sql_loader.load_notify_sql(
            connection, with_table_drop
        )  # Load notify function first
        sql_loader.load_worker_sql(connection, with_table_drop)

        # Create table if it doesn't exist
        if not self.check_table_existence():
            self.create_table()

    def check_table_existence(self) -> bool:
        """Check if worker table exists."""
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
        """Create worker table using SQL init function."""
        with self.db.instance.cursor() as cur:
            cur.execute("SELECT init_worker();")
        self.db.instance.commit()

    def drop_tables(self) -> None:
        """Drop worker tables."""
        with self.db.instance.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS worker CASCADE;")
        self.db.instance.commit()

    def insert_worker(self, worker: Worker) -> Worker:
        """
        Insert a worker into the database.
        Mirrors Go's InsertWorker method using SQL function.
        """
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
                return self._row_to_worker(row)
            else:
                raise RuntimeError("Failed to insert worker")

    def update_worker(self, worker: Worker) -> Worker:
        """
        Update a worker in the database.
        Mirrors Go's UpdateWorker method using SQL function.
        """
        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT * FROM update_worker(%s, %s, %s, %s, %s, %s, %s);
            """,
                (
                    worker.name,
                    json.dumps(worker.options.to_dict()) if worker.options else None,
                    worker.available_tasks,
                    worker.available_next_interval_funcs,
                    worker.max_concurrency,
                    worker.status,
                    worker.rid,
                ),
            )

            row = cur.fetchone()
            if row:
                return self._row_to_worker(row)
            else:
                raise RuntimeError("Failed to update worker")

    def update_stale_workers(
        self, stale_threshold: timedelta = timedelta(minutes=5)
    ) -> int:
        """
        Update stale workers (workers that haven't sent heartbeat recently).
        Mirrors Go's UpdateStaleWorkers method.
        """
        threshold_time = datetime.now() - stale_threshold

        with self.db.instance.cursor() as cur:
            cur.execute(
                """
                UPDATE worker 
                SET status = 'STOPPED', 
                    updated_at = CURRENT_TIMESTAMP
                WHERE status IN ('RUNNING', 'READY') 
                AND updated_at < %s;
            """,
                (threshold_time,),
            )
            return cur.rowcount

    def delete_worker(self, rid: UUID) -> int:
        """Delete a worker by RID."""
        with self.db.instance.cursor() as cur:
            cur.execute(
                """
                DELETE FROM worker WHERE rid = %s;
            """,
                (rid,),
            )
            return cur.rowcount

    def _row_to_worker(self, row: Dict[str, Any]) -> Worker:
        """Convert database row to Worker object."""
        worker = Worker()
        worker.id = row.get("output_id", 0)

        # Handle UUID fields
        if row.get("output_rid"):
            worker.rid = (
                row["output_rid"]
                if isinstance(row["output_rid"], UUID)
                else UUID(row["output_rid"])
            )

        worker.name = row.get("output_name", "")
        worker.max_concurrency = row.get("output_max_concurrency", 1)
        worker.available_tasks = row.get("output_available_tasks", []) or []
        worker.available_next_interval_funcs = (
            row.get("output_available_next_interval", []) or []
        )
        worker.status = row.get("output_status", WorkerStatus.READY)
        worker.created_at = row.get("output_created_at", datetime.now())
        worker.updated_at = row.get("output_updated_at", datetime.now())

        # Parse options if present
        if row.get("output_options"):
            from model.options_on_error import OnError

            options_data = (
                json.loads(row["output_options"])
                if isinstance(row["output_options"], str)
                else row["output_options"]
            )
            worker.options = OnError.from_dict(options_data)

        return worker
