"""
Database connection and SQL loading for Python queuer implementation.
Uses helper.database classes and references the same SQL submodule as Go.
"""

import os
import time
import threading
from psycopg import Connection, errors
from pathlib import Path

from helper.logging import get_logger

logger = get_logger(__name__)

# Global lock for DDL operations to prevent concurrent DDL deadlocks
_DDL_LOCK = threading.RLock()


def run_ddl(conn: Connection, sql_statement: str, max_retries: int = 3):
    """
    Executes DDL under a process lock, retrying on deadlocks or serialization errors.

    :param conn: The Psycopg 3 connection object.
    :param sql_statement: The DDL to execute (e.g., DROP FUNCTION, CREATE TABLE).
    """

    with _DDL_LOCK:
        for attempt in range(max_retries):
            try:
                conn.rollback()
                with conn.cursor() as cur:
                    cur.execute(sql_statement)
                conn.commit()
                return
            except (errors.DeadlockDetected, errors.SerializationFailure) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"ddl lock, attempt {attempt + 1}/{max_retries}")
                    time.sleep(0.5)
                else:
                    logger.error(f"ddl failed after {max_retries} retries: {e}")
                    raise
            except Exception as e:
                conn.rollback()
                raise e


class SQLLoader:
    """
    SQL file loader that references the same SQL submodule as Go.
    This class is deprecated - use the sql package directly instead.
    """

    def __init__(self, sql_base_path: str = None):
        """Initialize with path to SQL files (relative to Python project)."""
        if sql_base_path is None:
            current_dir = Path(__file__).parent.parent
            sql_base_path = str(current_dir / "sql")
        self.sql_base_path = sql_base_path

    def _load_sql_file(self, file_path: str) -> str:
        """Load SQL content from file. Internal method."""
        try:
            with open(file_path, "r") as f:
                return f.read()
        except FileNotFoundError:
            raise ValueError(f"SQL file not found: {file_path}")

    def _execute_sql_file(self, connection: Connection, file_path: str) -> None:
        """Execute SQL file content. Internal method."""
        sql_content = self._load_sql_file(file_path)
        run_ddl(connection, sql_content)

    def load_job_sql(
        self, connection: Connection, with_table_drop: bool = False
    ) -> None:
        """Load job-related SQL functions with deadlock protection."""
        job_sql_path = os.path.join(self.sql_base_path, "job.sql")

        if with_table_drop:
            ddl_statements = [
                "DROP FUNCTION IF EXISTS update_job_initial CASCADE;",
                "DROP FUNCTION IF EXISTS update_job_final CASCADE;",
                "DROP FUNCTION IF EXISTS update_job_final_encrypted CASCADE;",
                "DROP FUNCTION IF EXISTS insert_job CASCADE;",
                "DROP FUNCTION IF EXISTS insert_job_encrypted CASCADE;",
            ]

            for statement in ddl_statements:
                run_ddl(connection, statement)

        self._execute_sql_file(connection, job_sql_path)

    def load_worker_sql(
        self, connection: Connection, with_table_drop: bool = False
    ) -> None:
        """Load worker-related SQL functions with deadlock protection."""
        worker_sql_path = os.path.join(self.sql_base_path, "worker.sql")

        if with_table_drop:
            ddl_statements = [
                "DROP FUNCTION IF EXISTS insert_worker CASCADE;",
                "DROP FUNCTION IF EXISTS update_worker CASCADE;",
            ]

            for statement in ddl_statements:
                run_ddl(connection, statement)

        self._execute_sql_file(connection, worker_sql_path)

    def load_notify_sql(
        self, connection: Connection, with_table_drop: bool = False
    ) -> None:
        """Load notification-related SQL functions with deadlock protection."""
        notify_sql_path = os.path.join(self.sql_base_path, "notify.sql")

        if with_table_drop:
            run_ddl(
                connection,
                "DROP FUNCTION IF EXISTS notify_event CASCADE;",
            )

        self._execute_sql_file(connection, notify_sql_path)
