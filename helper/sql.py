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
    Matches the loadSql.go implementation exactly.
    """

    # Function lists matching Go implementation
    JOB_FUNCTIONS = [
        "update_job_initial",
        "update_job_final",
        "update_job_final_encrypted",
    ]
    WORKER_FUNCTIONS = ["insert_worker", "update_worker", "delete_worker"]
    NOTIFY_FUNCTIONS = ["notify_event"]

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

    def _check_functions(self, connection: Connection, sql_functions: list) -> bool:
        """Check if all SQL functions exist. Matches Go checkFunctions."""
        for func_name in sql_functions:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = %s);",
                    (func_name,),
                )
                exists = cur.fetchone()[0]

                if not exists:
                    return False

        return True

    def load_job_sql(self, connection: Connection, force: bool = False) -> None:
        """Load job-related SQL functions. Matches Go LoadJobSql."""
        # Check if functions already exist (unless force=True)
        if not force:
            if self._check_functions(connection, self.JOB_FUNCTIONS):
                return

        # Load the SQL file
        job_sql_path = os.path.join(self.sql_base_path, "job.sql")
        self._execute_sql_file(connection, job_sql_path)

        # Verify functions were created
        if not self._check_functions(connection, self.JOB_FUNCTIONS):
            raise RuntimeError("Not all required job SQL functions were created")

    def load_worker_sql(self, connection: Connection, force: bool = False) -> None:
        """Load worker-related SQL functions. Matches Go LoadWorkerSql."""
        # Check if functions already exist (unless force=True)
        if not force:
            if self._check_functions(connection, self.WORKER_FUNCTIONS):
                return

        # Load the SQL file
        worker_sql_path = os.path.join(self.sql_base_path, "worker.sql")
        self._execute_sql_file(connection, worker_sql_path)

        # Verify functions were created
        if not self._check_functions(connection, self.WORKER_FUNCTIONS):
            raise RuntimeError("Not all required worker SQL functions were created")

    def load_notify_sql(self, connection: Connection, force: bool = False) -> None:
        """Load notify-related SQL functions. Matches Go LoadNotifySql."""
        # Check if functions already exist (unless force=True)
        if not force:
            if self._check_functions(connection, self.NOTIFY_FUNCTIONS):
                return

        # Load the SQL file
        notify_sql_path = os.path.join(self.sql_base_path, "notify.sql")
        self._execute_sql_file(connection, notify_sql_path)

        # Verify functions were created
        if not self._check_functions(connection, self.NOTIFY_FUNCTIONS):
            raise RuntimeError("Not all required notify SQL functions were created")
