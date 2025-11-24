"""
Database connection and SQL loading for Python queuer implementation.
Uses helper.database classes and references the same SQL submodule as Go.
"""

import os
import time
import threading
from typing import List, Optional
from psycopg import Connection, errors
from pathlib import Path

from .logging import get_logger

logger = get_logger(__name__)

# Global lock for DDL operations to prevent concurrent DDL deadlocks
_DDL_LOCK = threading.RLock()


def run_ddl(conn: Connection, sql_statement: str, max_retries: int = 3) -> None:
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
                    cur.execute(sql_statement.encode("utf-8"))
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

    JOB_FUNCTIONS: List[str] = [
        "init_job",
        "insert_job",
        "update_job_initial",
        "update_job_final",
        "update_job_final_encrypted",
        "update_stale_jobs",
        "delete_job",
        "select_job",
        "select_all_jobs",
        "select_all_jobs_by_worker_rid",
        "select_all_jobs_by_search",
        "select_job_from_archive",
        "select_all_jobs_from_archive",
        "select_all_jobs_from_archive_by_search",
    ]
    WORKER_FUNCTIONS: List[str] = [
        "init_worker",
        "insert_worker",
        "update_worker",
        "delete_worker",
        "update_stale_workers",
        "select_worker",
        "select_all_workers",
        "select_all_workers_by_search",
        "select_all_connections",
    ]
    MASTER_FUNCTIONS: List[str] = [
        "init_master",
        "update_master",
        "select_master",
    ]
    NOTIFY_FUNCTIONS: List[str] = ["notify_event"]

    def __init__(self, sql_base_path: Optional[str] = None):
        """
        Initialize with path to SQL files (relative to Python project).

        :param sql_base_path: Base path to SQL files. If None, defaults to ./sql directory.
        """
        if sql_base_path is None:
            current_dir = Path(__file__).parent.parent
            sql_base_path = str(current_dir / "sql")
        self.sql_base_path = sql_base_path

    def load_sql_file(self, file_path: str) -> str:
        """
        Load SQL content from file. Internal method.

        :param file_path: Path to the SQL file.
        :returns: The content of the SQL file as a string.
        :raises ValueError: If the file does not exist.
        """
        try:
            with open(file_path, "r") as f:
                return f.read()
        except FileNotFoundError:
            raise ValueError(f"SQL file not found: {file_path}")

    def execute_sql_file(self, connection: Connection, file_path: str) -> None:
        """
        Execute SQL file content. Internal method.

        :param connection: The Psycopg 3 connection object.
        :param file_path: Path to the SQL file.
        :raises ValueError: If the file does not exist.
        """
        sql_content: str = self.load_sql_file(file_path)
        run_ddl(connection, sql_content)

    def check_functions(self, connection: Connection, sql_functions: List[str]) -> bool:
        """
        Check if all SQL functions exist. Matches Go checkFunctions.

        :param connection: The Psycopg 3 connection object.
        :param sql_functions: List of SQL function names to check.
        :returns: True if all functions exist, False otherwise.
        """
        for func_name in sql_functions:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = %s);",
                    (func_name,),
                )
                one = cur.fetchone()
                if one is None or not one[0]:
                    return False
        # All functions exist
        return True

    def load_job_sql(self, connection: Connection, force: bool = False) -> None:
        """
        Load job-related SQL functions. Matches Go LoadJobSql.

        :param connection: The Psycopg 3 connection object.
        :param force: If True, forces reloading even if functions exist.
        :raises RuntimeError: If not all required job SQL functions were created.
        """
        if not force:
            if self.check_functions(connection, self.JOB_FUNCTIONS):
                return

        # Load the SQL file
        job_sql_path = os.path.join(self.sql_base_path, "job.sql")
        self.execute_sql_file(connection, job_sql_path)

        # Verify functions were created
        if not self.check_functions(connection, self.JOB_FUNCTIONS):
            raise RuntimeError("Not all required job SQL functions were created")

    def load_worker_sql(self, connection: Connection, force: bool = False) -> None:
        """
        Load worker-related SQL functions. Matches Go LoadWorkerSql.

        :param connection: The Psycopg 3 connection object.
        :param force: If True, forces reloading even if functions exist.
        :raises RuntimeError: If not all required worker SQL functions were created.
        """
        if not force:
            if self.check_functions(connection, self.WORKER_FUNCTIONS):
                return

        # Load the SQL file
        worker_sql_path = os.path.join(self.sql_base_path, "worker.sql")
        self.execute_sql_file(connection, worker_sql_path)

        # Verify functions were created
        if not self.check_functions(connection, self.WORKER_FUNCTIONS):
            raise RuntimeError("Not all required worker SQL functions were created")

    def load_notify_sql(self, connection: Connection, force: bool = False) -> None:
        """
        Load notify-related SQL functions. Matches Go LoadNotifySql.

        :param connection: The Psycopg 3 connection object.
        :param force: If True, forces reloading even if functions exist.
        :raises RuntimeError: If not all required notify SQL functions were created.
        """
        # Check if functions already exist (unless force=True)
        if not force:
            if self.check_functions(connection, self.NOTIFY_FUNCTIONS):
                return

        # Load the SQL file
        notify_sql_path = os.path.join(self.sql_base_path, "notify.sql")
        self.execute_sql_file(connection, notify_sql_path)

        # Verify functions were created
        if not self.check_functions(connection, self.NOTIFY_FUNCTIONS):
            raise RuntimeError("Not all required notify SQL functions were created")

    def load_master_sql(self, connection: Connection, force: bool = False) -> None:
        """
        Load master-related SQL functions. Matches Go LoadMasterSql.

        :param connection: The Psycopg 3 connection object.
        :param force: If True, forces reloading even if functions exist.
        :raises RuntimeError: If not all required master SQL functions were created.
        """
        # Check if functions already exist (unless force=True)
        if not force:
            if self.check_functions(connection, self.MASTER_FUNCTIONS):
                return

        # Load the SQL file
        master_sql_path = os.path.join(self.sql_base_path, "master.sql")
        self.execute_sql_file(connection, master_sql_path)

        # Verify functions were created
        if not self.check_functions(connection, self.MASTER_FUNCTIONS):
            raise RuntimeError("Not all required master SQL functions were created")
