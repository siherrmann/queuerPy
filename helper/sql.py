"""
Database connection and SQL loading for Python queuer implementation.
Uses helper.database classes and references the same SQL submodule as Go.
"""

import os
from typing import Optional
import psycopg
from psycopg import Connection
from psycopg.rows import dict_row

from .database import Database, DatabaseConfiguration, new_database, new_database_from_env
from pathlib import Path


class SQLLoader:
    """
    SQL file loader that references the same SQL submodule as Go.
    This class is deprecated - use the sql package directly instead.
    """
    
    def __init__(self, sql_base_path: str = None):
        """Initialize with path to SQL files (relative to Python project)."""
        if sql_base_path is None:
            # Get the project root directory (where this file is located relative to helper/)
            current_dir = Path(__file__).parent.parent  # Go up from helper/ to project root
            sql_base_path = str(current_dir / "sql")
        self.sql_base_path = sql_base_path
    
    def _load_sql_file(self, file_path: str) -> str:
        """Load SQL content from file. Internal method."""
        try:
            with open(file_path, 'r') as f:
                return f.read()
        except FileNotFoundError:
            raise ValueError(f"SQL file not found: {file_path}")

    def _execute_sql_file(self, connection: Connection, file_path: str) -> None:
        """Execute SQL file content. Internal method."""
        sql_content = self._load_sql_file(file_path)
        
        with connection.cursor() as cur:
            cur.execute(sql_content)
        connection.commit()
    
    def load_job_sql(self, connection: Connection, with_table_drop: bool = False) -> None:
        """Load job-related SQL functions."""
        job_sql_path = os.path.join(self.sql_base_path, "job.sql")
        
        if with_table_drop:
            # Drop existing functions first
            with connection.cursor() as cur:
                cur.execute("DROP FUNCTION IF EXISTS update_job_initial CASCADE;")
                cur.execute("DROP FUNCTION IF EXISTS update_job_final CASCADE;")
                cur.execute("DROP FUNCTION IF EXISTS update_job_final_encrypted CASCADE;")
                cur.execute("DROP FUNCTION IF EXISTS insert_job CASCADE;")
                cur.execute("DROP FUNCTION IF EXISTS insert_job_encrypted CASCADE;")
                connection.commit()
        
        self._execute_sql_file(connection, job_sql_path)
    
    def load_worker_sql(self, connection: Connection, with_table_drop: bool = False) -> None:
        """Load worker-related SQL functions."""
        worker_sql_path = os.path.join(self.sql_base_path, "worker.sql")
        
        if with_table_drop:
            # Drop existing functions first
            with connection.cursor() as cur:
                cur.execute("DROP FUNCTION IF EXISTS insert_worker CASCADE;")
                cur.execute("DROP FUNCTION IF EXISTS update_worker CASCADE;")
                connection.commit()
        
        self._execute_sql_file(connection, worker_sql_path)
    
    def load_notify_sql(self, connection: Connection, with_table_drop: bool = False) -> None:
        """Load notification-related SQL functions."""
        notify_sql_path = os.path.join(self.sql_base_path, "notify.sql")
        
        if with_table_drop:
            # Drop existing functions first
            with connection.cursor() as cur:
                cur.execute("DROP FUNCTION IF EXISTS notify_event CASCADE;")
                connection.commit()
        
        self._execute_sql_file(connection, notify_sql_path)