"""
Job database handler for Python queuer implementation.
Mirrors Go's database/dbJob.go with psycopg3 and references same SQL functions.
"""

import json
from datetime import datetime
from typing import List, Optional, Dict, Any
from uuid import UUID
import psycopg
from psycopg import Connection
from psycopg.rows import dict_row

from helper.database import Database
from helper.sql import SQLLoader
from model.job import Job, JobStatus
from model.worker import Worker


class JobDBHandler:
    """
    Job database handler.
    Mirrors Go's JobDBHandler with psycopg3.
    """

    def __init__(
        self,
        db_connection: Database,
        with_table_drop: bool = False,
        encryption_key: str = "",
    ):
        """Initialize job database handler."""
        if db_connection is None:
            raise ValueError("database connection is None")

        self.db: Database = db_connection
        self.encryption_key: str = encryption_key

        # Load SQL functions using helper.sql
        connection: Connection = self.db.instance
        sql_loader: SQLLoader = SQLLoader()
        sql_loader.load_notify_sql(
            connection, with_table_drop
        )  # Load notify function first
        sql_loader.load_job_sql(connection, with_table_drop)

        # Create tables if they don't exist
        if not self.check_tables_existence():
            self.create_table()

    def check_tables_existence(self) -> bool:
        """Check if job tables exist."""
        with self.db.instance.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = current_schema() 
                    AND table_name = 'job'
                );
            """
            )
            result = cur.fetchone()
            return result[0] if result else False

    def create_table(self) -> None:
        """Create job table using SQL init function."""
        with self.db.instance.cursor() as cur:
            cur.execute("SELECT init_job();")
        self.db.instance.commit()

    def drop_tables(self) -> None:
        """Drop job tables."""
        with self.db.instance.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS job_archive CASCADE;")
            cur.execute("DROP TABLE IF EXISTS job CASCADE;")
        self.db.instance.commit()

    def insert_job(self, job: Job) -> Job:
        """
        Insert a job into the database.
        Mirrors Go's InsertJob method using direct INSERT statement.
        """
        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                INSERT INTO job (options, task_name, parameters, status, scheduled_at, started_at, schedule_count, attempts, results, error, worker_rid)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING
                    id,
                    rid,
                    worker_id,
                    worker_rid,
                    options,
                    task_name,
                    parameters,
                    status,
                    scheduled_at,
                    started_at,
                    schedule_count,
                    attempts,
                    results,
                    error,
                    created_at,
                    updated_at;
            """,
                (
                    json.dumps(job.options.to_dict()) if job.options else None,
                    job.task_name,
                    json.dumps(job.parameters),
                    (
                        job.status.value
                        if isinstance(job.status, JobStatus)
                        else job.status
                    ),
                    job.scheduled_at,
                    job.started_at,
                    job.schedule_count,
                    job.attempts,
                    json.dumps(job.results) if job.results else None,
                    job.error or None,
                    (
                        job.worker_rid
                        if job.worker_rid
                        != UUID("00000000-0000-0000-0000-000000000000")
                        else None
                    ),
                ),
            )

            row = cur.fetchone()
            if row:
                job_result = Job.from_row(row)
                # Commit the transaction to ensure the job is persisted
                self.db.instance.commit()
                return job_result
            else:
                raise RuntimeError("Failed to insert job")

    def update_jobs_initial(self, worker: Worker) -> List[Job]:
        """
        Update jobs for initial processing.
        Mirrors Go's UpdateJobsInitial method.
        Uses the update_job_initial SQL function.
        """
        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT * FROM update_job_initial(%s);
            """,
                (worker.id,),
            )

            jobs = []
            for row in cur.fetchall():
                jobs.append(Job.from_row(row))

            return jobs

    def update_job_final(self, job: Job) -> Job:
        """
        Update job with final status and results.
        Mirrors Go's UpdateJobFinal method.
        """
        with self.db.instance.cursor(row_factory=dict_row) as cur:
            if self.encryption_key:
                cur.execute(
                    """
                    SELECT * FROM update_job_final_encrypted(%s, %s, %s, %s, %s);
                """,
                    (
                        job.id,
                        job.status,
                        json.dumps(job.results),
                        job.error,
                        self.encryption_key,
                    ),
                )
            else:
                cur.execute(
                    """
                    SELECT * FROM update_job_final(%s, %s, %s, %s);
                """,
                    (job.id, job.status, json.dumps(job.results), job.error),
                )

            row = cur.fetchone()
            if row:
                return Job.from_row(row)
            else:
                raise RuntimeError("Failed to update job")

    def select_job(self, rid: UUID) -> Optional[Job]:
        """Select a job by RID."""
        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT * FROM job WHERE rid = %s;
            """,
                (rid,),
            )

            row = cur.fetchone()
            return Job.from_row(row) if row else None

    def select_job_from_archive(self, rid: UUID) -> Optional[Job]:
        """Select a job from archive by RID."""
        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT * FROM job_archive WHERE rid = %s;
            """,
                (rid,),
            )

            row = cur.fetchone()
            return Job.from_row(row) if row else None

    def select_all_jobs(self, last_id: int = 0, entries: int = 100) -> List[Job]:
        """Select all jobs with pagination."""
        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT * FROM job 
                WHERE id > %s 
                ORDER BY id 
                LIMIT %s;
            """,
                (last_id, entries),
            )

            jobs = []
            for row in cur.fetchall():
                jobs.append(Job.from_row(row))

            return jobs

    def select_all_jobs_by_worker_rid(
        self, worker_rid: UUID, last_id: int = 0, entries: int = 100
    ) -> List[Job]:
        """Select all jobs for a specific worker."""
        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT * FROM job 
                WHERE worker_rid = %s AND id > %s 
                ORDER BY id 
                LIMIT %s;
            """,
                (worker_rid, last_id, entries),
            )

            jobs = []
            for row in cur.fetchall():
                jobs.append(Job.from_row(row))

            return jobs

    def delete_job(self, rid: UUID) -> None:
        """Delete a job by RID."""
        with self.db.instance.cursor() as cur:
            cur.execute("DELETE FROM job WHERE rid = %s;", (rid,))
        self.db.instance.commit()

    def update_stale_jobs(self) -> int:
        """Update stale jobs (jobs that have been running too long)."""
        with self.db.instance.cursor() as cur:
            cur.execute(
                """
                UPDATE job 
                SET status = 'FAILED', 
                    error = 'Job timed out',
                    updated_at = CURRENT_TIMESTAMP
                WHERE status = 'RUNNING' 
                AND started_at < CURRENT_TIMESTAMP - INTERVAL '1 hour';
            """
            )
            return cur.rowcount
