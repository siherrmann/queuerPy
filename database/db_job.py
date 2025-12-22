"""
Job database handler for Python queuer implementation.
Mirrors Go's database/dbJob.go with psycopg3 and references same SQL functions.
"""

from datetime import datetime
import json
from typing import List, Optional, Tuple
from uuid import UUID
from psycopg.rows import dict_row

from ..helper.database import Database
from ..helper.error import QueuerError
from ..helper.sql import SQLLoader, run_ddl
from ..model.job import Job
from ..model.worker import Worker


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
        self.db: Database = db_connection

        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        self.encryption_key: str = encryption_key

        sql_loader: SQLLoader = SQLLoader()
        sql_loader.load_notify_sql(self.db.instance, force=with_table_drop)
        sql_loader.load_job_sql(self.db.instance, force=with_table_drop)

        if with_table_drop:
            self.drop_tables()

        self.create_table()

    def check_tables_existence(self) -> bool:
        """
        Check if job tables exist.

        :return: True if job table exists, False otherwise.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

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
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        run_ddl(self.db.instance, "SELECT init_job();")

    def drop_tables(self) -> None:
        """Drop job tables with DDL deadlock protection."""
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        run_ddl(self.db.instance, "DROP TABLE IF EXISTS job_archive CASCADE;")
        run_ddl(self.db.instance, "DROP TABLE IF EXISTS job CASCADE;")

    def insert_job(self, job: Job) -> Job:
        """
        Insert a job into the database using SQL function.
        Note: SQL function only supports basic job creation (no worker assignment, results, or error).

        :param job: Job instance to insert.
        :return: Inserted Job instance.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT * FROM insert_job(%s, %s, %s, %s, %s, %s, %s);",
                (
                    json.dumps(job.options.to_dict()) if job.options else None,
                    job.task_name,
                    json.dumps(job.parameters),
                    json.dumps(job.parameters_keyed),
                    job.status,
                    job.scheduled_at,
                    job.schedule_count,
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

    def insert_job_tx(self, job: Job) -> Job:
        """
        Insert a job within a transaction context.
        Placeholder method - mirrors Go's InsertJobTx method.
        TODO: Implement transaction-aware insert logic.

        :param job: Job instance to insert.
        :return: Inserted Job instance.
        """
        # For now, delegate to regular insert_job
        # In full implementation, this would handle explicit transaction context
        return self.insert_job(job)

    def batch_insert_jobs(self, jobs: List[Job]) -> None:
        """
        Insert multiple jobs in a batch operation using executemany.
        Mirrors Go's BatchInsertJobs method with direct INSERT statements.

        :param jobs: List of Job instances to insert.
        :return: List of inserted Job instances.
        :raises QueuerError: If batch insertion fails.
        """
        if not jobs:
            return

        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        try:
            batch_data: List[Tuple[str, str, str, str, Optional[datetime]]] = []
            for job in jobs:
                options_json = (
                    json.dumps(job.options.to_dict()) if job.options else "{}"
                )
                parameters_json = json.dumps(job.parameters)
                parameters_keyed_json = json.dumps(job.parameters_keyed)
                batch_data.append(
                    (
                        options_json,
                        job.task_name,
                        parameters_json,
                        parameters_keyed_json,
                        job.scheduled_at,
                    )
                )

            with self.db.instance.cursor(row_factory=dict_row) as cur:
                cur.executemany(
                    """
                    INSERT INTO job (options, task_name, parameters, parameters_keyed, scheduled_at) 
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    batch_data,
                )
                self.db.instance.commit()
        except Exception as e:
            self.db.instance.rollback()
            raise QueuerError("batch insert jobs", e)

    def update_jobs_initial(self, worker: Worker) -> List[Job]:
        """
        Update jobs for initial processing.

        :param worker: Worker instance for which to update jobs.
        :return: List of updated Job instances.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT * FROM update_job_initial(%s);
            """,
                (worker.id,),
            )

            jobs: List[Job] = []
            for row in cur.fetchall():
                jobs.append(Job.from_row(row))

            return jobs

    def update_job_final(self, job: Job) -> Job:
        """
        Update job with final status and results.

        :param job: Job instance to update.
        :return: Updated Job instance.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            results_param = json.dumps(job.results)
            if self.encryption_key:
                cur.execute(
                    "SELECT * FROM update_job_final_encrypted(%s, %s, %s, %s, %s);",
                    (
                        job.id,
                        job.status,
                        results_param,
                        job.error,
                        self.encryption_key,
                    ),
                )
            else:
                cur.execute(
                    "SELECT * FROM update_job_final(%s, %s, %s, %s);",
                    (job.id, job.status, results_param, job.error),
                )

            row = cur.fetchone()
            if row:
                job_result = Job.from_row(row)
                self.db.instance.commit()
                return job_result
            else:
                raise RuntimeError("Failed to update job")

    def update_stale_jobs(self) -> int:
        """
        Update stale jobs to CANCELLED status where the assigned worker is STOPPED.

        :return: Number of jobs updated.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor() as cur:
            try:
                cur.execute(
                    "SELECT update_stale_jobs(%s, %s, %s, %s, %s);",
                    ("CANCELLED", "SUCCEEDED", "CANCELLED", "FAILED", "STOPPED"),
                )
                result = cur.fetchone()
                return result[0] if result else 0
            except Exception as e:
                raise QueuerError("update stale jobs", e)

    def delete_job(self, rid: UUID) -> None:
        """
        Delete a job by RID using SQL function.
        Mirrors Go's DeleteJob method.

        :param rid: RID of the job to delete.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor() as cur:
            cur.execute("SELECT delete_job(%s);", (rid,))
        self.db.instance.commit()

    def select_job(self, rid: UUID) -> Optional[Job]:
        """
        Select a job by RID using SQL function.
        Mirrors Go's SelectJob method.

        :param rid: RID of the job to select.
        :return: Job instance if found, else None.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT * FROM select_job(%s, %s);",
                (self.encryption_key or "", rid),
            )

            row = cur.fetchone()
            return Job.from_row(row) if row else None

    def select_all_jobs(self, last_id: int = 0, entries: int = 100) -> List[Job]:
        """
        Select all jobs with pagination using SQL function.
        Mirrors Go's SelectAllJobs method.

        :param last_id: Last job ID from previous page.
        :param entries: Number of entries to retrieve.
        :return: List of Job instances.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT * FROM select_all_jobs(%s, %s, %s);",
                (self.encryption_key or "", last_id, entries),
            )

            jobs: List[Job] = []
            for row in cur.fetchall():
                jobs.append(Job.from_row(row))

            return jobs

    def select_all_jobs_by_worker_rid(
        self, worker_rid: UUID, last_id: int = 0, entries: int = 100
    ) -> List[Job]:
        """
        Select all jobs for a specific worker using SQL function.
        Mirrors Go's SelectAllJobsByWorkerRid method.

        :param worker_rid: RID of the worker.
        :param last_id: Last job ID from previous page.
        :param entries: Number of entries to retrieve.
        :return: List of Job instances.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT * FROM select_all_jobs_by_worker_rid(%s, %s, %s, %s);",
                (self.encryption_key or "", worker_rid, last_id, entries),
            )

            jobs: List[Job] = []
            for row in cur.fetchall():
                jobs.append(Job.from_row(row))

            return jobs

    def select_all_jobs_by_search(
        self, search: str, last_id: int = 0, entries: int = 100
    ) -> List[Job]:
        """
        Select all jobs filtered by search string using SQL function.
        Mirrors Go's SelectAllJobsBySearch method.
        Searches across 'rid', 'worker_id', 'task_name', and 'status' fields.

        :param search: Search string to filter jobs.
        :param last_id: Last job ID from previous page.
        :param entries: Number of entries to retrieve.
        :return: List of Job instances.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT * FROM select_all_jobs_by_search(%s, %s, %s, %s);",
                (self.encryption_key or "", search, last_id, entries),
            )

            jobs: List[Job] = []
            for row in cur.fetchall():
                jobs.append(Job.from_row(row))

            return jobs

    def add_retention_archive(self, days: int) -> None:
        """
        Add retention policy for archive cleanup.
        Mirrors Go's AddRetentionArchive method.

        :param days: Number of days to retain archived jobs
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor() as cur:
            cur.execute("SELECT add_retention_archive(%s);", (days,))
        self.db.instance.commit()

    def remove_retention_archive(self) -> None:
        """
        Remove retention policy for archive cleanup.
        Mirrors Go's RemoveRetentionArchive method.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor() as cur:
            cur.execute("SELECT remove_retention_archive();")
        self.db.instance.commit()

    def select_job_from_archive(self, rid: UUID) -> Optional[Job]:
        """
        Select a job from archive by RID using SQL function.
        Mirrors Go's SelectJobFromArchive method.

        :param rid: RID of the job to select.
        :return: Job instance if found, else None.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT * FROM select_job_from_archive(%s, %s);",
                (self.encryption_key or "", rid),
            )

            row = cur.fetchone()
            return Job.from_row(row) if row else None

    def select_all_jobs_from_archive(
        self, last_id: int = 0, entries: int = 100
    ) -> List[Job]:
        """
        Select all jobs from archive with pagination. Mirrors Go's SelectAllJobsFromArchive method.

        :param last_id: Last job ID from previous page.
        :param entries: Number of entries to retrieve.
        :return: List of Job instances.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT * FROM select_all_jobs_from_archive(%s, %s, %s);",
                (self.encryption_key or "", last_id, entries),
            )

            jobs: List[Job] = []
            for row in cur.fetchall():
                jobs.append(Job.from_row(row))

            return jobs

    def select_all_jobs_from_archive_by_search(
        self, search: str, last_id: int = 0, entries: int = 100
    ) -> List[Job]:
        """
        Select all archived jobs filtered by search string. Mirrors Go's SelectAllJobsFromArchiveBySearch method.
        Searches across 'rid', 'worker_id', 'task_name', and 'status' fields.

        :param search: Search string to filter jobs.
        :param last_id: Last job ID from previous page.
        :param entries: Number of entries to retrieve.
        :return: List of Job instances.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT * FROM select_all_jobs_from_archive_by_search(%s, %s, %s, %s);",
                (self.encryption_key or "", search, last_id, entries),
            )

            jobs: List[Job] = []
            for row in cur.fetchall():
                jobs.append(Job.from_row(row))

            return jobs
