"""
Job database handler for Python queuer implementation.
Mirrors Go's database/dbJob.go with psycopg3 and references same SQL functions.
"""

import json
from typing import List, Optional
from uuid import UUID
from psycopg import Connection
from psycopg.rows import dict_row

from helper.database import Database
from helper.error import QueuerError
from helper.sql import SQLLoader, run_ddl
from model.job import Job
from model.worker import Worker


class JobDBHandler:
    """
    Job database handler.
    Mirrors Go's JobDBHandler with psycopg3.
    """

    def __init__(
        self,
        db_connection: Optional[Database],
        with_table_drop: bool = False,
        encryption_key: str = "",
    ):
        """Initialize job database handler."""
        if db_connection is None:
            raise ValueError("Database connection is None")
        self.db: Database = db_connection
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        self.encryption_key: str = encryption_key

        # Load SQL functions using helper.sql
        connection: Connection = self.db.instance
        sql_loader: SQLLoader = SQLLoader()

        sql_loader.load_notify_sql(connection, force=with_table_drop)
        sql_loader.load_job_sql(connection, force=with_table_drop)
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

    def create_table(self):
        """Create job table using SQL init function."""
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        run_ddl(self.db.instance, "SELECT init_job();")

    def drop_tables(self):
        """Drop job tables with DDL deadlock protection."""
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        run_ddl(self.db.instance, "DROP TABLE IF EXISTS job_archive CASCADE;")
        run_ddl(self.db.instance, "DROP TABLE IF EXISTS job CASCADE;")

    def insert_job(self, job: Job) -> Job:
        """
        Insert a job into the database.

        :param job: Job instance to insert.
        :return: Inserted Job instance.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                INSERT INTO job (options, task_name, parameters, status, scheduled_at, schedule_count, worker_rid, started_at, results, error)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    schedule_count,
                    attempts,
                    started_at,
                    results,
                    error,
                    created_at,
                    updated_at;
            """,
                (
                    json.dumps(job.options.to_dict()) if job.options else None,
                    job.task_name,
                    json.dumps(job.parameters),
                    job.status,
                    job.scheduled_at,
                    job.schedule_count,
                    job.worker_rid,
                    job.started_at,
                    json.dumps(job.results) if job.results else None,
                    job.error,
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

    def batch_insert_jobs(self, jobs: List[Job]) -> List[Job]:
        """
        Insert multiple jobs in a batch operation.
        Placeholder method - mirrors Go's BatchInsertJobs method.
        TODO: Implement efficient batch insert logic.

        :param jobs: List of Job instances to insert.
        :return: List of inserted Job instances.
        """
        # For now, insert jobs one by one
        # In full implementation, this would use batch SQL operations
        result_jobs: List[Job] = []
        for job in jobs:
            result_jobs.append(self.insert_job(job))
        return result_jobs

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
                    """
                    SELECT
                        output_id as id,
                        output_rid as rid,
                        output_worker_id as worker_id,
                        output_worker_rid as worker_rid,
                        output_options as options,
                        output_task_name as task_name,
                        output_parameters as parameters,
                        output_status as status,
                        output_scheduled_at as scheduled_at,
                        output_started_at as started_at,
                        output_schedule_count as schedule_count,
                        output_attempts as attempts,
                        output_results as results,
                        output_error as error,
                        output_created_at as created_at,
                        output_updated_at as updated_at
                    FROM update_job_final_encrypted(%s, %s, %s, %s, %s);
                """,
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
                    """
                    SELECT
                        output_id as id,
                        output_rid as rid,
                        output_worker_id as worker_id,
                        output_worker_rid as worker_rid,
                        output_options as options,
                        output_task_name as task_name,
                        output_parameters as parameters,
                        output_status as status,
                        output_scheduled_at as scheduled_at,
                        output_started_at as started_at,
                        output_schedule_count as schedule_count,
                        output_attempts as attempts,
                        output_results as results,
                        output_error as error,
                        output_created_at as created_at,
                        output_updated_at as updated_at
                    FROM update_job_final(%s, %s, %s, %s);
                """,
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

    def delete_job(self, rid: UUID):
        """
        Delete a job by RID.

        :param rid: RID of the job to delete.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor() as cur:
            cur.execute("DELETE FROM job WHERE rid = %s;", (rid,))
        self.db.instance.commit()

    def select_job(self, rid: UUID) -> Optional[Job]:
        """
        Select a job by RID. Mirrors Go's SelectJob method.

        :param rid: RID of the job to select.
        :return: Job instance if found, else None.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
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
                    CASE
                        WHEN octet_length(results_encrypted) > 0 THEN pgp_sym_decrypt(results_encrypted, %s::text)::jsonb
                        ELSE results
                    END AS results,
                    error,
                    created_at,
                    updated_at
                FROM
                    job
                WHERE
                    rid = %s;
            """,
                (self.encryption_key or "", rid),
            )

            row = cur.fetchone()
            return Job.from_row(row) if row else None

    def select_all_jobs(self, last_id: int = 0, entries: int = 100) -> List[Job]:
        """
        Select all jobs with pagination.

        :param last_id: Last job ID from previous page.
        :param entries: Number of entries to retrieve.
        :return: List of Job instances.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

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

            jobs: List[Job] = []
            for row in cur.fetchall():
                jobs.append(Job.from_row(row))

            return jobs

    def select_all_jobs_by_worker_rid(
        self, worker_rid: UUID, last_id: int = 0, entries: int = 100
    ) -> List[Job]:
        """
        Select all jobs for a specific worker.

        :param worker_rid: RID of the worker.
        :param last_id: Last job ID from previous page.
        :param entries: Number of entries to retrieve.
        :return: List of Job instances.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

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

            jobs: List[Job] = []
            for row in cur.fetchall():
                jobs.append(Job.from_row(row))

            return jobs

    def select_all_jobs_by_search(
        self, search: str, last_id: int = 0, entries: int = 100
    ) -> List[Job]:
        """
        Select all jobs filtered by search string. Mirrors Go's SelectAllJobsBySearch method.
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
                """
                SELECT
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
                    CASE
                        WHEN octet_length(results_encrypted) > 0 THEN pgp_sym_decrypt(results_encrypted, %s::text)::jsonb
                        ELSE results
                    END AS results,
                    error,
                    created_at,
                    updated_at
                FROM job
                WHERE (rid::text ILIKE '%%' || %s || '%%'
                        OR worker_id::text ILIKE '%%' || %s || '%%'
                        OR task_name ILIKE '%%' || %s || '%%'
                        OR status ILIKE '%%' || %s || '%%')
                    AND (0 = %s
                        OR created_at < (
                            SELECT
                                u.created_at
                            FROM
                                job AS u
                            WHERE
                                u.id = %s))
                ORDER BY
                    created_at DESC
                LIMIT %s;
            """,
                (
                    self.encryption_key or "",
                    search,
                    search,
                    search,
                    search,
                    last_id,
                    last_id,
                    entries,
                ),
            )

            jobs: List[Job] = []
            for row in cur.fetchall():
                jobs.append(Job.from_row(row))

            return jobs

    def add_retention_archive(self, days: int):
        """
        Add retention policy for archive cleanup.
        Placeholder method - mirrors Go's AddRetentionArchive method.
        TODO: Implement archive retention policy.
        """
        # Placeholder implementation
        pass

    def remove_retention_archive(self):
        """
        Remove retention policy for archive cleanup.
        Placeholder method - mirrors Go's RemoveRetentionArchive method.
        TODO: Implement archive retention removal.
        """
        # Placeholder implementation
        pass

    def select_job_from_archive(self, rid: UUID) -> Optional[Job]:
        """
        Select a job from archive by RID. Mirrors Go's SelectJobFromArchive method.

        :param rid: RID of the job to select.
        :return: Job instance if found, else None.
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
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
                    CASE
                        WHEN octet_length(results_encrypted) > 0 THEN pgp_sym_decrypt(results_encrypted, %s::text)::jsonb
                        ELSE results
                    END AS results,
                    error,
                    created_at,
                    updated_at
                FROM
                    job_archive
                WHERE
                    rid = %s;
            """,
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
                """
                SELECT
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
                    CASE
                        WHEN octet_length(results_encrypted) > 0 THEN pgp_sym_decrypt(results_encrypted, %s::text)::jsonb
                        ELSE results
                    END AS results,
                    error,
                    created_at,
                    updated_at
                FROM
                    job_archive
                WHERE (0 = %s
                    OR created_at < (
                        SELECT
                            d.created_at
                        FROM
                            job_archive AS d
                        WHERE
                            d.id = %s))
                ORDER BY
                    created_at DESC
                LIMIT %s;
            """,
                (self.encryption_key or "", last_id, last_id, entries),
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
                """
                SELECT
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
                    CASE
                        WHEN octet_length(results_encrypted) > 0 THEN pgp_sym_decrypt(results_encrypted, %s::text)::jsonb
                        ELSE results
                    END AS results,
                    error,
                    created_at,
                    updated_at
                FROM job_archive
                WHERE (rid::text ILIKE '%%' || %s || '%%'
                        OR worker_id::text ILIKE '%%' || %s || '%%'
                        OR task_name ILIKE '%%' || %s || '%%'
                        OR status ILIKE '%%' || %s || '%%')
                    AND (0 = %s
                        OR created_at < (
                            SELECT
                                d.created_at
                            FROM
                                job_archive AS d
                            WHERE
                                d.id = %s))
                ORDER BY
                    created_at DESC
                LIMIT %s;
            """,
                (
                    self.encryption_key or "",
                    search,
                    search,
                    search,
                    search,
                    last_id,
                    last_id,
                    entries,
                ),
            )

            jobs: List[Job] = []
            for row in cur.fetchall():
                jobs.append(Job.from_row(row))

            return jobs
