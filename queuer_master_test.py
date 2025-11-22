"""
Master functionality tests for Python queuer implementation.
Mirrors the Go queuerMaster_test.go with Python unittest patterns.
"""

import unittest
from datetime import datetime, timedelta, timezone
from typing import Optional

from queuer import new_queuer
from model.master import Master, MasterSettings
from model.worker import new_worker, WorkerStatus
from model.job import new_job, JobStatus
from helper.test_database import DatabaseTestMixin


class TestMasterTicker(DatabaseTestMixin, unittest.TestCase):
    """Test master ticker functionality."""

    def test_master_ticker_fails_with_nil_old_master(self):
        """Test master ticker fails with None old master."""
        queuer = new_queuer("test", 10)
        self.assertIsNotNone(queuer, "Expected Queuer to be created successfully")

        old_master: Optional[Master] = None
        master_settings = MasterSettings(
            master_poll_interval=5.0, retention_archive=30, lock_timeout_minutes=5
        )

        # Test that master_ticker raises error with None old master
        with self.assertRaises(Exception) as context:
            import asyncio

            asyncio.run(queuer.master_ticker(old_master, master_settings))

        self.assertIn("old master is None", str(context.exception))


class TestCheckStaleWorkers(DatabaseTestMixin, unittest.TestCase):
    """Test stale worker checking functionality."""

    def test_check_stale_workers_with_no_workers(self):
        """Test checkStaleWorkers with no workers."""
        queuer = new_queuer("test", 10)
        self.assertIsNotNone(queuer, "Expected Queuer to be created successfully")

        # Should complete without error when no workers exist
        try:
            queuer.check_stale_workers()
        except Exception as e:
            self.fail(
                f"Expected checkStaleWorkers to complete without error when no workers exist: {e}"
            )

        queuer.stop()

    def test_check_stale_workers_with_fresh_workers(self):
        """Test checkStaleWorkers with fresh workers."""
        queuer = new_queuer("test", 10)
        self.assertIsNotNone(queuer, "Expected Queuer to be created successfully")

        queuer.start()
        try:
            # Check stale workers should not affect fresh workers
            queuer.check_stale_workers()

            # Verify worker remains RUNNING
            worker = queuer.db_worker.select_worker(queuer.worker.rid)
            self.assertIsNotNone(worker, "Expected to select worker successfully")
            if worker:
                self.assertEqual(
                    WorkerStatus.RUNNING,
                    worker.status,
                    "Expected fresh worker to remain RUNNING",
                )

        finally:
            queuer.stop()

    def test_check_stale_workers_marks_old_workers_as_offline(self):
        """Test checkStaleWorkers marks old workers as offline."""
        queuer = new_queuer("test", 10)
        self.assertIsNotNone(queuer, "Expected Queuer to be created successfully")

        try:
            # Create a non-running worker with old timestamp
            test_worker = new_worker("stale-worker", 5)
            self.assertIsNotNone(test_worker)
            inserted_worker = queuer.db_worker.insert_worker(test_worker)
            self.assertIsNotNone(inserted_worker)

            # Update worker timestamp directly to make it stale
            self.assertIsNotNone(queuer.database.instance)
            old_timestamp = datetime.now(timezone.utc) - timedelta(hours=1)
            if queuer.database.instance:
                with queuer.database.instance.cursor() as cur:
                    cur.execute(
                        "UPDATE worker SET updated_at = %s, status = %s WHERE rid = %s",
                        (old_timestamp, WorkerStatus.RUNNING, inserted_worker.rid),
                    )
                queuer.database.instance.commit()

            # Run the stale check
            queuer.check_stale_workers()

            # Verify worker was marked as STOPPED
            stale_worker_after = queuer.db_worker.select_worker(inserted_worker.rid)
            self.assertIsNotNone(stale_worker_after)
            if stale_worker_after:
                self.assertEqual(WorkerStatus.STOPPED, stale_worker_after.status)

        finally:
            queuer.stop()

    def test_check_stale_workers_ignores_already_stopped_workers(self):
        """Test checkStaleWorkers ignores already stopped workers."""
        queuer = new_queuer("test", 10)
        self.assertIsNotNone(queuer, "Expected Queuer to be created successfully")

        try:
            # Create a worker that's already stopped
            test_worker = new_worker("stopped-worker", 5)
            self.assertIsNotNone(test_worker, "Expected to create test worker")

            inserted_worker = queuer.db_worker.insert_worker(test_worker)
            self.assertIsNotNone(inserted_worker, "Expected to insert test worker")

            # Set worker to STOPPED with old timestamp
            old_timestamp = datetime.now(timezone.utc) - timedelta(minutes=10)
            self.assertIsNotNone(queuer.database.instance)
            if queuer.database.instance:
                with queuer.database.instance.cursor() as cur:
                    cur.execute(
                        "UPDATE worker SET updated_at = %s, status = %s WHERE rid = %s",
                        (old_timestamp, WorkerStatus.STOPPED, inserted_worker.rid),
                    )
                queuer.database.instance.commit()

            # Run the stale check
            queuer.check_stale_workers()

            # Verify worker remains STOPPED
            offline_worker = queuer.db_worker.select_worker(inserted_worker.rid)
            self.assertIsNotNone(offline_worker)
            if offline_worker:
                self.assertEqual(WorkerStatus.STOPPED, offline_worker.status)

        finally:
            queuer.stop()


class TestCheckStaleJobs(DatabaseTestMixin, unittest.TestCase):
    """Test stale job checking functionality."""

    def test_check_stale_jobs_with_no_jobs(self):
        """Test checkStaleJobs with no jobs."""
        queuer = new_queuer("test", 10)
        self.assertIsNotNone(queuer, "Expected Queuer to be created successfully")

        # Should complete without error when no jobs exist
        try:
            queuer.check_stale_jobs()
        except Exception as e:
            self.fail(
                f"Expected checkStaleJobs to complete without error when no jobs exist: {e}"
            )

        queuer.stop()

    def test_check_stale_jobs_cancels_jobs_with_stopped_workers(self):
        """Test checkStaleJobs cancels jobs with stopped workers."""
        queuer = new_queuer("test", 10)
        self.assertIsNotNone(queuer, "Expected Queuer to be created successfully")

        try:
            # Create a worker and set it to STOPPED
            test_worker = new_worker("stopped-worker", 3)
            self.assertIsNotNone(test_worker, "Expected to create test worker")

            inserted_worker = queuer.db_worker.insert_worker(test_worker)
            self.assertIsNotNone(inserted_worker, "Expected to insert test worker")

            inserted_worker.status = WorkerStatus.STOPPED
            updated_worker = queuer.db_worker.update_worker(inserted_worker)
            self.assertIsNotNone(updated_worker, "Expected to update worker to STOPPED")

            # Create a job assigned to the stopped worker
            test_job = new_job("test-task")
            self.assertIsNotNone(test_job, "Expected to create test job")
            test_job.worker_rid = inserted_worker.rid

            inserted_job = queuer.db_job.insert_job(test_job)
            self.assertIsNotNone(inserted_job, "Expected to insert test job")

            # Update job to have worker assignment
            self.assertIsNotNone(queuer.database.instance)
            if queuer.database.instance:
                with queuer.database.instance.cursor() as cur:
                    cur.execute(
                        "UPDATE job SET worker_rid = %s, status = %s WHERE rid = %s",
                        (inserted_worker.rid, JobStatus.RUNNING, inserted_job.rid),
                    )
                queuer.database.instance.commit()

            # Run the stale job check
            queuer.check_stale_jobs()

            # Verify job was cancelled
            updated_job = queuer.db_job.select_job(inserted_job.rid)
            self.assertIsNotNone(updated_job)
            if updated_job:
                self.assertEqual(JobStatus.CANCELLED, updated_job.status)

        finally:
            queuer.stop()

    def test_check_stale_jobs_ignores_jobs_with_final_statuses(self):
        """Test checkStaleJobs ignores jobs with final statuses."""
        queuer = new_queuer("test", 10)
        self.assertIsNotNone(queuer, "Expected Queuer to be created successfully")

        try:
            # Create a worker and set it to STOPPED
            test_worker = new_worker("stopped-worker", 3)
            self.assertIsNotNone(test_worker, "Expected to create test worker")

            inserted_worker = queuer.db_worker.insert_worker(test_worker)
            self.assertIsNotNone(inserted_worker, "Expected to insert test worker")

            inserted_worker.status = WorkerStatus.STOPPED
            updated_worker = queuer.db_worker.update_worker(inserted_worker)
            self.assertIsNotNone(updated_worker, "Expected to update worker to STOPPED")

            # Create a job with final status assigned to the stopped worker
            test_job = new_job("test-task")
            self.assertIsNotNone(test_job, "Expected to create test job")
            test_job.worker_rid = inserted_worker.rid

            inserted_job = queuer.db_job.insert_job(test_job)
            self.assertIsNotNone(inserted_job, "Expected to insert test job")

            # Update job to have final status
            self.assertIsNotNone(queuer.database.instance)
            if queuer.database.instance:
                with queuer.database.instance.cursor() as cur:
                    cur.execute(
                        "UPDATE job SET worker_rid = %s, status = %s WHERE rid = %s",
                        (inserted_worker.rid, JobStatus.SUCCEEDED, inserted_job.rid),
                    )
                queuer.database.instance.commit()

            # Run the stale job check
            queuer.check_stale_jobs()

            # Verify job status remained unchanged
            updated_job = queuer.db_job.select_job(inserted_job.rid)
            self.assertIsNotNone(updated_job, "Expected to select job successfully")
            if updated_job:
                self.assertEqual(JobStatus.SUCCEEDED, updated_job.status)

        finally:
            queuer.stop()

    def test_check_stale_jobs_ignores_jobs_with_ready_workers(self):
        """Test checkStaleJobs ignores jobs with ready workers."""
        queuer = new_queuer("test", 10)
        self.assertIsNotNone(queuer, "Expected Queuer to be created successfully")

        try:
            # Create a worker and keep it READY
            test_worker = new_worker("ready-worker", 3)
            self.assertIsNotNone(test_worker, "Expected to create test worker")

            inserted_worker = queuer.db_worker.insert_worker(test_worker)
            self.assertIsNotNone(inserted_worker, "Expected to insert test worker")

            # Create a job assigned to the ready worker
            test_job = new_job("test-task")
            self.assertIsNotNone(test_job, "Expected to create test job")
            test_job.worker_rid = inserted_worker.rid

            inserted_job = queuer.db_job.insert_job(test_job)
            self.assertIsNotNone(inserted_job, "Expected to insert test job")

            # Update job to have worker assignment
            self.assertIsNotNone(queuer.database.instance)
            if queuer.database.instance:
                with queuer.database.instance.cursor() as cur:
                    cur.execute(
                        "UPDATE job SET worker_rid = %s, status = %s WHERE rid = %s",
                        (inserted_worker.rid, JobStatus.RUNNING, inserted_job.rid),
                    )
                queuer.database.instance.commit()

            # Run the stale job check
            queuer.check_stale_jobs()

            # Verify job status remained unchanged
            updated_job = queuer.db_job.select_job(inserted_job.rid)
            self.assertIsNotNone(updated_job, "Expected to select job successfully")
            if updated_job:
                self.assertEqual(JobStatus.RUNNING, updated_job.status)

        finally:
            queuer.stop()


if __name__ == "__main__":
    unittest.main()
