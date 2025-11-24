"""
Master database handler tests for Python queuer implementation.
Mirrors Go's database/dbMaster_test.go with unittest and testcontainers.
"""

import unittest
import uuid

from .db_master import MasterDBHandler
from ..helper.test_database import DatabaseTestMixin
from ..model.master import MasterSettings
from ..model.worker import Worker


class TestMasterDBHandler(DatabaseTestMixin, unittest.TestCase):
    """Test cases for MasterDBHandler. Mirrors Go's dbMaster_test.go."""

    @classmethod
    def setUpClass(cls):
        """Set up for the entire test class."""
        super().setup_class()

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        super().teardown_class()

    def setUp(self):
        """Set up for each test method."""
        super().setup_method()

    def test_new_master_db_handler_valid(self):
        """Test valid call to create new MasterDBHandler. Mirrors TestMasterNewMasterDBHandler."""
        # Create MasterDBHandler (equivalent to NewMasterDBHandler)
        db_master = MasterDBHandler(self.db, with_table_drop=True)

        self.assertIsNotNone(db_master, "MasterDBHandler should not be None")
        self.assertIsNotNone(db_master.db, "Database connection should not be None")
        self.assertIsNotNone(
            db_master.db.instance, "Database instance should not be None"
        )

        # Check table existence
        exists = db_master.check_table_existence()
        self.assertTrue(exists, "Expected table to exist after creation")

        # Clean up
        db_master.drop_table()

    def test_drop_table(self):
        """Test dropping master table. Mirrors TestMasterDropTable."""
        db_master = MasterDBHandler(self.db, with_table_drop=True)

        # Drop the table
        db_master.drop_table()

        # Check that table no longer exists
        exists = db_master.check_table_existence()
        self.assertFalse(exists, "Expected table to not exist after drop")

    def test_create_table(self):
        """Test creating master table. Mirrors TestMasterCreateTable."""
        db_master = MasterDBHandler(self.db, with_table_drop=True)

        # Drop table first (since NewMasterDBHandler creates it)
        db_master.drop_table()

        # Create table
        db_master.create_table()

        # Check that table exists
        exists = db_master.check_table_existence()
        self.assertTrue(exists, "Expected table to exist after create")

    def test_update_master(self):
        """Test updating master record. Mirrors TestMasterUpdateMaster."""
        db_master = MasterDBHandler(self.db, with_table_drop=True)

        # Create first worker
        worker1 = Worker(
            id=1,
            rid=uuid.uuid4(),
            name="test-worker-1",
            status="ready",
            max_concurrency=10,
        )

        settings = MasterSettings(
            retention_archive=30, master_poll_interval=60, lock_timeout_minutes=10
        )

        # First update - should succeed and return old master
        master_old = db_master.update_master(worker1, settings)
        self.assertIsNotNone(master_old, "Expected old master to not be None")
        if master_old:
            self.assertEqual(master_old.id, 1, "Expected old master ID to be 1")
            self.assertEqual(
                master_old.worker_id, 0, "Expected old master WorkerID to be 0"
            )
            self.assertIsNone(
                master_old.worker_rid,
                "Expected old master WorkerRID to be None",
            )
            self.assertEqual(
                master_old.settings.retention_archive,
                30,
                "Expected old master settings to have default retention_archive",
            )

        # Select current master
        master = db_master.select_master()
        self.assertIsNotNone(master, "Expected master to not be None")
        self.assertEqual(
            master.worker_id, worker1.id, "Expected master worker ID to match worker ID"
        )
        self.assertEqual(
            master.worker_rid,
            worker1.rid,
            "Expected master worker RID to match worker RID",
        )
        self.assertEqual(
            master.settings.retention_archive,
            settings.retention_archive,
            "Expected master retention archive to match",
        )

        # Create second worker
        worker2 = Worker(
            id=2,
            rid=uuid.uuid4(),
            name="test-worker-2",
            status="ready",
            max_concurrency=10,
        )

        # Second update - should fail and return None (master already locked)
        master = db_master.update_master(worker2, settings)
        self.assertIsNone(master, "Expected master to be None (lock should fail)")

    def test_select_master(self):
        """Test selecting current master. Mirrors TestMasterSelectMaster."""
        db_master = MasterDBHandler(self.db, with_table_drop=True)

        # Create worker
        worker = Worker(
            id=2,
            rid=uuid.uuid4(),
            name="test-worker-select",
            status="ready",
            max_concurrency=10,
        )

        settings = MasterSettings(
            retention_archive=30, master_poll_interval=60, lock_timeout_minutes=10
        )

        # Update master first
        master_old = db_master.update_master(worker, settings)
        self.assertIsNotNone(master_old, "Expected master to not be None")

        # Select master
        master = db_master.select_master()
        self.assertIsNotNone(master, "Expected master to not be None")
        self.assertEqual(master.id, 1, "Expected master ID to be 1")
        self.assertEqual(
            master.worker_id, worker.id, "Expected master worker ID to match worker ID"
        )
        self.assertEqual(
            master.worker_rid,
            worker.rid,
            "Expected master worker RID to match worker RID",
        )
        self.assertEqual(
            master.settings.retention_archive,
            settings.retention_archive,
            "Expected master retention archive to be 30",
        )


if __name__ == "__main__":
    unittest.main()
