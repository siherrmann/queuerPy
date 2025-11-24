"""
Tests for Master model.
Mirrors Go's model/master_test.go functionality.
"""

import json
import unittest
from typing import Any, Dict

from .master import Master, MasterSettings


class TestMasterSettings(unittest.TestCase):
    """Test cases for MasterSettings model."""

    def test_marshal_and_unmarshal_json(self):
        """Test JSON serialization and deserialization of valid MasterSettings."""
        # Create MasterSettings instance
        settings = MasterSettings(retention_archive=30)
        settings_dict: Dict[str, Any] = {
            "retention_archive": settings.retention_archive,
            "master_poll_interval": settings.master_poll_interval,
            "lock_timeout_minutes": settings.lock_timeout_minutes,
        }

        # Marshal to JSON string
        json_data = json.dumps(settings_dict)
        self.assertIsInstance(json_data, str, "JSON data should be a string")

        parsed_data = json.loads(json_data)
        unmarshalled_settings = MasterSettings.from_dict(parsed_data)
        self.assertIsNotNone(unmarshalled_settings)
        self.assertEqual(
            settings.retention_archive, unmarshalled_settings.retention_archive
        )
        self.assertEqual(
            settings.master_poll_interval, unmarshalled_settings.master_poll_interval
        )
        self.assertEqual(
            settings.lock_timeout_minutes, unmarshalled_settings.lock_timeout_minutes
        )

    def test_unmarshal_mastersettings_already_correct_type(self):
        """Test unmarshalling MasterSettings from dictionary (already correct type)."""
        settings = MasterSettings(retention_archive=30)
        settings_dict: Dict[str, Any] = {
            "retention_archive": settings.retention_archive,
            "master_poll_interval": settings.master_poll_interval,
            "lock_timeout_minutes": settings.lock_timeout_minutes,
        }
        unmarshalled_settings = MasterSettings.from_dict(settings_dict)
        self.assertIsNotNone(unmarshalled_settings)
        self.assertEqual(
            settings.retention_archive, unmarshalled_settings.retention_archive
        )
        self.assertEqual(
            settings.master_poll_interval, unmarshalled_settings.master_poll_interval
        )
        self.assertEqual(
            settings.lock_timeout_minutes, unmarshalled_settings.lock_timeout_minutes
        )

    def test_unmarshal_invalid_mastersettings_json(self):
        """Test unmarshalling invalid MasterSettings JSON."""
        # Attempt to parse invalid JSON should raise an exception
        invalid_json = "invalid json"
        with self.assertRaises(
            json.JSONDecodeError,
            msg="Expected error while unmarshalling invalid MasterSettings JSON",
        ):
            json.loads(invalid_json)

    def test_from_dict_default_values(self):
        """Test MasterSettings.from_dict with missing keys uses default values."""
        # Empty dictionary should use default values
        empty_dict: Dict[str, Any] = {}
        settings = MasterSettings.from_dict(empty_dict)
        self.assertEqual(settings.retention_archive, 30)
        self.assertEqual(settings.master_poll_interval, 30.0)
        self.assertEqual(settings.lock_timeout_minutes, 5)

        # Partial dictionary should use defaults for missing keys
        partial_dict = {"retention_archive": 60}
        settings = MasterSettings.from_dict(partial_dict)

        self.assertEqual(settings.retention_archive, 60)
        self.assertEqual(settings.master_poll_interval, 30.0)
        self.assertEqual(settings.lock_timeout_minutes, 5)

    def test_from_dict_custom_values(self):
        """Test MasterSettings.from_dict with custom values."""
        custom_dict: Dict[str, Any] = {
            "retention_archive": 90,
            "master_poll_interval": 45.5,
            "lock_timeout_minutes": 10,
        }
        settings = MasterSettings.from_dict(custom_dict)
        self.assertEqual(settings.retention_archive, 90)
        self.assertEqual(settings.master_poll_interval, 45.5)
        self.assertEqual(settings.lock_timeout_minutes, 10)


class TestMaster(unittest.TestCase):
    """Test cases for Master model."""

    def test_to_dict_serialization(self):
        """Test Master.to_dict serialization."""
        master = Master(
            id=1,
            worker_id=123,
            worker_rid=None,
            settings=MasterSettings(
                retention_archive=45, master_poll_interval=60.0, lock_timeout_minutes=8
            ),
        )
        result_dict = master.to_dict()
        self.assertIsInstance(result_dict, dict)
        self.assertEqual(result_dict["id"], 1)
        self.assertEqual(result_dict["worker_id"], 123)
        self.assertIsNone(result_dict["worker_rid"])
        self.assertEqual(result_dict["settings"]["retention_archive"], 45)
        self.assertEqual(result_dict["settings"]["master_poll_interval"], 60.0)
        self.assertEqual(result_dict["settings"]["lock_timeout_minutes"], 8)

    def test_from_row_with_output_prefixed_columns(self):
        """Test Master.from_row with output_* prefixed columns."""
        row_data: Dict[str, Any] = {
            "output_id": 2,
            "output_worker_id": 456,
            "output_worker_rid": None,
            "output_settings": '{"retention_archive": 60, "master_poll_interval": 90.0, "lock_timeout_minutes": 12}',
            "output_created_at": None,
            "output_updated_at": None,
        }

        master = Master.from_row(row_data)
        self.assertEqual(master.id, 2)
        self.assertEqual(master.worker_id, 456)
        self.assertIsNone(master.worker_rid)
        self.assertEqual(master.settings.retention_archive, 60)
        self.assertEqual(master.settings.master_poll_interval, 90.0)
        self.assertEqual(master.settings.lock_timeout_minutes, 12)

    def test_from_row_with_empty_data(self):
        """Test Master.from_row with empty data uses defaults."""
        empty_row: Dict[str, Any] = {}
        master = Master.from_row(empty_row)

        self.assertEqual(master.id, 1)
        self.assertEqual(master.worker_id, 0)
        self.assertIsNone(master.worker_rid)
        self.assertEqual(master.settings.retention_archive, 30)


if __name__ == "__main__":
    unittest.main()
