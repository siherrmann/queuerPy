"""
Tests for next interval function-related methods in the Python queuer implementation.
Mirrors Go's queuerNextInterval_test.go functionality.
"""

import unittest
import pytest
from datetime import datetime, timedelta

from .model.worker import Worker
from .queuer import new_queuer_with_db
from .helper.test_database import DatabaseTestMixin


def nifunc_1_test(start: datetime, current_count: int) -> datetime:
    """Global next interval function for testing."""
    return start + timedelta(hours=current_count)


def nifunc_2_test(start: datetime, current_count: int) -> datetime:
    """Another global next interval function for testing."""
    return start + timedelta(hours=current_count * 2)


class TestQueuerNextInterval(DatabaseTestMixin, unittest.TestCase):
    """Test cases for queuer next interval functionality."""

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

    def test_add_next_interval_func_success(self):
        """Test successfully adding a next interval function."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        result: Worker = queuer.add_next_interval_func(nifunc_1_test)

        self.assertIsNotNone(queuer.worker)
        self.assertEqual(result.id, queuer.worker.id)
        self.assertIn("nifunc_1_test", queuer.next_interval_funcs)
        self.assertIn("nifunc_1_test", queuer.worker.available_next_interval_funcs)

        queuer.stop()

    def test_add_next_interval_func_already_exists(self):
        """Test adding a next interval function that already exists."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_next_interval_func(nifunc_1_test)

        with pytest.raises(RuntimeError, match="NextIntervalFunc already exists"):
            queuer.add_next_interval_func(nifunc_1_test)

        queuer.stop()

    def test_add_next_interval_func_with_name_success(self):
        """Test successfully adding a next interval function with specific name."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        result = queuer.add_next_interval_func_with_name(nifunc_1_test, "custom_name")

        self.assertIsNotNone(queuer)
        self.assertEqual(result.id, queuer.worker.id)
        self.assertIn("custom_name", queuer.next_interval_funcs)
        self.assertIn("custom_name", queuer.worker.available_next_interval_funcs)

        queuer.stop()

    def test_add_next_interval_func_with_name_already_exists(self):
        """Test adding a next interval function with name that already exists."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_next_interval_func_with_name(nifunc_1_test, "custom_name")

        with pytest.raises(
            RuntimeError, match="NextIntervalFunc with name already exists"
        ):
            queuer.add_next_interval_func_with_name(nifunc_2_test, "custom_name")

        queuer.stop()

    def test_add_multiple_next_interval_funcs(self):
        """Test adding multiple next interval functions."""
        queuer = new_queuer_with_db("test_queuer", 10, "", self.db_config)
        queuer.add_next_interval_func(nifunc_1_test)
        queuer.add_next_interval_func(nifunc_2_test)

        self.assertIn("nifunc_1_test", queuer.next_interval_funcs)
        self.assertIn("nifunc_2_test", queuer.next_interval_funcs)
        self.assertIn(
            "nifunc_1_test",
            queuer.worker.available_next_interval_funcs,
        )
        self.assertIn(
            "nifunc_2_test",
            queuer.worker.available_next_interval_funcs,
        )
        self.assertEqual(len(queuer.worker.available_next_interval_funcs), 2)

        queuer.stop()
