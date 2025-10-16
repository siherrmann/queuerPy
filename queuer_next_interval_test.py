"""
Tests for next interval function-related methods in the Python queuer implementation.
Mirrors Go's queuerNextInterval_test.go functionality.
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
from queuer_next_interval import QueuerNextIntervalMixin


def mock_next_interval_func_1(start: datetime, current_count: int) -> datetime:
    """Mock next interval function for testing."""
    return start + timedelta(hours=current_count)


def mock_next_interval_func_2(start: datetime, current_count: int) -> datetime:
    """Another mock next interval function for testing."""
    return start + timedelta(hours=current_count)


class MockQueuer(QueuerNextIntervalMixin):
    """Mock queuer class for testing next interval functionality."""
    
    def __init__(self):
        self.next_interval_funcs = {}
        self.worker = Mock()
        self.worker.available_next_interval_funcs = []
        self.db_worker = Mock()


class TestQueuerNextInterval:
    """Test cases for queuer next interval functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.queuer = MockQueuer()
        
    def test_add_next_interval_func_success(self):
        """Test successfully adding a next interval function."""
        with patch('helper.task.get_task_name_from_interface') as mock_get_name:
            mock_get_name.return_value = "mock_next_interval_func_1"
            self.queuer.db_worker.update_worker.return_value = self.queuer.worker
            
            result = self.queuer.add_next_interval_func(mock_next_interval_func_1)
            
            assert result == self.queuer.worker
            assert "mock_next_interval_func_1" in self.queuer.next_interval_funcs
            assert "mock_next_interval_func_1" in self.queuer.worker.available_next_interval_funcs
            mock_get_name.assert_called_once_with(mock_next_interval_func_1)
            self.queuer.db_worker.update_worker.assert_called_once_with(self.queuer.worker)
    
    def test_add_next_interval_func_none(self):
        """Test adding None as next interval function."""
        with pytest.raises(ValueError, match="NextIntervalFunc cannot be None"):
            self.queuer.add_next_interval_func(None)
    
    def test_add_next_interval_func_name_error(self):
        """Test error during name extraction."""
        with patch('helper.task.get_task_name_from_interface') as mock_get_name:
            mock_get_name.side_effect = Exception("Name extraction failed")
            
            with pytest.raises(RuntimeError, match="Error getting function name"):
                self.queuer.add_next_interval_func(mock_next_interval_func_1)
    
    def test_add_next_interval_func_already_exists(self):
        """Test adding a next interval function that already exists."""
        self.queuer.worker.available_next_interval_funcs = ["mock_next_interval_func_1"]
        
        with patch('helper.task.get_task_name_from_interface') as mock_get_name:
            mock_get_name.return_value = "mock_next_interval_func_1"
            
            with pytest.raises(RuntimeError, match="NextIntervalFunc already exists"):
                self.queuer.add_next_interval_func(mock_next_interval_func_1)
    
    def test_add_next_interval_func_db_update_error(self):
        """Test error during database update."""
        with patch('helper.task.get_task_name_from_interface') as mock_get_name:
            mock_get_name.return_value = "mock_next_interval_func_1"
            self.queuer.db_worker.update_worker.side_effect = Exception("DB update failed")
            
            with pytest.raises(RuntimeError, match="Error updating worker"):
                self.queuer.add_next_interval_func(mock_next_interval_func_1)
    
    def test_add_next_interval_func_with_name_success(self):
        """Test successfully adding a next interval function with specific name."""
        self.queuer.db_worker.update_worker.return_value = self.queuer.worker
        
        result = self.queuer.add_next_interval_func_with_name(mock_next_interval_func_1, "custom_name")
        
        assert result == self.queuer.worker
        assert "custom_name" in self.queuer.next_interval_funcs
        assert "custom_name" in self.queuer.worker.available_next_interval_funcs
        self.queuer.db_worker.update_worker.assert_called_once_with(self.queuer.worker)
    
    def test_add_next_interval_func_with_name_none(self):
        """Test adding None as next interval function with name."""
        with pytest.raises(ValueError, match="NextIntervalFunc cannot be None"):
            self.queuer.add_next_interval_func_with_name(None, "custom_name")
    
    def test_add_next_interval_func_with_name_already_exists(self):
        """Test adding a next interval function with name that already exists."""
        self.queuer.worker.available_next_interval_funcs = ["custom_name"]
        
        with pytest.raises(RuntimeError, match="NextIntervalFunc with name already exists"):
            self.queuer.add_next_interval_func_with_name(mock_next_interval_func_1, "custom_name")
    
    def test_add_next_interval_func_with_name_db_update_error(self):
        """Test error during database update with named function."""
        self.queuer.db_worker.update_worker.side_effect = Exception("DB update failed")
        
        with pytest.raises(RuntimeError, match="Error updating worker"):
            self.queuer.add_next_interval_func_with_name(mock_next_interval_func_1, "custom_name")
    
    def test_add_multiple_next_interval_funcs(self):
        """Test adding multiple next interval functions."""
        with patch('helper.task.get_task_name_from_interface') as mock_get_name:
            mock_get_name.side_effect = ["func_1", "func_2"]
            self.queuer.db_worker.update_worker.return_value = self.queuer.worker
            
            # Add first function
            self.queuer.add_next_interval_func(mock_next_interval_func_1)
            # Add second function
            self.queuer.add_next_interval_func(mock_next_interval_func_2)
            
            assert "func_1" in self.queuer.next_interval_funcs
            assert "func_2" in self.queuer.next_interval_funcs
            assert "func_1" in self.queuer.worker.available_next_interval_funcs
            assert "func_2" in self.queuer.worker.available_next_interval_funcs
            assert len(self.queuer.worker.available_next_interval_funcs) == 2