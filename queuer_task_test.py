"""
Tests for task-related methods in the Python queuer implementation.
Mirrors Go's queuerTask_test.go functionality.
"""

import pytest
from unittest.mock import Mock, patch
from queuer_task import QueuerTaskMixin
from model.task import Task


class MockQueuer(QueuerTaskMixin):
    """Mock queuer class for testing task functionality."""
    
    def __init__(self):
        self.tasks = {}
        self.worker = Mock()
        self.worker.available_tasks = []
        self.db_worker = Mock()


class TestQueuerTask:
    """Test cases for queuer task functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.queuer = MockQueuer()
        self.mock_task = Mock()
        self.mock_task.name = "test_task"
        
    def test_add_task_success(self):
        """Test successfully adding a task."""
        # Mock the task creation
        with patch('model.task.new_task') as mock_new_task:
            mock_new_task.return_value = self.mock_task
            self.queuer.db_worker.update_worker.return_value = self.queuer.worker
            
            def test_function():
                pass
            
            result = self.queuer.add_task(test_function)
            
            assert result == self.mock_task
            assert "test_task" in self.queuer.tasks
            assert "test_task" in self.queuer.worker.available_tasks
            mock_new_task.assert_called_once_with(test_function)
            self.queuer.db_worker.update_worker.assert_called_once_with(self.queuer.worker)
    
    def test_add_task_creation_error(self):
        """Test error during task creation."""
        with patch('model.task.new_task') as mock_new_task:
            mock_new_task.side_effect = Exception("Task creation failed")
            
            def test_function():
                pass
            
            with pytest.raises(RuntimeError, match="Error creating new task"):
                self.queuer.add_task(test_function)
    
    def test_add_task_already_exists(self):
        """Test adding a task that already exists."""
        self.queuer.worker.available_tasks = ["test_task"]
        
        with patch('model.task.new_task') as mock_new_task:
            mock_new_task.return_value = self.mock_task
            
            def test_function():
                pass
            
            with pytest.raises(RuntimeError, match="Task already exists"):
                self.queuer.add_task(test_function)
    
    def test_add_task_db_update_error(self):
        """Test error during database update."""
        with patch('model.task.new_task') as mock_new_task:
            mock_new_task.return_value = self.mock_task
            self.queuer.db_worker.update_worker.side_effect = Exception("DB update failed")
            
            def test_function():
                pass
            
            with pytest.raises(RuntimeError, match="Error updating worker"):
                self.queuer.add_task(test_function)
    
    def test_add_task_with_name_success(self):
        """Test successfully adding a task with a specific name."""
        custom_task = Mock()
        custom_task.name = "custom_name"
        
        with patch('model.task.new_task_with_name') as mock_new_task_with_name:
            mock_new_task_with_name.return_value = custom_task
            self.queuer.db_worker.update_worker.return_value = self.queuer.worker
            
            def test_function():
                pass
            
            result = self.queuer.add_task_with_name(test_function, "custom_name")
            
            assert result == custom_task
            assert "custom_name" in self.queuer.tasks
            assert "custom_name" in self.queuer.worker.available_tasks
            mock_new_task_with_name.assert_called_once_with(test_function, "custom_name")
            self.queuer.db_worker.update_worker.assert_called_once_with(self.queuer.worker)
    
    def test_add_task_with_name_creation_error(self):
        """Test error during task creation with name."""
        with patch('model.task.new_task_with_name') as mock_new_task_with_name:
            mock_new_task_with_name.side_effect = Exception("Task creation failed")
            
            def test_function():
                pass
            
            with pytest.raises(RuntimeError, match="Error creating new task"):
                self.queuer.add_task_with_name(test_function, "custom_name")
    
    def test_add_task_with_name_already_exists(self):
        """Test adding a task with name that already exists."""
        self.queuer.worker.available_tasks = ["custom_name"]
        
        with patch('model.task.new_task_with_name') as mock_new_task_with_name:
            mock_new_task_with_name.return_value = self.mock_task
            
            def test_function():
                pass
            
            with pytest.raises(RuntimeError, match="Task already exists"):
                self.queuer.add_task_with_name(test_function, "custom_name")
    
    def test_add_task_with_name_db_update_error(self):
        """Test error during database update with named task."""
        custom_task = Mock()
        custom_task.name = "custom_name"
        
        with patch('model.task.new_task_with_name') as mock_new_task_with_name:
            mock_new_task_with_name.return_value = custom_task
            self.queuer.db_worker.update_worker.side_effect = Exception("DB update failed")
            
            def test_function():
                pass
            
            with pytest.raises(RuntimeError, match="Error updating worker"):
                self.queuer.add_task_with_name(test_function, "custom_name")