"""
Tests for listener-related methods in the Python queuer implementation.
Mirrors Go's queuerListener_test.go functionality.
"""

import pytest
from unittest.mock import Mock, AsyncMock
from queuer_listener import QueuerListenerMixin
from model.job import Job


class MockQueuer(QueuerListenerMixin):
    """Mock queuer class for testing listener functionality."""
    
    def __init__(self, running=True):
        self._running = running
        self.job_update_listener = Mock()
        self.job_delete_listener = Mock()
        self.job_insert_listener = Mock()


class TestQueuerListener:
    """Test cases for queuer listener functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.queuer = MockQueuer()
        self.mock_job = Mock()
        self.mock_notify_function = Mock()
        
    @pytest.mark.asyncio
    async def test_listen_for_job_update_success(self):
        """Test successfully listening for job update events."""
        self.queuer.job_update_listener.listen = AsyncMock()
        
        await self.queuer.listen_for_job_update(self.mock_notify_function)
        
        self.queuer.job_update_listener.listen.assert_called_once_with(self.mock_notify_function)
    
    @pytest.mark.asyncio
    async def test_listen_for_job_update_not_running(self):
        """Test listening for job updates when queuer is not running."""
        self.queuer._running = False
        
        with pytest.raises(RuntimeError, match="Cannot listen with uninitialized or not running Queuer"):
            await self.queuer.listen_for_job_update(self.mock_notify_function)
    
    @pytest.mark.asyncio
    async def test_listen_for_job_update_no_listener(self):
        """Test listening for job updates when listener is not initialized."""
        self.queuer.job_update_listener = None
        
        with pytest.raises(RuntimeError, match="Job update listener not initialized"):
            await self.queuer.listen_for_job_update(self.mock_notify_function)
    
    @pytest.mark.asyncio
    async def test_listen_for_job_update_listener_error(self):
        """Test error during job update listening."""
        self.queuer.job_update_listener.listen = AsyncMock(side_effect=Exception("Listener error"))
        
        with pytest.raises(RuntimeError, match="Failed to listen for job updates"):
            await self.queuer.listen_for_job_update(self.mock_notify_function)
    
    @pytest.mark.asyncio
    async def test_listen_for_job_delete_success(self):
        """Test successfully listening for job delete events."""
        self.queuer.job_delete_listener.listen = AsyncMock()
        
        await self.queuer.listen_for_job_delete(self.mock_notify_function)
        
        self.queuer.job_delete_listener.listen.assert_called_once_with(self.mock_notify_function)
    
    @pytest.mark.asyncio
    async def test_listen_for_job_delete_not_running(self):
        """Test listening for job deletes when queuer is not running."""
        self.queuer._running = False
        
        with pytest.raises(RuntimeError, match="Cannot listen with uninitialized or not running Queuer"):
            await self.queuer.listen_for_job_delete(self.mock_notify_function)
    
    @pytest.mark.asyncio
    async def test_listen_for_job_delete_no_listener(self):
        """Test listening for job deletes when listener is not initialized."""
        self.queuer.job_delete_listener = None
        
        with pytest.raises(RuntimeError, match="Job delete listener not initialized"):
            await self.queuer.listen_for_job_delete(self.mock_notify_function)
    
    @pytest.mark.asyncio
    async def test_listen_for_job_delete_listener_error(self):
        """Test error during job delete listening."""
        self.queuer.job_delete_listener.listen = AsyncMock(side_effect=Exception("Listener error"))
        
        with pytest.raises(RuntimeError, match="Failed to listen for job deletes"):
            await self.queuer.listen_for_job_delete(self.mock_notify_function)
    
    @pytest.mark.asyncio
    async def test_listen_for_job_insert_success(self):
        """Test successfully listening for job insert events."""
        self.queuer.job_insert_listener.listen = AsyncMock()
        
        await self.queuer.listen_for_job_insert(self.mock_notify_function)
        
        self.queuer.job_insert_listener.listen.assert_called_once_with(self.mock_notify_function)
    
    @pytest.mark.asyncio
    async def test_listen_for_job_insert_not_running(self):
        """Test listening for job inserts when queuer is not running."""
        self.queuer._running = False
        
        with pytest.raises(RuntimeError, match="Cannot listen with uninitialized or not running Queuer"):
            await self.queuer.listen_for_job_insert(self.mock_notify_function)
    
    @pytest.mark.asyncio
    async def test_listen_for_job_insert_no_listener(self):
        """Test listening for job inserts when listener is not initialized."""
        self.queuer.job_insert_listener = None
        
        with pytest.raises(RuntimeError, match="Job insert listener not initialized"):
            await self.queuer.listen_for_job_insert(self.mock_notify_function)
    
    @pytest.mark.asyncio
    async def test_listen_for_job_insert_listener_error(self):
        """Test error during job insert listening."""
        self.queuer.job_insert_listener.listen = AsyncMock(side_effect=Exception("Listener error"))
        
        with pytest.raises(RuntimeError, match="Failed to listen for job inserts"):
            await self.queuer.listen_for_job_insert(self.mock_notify_function)