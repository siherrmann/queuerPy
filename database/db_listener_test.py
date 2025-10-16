"""
Test module for QueuerListener.
Mirrors Go's database/dbListener_test.go.
"""

import threading
import time
import pytest
from unittest.mock import Mock, MagicMock, patch

from database.db_listener import QueuerListener, new_queuer_db_listener
from helper.database import DatabaseConfiguration


class TestQueuerListener:
    """Test class for QueuerListener functionality."""
    
    def test_new_queuer_db_listener(self):
        """Test creating a new QueuerListener instance."""
        # Mock database configuration
        db_config = MagicMock(spec=DatabaseConfiguration)
        db_config.connection_string.return_value = "postgresql://test"
        
        with patch('database.db_listener.psycopg.connect') as mock_connect:
            mock_connection = MagicMock()
            mock_connect.return_value = mock_connection
            
            listener = new_queuer_db_listener(db_config, "test_channel")
            
            assert listener is not None
            assert listener.channel == "test_channel"
            assert listener.db_config == db_config
            mock_connect.assert_called_once()
    
    def test_listen(self):
        """Test the listen functionality."""
        # Mock database configuration
        db_config = MagicMock(spec=DatabaseConfiguration)
        db_config.connection_string.return_value = "postgresql://test"
        
        with patch('database.db_listener.psycopg.connect') as mock_connect:
            mock_connection = MagicMock()
            mock_connect.return_value = mock_connection
            
            # Mock the notifies method to return an empty deque
            from collections import deque
            mock_connection.notifies.return_value = deque()
            
            listener = QueuerListener(db_config, "test_channel")
            
            # Track if notify function was called
            notify_called = threading.Event()
            received_data = []
            
            def notify_function(data):
                received_data.append(data)
                notify_called.set()
            
            # Start listening
            listener.listen(notify_function)
            
            # Verify the notify function is stored
            assert listener._notify_function is not None
            assert listener._listening is True
            
            # Stop the listener
            listener.stop()
            
            # Verify it has stopped
            assert listener._listening is False
    
    def test_listen_with_timeout(self):
        """Test the listen_with_timeout functionality."""
        # Mock database configuration
        db_config = MagicMock(spec=DatabaseConfiguration)
        db_config.connection_string.return_value = "postgresql://test"
        
        with patch('database.db_listener.psycopg.connect') as mock_connect:
            mock_connection = MagicMock()
            mock_connect.return_value = mock_connection
            
            listener = QueuerListener(db_config, "test_ping_channel")
            
            notify_function = Mock()
            
            # Start listening with short timeout
            listener.listen_with_timeout(notify_function, 0.05)  # 50ms timeout
            
            # Wait for timeout to occur
            time.sleep(0.1)
            
            # The listener should still be running (ping timeout case)
            assert listener._listening is True
            
            # Stop the listener
            listener.stop()
            
            # Verify that the listening has stopped
            assert listener._listening is False
    
    def test_stop(self):
        """Test stopping the listener."""
        # Mock database configuration
        db_config = MagicMock(spec=DatabaseConfiguration)
        db_config.connection_string.return_value = "postgresql://test"
        
        with patch('database.db_listener.psycopg.connect') as mock_connect:
            mock_connection = MagicMock()
            mock_connect.return_value = mock_connection
            
            listener = QueuerListener(db_config, "test_channel")
            
            # Start listening
            listener.listen(lambda x: None)
            
            # Verify it's listening
            assert listener._listening is True
            
            # Stop the listener
            listener.stop()
            
            # Verify it has stopped
            assert listener._listening is False


if __name__ == "__main__":
    pytest.main([__file__])