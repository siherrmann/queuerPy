"""
Test cases for the retryer component.

These tests verify retry functionality with various backoff strategies.
"""

import time
import pytest
from unittest.mock import Mock, call
from core.retryer import Retryer
from model.options_on_error import OnError, RetryBackoff


class TestRetryer:
    """Test cases for simplified Retryer."""
    
    def test_successful_execution(self):
        """Test successful function execution without retries."""
        mock_function = Mock()
        options = OnError(max_retries=3, retry_delay=0.1, retry_backoff=RetryBackoff.NONE)
        
        retryer = Retryer(mock_function, options)
        result = retryer.retry()
        
        assert result is None
        mock_function.assert_called_once()
    
    def test_retry_with_eventual_success(self):
        """Test retry mechanism with eventual success."""
        call_count = 0
        
        def failing_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception(f"Failure {call_count}")
        
        options = OnError(max_retries=5, retry_delay=0.01, retry_backoff=RetryBackoff.NONE)
        retryer = Retryer(failing_function, options)
        result = retryer.retry()
        
        assert result is None
        assert call_count == 3
    
    def test_retry_with_permanent_failure(self):
        """Test retry mechanism with permanent failure."""
        mock_function = Mock(side_effect=Exception("Persistent error"))
        options = OnError(max_retries=3, retry_delay=0.01, retry_backoff=RetryBackoff.NONE)
        
        retryer = Retryer(mock_function, options)
        result = retryer.retry()
        
        assert result is not None
        assert isinstance(result, Exception)
        assert str(result) == "Persistent error"
        assert mock_function.call_count == 3
    
    def test_linear_backoff(self):
        """Test linear backoff strategy."""
        call_times = []
        
        def failing_function():
            call_times.append(time.time())
            raise Exception("Always fails")
        
        options = OnError(max_retries=3, retry_delay=0.1, retry_backoff=RetryBackoff.LINEAR)
        retryer = Retryer(failing_function, options)
        
        start_time = time.time()
        retryer.retry()
        total_time = time.time() - start_time
        
        # Should have 3 calls with linear delays: 0.1, 0.2 (total ~0.3s + execution time)
        assert len(call_times) == 3
        assert total_time >= 0.3  # At least the sum of delays
    
    def test_exponential_backoff(self):
        """Test exponential backoff strategy."""
        call_times = []
        
        def failing_function():
            call_times.append(time.time())
            raise Exception("Always fails")
        
        options = OnError(max_retries=3, retry_delay=0.05, retry_backoff=RetryBackoff.EXPONENTIAL)
        retryer = Retryer(failing_function, options)
        
        start_time = time.time()
        retryer.retry()
        total_time = time.time() - start_time
        
        # Should have 3 calls with exponential delays: 0.05, 0.1 (total ~0.15s + execution time)
        assert len(call_times) == 3
        assert total_time >= 0.15  # At least the sum of delays
    
    def test_invalid_options(self):
        """Test that invalid options raise ValueError."""
        with pytest.raises(ValueError, match="No valid retry options provided"):
            Retryer(lambda: None, None)
        
        invalid_options = OnError(max_retries=0, retry_delay=0.1, retry_backoff=RetryBackoff.NONE)
        with pytest.raises(ValueError, match="No valid retry options provided"):
            Retryer(lambda: None, invalid_options)
        
        # Test negative delay - this should fail at OnError creation time
        with pytest.raises(ValueError, match="retry delay cannot be negative"):
            OnError(max_retries=3, retry_delay=-1, retry_backoff=RetryBackoff.NONE)
    
    def test_no_backoff_strategy(self):
        """Test no backoff strategy (immediate retries)."""
        call_times = []
        
        def failing_function():
            call_times.append(time.time())
            raise Exception("Always fails")
        
        options = OnError(max_retries=3, retry_delay=0.1, retry_backoff=RetryBackoff.NONE)
        retryer = Retryer(failing_function, options)
        
        start_time = time.time()
        retryer.retry()
        total_time = time.time() - start_time
        
        # Should have 3 calls with constant delay: 0.1, 0.1 (total ~0.2s + execution time)
        assert len(call_times) == 3
        assert total_time >= 0.2  # At least the sum of delays
