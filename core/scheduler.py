"""
Scheduler component using asyncio - mirrors Go implementation.
"""

import asyncio
from typing import Callable, Optional
from datetime import datetime, timedelta


class Scheduler:
    """
    Scheduler manages scheduled tasks using goless channels.
    Mirrors the Go Scheduler implementation.
    """
    
    def __init__(self):
        """Initialize a new scheduler."""
        # Asyncio coordination
        self.schedule_queue = asyncio.Queue()
        self._running = False
        self._stopped = False
        self._worker_task: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
    
    def start(self):
        """Start the scheduler worker. Mirrors Go's Start() method."""
        if self._running:
            return  # Already running
        
        self._running = True
        self._stopped = False
        
        # Get or create event loop
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            # No loop running, we need to handle this differently
            # For now, we'll use asyncio.run in a thread
            import threading
            
            def run_in_thread():
                asyncio.run(self._async_worker())
            
            worker_thread = threading.Thread(target=run_in_thread, daemon=True)
            worker_thread.start()
            return
        
        # Start worker task in existing loop
        self._worker_task = self._loop.create_task(self._async_worker())
    
    async def _async_worker(self):
        """Main scheduler worker loop using asyncio."""
        scheduled_tasks = []  # List of (execute_time, task, args, kwargs)
        
        while self._running:
            try:
                # Check for new scheduled tasks (non-blocking)
                try:
                    task_info = self.schedule_queue.get_nowait()
                    if task_info is None:  # Stop signal
                        break
                    scheduled_tasks.append(task_info)
                except asyncio.QueueEmpty:
                    pass
                
                # Execute ready tasks
                current_time = datetime.now()
                ready_tasks = []
                remaining_tasks = []
                
                for task_info in scheduled_tasks:
                    execute_time, task, args, kwargs = task_info
                    if current_time >= execute_time:
                        ready_tasks.append((task, args, kwargs))
                    else:
                        remaining_tasks.append(task_info)
                
                scheduled_tasks = remaining_tasks
                
                # Execute ready tasks as asyncio tasks
                for task, args, kwargs in ready_tasks:
                    if asyncio.iscoroutinefunction(task):
                        asyncio.create_task(task(*args, **kwargs))
                    else:
                        # Run sync function in thread pool
                        loop = asyncio.get_running_loop()
                        loop.run_in_executor(None, lambda: task(*args, **kwargs))
                
                # Cooperative yield
                await asyncio.sleep(0.01)
                
            except Exception:
                # Continue on error, but yield control
                await asyncio.sleep(0.01)
        
        self._stopped = True
    
    def stop(self):
        """Stop the scheduler. Mirrors Go's Stop() method."""
        if not self._running:
            return  # Not running
        
        self._running = False
        
        # Send stop signal
        try:
            if self._loop and not self._loop.is_closed():
                self._loop.call_soon_threadsafe(
                    lambda: asyncio.create_task(self.schedule_queue.put(None))
                )
            else:
                # If no loop, just put None directly (will be handled by worker)
                try:
                    self.schedule_queue.put_nowait(None)
                except:
                    pass
        except:
            pass
        
        # Give time for worker to stop
        import time
        time.sleep(0.1)
    
    def schedule(self, delay: float, task: Callable, *args, **kwargs):
        """
        Schedule a task to run after delay seconds.
        Mirrors Go's Schedule() method.
        """
        if not self._running:
            raise RuntimeError("Scheduler is not running")
        
        execute_time = datetime.now() + timedelta(seconds=delay)
        task_info = (execute_time, task, args, kwargs)
        
        try:
            if self._loop and not self._loop.is_closed():
                self._loop.call_soon_threadsafe(
                    lambda: asyncio.create_task(self.schedule_queue.put(task_info))
                )
            else:
                self.schedule_queue.put_nowait(task_info)
        except:
            raise RuntimeError("Failed to schedule task")
    
    def schedule_at(self, execute_time: datetime, task: Callable, *args, **kwargs):
        """
        Schedule a task to run at a specific time.
        Extension of Go's Schedule() method.
        """
        if not self._running:
            raise RuntimeError("Scheduler is not running")
        
        task_info = (execute_time, task, args, kwargs)
        
        try:
            if self._loop and not self._loop.is_closed():
                self._loop.call_soon_threadsafe(
                    lambda: asyncio.create_task(self.schedule_queue.put(task_info))
                )
            else:
                self.schedule_queue.put_nowait(task_info)
        except:
            raise RuntimeError("Failed to schedule task")
    
    def is_running(self) -> bool:
        """Check if the scheduler is running."""
        return self._running