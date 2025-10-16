"""
Python queuer example - Workflow demonstration with testcontainers.
Shows real database integration and task processing.
"""

import time
import logging
import sys
from pathlib import Path
from typing import Any, Dict

# Add parent directory to path to allow imports
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def short_task(job_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Short running example task function.
    """
    # Extract parameters from job data
    param1 = job_data.get('param1', 0)
    param2 = job_data.get('param2', '0')
    
    # Simulate some work
    time.sleep(1)
    
    # Example error handling
    try:
        param2_int = int(param2)
    except ValueError as e:
        raise ValueError(f"Cannot convert param2 '{param2}' to int") from e
    
    result = param1 + param2_int
    logger.info(f"short_task completed: {param1} + {param2_int} = {result}")
    
    return {
        "status": "completed",
        "result": result,
        "original_params": {"param1": param1, "param2": param2}
    }


def long_task(job_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Long running example task function.
    """
    # Extract parameters from job data
    param1 = job_data.get('param1', 0)
    param2 = job_data.get('param2', '0')
    
    # Simulate longer work
    time.sleep(3)  # Reduced from 5 to 3 for example
    
    # Example error handling
    try:
        param2_int = int(param2)
    except ValueError as e:
        raise ValueError(f"Cannot convert param2 '{param2}' to int") from e
    
    result = param1 + param2_int
    logger.info(f"long_task completed: {param1} + {param2_int} = {result}")
    
    return {
        "status": "completed", 
        "result": result,
        "original_params": {"param1": param1, "param2": param2}
    }


class WorkflowExample:
    """Example using DatabaseTestMixin for testcontainers integration."""
    
    def __init__(self):
        """Initialize the workflow example."""
        # Import here to avoid circular imports
        from helper.test_database import DatabaseTestMixin
        
        # Create a mixin instance and set up database
        self.db_mixin = DatabaseTestMixin()
        self.db_mixin.setup_class()
        self.db_mixin.setup_method()
        
    def cleanup(self):
        """Clean up resources."""
        self.db_mixin.teardown_method()
        self.db_mixin.teardown_class()
        
    def example_easy(self):
        """
        Simple queuer example with database integration.
        """
        logger.info("Starting example_easy...")
        
        # Import queuer components
        from queuer import new_queuer
        from model.task import new_task
        from database.db_job import JobDBHandler
        from database.db_worker import WorkerDBHandler
        
        # Create a new queuer instance
        q = new_queuer("exampleEasyWorker", 3)
        logger.info(f"Created queuer: {q.name} with max_concurrency: {q.max_concurrency}")
        
        # Set up database handlers with test database
        q.db_job = JobDBHandler(self.db_mixin.db, with_table_drop=True)
        q.db_worker = WorkerDBHandler(self.db_mixin.db, with_table_drop=True)
        
        # Insert the worker into the database
        q.worker = q.db_worker.insert_worker(q.worker)
        
        # Add a short task to the queuer
        task = new_task("short_task", short_task)
        q.add_task(task)
        logger.info(f"Added task: {task.name}")
        
        # Start the queuer
        q.start()
        logger.info("Queuer started")
        
        try:
            # Add a job to the queue
            job_data = {"param1": 5, "param2": "12"}
            job_id = q.add_job("short_task", job_data)
            logger.info(f"Added job with ID: {job_id}")
            
            # Let the job process
            time.sleep(3)
            
            # Check if job completed
            logger.info("Job processing completed")
        
        finally:
            # Stop the queuer gracefully
            q.stop()
            logger.info("Queuer stopped")
        
        logger.info("example_easy completed")
    
    def example_multiple_jobs(self):
        """
        Example with multiple jobs running concurrently.
        """
        logger.info("Starting example_multiple_jobs...")
        
        # Import queuer components
        from queuer import new_queuer
        from model.task import new_task
        from database.db_job import JobDBHandler
        from database.db_worker import WorkerDBHandler
        
        # Create queuer with higher concurrency
        q = new_queuer("multiJobWorker", 3)
        
        # Set up database handlers with test database
        q.db_job = JobDBHandler(self.db_mixin.db, with_table_drop=True)
        q.db_worker = WorkerDBHandler(self.db_mixin.db, with_table_drop=True)
        
        # Insert the worker into the database
        q.worker = q.db_worker.insert_worker(q.worker)
        
        # Add both tasks
        short_task_obj = new_task("short_task", short_task)
        long_task_obj = new_task("long_task", long_task)
        q.add_task(short_task_obj)
        q.add_task(long_task_obj)
        
        q.start()
        
        try:
            # Add multiple jobs
            job_data1 = {"param1": 10, "param2": "5"}
            job_data2 = {"param1": 20, "param2": "8"}
            job_data3 = {"param1": 100, "param2": "50"}
            
            job_id1 = q.add_job("short_task", job_data1)
            job_id2 = q.add_job("short_task", job_data2)
            job_id3 = q.add_job("long_task", job_data3)
            
            logger.info(f"Added 3 jobs: {job_id1}, {job_id2}, {job_id3}")
            
            # Wait for all jobs to complete
            time.sleep(8)  # Enough time for all jobs including the long one
            logger.info("All jobs processing completed")
        
        finally:
            q.stop()
        
        logger.info("example_multiple_jobs completed")
    
    def example_with_database(self):
        """
        Example that tests the full database workflow.
        """
        logger.info("Starting example_with_database...")
        
        # Import database components
        from database.db_job import JobDBHandler
        from database.db_worker import WorkerDBHandler
        from model.job import Job, JobStatus, new_job
        from model.worker import Worker, WorkerStatus, new_worker
        from uuid import uuid4
        
        logger.info("Database connected")
        
        # Create database handlers
        job_handler = JobDBHandler(self.db_mixin.db, with_table_drop=True)
        worker_handler = WorkerDBHandler(self.db_mixin.db, with_table_drop=True)
        
        # Create a worker
        worker = new_worker("example-worker")
        worker.available_tasks = ['short_task', 'long_task']
        worker.max_concurrency = 2
        
        created_worker = worker_handler.insert_worker(worker)
        logger.info(f"Created worker: {created_worker.name} with ID: {created_worker.id}")
        
        # Create some jobs
        job1 = new_job("short_task", {"param1": 10, "param2": "5"})
        job2 = new_job("long_task", {"param1": 100, "param2": "25"})
        
        # Insert jobs
        created_job1 = job_handler.insert_job(job1)
        created_job2 = job_handler.insert_job(job2)
        
        logger.info(f"Created job1: {created_job1.task_name} with ID: {created_job1.id}")
        logger.info(f"Created job2: {created_job2.task_name} with ID: {created_job2.id}")
        
        # Test job completion manually
        created_job1.status = JobStatus.SUCCEEDED
        created_job1.results = [{"result": 15}]  # Example result
        
        # Use update_job method
        updated_job = job_handler.update_job(created_job1)
        logger.info(f"Completed job: {updated_job.task_name} with status: {updated_job.status}")
        
        # Check worker and job counts
        all_workers = worker_handler.select_all_workers(0, 10)
        all_jobs = job_handler.select_all_jobs(0, 10)
        
        logger.info(f"Total workers in database: {len(all_workers)}")
        logger.info(f"Total jobs in database: {len(all_jobs)}")
        
        # Test basic database functions
        selected_worker = worker_handler.select_worker(created_worker.rid)
        logger.info(f"Selected worker by RID: {selected_worker.name if selected_worker else 'Not found'}")
        
        selected_job = job_handler.select_job(created_job1.rid)
        logger.info(f"Selected job by RID: {selected_job.task_name if selected_job else 'Not found'}")
        
        logger.info("example_with_database completed")


def main():
    """
    Main function that runs all examples.
    """
    logger.info("🧪 Starting Python queuer workflow examples...")
    
    example = WorkflowExample()
    
    examples = [
        ("Easy Example", example.example_easy),
        ("Multiple Jobs Example", example.example_multiple_jobs),
        ("Database Workflow Example", example.example_with_database),
    ]
    
    try:
        for name, example_func in examples:
            logger.info(f"\n{'='*50}")
            logger.info(f"Running: {name}")
            logger.info(f"{'='*50}")
            
            try:
                example_func()
                logger.info(f"✅ {name} completed successfully")
            except Exception as e:
                logger.error(f"❌ {name} failed: {e}")
                import traceback
                traceback.print_exc()
    
    finally:
        example.cleanup()
    
    logger.info("\n🎉 All Python queuer workflow examples finished!")


if __name__ == "__main__":
    # Run the examples
    main()