"""
Simple example demonstrating the Queuer library with testcontainers.
Shows basic task creation, job processing, and waiting for completion.
"""

import asyncio
import time
from typing import Dict, Any
import logging
import sys
import os

# Add the parent directory to the path to import from the queuer module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import the queuer and related components
from queuer import new_queuer_with_db
from helper.test_database import DatabaseTestMixin


def example_task(job_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simple example task that processes data.
    
    Args:
        job_data: Dictionary containing job parameters
        
    Returns:
        Dictionary with processing results
    """
    message = job_data.get('message', 'Hello')
    number = job_data.get('number', 0)
    
    print(f"Processing: {message} with number {number}")
    
    # Simulate some work
    time.sleep(1)
    
    result = {
        "status": "completed",
        "processed_message": f"Processed: {message}",
        "calculated_value": number * 2,
        "timestamp": time.time()
    }
    
    print(f"Task completed: {result}")
    return result


class QueuerExample(DatabaseTestMixin):
    """Example using DatabaseTestMixin for testcontainers integration."""
    
    def __init__(self):
        """Initialize the example with database setup."""
        self.setup_class()
        self.setup_method()
        
    def cleanup(self):
        """Clean up database resources."""
        self.teardown_method()
        self.teardown_class()
        
    async def run_example(self):
        """Run the main example demonstration."""
        print("=== Simple Queuer Example ===")
        
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        
        # Create a new queuer with the testcontainers database
        print("1. Creating queuer with database...")
        queuer = new_queuer_with_db(
            name="example_queuer",
            max_concurrency=2,
            encryption_key="",
            db_config=self.db.config,
            options=None
        )
        print(f"   Queuer created: {queuer.name}")
        
        # Add the task
        print("2. Adding task...")
        queuer.add_task(example_task)
        print(f"   Task added: example_task")
        
        # Start the queuer
        print("3. Starting queuer...")
        queuer.start()
        print("   Queuer is now running")
        
        # Add a job
        print("4. Adding job...")
        job_data = {"message": "Hello World", "number": 42}
        job = queuer.add_job(example_task, job_data)
        print(f"   Added job with RID: {job.rid}")
        
        # Wait for the job to finish
        print("5. Waiting for job to complete...")
        finished_job = await queuer.wait_for_job_finished(job.rid)
        
        if finished_job:
            print(f"   Job completed with status: {finished_job.status.name}")
            if finished_job.results:
                print(f"   Results: {finished_job.results}")
        else:
            print("   Job did not complete (cancelled or timeout)")
        
        print("6. Example completed!")
        return finished_job


async def main():
    """Main function."""
    example = QueuerExample()
    try:
        result = await example.run_example()
        if result:
            print(f"\n✅ Successfully processed job: {result.task_name}")
        else:
            print(f"\n❌ Job processing failed or was cancelled")
    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        example.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
