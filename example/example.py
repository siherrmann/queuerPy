"""
Simple example demonstrating the Queuer library matching the Go example structure.
Shows basic task creation, job processing, and waiting for completion.
"""

import asyncio
import logging
import time
import sys
import os

# Add the parent directory to the path to import from the queuer module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Import the queuer and related components
from queuer import Queuer, new_queuer_with_db
from helper.test_database import DatabaseTestMixin


def short_task(param1: int, param2: str) -> int:
    """
    Short running example task function.
    Mirrors the Go ShortTask function exactly.

    Args:
        param1: Integer parameter
        param2: String parameter that should be convertible to int

    Returns:
        Sum of param1 and param2 (converted to int)

    Raises:
        ValueError: If param2 cannot be converted to int
    """
    # Simulate some work
    time.sleep(param1)

    # Example for some error handling
    try:
        param2_int = int(param2)
    except ValueError as e:
        raise ValueError(f"Cannot convert param2 '{param2}' to int") from e

    return param1 + param2_int


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
        """Run the main example demonstration matching Go structure."""
        try:
            # Create a new queuer instance
            q: Queuer = new_queuer_with_db(
                name="exampleEasyWorker",
                max_concurrency=3,
                encryption_key=None,
                db_config=self.db.config,
            )
            q.add_task(short_task)
            q.start()

            job = q.add_job(short_task, 1, "12")
            if not job:
                logging.error("Error adding job")
                return None

            logging.info(f"Job created: {job.rid}")

            # Use the proper wait method with a longer timeout
            job = q.wait_for_job_finished(job.rid, timeout_seconds=30.0)

            if job:
                logging.info(
                    f"Job completed with status: {job.status} and results: {job.results}"
                )
            else:
                logging.error("Job timed out or failed to complete")

            logging.info("Exiting...")
            q.stop()
            return job

        except Exception as e:
            logging.error(f"Error in example: {e}")
            raise


async def main():
    """Main function."""
    # Configure logging to match Go's log output style
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    example = QueuerExample()
    try:
        await example.run_example()
    except Exception as e:
        logging.error(f"Error in example: {e}")
    finally:
        example.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
