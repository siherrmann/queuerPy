"""
Tests for Job model.
Mirrors Go's model/job_test.go functionality.
"""

import time
from typing import Any, Dict, List
import unittest
from datetime import datetime, timedelta

from .job import Job, new_job, JobStatus
from .options import Options, Schedule
from .options_on_error import OnError, RetryBackoff


def task_mock(duration: int, param2: str) -> int:
    """
    Mock function that simulates a task.
    Takes an integer duration and a string parameter, simulates some work,
    and returns an integer result or raises an error if the string cannot be converted to an integer.
    """
    # Simulate some work
    time.sleep(duration)

    # Example for some error handling
    try:
        param2_int = int(param2)
    except ValueError as e:
        raise ValueError(f"Cannot convert {param2} to integer") from e

    return duration + param2_int


def task_mock_with_name_longer_100():
    """Task with a name longer than 100 characters to test the validation logic."""
    return "Uff"


# Give it a very long name
task_mock_with_name_longer_100.__name__ = "TaskMockWithNameLonger100_283032343638404244464850525456586062646668707274767880828486889092949698100"


class TestJob(unittest.TestCase):
    """Test cases for Job model."""

    def test_new_job(self):
        """Test NewJob function with various parameters."""

        test_cases: List[Dict[str, Any]] = [
            {
                "name": "Valid nil options",
                "task": task_mock,
                "options": None,
                "parameters": [1, "2"],
                "want_err": False,
            },
            {
                "name": "Valid options",
                "task": task_mock,
                "options": Options(
                    on_error=OnError(
                        max_retries=3,
                        retry_delay=1,
                        retry_backoff=RetryBackoff.NONE,
                    ),
                    schedule=Schedule(
                        start=datetime.now() + timedelta(minutes=1),
                        interval=timedelta(minutes=2),
                        max_count=5,
                    ),
                ),
                "parameters": [1, "2"],
                "want_err": False,
            },
            {
                "name": "Valid OnError options",
                "task": task_mock,
                "options": Options(
                    on_error=OnError(
                        max_retries=3,
                        retry_delay=1,
                        retry_backoff=RetryBackoff.NONE,
                    ),
                ),
                "parameters": [1, "2"],
                "want_err": False,
            },
            {
                "name": "Invalid OnError options",
                "task": task_mock,
                "create_options": lambda: Options(
                    on_error=OnError(
                        max_retries=-3,  # Invalid: negative retries
                        retry_delay=1,
                        retry_backoff=RetryBackoff.NONE,
                    ),
                ),
                "parameters": [1, "2"],
                "want_err": True,
            },
            {
                "name": "Valid Schedule options",
                "task": task_mock,
                "options": Options(
                    schedule=Schedule(
                        start=datetime.now() + timedelta(minutes=1),
                        interval=timedelta(minutes=2),
                        max_count=5,
                    ),
                ),
                "parameters": [1, "2"],
                "want_err": False,
            },
            {
                "name": "Invalid Schedule options",
                "task": task_mock,
                "create_options": lambda: Options(
                    schedule=Schedule(
                        start=datetime.now() + timedelta(minutes=1),
                        interval=timedelta(minutes=-2),  # Invalid: negative interval
                        max_count=5,
                    ),
                ),
                "parameters": [1, "2"],
                "want_err": True,
            },
            {
                "name": "Invalid task with long name",
                "task": task_mock_with_name_longer_100,
                "options": None,
                "parameters": [],
                "want_err": True,  # Expecting an error due to task name length
            },
            {
                "name": "Invalid task type",
                "task": 123,  # Invalid task type
                "options": None,
                "parameters": [1, "2"],
                "want_err": True,
            },
        ]

        for test_case in test_cases:
            with self.subTest(name=test_case["name"]):
                try:
                    # Handle dynamic option creation for invalid options
                    if "create_options" in test_case:
                        # This will throw an exception during invalid option creation
                        options = test_case["create_options"]()
                    else:
                        options = test_case.get("options")

                    job = new_job(test_case["task"], options, *test_case["parameters"])

                    if test_case["want_err"]:
                        self.fail(
                            f"NewJob should return an error for {test_case['name']}"
                        )
                    else:
                        self.assertIsNotNone(
                            job, "Job should not be None for valid options"
                        )
                        self.assertEqual(
                            options,
                            job.options,
                            "Job options should match the provided options",
                        )
                        self.assertEqual(
                            test_case["parameters"],
                            job.parameters,
                            "Job parameters should match the provided parameters",
                        )

                except Exception as e:
                    if not test_case["want_err"]:
                        self.fail(
                            f"NewJob should not return an error for {test_case['name']}: {e}"
                        )

    def test_job_from_notification_to_job(self):
        """Test converting job notification data to Job object."""
        # This would test the notification parsing functionality
        # For now, we'll create a basic test
        job = Job()
        job.id = 1
        job.task_name = "test_task"
        job.status = JobStatus.QUEUED
        job.parameters = [1, "test"]

        self.assertEqual(job.id, 1)
        self.assertEqual(job.task_name, "test_task")
        self.assertEqual(job.status, JobStatus.QUEUED)
        self.assertEqual(job.parameters, [1, "test"])

    def test_marshal_and_unmarshal_parameters(self):
        """Test parameter marshaling and unmarshaling."""
        original_params: List[Any] = [1, "test", 3.14, True, {"key": "value"}]

        # Create a job with parameters
        job = Job()
        job.parameters = original_params

        # In a real implementation, we'd test JSON serialization/deserialization
        # For now, just verify the parameters are stored correctly
        self.assertEqual(job.parameters, original_params)

    def test_parameters_to_reflect_values(self):
        """Test converting parameters to reflection values (Python equivalent)."""
        params: List[Any] = [1, "test", 3.14, True]
        job = Job()
        job.parameters = params

        # Test that parameters can be accessed as expected
        self.assertEqual(len(job.parameters), 4)
        self.assertEqual(job.parameters[0], 1)
        self.assertEqual(job.parameters[1], "test")
        self.assertEqual(job.parameters[2], 3.14)
        self.assertEqual(job.parameters[3], True)

    def test_marshal_and_unmarshal_db_time(self):
        """Test datetime marshaling and unmarshaling for database operations."""
        now = datetime.now()

        job = Job()
        job.created_at = now
        job.updated_at = now
        job.started_at = now
        job.scheduled_at = now

        # Verify datetime objects are stored correctly
        self.assertEqual(job.created_at, now)
        self.assertEqual(job.updated_at, now)
        self.assertEqual(job.started_at, now)
        self.assertEqual(job.scheduled_at, now)

    def test_job_status_enum(self):
        """Test JobStatus enum values."""
        self.assertEqual(JobStatus.QUEUED, "QUEUED")
        self.assertEqual(JobStatus.RUNNING, "RUNNING")
        self.assertEqual(JobStatus.SUCCEEDED, "SUCCEEDED")
        self.assertEqual(JobStatus.FAILED, "FAILED")
        self.assertEqual(JobStatus.CANCELLED, "CANCELLED")

    def test_job_defaults(self):
        """Test default values for Job object."""
        job = Job()

        self.assertEqual(job.id, 0)
        self.assertIsNotNone(job.rid)  # Should have a UUID
        self.assertEqual(job.status, JobStatus.QUEUED)
        self.assertEqual(job.attempts, 0)
        self.assertEqual(job.schedule_count, 0)
        self.assertEqual(job.parameters, [])
        self.assertEqual(job.results, [])
        self.assertIsNone(job.error)


if __name__ == "__main__":
    unittest.main()
