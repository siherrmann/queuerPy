"""
Tests for Worker model.
Mirrors Go's model/worker_test.go functionality.
"""

from typing import Any, Dict, List
import unittest

from .worker import Worker, new_worker, new_worker_with_options, WorkerStatus
from .options_on_error import OnError, RetryBackoff


class TestWorker(unittest.TestCase):
    """Test cases for Worker model."""

    def test_new_worker(self):
        """Test NewWorker function with various parameters."""

        test_cases: List[Dict[str, Any]] = [
            {
                "name": "Valid worker",
                "worker_name": "ExampleWorker",
                "max_concurrency": 5,
                "want_err": False,
            },
            {
                "name": "Invalid worker name (empty)",
                "worker_name": "",
                "max_concurrency": 5,
                "want_err": True,
            },
            {
                "name": "Invalid worker name (too long)",
                "worker_name": "WorkerMockWithNameLonger100_3032343638404244464850525456586062646668707274767880828486889092949698100",
                "max_concurrency": 5,
                "want_err": True,
            },
            {
                "name": "Invalid max concurrency (too low)",
                "worker_name": "ExampleWorker",
                "max_concurrency": 0,
                "want_err": True,
            },
        ]

        for test_case in test_cases:
            with self.subTest(name=test_case["name"]):
                try:
                    worker = new_worker(
                        test_case["worker_name"], test_case["max_concurrency"]
                    )

                    if test_case["want_err"]:
                        self.fail(
                            f"NewWorker should return an error for {test_case['name']}"
                        )
                    else:
                        self.assertIsNotNone(
                            worker, "Worker should not be None for valid options"
                        )
                        self.assertEqual(
                            test_case["worker_name"],
                            worker.name,
                            "Worker name should match the provided name",
                        )
                        self.assertEqual(
                            test_case["max_concurrency"],
                            worker.max_concurrency,
                            "Worker max concurrency should match the provided value",
                        )
                        self.assertEqual(
                            WorkerStatus.READY,
                            worker.status,
                            "Worker status should be READY by default",
                        )

                except Exception as e:
                    if not test_case["want_err"]:
                        self.fail(
                            f"NewWorker should not return an error for {test_case['name']}: {e}"
                        )

    def test_new_worker_with_options(self):
        """Test NewWorkerWithOptions function with various parameters."""

        test_cases: List[Dict[str, Any]] = [
            {
                "name": "Valid worker with options",
                "worker_name": "ExampleWorker",
                "max_concurrency": 5,
                "options": OnError(
                    timeout=1,
                    max_retries=3,
                    retry_delay=2,
                    retry_backoff=RetryBackoff.LINEAR,
                ),
                "want_err": False,
            },
            {
                "name": "Invalid worker name (empty)",
                "worker_name": "",
                "max_concurrency": 5,
                "options": OnError(
                    timeout=1,
                    max_retries=3,
                    retry_delay=2,
                    retry_backoff=RetryBackoff.LINEAR,
                ),
                "want_err": True,
            },
            {
                "name": "Invalid max concurrency (too high)",
                "worker_name": "ExampleWorker",
                "max_concurrency": 1001,
                "options": OnError(
                    timeout=1,
                    max_retries=3,
                    retry_delay=2,
                    retry_backoff=RetryBackoff.LINEAR,
                ),
                "want_err": True,
            },
        ]

        for test_case in test_cases:
            with self.subTest(name=test_case["name"]):
                try:
                    worker = new_worker_with_options(
                        test_case["worker_name"],
                        test_case["max_concurrency"],
                        test_case["options"],
                    )

                    if test_case["want_err"]:
                        self.fail(
                            f"NewWorkerWithOptions should return an error for {test_case['name']}"
                        )
                    else:
                        self.assertIsNotNone(
                            worker, "Worker should not be None for valid options"
                        )
                        self.assertEqual(
                            test_case["worker_name"],
                            worker.name,
                            "Worker name should match the provided name",
                        )
                        self.assertEqual(
                            test_case["max_concurrency"],
                            worker.max_concurrency,
                            "Worker max concurrency should match the provided value",
                        )
                        self.assertEqual(
                            WorkerStatus.READY,
                            worker.status,
                            "Worker status should be READY by default",
                        )
                        self.assertEqual(
                            test_case["options"],
                            worker.options,
                            "Worker options should match the provided options",
                        )

                except Exception as e:
                    if not test_case["want_err"]:
                        self.fail(
                            f"NewWorkerWithOptions should not return an error for {test_case['name']}: {e}"
                        )

    def test_worker_status_enum(self):
        """Test WorkerStatus enum values."""
        self.assertEqual(WorkerStatus.READY, "READY")
        self.assertEqual(WorkerStatus.RUNNING, "RUNNING")
        self.assertEqual(WorkerStatus.STOPPED, "STOPPED")

    def test_worker_defaults(self):
        """Test default values for Worker object."""
        worker = Worker()

        self.assertEqual(worker.id, 0)
        self.assertIsNotNone(worker.rid)  # Should have a UUID
        self.assertEqual(worker.name, "")
        self.assertEqual(worker.max_concurrency, 1)
        self.assertEqual(worker.status, WorkerStatus.READY)
        self.assertEqual(worker.available_tasks, [])
        self.assertEqual(worker.available_next_interval_funcs, [])
        self.assertIsNone(worker.options)

    def test_worker_to_dict(self):
        """Test worker to_dict method."""
        options = OnError(max_retries=3, retry_delay=1)
        worker = Worker(
            name="test_worker",
            max_concurrency=5,
            options=options,
            available_tasks=["task1", "task2"],
        )

        worker_dict: Dict[str, Any] = worker.to_dict()
        self.assertEqual(worker_dict["name"], "test_worker")
        self.assertEqual(worker_dict["max_concurrency"], 5)
        self.assertIsNotNone(worker_dict["options"])
        self.assertEqual(worker_dict["available_tasks"], ["task1", "task2"])


if __name__ == "__main__":
    unittest.main()
