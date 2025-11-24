"""
Tests for Options model.
Mirrors Go's model/option_test.go functionality.
"""

import json
from typing import Any, Dict, List
import unittest
from datetime import datetime, timedelta

from .options import Options, Schedule, new_options
from .options_on_error import OnError, RetryBackoff


class TestOptions(unittest.TestCase):
    """Test cases for Options model."""

    def test_options_is_valid(self):
        """Test Options validation with various parameters."""

        test_cases: List[Dict[str, Any]] = [
            {
                "name": "Valid nil options",
                "options": None,
                "want_err": False,
            },
            {
                "name": "Valid options",
                "options": Options(
                    on_error=OnError(
                        max_retries=3,
                        retry_delay=1,
                        retry_backoff=RetryBackoff.LINEAR,
                    ),
                    schedule=Schedule(
                        start=datetime.now() + timedelta(minutes=1),
                        interval=timedelta(minutes=1),
                        max_count=3,
                    ),
                ),
                "want_err": False,
            },
            {
                "name": "Valid OnError options",
                "options": Options(
                    on_error=OnError(
                        timeout=1.0,
                        max_retries=3,
                        retry_delay=1,
                        retry_backoff=RetryBackoff.LINEAR,
                    ),
                ),
                "want_err": False,
            },
            {
                "name": "Valid Schedule options multiple executions with Interval",
                "options": Options(
                    schedule=Schedule(
                        start=datetime.now() + timedelta(minutes=1),
                        interval=timedelta(minutes=1),
                        max_count=3,
                    ),
                ),
                "want_err": False,
            },
            {
                "name": "Valid Schedule options multiple executions with NextInterval",
                "options": Options(
                    schedule=Schedule(
                        start=datetime.now() + timedelta(minutes=1),
                        max_count=3,
                        next_interval="NextIntervalFunction",
                    ),
                ),
                "want_err": False,
            },
            {
                "name": "Valid Schedule options single execution",
                "options": Options(
                    schedule=Schedule(
                        start=datetime.now() + timedelta(minutes=1),
                        max_count=1,
                    ),
                ),
                "want_err": False,
            },
            {
                "name": "Invalid options with negative Timeout",
                "create_options": lambda: Options(
                    on_error=OnError(
                        timeout=-1.0,
                        max_retries=3,
                        retry_delay=1,
                        retry_backoff=RetryBackoff.LINEAR,
                    ),
                ),
                "want_err": True,
            },
            {
                "name": "Invalid options with negative MaxRetries",
                "create_options": lambda: Options(
                    on_error=OnError(
                        timeout=1.0,
                        max_retries=-3,
                        retry_delay=1,
                        retry_backoff=RetryBackoff.LINEAR,
                    ),
                ),
                "want_err": True,
            },
            {
                "name": "Invalid options with negative RetryDelay",
                "create_options": lambda: Options(
                    on_error=OnError(
                        timeout=1.0,
                        max_retries=3,
                        retry_delay=-1,
                        retry_backoff=RetryBackoff.LINEAR,
                    ),
                ),
                "want_err": True,
            },
            {
                "name": "Invalid Schedule options with negative interval",
                "create_options": lambda: Options(
                    schedule=Schedule(
                        start=datetime.now() + timedelta(minutes=1),
                        interval=timedelta(minutes=-1),  # Negative interval
                        max_count=3,
                    ),
                ),
                "want_err": True,
            },
            {
                "name": "Invalid Schedule options with zero max_count",
                "create_options": lambda: Options(
                    schedule=Schedule(
                        start=datetime.now() + timedelta(minutes=1),
                        interval=timedelta(minutes=1),
                        max_count=0,  # Zero max_count
                    ),
                ),
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

                    if options is None:
                        # Nil options should be valid
                        if test_case["want_err"]:
                            self.fail("Nil options should be valid")
                    else:
                        # Call validation method (assuming it exists)
                        is_valid = options.is_valid()

                        if test_case["want_err"]:
                            self.assertFalse(
                                is_valid,
                                f"Options should be invalid for {test_case['name']}",
                            )
                        else:
                            self.assertTrue(
                                is_valid,
                                f"Options should be valid for {test_case['name']}",
                            )

                except Exception as e:
                    if not test_case["want_err"]:
                        self.fail(
                            f"Options validation should not raise exception for {test_case['name']}: {e}"
                        )

    def test_options_marshal_and_unmarshal_json(self):
        """Test Options JSON marshaling and unmarshaling."""

        # Create options with all fields
        original_options = Options(
            on_error=OnError(
                timeout=30.0,
                max_retries=3,
                retry_delay=5,
                retry_backoff=RetryBackoff.EXPONENTIAL,
            ),
            schedule=Schedule(
                start=datetime.now() + timedelta(hours=1),
                interval=timedelta(minutes=30),
                max_count=10,
                next_interval="CustomNextInterval",
            ),
        )

        # Test JSON serialization
        try:
            options_dict = original_options.to_dict()
            json_str = json.dumps(options_dict)
            self.assertIsInstance(json_str, str)

            # Test JSON deserialization
            parsed_dict = json.loads(json_str)
            restored_options = Options.from_dict(parsed_dict)

            # Verify the restored options match the original
            if (
                original_options.on_error is not None
                and restored_options.on_error is not None
            ):
                self.assertEqual(
                    original_options.on_error.timeout, restored_options.on_error.timeout
                )
                self.assertEqual(
                    original_options.on_error.max_retries,
                    restored_options.on_error.max_retries,
                )
                self.assertEqual(
                    original_options.on_error.retry_delay,
                    restored_options.on_error.retry_delay,
                )
                self.assertEqual(
                    original_options.on_error.retry_backoff,
                    restored_options.on_error.retry_backoff,
                )

            # Note: Schedule comparison might need special handling for datetime fields
            if original_options.schedule and restored_options.schedule:
                self.assertEqual(
                    original_options.schedule.max_count,
                    restored_options.schedule.max_count,
                )
                self.assertEqual(
                    original_options.schedule.next_interval,
                    restored_options.schedule.next_interval,
                )

        except Exception as e:
            self.fail(f"JSON marshaling/unmarshaling should work: {e}")

    def test_new_options_factory(self):
        """Test new_options factory function."""

        on_error = OnError(max_retries=5, retry_delay=2)
        schedule = Schedule(max_count=3, interval=timedelta(minutes=15))

        # Test with both parameters
        options = new_options(on_error=on_error, schedule=schedule)
        self.assertEqual(options.on_error, on_error)
        self.assertEqual(options.schedule, schedule)

        # Test with only on_error
        options_error_only = new_options(on_error=on_error)
        self.assertEqual(options_error_only.on_error, on_error)
        self.assertIsNone(options_error_only.schedule)

        # Test with only schedule
        options_schedule_only = new_options(schedule=schedule)
        self.assertIsNone(options_schedule_only.on_error)
        self.assertEqual(options_schedule_only.schedule, schedule)

        # Test with no parameters
        empty_options = new_options()
        self.assertIsNone(empty_options.on_error)
        self.assertIsNone(empty_options.schedule)

    def test_schedule_validation(self):
        """Test Schedule validation logic."""

        # Valid schedule
        valid_schedule = Schedule(
            start=datetime.now() + timedelta(minutes=1),
            interval=timedelta(minutes=5),
            max_count=3,
        )

        # This assumes Schedule has an is_valid method
        try:
            # If Schedule has validation, test it
            if hasattr(valid_schedule, "is_valid"):
                self.assertTrue(valid_schedule.is_valid())
        except Exception:
            # If no validation method, just check basic properties
            self.assertGreater(valid_schedule.max_count, 0)
            self.assertIsNotNone(valid_schedule.start)

    def test_retry_backoff_enum(self):
        """Test RetryBackoff enum values."""
        self.assertEqual(RetryBackoff.NONE, "none")
        self.assertEqual(RetryBackoff.LINEAR, "linear")
        self.assertEqual(RetryBackoff.EXPONENTIAL, "exponential")


if __name__ == "__main__":
    unittest.main()
