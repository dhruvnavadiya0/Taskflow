"""Tests for the Redis queue abstraction and job handlers."""

from unittest.mock import MagicMock, patch, call

import pytest

from app.queue.redis_queue import RedisQueue
from app.workers.handlers import (
    registry,
    handle_sum_numbers,
    handle_text_length,
    handle_always_fail,
    handle_fail_n_times,
)


class TestRedisQueue:
    def test_enqueue_and_dequeue(self):
        """Test queue operations using a mocked Redis client."""
        mock_client = MagicMock()
        queue = RedisQueue.__new__(RedisQueue)
        queue.client = mock_client
        queue.queue_name = "test:queue"

        # Enqueue
        queue.enqueue("job-1")
        mock_client.rpush.assert_called_once_with("test:queue", "job-1")

        # Dequeue returns (queue_name, value)
        mock_client.blpop.return_value = ("test:queue", "job-1")
        result = queue.dequeue(timeout=1)
        assert result == "job-1"
        mock_client.blpop.assert_called_once_with("test:queue", timeout=1)

    def test_dequeue_returns_none_on_timeout(self):
        mock_client = MagicMock()
        queue = RedisQueue.__new__(RedisQueue)
        queue.client = mock_client
        queue.queue_name = "test:queue"

        mock_client.blpop.return_value = None
        result = queue.dequeue(timeout=1)
        assert result is None

    def test_queue_length(self):
        mock_client = MagicMock()
        queue = RedisQueue.__new__(RedisQueue)
        queue.client = mock_client
        queue.queue_name = "test:queue"

        mock_client.llen.return_value = 5
        assert queue.length() == 5


class TestPriorityQueue:
    """Phase 2: priority queue operations."""

    def test_enqueue_priority_high(self):
        mock_client = MagicMock()
        queue = RedisQueue.__new__(RedisQueue)
        queue.client = mock_client

        queue.enqueue_priority("job-1", "HIGH")
        mock_client.rpush.assert_called_once_with("taskflow:queue:high", "job-1")

    def test_enqueue_priority_normal(self):
        mock_client = MagicMock()
        queue = RedisQueue.__new__(RedisQueue)
        queue.client = mock_client

        queue.enqueue_priority("job-2", "NORMAL")
        mock_client.rpush.assert_called_once_with("taskflow:queue:normal", "job-2")

    def test_enqueue_priority_low(self):
        mock_client = MagicMock()
        queue = RedisQueue.__new__(RedisQueue)
        queue.client = mock_client

        queue.enqueue_priority("job-3", "LOW")
        mock_client.rpush.assert_called_once_with("taskflow:queue:low", "job-3")

    def test_enqueue_priority_unknown_defaults_to_normal(self):
        mock_client = MagicMock()
        queue = RedisQueue.__new__(RedisQueue)
        queue.client = mock_client

        queue.enqueue_priority("job-4", "UNKNOWN")
        mock_client.rpush.assert_called_once_with("taskflow:queue:normal", "job-4")

    def test_dequeue_priority(self):
        mock_client = MagicMock()
        queue = RedisQueue.__new__(RedisQueue)
        queue.client = mock_client

        mock_client.blpop.return_value = ("taskflow:queue:high", "job-1")
        result = queue.dequeue_priority(timeout=1)
        assert result == "job-1"
        # Should pass all three queues in priority order
        mock_client.blpop.assert_called_once_with(
            ("taskflow:queue:high", "taskflow:queue:normal", "taskflow:queue:low"),
            timeout=1,
        )

    def test_dequeue_priority_returns_none_on_timeout(self):
        mock_client = MagicMock()
        queue = RedisQueue.__new__(RedisQueue)
        queue.client = mock_client

        mock_client.blpop.return_value = None
        result = queue.dequeue_priority(timeout=1)
        assert result is None

    def test_priority_lengths(self):
        mock_client = MagicMock()
        queue = RedisQueue.__new__(RedisQueue)
        queue.client = mock_client

        mock_client.llen.side_effect = [3, 5, 2]
        lengths = queue.priority_lengths()
        assert lengths == {"high": 3, "normal": 5, "low": 2}


class TestDeadLetterQueue:
    """Phase 2: dead-letter queue operations."""

    def test_move_to_dead_letter(self):
        mock_client = MagicMock()
        queue = RedisQueue.__new__(RedisQueue)
        queue.client = mock_client

        queue.move_to_dead_letter("job-1")
        mock_client.rpush.assert_called_once_with("taskflow:queue:dead", "job-1")

    def test_dead_letter_length(self):
        mock_client = MagicMock()
        queue = RedisQueue.__new__(RedisQueue)
        queue.client = mock_client

        mock_client.llen.return_value = 7
        assert queue.dead_letter_length() == 7


class TestRetryScheduling:
    """Phase 2: sorted-set-based retry scheduling."""

    def test_schedule_retry(self):
        mock_client = MagicMock()
        queue = RedisQueue.__new__(RedisQueue)
        queue.client = mock_client

        queue.schedule_retry("job-1", 1700000000.0)
        mock_client.zadd.assert_called_once_with(
            "taskflow:retry:scheduled", {"job-1": 1700000000.0}
        )

    def test_get_due_retries(self):
        mock_client = MagicMock()
        mock_pipe = MagicMock()
        mock_client.pipeline.return_value = mock_pipe
        mock_pipe.execute.return_value = [["job-1", "job-2"], 2]

        queue = RedisQueue.__new__(RedisQueue)
        queue.client = mock_client

        result = queue.get_due_retries()
        assert result == ["job-1", "job-2"]


class TestSumNumbersHandler:
    def test_sum_valid_numbers(self):
        result = handle_sum_numbers({"numbers": [10, 20, 30]})
        assert result == {"sum": 60}

    def test_sum_empty_list(self):
        result = handle_sum_numbers({"numbers": []})
        assert result == {"sum": 0}

    def test_sum_floats(self):
        result = handle_sum_numbers({"numbers": [1.5, 2.5]})
        assert result == {"sum": 4.0}

    def test_sum_missing_numbers_key(self):
        with pytest.raises(ValueError, match="'numbers' must be a list"):
            handle_sum_numbers({})

    def test_sum_non_list(self):
        with pytest.raises(ValueError, match="'numbers' must be a list"):
            handle_sum_numbers({"numbers": "not a list"})

    def test_sum_non_numeric_elements(self):
        with pytest.raises(ValueError, match="numeric"):
            handle_sum_numbers({"numbers": [1, "two", 3]})


class TestTextLengthHandler:
    def test_text_length(self):
        result = handle_text_length({"text": "TaskFlow"})
        assert result == {"length": 8}

    def test_empty_text(self):
        result = handle_text_length({"text": ""})
        assert result == {"length": 0}

    def test_missing_text_key(self):
        with pytest.raises(ValueError, match="'text' must be a string"):
            handle_text_length({})

    def test_non_string_text(self):
        with pytest.raises(ValueError, match="'text' must be a string"):
            handle_text_length({"text": 123})


class TestAlwaysFailHandler:
    """Phase 2: test-only handler."""

    def test_always_fails(self):
        with pytest.raises(RuntimeError, match="ALWAYS_FAIL"):
            handle_always_fail({})


class TestFailNTimesHandler:
    """Phase 2: test-only handler."""

    def test_fails_then_succeeds(self):
        from app.workers.handlers import _fail_counter
        _fail_counter.clear()

        with pytest.raises(RuntimeError, match="FAIL_N_TIMES"):
            handle_fail_n_times({"failures_before_success": 2, "job_id": "test-1"})

        with pytest.raises(RuntimeError, match="FAIL_N_TIMES"):
            handle_fail_n_times({"failures_before_success": 2, "job_id": "test-1"})

        result = handle_fail_n_times({"failures_before_success": 2, "job_id": "test-1"})
        assert result["message"] == "succeeded after failures"
        assert result["total_attempts"] == 3


class TestJobRegistry:
    def test_registered_types(self):
        assert "SUM_NUMBERS" in registry.registered_types
        assert "TEXT_LENGTH" in registry.registered_types
        assert "ALWAYS_FAIL" in registry.registered_types
        assert "FAIL_N_TIMES" in registry.registered_types

    def test_get_handler_for_known_type(self):
        handler = registry.get_handler("SUM_NUMBERS")
        assert callable(handler)

    def test_get_handler_for_unknown_type(self):
        with pytest.raises(ValueError, match="Unknown job type"):
            registry.get_handler("DOES_NOT_EXIST")
