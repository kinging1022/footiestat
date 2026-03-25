"""
Tests for the Redis-based APIRateLimiter.

Redis is mocked so these tests run without a live Redis instance.
"""
import pytest
from unittest.mock import MagicMock, patch
from football.utils.rate_limiter import APIRateLimiter


def make_redis_mock(current_count: int = 0, oldest_score: float = None):
    """
    Build a MagicMock that mimics the Redis client surface used by
    APIRateLimiter — pipeline, zadd, expire, zcard, zrangebyscore, delete.
    """
    mock = MagicMock()

    pipe = MagicMock()
    # pipeline().execute() returns [zremrangebyscore_result, zcard_result]
    pipe.execute.return_value = [None, current_count]
    mock.pipeline.return_value = pipe

    mock.zcard.return_value = current_count

    if oldest_score is not None:
        mock.zrangebyscore.return_value = [(b"score", oldest_score)]
    else:
        mock.zrangebyscore.return_value = []

    return mock


class TestCanMakeRequest:
    def test_allows_request_when_under_limit(self):
        redis = make_redis_mock(current_count=3)
        limiter = APIRateLimiter(redis_client=redis, rate_limit=10, window_seconds=60)
        assert limiter.can_make_request() is True

    def test_blocks_request_when_at_limit(self):
        redis = make_redis_mock(current_count=10)
        limiter = APIRateLimiter(redis_client=redis, rate_limit=10, window_seconds=60)
        assert limiter.can_make_request() is False

    def test_blocks_request_when_over_limit(self):
        redis = make_redis_mock(current_count=15)
        limiter = APIRateLimiter(redis_client=redis, rate_limit=10, window_seconds=60)
        assert limiter.can_make_request() is False

    def test_allows_last_slot_before_limit(self):
        # 9 used, limit is 10 — the 10th request should go through
        redis = make_redis_mock(current_count=9)
        limiter = APIRateLimiter(redis_client=redis, rate_limit=10, window_seconds=60)
        assert limiter.can_make_request() is True

    def test_zadd_called_on_allowed_request(self):
        redis = make_redis_mock(current_count=0)
        limiter = APIRateLimiter(redis_client=redis, rate_limit=10, window_seconds=60)
        limiter.can_make_request()
        redis.zadd.assert_called_once()

    def test_zadd_not_called_when_blocked(self):
        redis = make_redis_mock(current_count=10)
        limiter = APIRateLimiter(redis_client=redis, rate_limit=10, window_seconds=60)
        limiter.can_make_request()
        redis.zadd.assert_not_called()

    def test_expire_set_after_allowed_request(self):
        redis = make_redis_mock(current_count=0)
        limiter = APIRateLimiter(redis_client=redis, rate_limit=10, window_seconds=60)
        limiter.can_make_request()
        redis.expire.assert_called_once()

    def test_fails_open_on_redis_error(self):
        """If Redis raises, we should fail open (return True) not crash."""
        redis = MagicMock()
        redis.pipeline.side_effect = Exception("Redis unavailable")
        limiter = APIRateLimiter(redis_client=redis, rate_limit=10, window_seconds=60)
        assert limiter.can_make_request() is True


class TestGetCurrentUsage:
    def test_returns_expected_keys(self):
        redis = make_redis_mock(current_count=3)
        limiter = APIRateLimiter(redis_client=redis, rate_limit=10, window_seconds=60)
        usage = limiter.get_current_usage()

        assert 'current_calls' in usage
        assert 'limit' in usage
        assert 'window_seconds' in usage
        assert 'remaining' in usage
        assert 'next_reset_in' in usage

    def test_remaining_correct(self):
        redis = make_redis_mock(current_count=3)
        limiter = APIRateLimiter(redis_client=redis, rate_limit=10, window_seconds=60)
        usage = limiter.get_current_usage()
        assert usage['remaining'] == 7

    def test_remaining_never_negative(self):
        redis = make_redis_mock(current_count=15)  # over limit
        limiter = APIRateLimiter(redis_client=redis, rate_limit=10, window_seconds=60)
        usage = limiter.get_current_usage()
        assert usage['remaining'] == 0

    def test_limit_reflects_config(self):
        redis = make_redis_mock(current_count=0)
        limiter = APIRateLimiter(redis_client=redis, rate_limit=400, window_seconds=60)
        usage = limiter.get_current_usage()
        assert usage['limit'] == 400
        assert usage['window_seconds'] == 60

    def test_returns_error_key_on_redis_failure(self):
        redis = MagicMock()
        redis.zremrangebyscore.side_effect = Exception("Redis down")
        redis.zcard.side_effect = Exception("Redis down")
        limiter = APIRateLimiter(redis_client=redis, rate_limit=10, window_seconds=60)
        usage = limiter.get_current_usage()
        assert 'error' in usage
        assert usage['remaining'] == 10  # default fallback


class TestReset:
    def test_delete_called_on_reset(self):
        redis = make_redis_mock()
        limiter = APIRateLimiter(redis_client=redis, rate_limit=10, window_seconds=60)
        result = limiter.reset()
        assert result is True
        redis.delete.assert_called_once_with(limiter.key)

    def test_reset_returns_false_on_error(self):
        redis = MagicMock()
        redis.delete.side_effect = Exception("Redis down")
        limiter = APIRateLimiter(redis_client=redis, rate_limit=10, window_seconds=60)
        assert limiter.reset() is False


class TestWaitTime:
    def test_no_wait_when_queue_empty(self):
        redis = make_redis_mock(oldest_score=None)
        limiter = APIRateLimiter(redis_client=redis, rate_limit=10, window_seconds=60)
        assert limiter.wait_time_until_next_slot() == 0.0

    def test_returns_zero_on_redis_error(self):
        redis = MagicMock()
        redis.zrangebyscore.side_effect = Exception("Redis down")
        limiter = APIRateLimiter(redis_client=redis, rate_limit=10, window_seconds=60)
        assert limiter.wait_time_until_next_slot() == 0.0

    def test_wait_time_never_negative(self):
        import time
        # Oldest entry is in the distant past — wait time should be 0, not negative
        redis = make_redis_mock(oldest_score=time.time() - 120)  # 2 min ago
        limiter = APIRateLimiter(redis_client=redis, rate_limit=10, window_seconds=60)
        assert limiter.wait_time_until_next_slot() >= 0.0


class TestFactoryFunctions:
    def test_football_api_limiter_config(self):
        with patch("football.utils.rate_limiter.get_redis_connection", return_value=MagicMock()):
            from football.utils.rate_limiter import create_football_api_limiter
            limiter = create_football_api_limiter()
            assert limiter.rate_limit == 400
            assert limiter.window_seconds == 60

    def test_football_api_second_limiter_config(self):
        with patch("football.utils.rate_limiter.get_redis_connection", return_value=MagicMock()):
            from football.utils.rate_limiter import create_football_api_second_limiter
            limiter = create_football_api_second_limiter()
            assert limiter.rate_limit == 7
            assert limiter.window_seconds == 1
