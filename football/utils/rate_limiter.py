"""
API Rate Limiter Utility
"""

import time
import logging
from typing import Dict, Any, Optional
from django_redis import get_redis_connection

logger = logging.getLogger(__name__)


class APIRateLimiter:
    """
    Redis-based sliding window rate limiter for API calls.
    """

    def __init__(self,
                 redis_client=None,
                 rate_limit: int = 10,
                 window_seconds: int = 60,
                 key_prefix: str = "api_rate_limit"):
        self.redis          = redis_client or get_redis_connection()
        self.rate_limit     = rate_limit
        self.window_seconds = window_seconds
        self.key            = f"{key_prefix}:{rate_limit}:{window_seconds}"
        logger.debug(f"Initialized rate limiter: {rate_limit}/{window_seconds}s")

    def can_make_request(self) -> bool:
        now          = time.time()
        window_start = now - self.window_seconds

        try:
            pipe = self.redis.pipeline()
            pipe.zremrangebyscore(self.key, 0, window_start)
            pipe.zcard(self.key)
            results       = pipe.execute()
            current_count = results[1]

            if current_count >= self.rate_limit:
                logger.debug(f"Rate limit exceeded: {current_count}/{self.rate_limit}")
                return False

            self.redis.zadd(self.key, {str(now): now})
            self.redis.expire(self.key, self.window_seconds + 10)
            logger.debug(f"Request allowed: {current_count + 1}/{self.rate_limit}")
            return True

        except Exception as e:
            logger.error(f"Rate limiter error: {e}")
            return True  # fail open

    def wait_time_until_next_slot(self) -> float:
        now          = time.time()
        window_start = now - self.window_seconds

        try:
            oldest_requests = self.redis.zrangebyscore(
                self.key, window_start, now, start=0, num=1, withscores=True
            )
            if not oldest_requests:
                return 0.0
            oldest_time = oldest_requests[0][1]
            wait_time   = (oldest_time + self.window_seconds) - now
            return max(0.0, wait_time)

        except Exception as e:
            logger.error(f"Error calculating wait time: {e}")
            return 0.0

    def get_current_usage(self) -> Dict[str, Any]:
        now          = time.time()
        window_start = now - self.window_seconds

        try:
            self.redis.zremrangebyscore(self.key, 0, window_start)
            current_count = self.redis.zcard(self.key)
            return {
                'current_calls':  current_count,
                'limit':          self.rate_limit,
                'window_seconds': self.window_seconds,
                'remaining':      max(0, self.rate_limit - current_count),
                'next_reset_in':  self.wait_time_until_next_slot()
            }

        except Exception as e:
            logger.error(f"Error getting usage stats: {e}")
            return {
                'current_calls':  0,
                'limit':          self.rate_limit,
                'window_seconds': self.window_seconds,
                'remaining':      self.rate_limit,
                'next_reset_in':  0.0,
                'error':          str(e)
            }

    def reset(self) -> bool:
        try:
            self.redis.delete(self.key)
            logger.info(f"Rate limiter reset: {self.key}")
            return True
        except Exception as e:
            logger.error(f"Error resetting rate limiter: {e}")
            return False


# ── FACTORY FUNCTIONS ────────────────────────────────────────────────────────

def create_football_api_limiter() -> APIRateLimiter:
    """
    Per-minute limiter for football API.
    Real limit: 450/min — we use 400 as safety buffer.
    CHANGED: was 10/min, now correctly set to 400/min.
    """
    return APIRateLimiter(
        rate_limit=400,
        window_seconds=60,
        key_prefix="football_api_minute"
    )


def create_football_api_second_limiter() -> APIRateLimiter:
    """
    Per-second limiter for football API.
    NEW: enforces the 7.5 calls/sec limit — we use 7 as safety buffer.
    This is what was missing and causing rate limit hits.
    """
    return APIRateLimiter(
        rate_limit=7,
        window_seconds=1,
        key_prefix="football_api_second"
    )


def create_general_api_limiter(calls_per_minute: int = 60) -> APIRateLimiter:
    """General purpose API limiter — unchanged."""
    return APIRateLimiter(
        rate_limit=calls_per_minute,
        window_seconds=60,
        key_prefix="general_api"
    )


def create_strict_limiter(calls_per_hour: int = 100) -> APIRateLimiter:
    """Strict hourly limiter — unchanged."""
    return APIRateLimiter(
        rate_limit=calls_per_hour,
        window_seconds=3600,
        key_prefix="strict_api"
    )


def rate_limit(calls: int, seconds: int, key_prefix: str = "decorated"):
    """Decorator for rate limiting — unchanged."""
    def decorator(func):
        limiter = APIRateLimiter(
            rate_limit=calls,
            window_seconds=seconds,
            key_prefix=f"{key_prefix}_{func.__name__}"
        )

        def wrapper(*args, **kwargs):
            if not limiter.can_make_request():
                wait_time = limiter.wait_time_until_next_slot()
                raise Exception(f"Rate limited! Wait {wait_time:.1f} seconds")
            return func(*args, **kwargs)

        return wrapper
    return decorator


# ── GLOBAL INSTANCES ─────────────────────────────────────────────────────────

FOOTBALL_API_LIMITER        = None  # per-minute — unchanged name
FOOTBALL_API_SECOND_LIMITER = None  # per-second — NEW
GENERAL_API_LIMITER         = None  # unchanged


def get_football_api_limiter() -> APIRateLimiter:
    """Per-minute limiter — same function name, now returns 400/min not 10/min."""
    global FOOTBALL_API_LIMITER
    if FOOTBALL_API_LIMITER is None:
        FOOTBALL_API_LIMITER = create_football_api_limiter()
    return FOOTBALL_API_LIMITER


def get_football_api_second_limiter() -> APIRateLimiter:
    """Per-second limiter — NEW function."""
    global FOOTBALL_API_SECOND_LIMITER
    if FOOTBALL_API_SECOND_LIMITER is None:
        FOOTBALL_API_SECOND_LIMITER = create_football_api_second_limiter()
    return FOOTBALL_API_SECOND_LIMITER


def get_general_api_limiter() -> APIRateLimiter:
    """General limiter — unchanged."""
    global GENERAL_API_LIMITER
    if GENERAL_API_LIMITER is None:
        GENERAL_API_LIMITER = create_general_api_limiter()
    return GENERAL_API_LIMITER