"""
API Rate Limiter Utility

A reusable rate limiter that can be used across your entire Django project.
Prevents exceeding API rate limits by tracking requests in Redis.
"""

import time
import logging
from typing import Dict, Any, Optional
from django_redis import get_redis_connection

logger = logging.getLogger(__name__)


class APIRateLimiter:
    """
    Redis-based sliding window rate limiter for API calls.
    
    Usage:
        limiter = APIRateLimiter(rate_limit=10, window_seconds=60)
        
        if limiter.can_make_request():
            # Make your API call here
            make_api_call()
        else:
            wait_time = limiter.wait_time_until_next_slot()
            print(f"Rate limited! Wait {wait_time} seconds")
    """
    
    def __init__(self, 
                 redis_client=None, 
                 rate_limit: int = 10, 
                 window_seconds: int = 60,
                 key_prefix: str = "api_rate_limit"):
        """
        Initialize the rate limiter.
        
        Args:
            redis_client: Redis connection (uses default if None)
            rate_limit: Maximum number of requests allowed
            window_seconds: Time window in seconds
            key_prefix: Redis key prefix for this rate limiter
        """
        self.redis = redis_client or get_redis_connection()
        self.rate_limit = rate_limit
        self.window_seconds = window_seconds
        self.key = f"{key_prefix}:{rate_limit}:{window_seconds}"
        
        logger.debug(f"Initialized rate limiter: {rate_limit}/{window_seconds}s")
    
    def can_make_request(self) -> bool:
        """
        Check if we can make an API request right now.
        If yes, records the request timestamp.
        
        Returns:
            True if request is allowed, False if rate limited
        """
        now = time.time()
        window_start = now - self.window_seconds
        
        try:
            # Use Redis pipeline for atomic operations
            pipe = self.redis.pipeline()
            
            # Remove old entries outside the current window
            pipe.zremrangebyscore(self.key, 0, window_start)
            
            # Count current requests in the window
            pipe.zcard(self.key)
            
            # Execute both commands
            results = pipe.execute()
            current_count = results[1]
            
            # Check if we've exceeded the rate limit
            if current_count >= self.rate_limit:
                logger.debug(f"Rate limit exceeded: {current_count}/{self.rate_limit}")
                return False
            
            # Record this request
            self.redis.zadd(self.key, {str(now): now})
            
            # Set expiration for cleanup (window + buffer)
            self.redis.expire(self.key, self.window_seconds + 10)
            
            logger.debug(f"Request allowed: {current_count + 1}/{self.rate_limit}")
            return True
            
        except Exception as e:
            logger.error(f"Rate limiter error: {e}")
            # On Redis errors, allow the request (fail open)
            return True
    
    def wait_time_until_next_slot(self) -> float:
        """
        Calculate how long to wait before the next request is allowed.
        
        Returns:
            Seconds to wait (0 if can make request immediately)
        """
        now = time.time()
        window_start = now - self.window_seconds
        
        try:
            # Get the oldest request in the current window
            oldest_requests = self.redis.zrangebyscore(
                self.key, window_start, now, start=0, num=1, withscores=True
            )
            
            if not oldest_requests:
                return 0.0
            
            # Calculate when the oldest request expires from the window
            oldest_time = oldest_requests[0][1]
            wait_time = (oldest_time + self.window_seconds) - now
            
            return max(0.0, wait_time)
            
        except Exception as e:
            logger.error(f"Error calculating wait time: {e}")
            return 0.0  # If error, don't block
    
    def get_current_usage(self) -> Dict[str, Any]:
        """
        Get current rate limiter usage statistics.
        
        Returns:
            Dictionary with usage information
        """
        now = time.time()
        window_start = now - self.window_seconds
        
        try:
            # Clean old entries and count current
            self.redis.zremrangebyscore(self.key, 0, window_start)
            current_count = self.redis.zcard(self.key)
            
            return {
                'current_calls': current_count,
                'limit': self.rate_limit,
                'window_seconds': self.window_seconds,
                'remaining': max(0, self.rate_limit - current_count),
                'next_reset_in': self.wait_time_until_next_slot()
            }
            
        except Exception as e:
            logger.error(f"Error getting usage stats: {e}")
            return {
                'current_calls': 0,
                'limit': self.rate_limit,
                'window_seconds': self.window_seconds,
                'remaining': self.rate_limit,
                'next_reset_in': 0.0,
                'error': str(e)
            }
    
    def reset(self) -> bool:
        """
        Reset the rate limiter (clear all recorded requests).
        Useful for testing or emergency situations.
        
        Returns:
            True if reset successful, False otherwise
        """
        try:
            self.redis.delete(self.key)
            logger.info(f"Rate limiter reset: {self.key}")
            return True
        except Exception as e:
            logger.error(f"Error resetting rate limiter: {e}")
            return False


# Convenience functions for common use cases

def create_football_api_limiter() -> APIRateLimiter:
    """Create a rate limiter specifically for football API (10 calls/minute)."""
    return APIRateLimiter(
        rate_limit=10,
        window_seconds=60,
        key_prefix="football_api"
    )

def create_general_api_limiter(calls_per_minute: int = 60) -> APIRateLimiter:
    """Create a general API rate limiter."""
    return APIRateLimiter(
        rate_limit=calls_per_minute,
        window_seconds=60,
        key_prefix="general_api"
    )

def create_strict_limiter(calls_per_hour: int = 100) -> APIRateLimiter:
    """Create a strict hourly rate limiter."""
    return APIRateLimiter(
        rate_limit=calls_per_hour,
        window_seconds=3600,  # 1 hour
        key_prefix="strict_api"
    )


# Decorator for easy rate limiting
def rate_limit(calls: int, seconds: int, key_prefix: str = "decorated"):
    """
    Decorator to add rate limiting to any function.
    
    Usage:
        @rate_limit(calls=10, seconds=60)
        def my_api_call():
            # Your API call here
            pass
    """
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


# Global instances for easy import
FOOTBALL_API_LIMITER = None
GENERAL_API_LIMITER = None

def get_football_api_limiter() -> APIRateLimiter:
    """Get the global football API rate limiter instance."""
    global FOOTBALL_API_LIMITER
    if FOOTBALL_API_LIMITER is None:
        FOOTBALL_API_LIMITER = create_football_api_limiter()
    return FOOTBALL_API_LIMITER

def get_general_api_limiter() -> APIRateLimiter:
    """Get the global general API rate limiter instance."""
    global GENERAL_API_LIMITER
    if GENERAL_API_LIMITER is None:
        GENERAL_API_LIMITER = create_general_api_limiter()
    return GENERAL_API_LIMITER