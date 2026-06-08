"""Thread-safe token bucket rate limiter."""

from __future__ import annotations

import threading
import time


class RateLimiter:
    """
    Token bucket with continuous replenishment.

    Tokens accumulate at a steady rate (requests_per_minute / 60 per second) up to
    a bucket capacity of requests_per_minute. `acquire()` blocks until at least one
    token is available, then consumes it.
    """

    def __init__(self, requests_per_minute: int) -> None:
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be > 0")
        self._capacity = float(requests_per_minute)
        self._refill_rate = self._capacity / 60.0
        self._tokens = self._capacity
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block until a token is available, then consume one."""
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                # Not enough tokens — compute wait time to accumulate 1 token.
                deficit = 1.0 - self._tokens
                wait_seconds = deficit / self._refill_rate
            time.sleep(wait_seconds)

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._last_refill = now
        self._tokens = min(self._capacity, self._tokens + elapsed * self._refill_rate)
