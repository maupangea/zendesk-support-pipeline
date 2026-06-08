from __future__ import annotations

import threading
import time

from zendesk_ingestion.api.rate_limiter import RateLimiter


def test_acquire_does_not_block_when_tokens_available() -> None:
    limiter = RateLimiter(requests_per_minute=600)
    start = time.monotonic()
    for _ in range(5):
        limiter.acquire()
    elapsed = time.monotonic() - start
    assert elapsed < 0.1


def test_acquire_blocks_when_bucket_empty() -> None:
    # 60/min = 1 token per second. Drain bucket, then time a single acquire.
    limiter = RateLimiter(requests_per_minute=60)
    for _ in range(60):
        limiter.acquire()
    start = time.monotonic()
    limiter.acquire()
    elapsed = time.monotonic() - start
    # Expect ~1 second wait (1 token / 1 token-per-sec). Allow slack for jitter.
    assert elapsed >= 0.8


def test_thread_safety() -> None:
    # 20 threads * 5 acquires = 100 acquires. With capacity=60, the first 60 are
    # instant; the remaining 40 require ~40 seconds at 1 tps. Use 1200/min = 20 tps
    # to keep the test fast: 100 - 1200 cap means 0 waits; speed up by capping smaller.
    # Use 600/min = 10 tps, capacity 600 -> 100 acquires all instant; we want some
    # waiting, so drain first.
    rate_per_min = 1200  # 20 tokens/sec
    limiter = RateLimiter(requests_per_minute=rate_per_min)
    # Drain to force replenishment.
    for _ in range(rate_per_min):
        limiter.acquire()

    threads_count = 20
    per_thread = 5
    total = threads_count * per_thread  # 100 acquires post-drain
    expected_min_seconds = total / (rate_per_min / 60.0)  # 100 / 20 = 5s

    def worker() -> None:
        for _ in range(per_thread):
            limiter.acquire()

    threads = [threading.Thread(target=worker) for _ in range(threads_count)]
    start = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - start

    # Allow some slack but ensure we observe the throttling.
    assert elapsed >= expected_min_seconds * 0.8
    assert elapsed <= expected_min_seconds * 2.0
