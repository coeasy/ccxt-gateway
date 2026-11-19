import asyncio

import pytest

from app.cache import AsyncTTLCache
from app.rate_limit import TokenBucketLimiter


@pytest.mark.asyncio
async def test_cache_coalesces_concurrent_requests_and_expires():
    now = [10.0]
    cache = AsyncTTLCache(clock=lambda: now[0])
    calls = 0

    async def factory():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return {"value": 1}

    left, right = await asyncio.gather(
        cache.get_or_create("k", 2, factory), cache.get_or_create("k", 2, factory)
    )
    assert left == right == {"value": 1}
    assert calls == 1
    now[0] = 13
    assert await cache.get_or_create("k", 2, factory) == {"value": 1}
    assert calls == 2


@pytest.mark.asyncio
async def test_cache_keeps_shared_request_alive_when_first_waiter_disconnects():
    cache = AsyncTTLCache()
    started = asyncio.Event()
    finish = asyncio.Event()
    calls = 0

    async def factory():
        nonlocal calls
        calls += 1
        started.set()
        await finish.wait()
        return "result"

    first = asyncio.create_task(cache.get_or_create("k", 10, factory))
    await started.wait()
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    second = asyncio.create_task(cache.get_or_create("k", 10, factory))
    await asyncio.sleep(0)
    finish.set()

    assert await second == "result"
    assert await cache.get_or_create("k", 10, factory) == "result"
    assert calls == 1


@pytest.mark.asyncio
async def test_cache_drops_failed_request_and_allows_retry():
    cache = AsyncTTLCache()
    calls = 0

    async def factory():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary upstream failure")
        return "recovered"

    with pytest.raises(RuntimeError, match="temporary upstream failure"):
        await cache.get_or_create("k", 10, factory)
    assert await cache.get_or_create("k", 10, factory) == "recovered"
    assert calls == 2


@pytest.mark.asyncio
async def test_cache_close_cancels_and_joins_in_flight_work():
    cache = AsyncTTLCache()
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def factory():
        started.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    request = asyncio.create_task(cache.get_or_create("k", 10, factory))
    await started.wait()
    await cache.close()

    assert cancelled.is_set()
    with pytest.raises(asyncio.CancelledError):
        await request
    assert cache.size == 0


def test_token_bucket_refills_and_rejects_excess():
    now = [0.0]
    limiter = TokenBucketLimiter(capacity=2, window_seconds=10, clock=lambda: now[0])
    assert limiter.consume("client")[0]
    assert limiter.consume("client")[0]
    allowed, retry_after = limiter.consume("client")
    assert not allowed and retry_after == 5
    now[0] = 5
    assert limiter.consume("client")[0]
