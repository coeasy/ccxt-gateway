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


def test_token_bucket_refills_and_rejects_excess():
    now = [0.0]
    limiter = TokenBucketLimiter(capacity=2, window_seconds=10, clock=lambda: now[0])
    assert limiter.consume("client")[0]
    assert limiter.consume("client")[0]
    allowed, retry_after = limiter.consume("client")
    assert not allowed and retry_after == 5
    now[0] = 5
    assert limiter.consume("client")[0]
