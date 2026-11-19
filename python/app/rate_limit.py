from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(slots=True)
class Bucket:
    tokens: float
    updated_at: float


class TokenBucketLimiter:
    """Per-client in-process token bucket. Configure one replica or shared storage."""

    def __init__(
        self,
        capacity: int = 120,
        window_seconds: float = 60,
        max_clients: int = 20_000,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.capacity = float(capacity)
        self.refill_per_second = capacity / window_seconds
        self.max_clients = max_clients
        self.clock = clock
        self._buckets: OrderedDict[str, Bucket] = OrderedDict()

    def consume(self, client: str, cost: float = 1) -> tuple[bool, int]:
        now = self.clock()
        bucket = self._buckets.get(client)
        if bucket is None:
            bucket = Bucket(self.capacity, now)
        else:
            bucket.tokens = min(
                self.capacity, bucket.tokens + (now - bucket.updated_at) * self.refill_per_second
            )
            bucket.updated_at = now
        allowed = bucket.tokens >= cost
        if allowed:
            bucket.tokens -= cost
        self._buckets[client] = bucket
        self._buckets.move_to_end(client)
        while len(self._buckets) > self.max_clients:
            self._buckets.popitem(last=False)
        retry_after = max(1, int((cost - bucket.tokens) / self.refill_per_second + 0.999))
        return allowed, retry_after
