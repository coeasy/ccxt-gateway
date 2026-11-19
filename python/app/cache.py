from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

T = TypeVar("T")


class AsyncTTLCache:
    """Bounded TTL cache with in-flight request coalescing."""

    def __init__(self, max_entries: int = 10_000, clock: Callable[[], float] = time.monotonic):
        self.max_entries = max_entries
        self.clock = clock
        self._values: OrderedDict[str, tuple[float, Any]] = OrderedDict()
        self._inflight: dict[str, asyncio.Task[Any]] = {}
        self._lock = asyncio.Lock()

    async def get_or_create(self, key: str, ttl: float, factory: Callable[[], Awaitable[T]]) -> T:
        now = self.clock()
        async with self._lock:
            cached = self._values.get(key)
            if cached and cached[0] > now:
                self._values.move_to_end(key)
                return cached[1]
            if cached:
                self._values.pop(key, None)
            task = self._inflight.get(key)
            if task is None:
                task = asyncio.create_task(factory())
                self._inflight[key] = task
                owner = True
            else:
                owner = False
        try:
            value = await asyncio.shield(task)
        except BaseException:
            if owner:
                async with self._lock:
                    if self._inflight.get(key) is task:
                        self._inflight.pop(key, None)
            raise
        if owner:
            async with self._lock:
                if self._inflight.get(key) is task:
                    self._inflight.pop(key, None)
                    self._values[key] = (self.clock() + max(0, ttl), value)
                    self._values.move_to_end(key)
                    while len(self._values) > self.max_entries:
                        self._values.popitem(last=False)
        return value

    async def clear(self) -> None:
        async with self._lock:
            self._values.clear()

    @property
    def size(self) -> int:
        return len(self._values)
