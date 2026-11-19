from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from typing import Any


class IdempotencyConflictError(Exception):
    pass


class IdempotencyCapacityError(Exception):
    pass


class IdempotencyStore:
    def __init__(self, ttl_seconds: int = 86_400, max_entries: int = 10_000) -> None:
        self.ttl_seconds = ttl_seconds
        self.max_entries = max_entries
        self.entries: OrderedDict[str, tuple[str, float, asyncio.Task[Any]]] = OrderedDict()
        self.lock = asyncio.Lock()

    @staticmethod
    def fingerprint(value: Any) -> str:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return hashlib.sha256(encoded.encode()).hexdigest()

    async def execute(self, key: str, payload: Any, factory: Callable[[], Awaitable[Any]]) -> Any:
        fingerprint = self.fingerprint(payload)
        now = time.monotonic()
        async with self.lock:
            for old_key, (_, expires, _) in tuple(self.entries.items()):
                if expires <= now:
                    self.entries.pop(old_key, None)
            existing = self.entries.get(key)
            if existing:
                if existing[0] != fingerprint:
                    raise IdempotencyConflictError(
                        "Idempotency-Key was already used with a different order payload"
                    )
                self.entries.move_to_end(key)
                task = existing[2]
            else:
                if len(self.entries) >= self.max_entries:
                    raise IdempotencyCapacityError(
                        "Idempotency store capacity reached; retry after existing keys expire"
                    )
                task = asyncio.create_task(factory())
                self.entries[key] = (fingerprint, now + self.ttl_seconds, task)
        return await asyncio.shield(task)
