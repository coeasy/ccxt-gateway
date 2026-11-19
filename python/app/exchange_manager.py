from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from types import ModuleType
from typing import Any


class ExchangeManagerError(Exception):
    def __init__(self, message: str, status_code: int = 500, code: str = "exchange_error"):
        super().__init__(message)
        self.status_code = status_code
        self.code = code


@dataclass(slots=True)
class ExchangeEntry:
    rest: Any
    pro: Any | None = None
    references: int = 0
    last_used: float = 0
    idle_task: asyncio.Task[None] | None = None


@dataclass(slots=True)
class ExchangeLease:
    exchange: Any
    release: Any


class ExchangeManager:
    """Creates one paired Async REST/Pro client per exchange and reference-counts use."""

    def __init__(
        self,
        async_ccxt: ModuleType | Any,
        pro_ccxt: ModuleType | Any,
        credentials: dict[str, dict[str, Any]] | None = None,
        idle_ttl: float = 60,
        max_instances: int = 32,
        timeout_ms: int = 30_000,
    ):
        self.async_ccxt = async_ccxt
        self.pro_ccxt = pro_ccxt
        self.credentials = credentials or {}
        self.idle_ttl = idle_ttl
        self.max_instances = max_instances
        self.timeout_ms = timeout_ms
        self.entries: dict[str, ExchangeEntry] = {}
        self._lock = asyncio.Lock()
        self.closed = False
        self.logger = logging.getLogger("ccxt_gateway.exchanges")

    def list_exchanges(self) -> list[str]:
        return list(self.async_ccxt.exchanges)

    def has_credentials(self, exchange_id: str) -> bool:
        entry = self.credentials.get(exchange_id, {})
        return bool(entry.get("apiKey") and entry.get("secret"))

    async def acquire(self, exchange_id: str, client: str = "rest") -> ExchangeLease:
        async with self._lock:
            if self.closed:
                raise ExchangeManagerError(
                    "Exchange manager is shutting down", 503, "manager_closed"
                )
            if exchange_id not in self.async_ccxt.exchanges:
                raise ExchangeManagerError(
                    f"Unsupported exchange: {exchange_id}", 404, "unsupported_exchange"
                )
            if client not in {"rest", "pro"}:
                raise ValueError("client must be 'rest' or 'pro'")
            rest_cls = getattr(self.async_ccxt, exchange_id, None)
            pro_cls = getattr(self.pro_ccxt, exchange_id, None)
            if rest_cls is None:
                raise ExchangeManagerError(
                    "Exchange is not available in CCXT Async", 501, "exchange_unavailable"
                )
            if client == "pro" and pro_cls is None:
                raise ExchangeManagerError(
                    "Exchange is not available in CCXT Pro", 501, "exchange_unavailable"
                )
            options = {
                "enableRateLimit": True,
                "timeout": self.timeout_ms,
                **self.credentials.get(exchange_id, {}),
            }

            entry = self.entries.get(exchange_id)
            if entry is None:
                if len(self.entries) >= self.max_instances:
                    idle = min(
                        (
                            (key, value)
                            for key, value in self.entries.items()
                            if value.references == 0
                        ),
                        key=lambda item: item[1].last_used,
                        default=None,
                    )
                    if idle is None:
                        raise ExchangeManagerError(
                            "Exchange capacity reached", 503, "exchange_capacity_reached"
                        )
                    self.entries.pop(idle[0])
                    await self._close_entry(idle[1])
                rest = rest_cls(dict(options))
                entry = ExchangeEntry(rest=rest, last_used=time.monotonic())
                created = True
            else:
                created = False

            if client == "pro" and entry.pro is None:
                try:
                    entry.pro = pro_cls(dict(options))
                except Exception:
                    if created:
                        await entry.rest.close()
                    raise
            if created:
                self.entries[exchange_id] = entry
            if entry.idle_task:
                entry.idle_task.cancel()
                entry.idle_task = None
            entry.references += 1
            entry.last_used = time.monotonic()
            exchange = entry.pro if client == "pro" else entry.rest
            assert exchange is not None
        released = False

        async def release() -> None:
            nonlocal released
            if released:
                return
            released = True
            async with self._lock:
                entry.references = max(0, entry.references - 1)
                entry.last_used = time.monotonic()
                if entry.references == 0 and not self.closed:
                    entry.idle_task = asyncio.create_task(self._expire(exchange_id, entry))

        return ExchangeLease(exchange, release)

    async def with_exchange(self, exchange_id: str, operation: Any, client: str = "rest") -> Any:
        lease = await self.acquire(exchange_id, client)
        try:
            return await operation(lease.exchange)
        finally:
            await lease.release()

    async def _expire(self, exchange_id: str, entry: ExchangeEntry) -> None:
        try:
            await asyncio.sleep(self.idle_ttl)
            async with self._lock:
                if self.entries.get(exchange_id) is not entry or entry.references or self.closed:
                    return
                self.entries.pop(exchange_id, None)
            await self._close_entry(entry)
        except asyncio.CancelledError:
            raise

    async def _close_entry(self, entry: ExchangeEntry) -> None:
        if entry.idle_task and entry.idle_task is not asyncio.current_task():
            entry.idle_task.cancel()
        clients = [entry.rest, entry.pro]
        await asyncio.gather(
            *(client.close() for client in clients if client is not None),
            return_exceptions=True,
        )

    async def close(self) -> None:
        async with self._lock:
            if self.closed:
                return
            self.closed = True
            entries = list(self.entries.values())
            self.entries.clear()
            for entry in entries:
                if entry.idle_task:
                    entry.idle_task.cancel()
        await asyncio.gather(
            *(self._close_entry(entry) for entry in entries), return_exceptions=True
        )
