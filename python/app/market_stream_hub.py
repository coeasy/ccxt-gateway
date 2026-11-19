from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

from app.exchange_manager import ExchangeManager, ExchangeManagerError

STREAMS = {
    "ticker": ("watch_ticker", "un_watch_ticker", "watchTicker"),
    "orderbook": ("watch_order_book", "un_watch_order_book", "watchOrderBook"),
    "trades": ("watch_trades", "un_watch_trades", "watchTrades"),
    "ohlcv": ("watch_ohlcv", "un_watch_ohlcv", "watchOHLCV"),
    "fundingRate": ("watch_funding_rate", "un_watch_funding_rate", "watchFundingRate"),
    "markPrice": ("watch_mark_price", "un_watch_mark_price", "watchMarkPrice"),
}


def _key(options: dict[str, Any]) -> tuple[Any, ...]:
    limit = None if options["channel"] == "ticker" else options.get("limit")
    return (
        options["exchange"],
        options["channel"],
        options["symbol"],
        options.get("timeframe"),
        limit,
    )


def _arguments(stream: Stream) -> tuple[Any, ...]:
    if stream.channel == "ticker":
        return (stream.symbol,)
    if stream.channel == "orderbook":
        return (stream.symbol, stream.limit)
    if stream.channel == "trades":
        return (stream.symbol, None, stream.limit)
    if stream.channel in {"fundingRate", "markPrice"}:
        return (stream.symbol,)
    return (stream.symbol, stream.timeframe, None, stream.limit)


@dataclass(slots=True)
class Stream:
    key: tuple[Any, ...]
    exchange: str
    channel: str
    symbol: str
    timeframe: str | None
    limit: int | None
    lease: Any
    subscribers: dict[Any, set[str]] = field(default_factory=dict)
    idle_task: asyncio.Task[None] | None = None
    task: asyncio.Task[None] | None = None
    stop_task: asyncio.Task[None] | None = None
    stopped: bool = False
    failures: int = 0


class MarketStreamHub:
    """One upstream CCXT Pro watch loop is shared by all local subscribers."""

    def __init__(self, manager: ExchangeManager, idle_grace: float = 10, max_streams: int = 1_000):
        self.manager = manager
        self.idle_grace = idle_grace
        self.max_streams = max_streams
        self.streams: dict[tuple[Any, ...], Stream] = {}
        self.pending: dict[tuple[Any, ...], asyncio.Task[Stream]] = {}
        self._lock = asyncio.Lock()
        self.closed = False
        self.logger = logging.getLogger("ccxt_gateway.streams")

    async def subscribe(self, socket: Any, options: dict[str, Any], subscription_id: str) -> None:
        if self.closed:
            raise ExchangeManagerError(
                "WebSocket gateway is shutting down", 503, "gateway_shutting_down"
            )
        if options["channel"] not in STREAMS:
            raise ExchangeManagerError("Unsupported stream channel", 400, "unsupported_channel")
        key = _key(options)
        while True:
            async with self._lock:
                stream = self.streams.get(key)
                if stream and stream.stopped:
                    stream = None
                pending = self.pending.get(key) if stream is None else None
                if stream is None and pending is None:
                    active_stream_count = sum(not item.stopped for item in self.streams.values())
                    if active_stream_count + len(self.pending) >= self.max_streams:
                        raise ExchangeManagerError(
                            "WebSocket stream capacity reached", 503, "stream_capacity_reached"
                        )
                    pending = asyncio.create_task(self._create_stream(key, options))
                    self.pending[key] = pending
            if stream is None:
                # A disconnecting subscriber must not cancel the shared stream setup task.
                stream = await asyncio.shield(pending)
            async with self._lock:
                if self.closed:
                    raise ExchangeManagerError(
                        "WebSocket gateway is shutting down", 503, "gateway_shutting_down"
                    )
                if stream.stopped:
                    continue
                if stream.idle_task:
                    stream.idle_task.cancel()
                    stream.idle_task = None
                stream.subscribers.setdefault(socket, set()).add(subscription_id)
                outgoing = getattr(socket.state, "outgoing", None)
                if outgoing is not None:
                    outgoing.put_nowait({"type": "subscribed", "id": subscription_id})
                return

    async def unsubscribe(self, socket: Any, subscription_id: str) -> None:
        async with self._lock:
            for stream in self.streams.values():
                ids = stream.subscribers.get(socket)
                if not ids:
                    continue
                ids.discard(subscription_id)
                if not ids:
                    stream.subscribers.pop(socket, None)
                    self._schedule_idle_stop(stream)

    async def disconnect(self, socket: Any) -> None:
        async with self._lock:
            for stream in self.streams.values():
                if stream.subscribers.pop(socket, None) is not None:
                    self._schedule_idle_stop(stream)

    def _schedule_idle_stop(self, stream: Stream) -> None:
        if stream.subscribers or stream.idle_task or stream.stopped:
            return

        async def stop_after_grace() -> None:
            try:
                await asyncio.sleep(self.idle_grace)
                async with self._lock:
                    if stream.subscribers or stream.stopped:
                        return
                    stream.stopped = True
                await self._stop_stream(stream)
            except asyncio.CancelledError:
                raise

        stream.idle_task = asyncio.create_task(stop_after_grace())

    async def _create_stream(self, key: tuple[Any, ...], options: dict[str, Any]) -> Stream:
        lease = None
        stream = None
        try:
            lease = await self.manager.acquire(options["exchange"], "pro")
            if options["channel"] in {"fundingRate", "markPrice"}:
                markets = await lease.exchange.load_markets()
                market = markets.get(options["symbol"])
                if not market or market.get("type") not in {"swap", "future"}:
                    raise ExchangeManagerError(
                        "The symbol is not an available swap or future market",
                        400,
                        "invalid_market_type",
                    )
            method, _, capability = STREAMS[options["channel"]]
            if (
                not callable(getattr(lease.exchange, method, None))
                or getattr(lease.exchange, "has", {}).get(capability) is not True
            ):
                raise ExchangeManagerError(
                    f"Exchange does not support {method}", 501, "method_not_supported"
                )
            stream = Stream(
                key=key,
                exchange=options["exchange"],
                channel=options["channel"],
                symbol=options["symbol"],
                timeframe=options.get("timeframe"),
                limit=options.get("limit"),
                lease=lease,
            )
            async with self._lock:
                if self.closed:
                    raise ExchangeManagerError(
                        "WebSocket gateway is shutting down", 503, "gateway_shutting_down"
                    )
                self.streams[key] = stream
                stream.task = asyncio.create_task(self._run(stream))
            return stream
        except BaseException:
            if lease is not None:
                await lease.release()
            raise
        finally:
            async with self._lock:
                if self.pending.get(key) is asyncio.current_task():
                    self.pending.pop(key, None)
                # Setup can finish after its only subscriber disconnects. Retire that
                # otherwise-orphaned upstream stream after the normal idle grace period.
                if (
                    stream is not None
                    and self.streams.get(key) is stream
                    and not stream.subscribers
                ):
                    self._schedule_idle_stop(stream)

    async def _run(self, stream: Stream) -> None:
        method = STREAMS[stream.channel][0]
        while not stream.stopped:
            try:
                data = await getattr(stream.lease.exchange, method)(*_arguments(stream))
                stream.failures = 0
                await self._broadcast(
                    stream,
                    {
                        "type": "data",
                        "exchange": stream.exchange,
                        "channel": stream.channel,
                        "symbol": stream.symbol,
                        "timeframe": stream.timeframe,
                        "data": data,
                    },
                )
            except asyncio.CancelledError:
                break
            except (
                Exception
            ) as exc:  # CCXT network errors should reconnect without leaking details.
                if stream.stopped:
                    break
                stream.failures += 1
                self.logger.warning(
                    "Upstream stream failed (%s/%s): %s",
                    stream.exchange,
                    stream.channel,
                    type(exc).__name__,
                )
                await self._broadcast(
                    stream,
                    {
                        "type": "error",
                        "exchange": stream.exchange,
                        "channel": stream.channel,
                        "symbol": stream.symbol,
                        "error": "upstream_stream_error",
                        "retrying": True,
                    },
                )
                await asyncio.sleep(min(10.0, 0.25 * (2 ** min(stream.failures, 6))))
        async with self._lock:
            if self.streams.get(stream.key) is stream:
                self.streams.pop(stream.key, None)

    async def _broadcast(self, stream: Stream, payload: dict[str, Any]) -> None:
        async with self._lock:
            targets = list(stream.subscribers.items())
        for socket, ids in targets:
            queue = getattr(socket, "state", None)
            outgoing = getattr(queue, "outgoing", None)
            if outgoing is None or outgoing.full():
                await self.disconnect(socket)
                try:
                    await socket.close(code=1013, reason="Client is too slow")
                except Exception:
                    pass
                continue
            outgoing.put_nowait({**payload, "subscriptionIds": sorted(ids.copy())})

    async def _stop_stream(self, stream: Stream) -> None:
        async with self._lock:
            if stream.stop_task is None:
                stream.stopped = True
                stream.stop_task = asyncio.create_task(self._finish_stop(stream))
            stop_task = stream.stop_task
        await stop_task

    async def _finish_stop(self, stream: Stream) -> None:
        idle_task = stream.idle_task
        if idle_task and idle_task is not asyncio.current_task():
            idle_task.cancel()
        method = STREAMS[stream.channel][1]
        unwatch = getattr(stream.lease.exchange, method, None)
        if callable(unwatch):
            try:
                await asyncio.wait_for(unwatch(*_arguments(stream)), timeout=2)
            except Exception:
                pass
        if stream.task and not stream.task.done():
            stream.task.cancel()
        if stream.task:
            await asyncio.gather(stream.task, return_exceptions=True)
        await stream.lease.release()
        async with self._lock:
            if self.streams.get(stream.key) is stream:
                self.streams.pop(stream.key, None)

    async def close(self) -> None:
        async with self._lock:
            if self.closed:
                return
            self.closed = True
            pending = list(self.pending.values())
            streams = list(self.streams.values())
            for stream in streams:
                stream.stopped = True
                if stream.idle_task:
                    stream.idle_task.cancel()
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        await asyncio.gather(
            *(self._stop_stream(stream) for stream in streams), return_exceptions=True
        )
