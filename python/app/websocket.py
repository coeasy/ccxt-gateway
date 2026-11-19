from __future__ import annotations

import asyncio
import hmac
import json
import logging
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect

from app.exchange_manager import ExchangeManagerError
from app.market_stream_hub import MarketStreamHub
from app.rate_limit import TokenBucketLimiter

logger = logging.getLogger("ccxt_gateway.websocket")
MAX_SUBSCRIPTIONS_PER_CLIENT = 50


class SocketState:
    def __init__(self) -> None:
        self.outgoing: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=256)
        self.subscription_ids: set[str] = set()


def _valid_subscription(message: dict[str, Any]) -> dict[str, Any]:
    exchange = message.get("exchange")
    channel = message.get("channel")
    symbol = message.get("symbol")
    sub_id = message.get("id")
    if not isinstance(exchange, str) or not exchange.isascii() or not exchange.isalnum():
        raise ValueError("exchange must be a CCXT exchange id")
    if channel not in {
        "ticker",
        "orderbook",
        "trades",
        "ohlcv",
        "fundingRate",
        "markPrice",
    }:
        raise ValueError("unsupported channel")
    if not isinstance(symbol, str) or not 1 <= len(symbol) <= 64:
        raise ValueError("symbol must be 1-64 characters")
    if not isinstance(sub_id, str) or not 1 <= len(sub_id) <= 64:
        raise ValueError("id must be 1-64 characters")
    limit = message.get("limit")
    if limit is not None and (
        not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 1000
    ):
        raise ValueError("limit must be from 1 to 1000")
    timeframe = message.get("timeframe")
    if channel == "ohlcv" and (not isinstance(timeframe, str) or not 1 <= len(timeframe) <= 16):
        raise ValueError("timeframe is required for ohlcv")
    return {
        "exchange": exchange,
        "channel": channel,
        "symbol": symbol,
        "limit": limit,
        "timeframe": timeframe,
    }


async def serve_websocket(
    websocket: WebSocket,
    hub: MarketStreamHub,
    api_key: str,
    max_clients: int,
    limiter: TokenBucketLimiter,
) -> None:
    state = SocketState()
    websocket.state.outgoing = state.outgoing
    await websocket.accept()
    active = getattr(websocket.app.state, "ws_clients", set())
    if len(active) >= max_clients:
        await websocket.close(code=1013, reason="Client capacity reached")
        return
    active.add(websocket)
    websocket.app.state.ws_clients = active
    writer: asyncio.Task[None] | None = None
    try:
        try:
            first = await asyncio.wait_for(websocket.receive_text(), timeout=5)
            auth_message = json.loads(first)
        except (TimeoutError, json.JSONDecodeError, ValueError):
            await websocket.close(code=1008, reason="Authenticate with the first JSON frame")
            return
        if (
            not isinstance(auth_message, dict)
            or auth_message.get("op") != "auth"
            or not isinstance(auth_message.get("token"), str)
            or not hmac.compare_digest(auth_message["token"].encode(), api_key.encode())
        ):
            await websocket.close(code=1008, reason="Unauthorized")
            return
        await state.outgoing.put({"type": "ready"})

        async def write_loop() -> None:
            while True:
                message = await state.outgoing.get()
                await websocket.send_json(message)

        async def report_error(code: str, message: str) -> None:
            try:
                state.outgoing.put_nowait(
                    {"type": "error", "error": code, "message": message[:160]}
                )
            except asyncio.QueueFull:
                await websocket.close(code=1013, reason="Client is too slow")

        writer = asyncio.create_task(write_loop())
        while True:
            try:
                message = await websocket.receive_json()
            except (json.JSONDecodeError, ValueError):
                await report_error("invalid_json", "Frame must contain valid JSON")
                continue
            peer = websocket.client.host if websocket.client else "unknown"
            allowed, retry_after = limiter.consume(f"ws:{peer}")
            if not allowed:
                await report_error("rate_limit_exceeded", f"Retry after {retry_after} seconds")
                await websocket.close(code=1013, reason="Message rate limit exceeded")
                break
            try:
                if not isinstance(message, dict):
                    raise ValueError("frame must be a JSON object")
                operation = message.get("op")
                if operation == "ping":
                    state.outgoing.put_nowait({"type": "pong", "id": message.get("id")})
                elif operation == "subscribe":
                    options = _valid_subscription(message)
                    sub_id = message["id"]
                    if sub_id in state.subscription_ids:
                        raise ValueError("subscription id is already in use")
                    if len(state.subscription_ids) >= MAX_SUBSCRIPTIONS_PER_CLIENT:
                        raise ValueError("subscription limit reached")
                    await hub.subscribe(websocket, options, sub_id)
                    state.subscription_ids.add(sub_id)
                elif operation == "unsubscribe":
                    sub_id = message.get("id")
                    if not isinstance(sub_id, str):
                        raise ValueError("subscription id is required")
                    if sub_id in state.subscription_ids:
                        state.subscription_ids.discard(sub_id)
                        await hub.unsubscribe(websocket, sub_id)
                    state.outgoing.put_nowait({"type": "unsubscribed", "id": sub_id})
                else:
                    raise ValueError("unsupported operation")
            except (ValueError, ExchangeManagerError) as exc:
                await report_error(getattr(exc, "code", "invalid_message"), str(exc))
            except asyncio.QueueFull:
                await websocket.close(code=1013, reason="Client is too slow")
                break
    except WebSocketDisconnect:
        pass
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning("WebSocket session failed: %s", type(exc).__name__)
    finally:
        await hub.disconnect(websocket)
        active.discard(websocket)
        if writer:
            writer.cancel()
            await asyncio.gather(writer, return_exceptions=True)
        try:
            await websocket.close()
        except Exception:
            pass
