from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.config import GatewayConfig


class FakeExchange:
    def __init__(self, options, metrics, is_pro=False):
        self.options = options
        self.metrics = metrics
        self.is_pro = is_pro
        self.has = {
            name: True
            for name in (
                "load_markets",
                "fetch_ticker",
                "fetch_order_book",
                "fetch_trades",
                "fetch_ohlcv",
                "fetch_balance",
                "create_order",
                "cancel_order",
            )
        }
        metrics["instances"] += 1

    async def load_markets(self):
        return {"BTC/USDT": {"symbol": "BTC/USDT"}}

    async def fetch_ticker(self, symbol):
        self.metrics["ticker_calls"] += 1
        return {"symbol": symbol, "last": 42.5}

    async def fetch_order_book(self, symbol, limit=20):
        return {"symbol": symbol, "limit": limit, "bids": [], "asks": []}

    async def fetch_trades(self, symbol, since=None, limit=None):
        return [{"symbol": symbol, "since": since, "limit": limit}]

    async def fetch_ohlcv(self, symbol, timeframe, since=None, limit=None):
        return [[since or 0, 1, 2, 3, 4, 5, timeframe, limit]]

    async def fetch_balance(self):
        self.metrics["balance_calls"] += 1
        return {"USDT": {"free": 10}}

    async def create_order(self, symbol, order_type, side, amount, price=None, params=None):
        self.metrics["order_calls"] += 1
        return {
            "id": "order-1",
            "symbol": symbol,
            "type": order_type,
            "side": side,
            "amount": amount,
            "price": price,
        }

    async def cancel_order(self, order_id, symbol):
        return {"id": order_id, "symbol": symbol, "status": "canceled"}

    async def watch_ticker(self, symbol):
        self.metrics["watch_calls"] += 1
        self.metrics["watch_active"] += 1
        self.metrics["watch_active_max"] = max(
            self.metrics["watch_active_max"], self.metrics["watch_active"]
        )
        try:
            import asyncio

            await asyncio.sleep(0.015)
            return {"symbol": symbol, "last": 42.5}
        finally:
            self.metrics["watch_active"] -= 1

    async def un_watch_ticker(self, symbol):
        return None

    async def close(self):
        self.metrics["closes"] += 1


@pytest.fixture
def exchange_modules():
    metrics = {
        "instances": 0,
        "closes": 0,
        "ticker_calls": 0,
        "balance_calls": 0,
        "order_calls": 0,
        "watch_calls": 0,
        "watch_active": 0,
        "watch_active_max": 0,
    }

    def rest_cls(options):
        return FakeExchange(options, metrics)

    def pro_cls(options):
        return FakeExchange(options, metrics, is_pro=True)

    return (
        SimpleNamespace(exchanges=["binance", "kraken"], binance=rest_cls, kraken=rest_cls),
        SimpleNamespace(binance=pro_cls, kraken=pro_cls),
        metrics,
    )


@pytest.fixture
def gateway_config():
    return GatewayConfig(
        api_key="test-key-0123456789abcdef-0123456789",
        exchange_credentials={"binance": {"apiKey": "public", "secret": "secret"}},
        exchange_idle_ttl=60,
        ws_idle_grace=0.02,
        rate_limit_requests=100,
    )
