import asyncio
import os
import time
from types import SimpleNamespace

import pytest

from app.exchange_manager import ExchangeManager
from app.market_stream_hub import MarketStreamHub

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_LIVE_INTEGRATION") != "1",
        reason="set RUN_LIVE_INTEGRATION=1",
    ),
]


def kraken_modules():
    import ccxt.async_support as ccxt_async
    import ccxt.pro as ccxt_pro

    return ccxt_async, ccxt_pro


@pytest.mark.asyncio
async def test_kraken_public_async_rest_read_only():
    rest_module, pro_module = kraken_modules()
    manager = ExchangeManager(rest_module, pro_module, idle_ttl=1, timeout_ms=20_000)
    try:
        ticker = await asyncio.wait_for(
            manager.with_exchange("kraken", lambda exchange: exchange.fetch_ticker("BTC/USD")),
            timeout=30,
        )
        assert ticker["symbol"] == "BTC/USD"
        assert ticker.get("last") is not None
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_kraken_public_ccxt_pro_websocket_read_only():
    rest_module, pro_module = kraken_modules()
    manager = ExchangeManager(rest_module, pro_module, idle_ttl=1, timeout_ms=20_000)
    hub = MarketStreamHub(manager, idle_grace=0.1, max_streams=4)

    class LocalSocket:
        def __init__(self):
            self.state = SimpleNamespace(outgoing=asyncio.Queue(maxsize=16))

    socket = LocalSocket()
    try:
        await hub.subscribe(
            socket,
            {"exchange": "kraken", "channel": "ticker", "symbol": "BTC/USD"},
            "live-check",
        )
        ack = await asyncio.wait_for(socket.state.outgoing.get(), timeout=5)
        assert ack["type"] == "subscribed"
        deadline = time.monotonic() + 35
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                pytest.fail("Kraken Pro did not deliver a ticker within 35 seconds")
            update = await asyncio.wait_for(socket.state.outgoing.get(), timeout=remaining)
            if update["type"] == "data":
                break
            assert update["type"] == "error", update
        assert update["data"]["symbol"] == "BTC/USD"
        assert update["data"].get("last") is not None
    finally:
        await hub.close()
        await manager.close()
