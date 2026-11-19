import asyncio
from types import SimpleNamespace

import pytest

from app.exchange_manager import ExchangeManager
from app.market_stream_hub import MarketStreamHub


class DelayedAcquireManager:
    def __init__(self, manager):
        self.manager = manager
        self.started = asyncio.Event()
        self.resume = asyncio.Event()
        self.calls = 0

    async def acquire(self, exchange_id, client):
        self.calls += 1
        self.started.set()
        await self.resume.wait()
        return await self.manager.acquire(exchange_id, client)


def socket_stub():
    socket = type("SocketStub", (), {})()
    socket.state = SimpleNamespace(outgoing=asyncio.Queue(maxsize=8))
    return socket


@pytest.mark.asyncio
async def test_cancelled_subscriber_does_not_cancel_shared_stream_setup(exchange_modules):
    rest, pro, metrics = exchange_modules
    manager = ExchangeManager(rest, pro)
    delayed_manager = DelayedAcquireManager(manager)
    hub = MarketStreamHub(delayed_manager, idle_grace=0.02)
    options = {"exchange": "binance", "channel": "ticker", "symbol": "BTC/USDT"}

    first = asyncio.create_task(hub.subscribe(socket_stub(), options, "first"))
    await delayed_manager.started.wait()
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    second_socket = socket_stub()
    second = asyncio.create_task(hub.subscribe(second_socket, options, "second"))
    await asyncio.sleep(0)
    delayed_manager.resume.set()
    try:
        await second
        acknowledgement = await asyncio.wait_for(second_socket.state.outgoing.get(), 1)
        assert acknowledgement == {"type": "subscribed", "id": "second"}
        assert len(hub.streams) == 1
        assert delayed_manager.calls == 1
        assert metrics["instances"] == 2  # paired Async REST and Pro clients
    finally:
        await hub.close()
        await manager.close()
