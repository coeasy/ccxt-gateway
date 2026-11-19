import asyncio

import pytest

from app.exchange_manager import ExchangeManager, ExchangeManagerError


@pytest.mark.asyncio
async def test_manager_reuses_async_and_pro_clients_and_closes(exchange_modules):
    rest, pro, metrics = exchange_modules
    manager = ExchangeManager(rest, pro, idle_ttl=60)
    first = await manager.acquire("binance")
    assert metrics["instances"] == 1
    second = await manager.acquire("binance", "pro")
    assert first.exchange is not second.exchange
    assert metrics["instances"] == 2
    await first.release()
    await second.release()
    assert len(manager.entries) == 1
    await manager.close()
    assert metrics["closes"] == 2


@pytest.mark.asyncio
async def test_rest_exchange_does_not_require_a_pro_constructor(exchange_modules):
    rest, pro, _ = exchange_modules
    del pro.kraken
    manager = ExchangeManager(rest, pro)
    lease = await manager.acquire("kraken", "rest")
    with pytest.raises(ExchangeManagerError) as error:
        await manager.acquire("kraken", "pro")
    assert error.value.code == "exchange_unavailable"
    await lease.release()
    await manager.close()


@pytest.mark.asyncio
async def test_manager_rejects_unknown_exchange(exchange_modules):
    manager = ExchangeManager(exchange_modules[0], exchange_modules[1])
    with pytest.raises(ExchangeManagerError) as error:
        await manager.acquire("not-an-exchange")
    assert error.value.status_code == 404
    await manager.close()


@pytest.mark.asyncio
async def test_manager_coalesces_concurrent_constructors_and_idle_closes(exchange_modules):
    rest, pro, metrics = exchange_modules
    manager = ExchangeManager(rest, pro, idle_ttl=0.02)
    leases = await asyncio.gather(*(manager.acquire("binance") for _ in range(8)))
    assert metrics["instances"] == 1
    await asyncio.gather(*(lease.release() for lease in leases))
    await asyncio.sleep(0.05)
    assert manager.entries == {}
    assert metrics["closes"] == 1
    await manager.close()


@pytest.mark.asyncio
async def test_manager_evicts_idle_entry_but_rejects_when_all_are_leased(exchange_modules):
    rest, pro, metrics = exchange_modules
    manager = ExchangeManager(rest, pro, max_instances=1, idle_ttl=60)
    first = await manager.acquire("binance")
    with pytest.raises(ExchangeManagerError) as error:
        await manager.acquire("kraken")
    assert error.value.code == "exchange_capacity_reached"
    await first.release()
    second = await manager.acquire("kraken")
    assert set(manager.entries) == {"kraken"}
    assert metrics["closes"] == 1
    await second.release()
    await manager.close()
