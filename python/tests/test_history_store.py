import json
import os
import time

import pytest

from app.history_store import HistoryStore


@pytest.mark.skipif(
    not os.getenv("TEST_DATABASE_URL"), reason="PostgreSQL CI service is not configured"
)
async def test_postgres_persists_and_queries_candles_trades_and_audit():
    store = HistoryStore(os.environ["TEST_DATABASE_URL"])
    exchange = f"python-ci-{os.getpid()}-{time.time_ns()}"
    symbol = "BTC/USDT"
    timestamp = time.time_ns() // 1_000_000
    idempotency_hash = f"sha256-{timestamp}"
    await store.initialize()
    try:
        candles = [[timestamp, 1, 2, 0.5, 1.5, 10]]
        trades = [{"id": f"trade-{timestamp}", "timestamp": timestamp, "price": 1.5, "amount": 2}]
        await store.save_candles(exchange, symbol, "1m", candles)
        await store.save_trades(exchange, symbol, trades)
        await store.audit(
            "order.created", exchange, f"order-{timestamp}", idempotency_hash, {"symbol": symbol}
        )

        assert await store.query("ohlcv", exchange, symbol, "1m:last") == candles
        assert await store.query("trades", exchange, symbol) == trades
        audit = await store.pool.fetchrow(
            "SELECT payload::text AS payload FROM gateway_audit_events WHERE idempotency_hash = $1",
            idempotency_hash,
        )
        assert json.loads(audit["payload"]) == {"symbol": symbol}
    finally:
        await store.pool.execute("DELETE FROM market_history WHERE exchange = $1", exchange)
        await store.pool.execute(
            "DELETE FROM gateway_audit_events WHERE idempotency_hash = $1", idempotency_hash
        )
        await store.close()
