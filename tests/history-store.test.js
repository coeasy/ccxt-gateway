import test from 'node:test';
import assert from 'node:assert/strict';
import { HistoryStore } from '../src/history-store.js';

test('PostgreSQL persists and queries candles, trades, and audit events', {
  skip: !process.env.TEST_DATABASE_URL,
}, async () => {
  const store = new HistoryStore(process.env.TEST_DATABASE_URL);
  const exchange = `node-ci-${process.pid}-${Date.now()}`;
  const symbol = 'BTC/USDT';
  const timestamp = Date.now();
  const idempotencyHash = `sha256-${timestamp}`;
  await store.initialize();
  try {
    const candles = [[timestamp, 1, 2, 0.5, 1.5, 10]];
    const trades = [{ id: `trade-${timestamp}`, timestamp, price: 1.5, amount: 2 }];
    await store.saveCandles(exchange, symbol, '1m', candles);
    await store.saveTrades(exchange, symbol, trades);
    await store.audit('order.created', exchange, `order-${timestamp}`, idempotencyHash, { symbol });

    assert.deepEqual(await store.query('ohlcv', { exchange, symbol, timeframe: '1m:last' }), candles);
    assert.deepEqual(await store.query('trades', { exchange, symbol }), trades);
    const audit = await store.pool.query(
      'SELECT payload FROM gateway_audit_events WHERE idempotency_hash = $1',
      [idempotencyHash],
    );
    assert.deepEqual(audit.rows[0].payload, { symbol });
  } finally {
    await store.pool.query('DELETE FROM market_history WHERE exchange = $1', [exchange]);
    await store.pool.query('DELETE FROM gateway_audit_events WHERE idempotency_hash = $1', [idempotencyHash]);
    await store.close();
  }
});
