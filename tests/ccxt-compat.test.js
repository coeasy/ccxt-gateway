import test from 'node:test';
import assert from 'node:assert/strict';
import ccxt from 'ccxt';

test('installed CCXT exposes the REST and Pro constructors used by the gateway', async () => {
  assert.ok(ccxt.exchanges.includes('binance'));
  assert.equal(typeof ccxt.binance, 'function');
  assert.equal(typeof ccxt.pro?.binance, 'function');

  const exchange = new ccxt.pro.binance({ enableRateLimit: true });
  assert.equal(typeof exchange.fetchTicker, 'function');
  assert.equal(typeof exchange.watchTicker, 'function');
  await exchange.close();
});
