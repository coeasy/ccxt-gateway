import test from 'node:test';
import assert from 'node:assert/strict';
import { ExchangeManager, ExchangeManagerError } from '../src/exchange-manager.js';
import { createFakeCcxt } from './fakes.js';

test('reuses one exchange instance for concurrent leases and closes it on shutdown', async () => {
  const { ccxt, metrics } = createFakeCcxt();
  const manager = new ExchangeManager({ ccxt, idleTtlMs: 60_000 });
  const leases = await Promise.all(Array.from({ length: 12 }, () => manager.acquire('binance')));

  assert.equal(metrics.instances, 1);
  assert.equal(manager.size, 1);
  for (const lease of leases) lease.release();
  await manager.close();
  assert.equal(metrics.closes, 1);
  assert.equal(manager.size, 0);
});

test('evicts the least recently used idle exchange when capacity is reached', async () => {
  const { ccxt, metrics } = createFakeCcxt();
  const manager = new ExchangeManager({ ccxt, maxInstances: 1, idleTtlMs: 60_000 });
  const binance = await manager.acquire('binance');
  binance.release();
  const kraken = await manager.acquire('kraken');

  assert.equal(metrics.instances, 2);
  assert.equal(metrics.closes, 1);
  kraken.release();
  await manager.close();
});

test('refuses unsupported exchanges and full capacity while all instances are in use', async () => {
  const { ccxt } = createFakeCcxt();
  const manager = new ExchangeManager({ ccxt, maxInstances: 1 });
  await assert.rejects(manager.acquire('unknown'), (error) => error instanceof ExchangeManagerError && error.statusCode === 404);

  const binance = await manager.acquire('binance');
  await assert.rejects(manager.acquire('kraken'), (error) => error instanceof ExchangeManagerError && error.statusCode === 503);
  binance.release();
  await manager.close();
});

test('closes an unused connection after its idle TTL', async () => {
  const { ccxt, metrics } = createFakeCcxt();
  const manager = new ExchangeManager({ ccxt, idleTtlMs: 10 });
  const lease = await manager.acquire('binance');
  lease.release();
  await new Promise((resolve) => setTimeout(resolve, 30));

  assert.equal(manager.size, 0);
  assert.equal(metrics.closes, 1);
  await manager.close();
});
