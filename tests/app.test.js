import test from 'node:test';
import assert from 'node:assert/strict';
import { buildApp } from '../src/app.js';
import { createFakeCcxt, testConfig } from './fakes.js';

const authorization = { authorization: 'Bearer test-api-key-0123456789abcdef-0123456789' };

test('health endpoints are public while gateway endpoints require bearer authentication', async (t) => {
  const { ccxt } = createFakeCcxt();
  const app = buildApp({ config: testConfig(), ccxt, logger: false }).fastify;
  t.after(() => app.close());

  const health = await app.inject({ method: 'GET', url: '/health/live' });
  const denied = await app.inject({ method: 'GET', url: '/v1/exchanges' });
  const deniedBeforeValidation = await app.inject({ method: 'GET', url: '/v1/binance/ticker' });
  const allowed = await app.inject({ method: 'GET', url: '/v1/exchanges', headers: authorization });

  assert.equal(health.statusCode, 200);
  assert.equal(denied.statusCode, 401);
  assert.equal(deniedBeforeValidation.statusCode, 401);
  assert.equal(allowed.statusCode, 200);
  assert.ok(allowed.json().exchanges.includes('binance'));
});

test('REST requests share a cached exchange instance and validate request input', async (t) => {
  const { ccxt, metrics } = createFakeCcxt();
  const app = buildApp({ config: testConfig(), ccxt, logger: false }).fastify;
  t.after(() => app.close());

  const first = await app.inject({ method: 'GET', url: '/v1/binance/ticker?symbol=BTC%2FUSDT', headers: authorization });
  const second = await app.inject({ method: 'GET', url: '/v1/binance/ticker?symbol=ETH%2FUSDT', headers: authorization });
  const invalid = await app.inject({ method: 'GET', url: '/v1/binance/ticker', headers: authorization });

  assert.equal(first.statusCode, 200);
  assert.equal(first.json().ticker.last, 123.45);
  assert.equal(second.statusCode, 200);
  assert.equal(metrics.instances, 1);
  assert.equal(metrics.tickerCalls, 2);
  assert.equal(invalid.statusCode, 400);
});

test('private methods require configured exchange credentials and reject unsafe limit orders', async (t) => {
  const { ccxt, metrics } = createFakeCcxt();
  const app = buildApp({
    config: testConfig({ exchangeCredentials: { binance: { apiKey: 'test-key', secret: 'test-secret' } } }),
    ccxt,
    logger: false,
  }).fastify;
  t.after(() => app.close());

  const balance = await app.inject({ method: 'GET', url: '/v1/binance/balance', headers: authorization });
  const missingPrice = await app.inject({
    method: 'POST', url: '/v1/binance/order', headers: { ...authorization, 'content-type': 'application/json' },
    payload: { symbol: 'BTC/USDT', type: 'limit', side: 'buy', amount: 0.1 },
  });
  const order = await app.inject({
    method: 'POST', url: '/v1/binance/order', headers: { ...authorization, 'content-type': 'application/json' },
    payload: { symbol: 'BTC/USDT', type: 'market', side: 'buy', amount: 0.1 },
  });

  assert.equal(balance.statusCode, 200);
  assert.equal(missingPrice.statusCode, 400);
  assert.equal(order.statusCode, 201);
  assert.equal(metrics.orderCalls, 1);
});

test('private methods fail closed when exchange credentials are absent', async (t) => {
  const { ccxt } = createFakeCcxt();
  const app = buildApp({ config: testConfig(), ccxt, logger: false }).fastify;
  t.after(() => app.close());

  const response = await app.inject({ method: 'GET', url: '/v1/binance/balance', headers: authorization });
  assert.equal(response.statusCode, 403);
  assert.equal(response.json().error, 'credentials_not_configured');
});
