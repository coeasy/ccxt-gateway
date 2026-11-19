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
  const cached = await app.inject({ method: 'GET', url: '/v1/binance/ticker?symbol=BTC%2FUSDT', headers: authorization });
  const invalid = await app.inject({ method: 'GET', url: '/v1/binance/ticker', headers: authorization });

  assert.equal(first.statusCode, 200);
  assert.equal(first.json().ticker.last, 123.45);
  assert.equal(second.statusCode, 200);
  assert.equal(cached.statusCode, 200);
  assert.equal(metrics.instances, 1);
  assert.equal(metrics.tickerCalls, 2);
  assert.equal(invalid.statusCode, 400);
});

test('market filters, historical query parameters, and contract metrics reach CCXT', async (t) => {
  const { ccxt } = createFakeCcxt();
  const app = buildApp({ config: testConfig(), ccxt, logger: false }).fastify;
  t.after(() => app.close());

  const markets = await app.inject({
    method: 'GET', url: '/v1/binance/markets?type=swap&base=btc&quote=usdt', headers: authorization,
  });
  const contracts = await app.inject({
    method: 'GET', url: '/v1/binance/markets?type=contract', headers: authorization,
  });
  const marginMarkets = await app.inject({
    method: 'GET', url: '/v1/binance/markets?type=margin', headers: authorization,
  });
  const trades = await app.inject({
    method: 'GET', url: '/v1/binance/trades?symbol=BTC%2FUSDT%3AUSDT&since=1700000000000&limit=50&paginate=true&paginationCalls=3', headers: authorization,
  });
  const funding = await app.inject({
    method: 'GET', url: '/v1/binance/funding-rate-history?symbol=BTC%2FUSDT%3AUSDT&since=1700000000000&limit=20&paginate=true&paginationCalls=3', headers: authorization,
  });
  const spotFunding = await app.inject({
    method: 'GET', url: '/v1/binance/funding-rate?symbol=BTC%2FUSDT', headers: authorization,
  });
  const interest = await app.inject({
    method: 'GET', url: '/v1/binance/open-interest-history?symbol=BTC%2FUSDT%3AUSDT&timeframe=1h&since=1700000000000', headers: authorization,
  });
  const markPrice = await app.inject({
    method: 'GET', url: '/v1/binance/mark-price?symbol=BTC%2FUSDT%3AUSDT', headers: authorization,
  });
  const markCandles = await app.inject({
    method: 'GET', url: '/v1/binance/mark-price-ohlcv?symbol=BTC%2FUSDT%3AUSDT&timeframe=1h&since=1700000000000&paginate=true&paginationCalls=2', headers: authorization,
  });
  const indexCandles = await app.inject({
    method: 'GET', url: '/v1/binance/ohlcv?symbol=BTC%2FUSDT%3AUSDT&timeframe=1h&price=index', headers: authorization,
  });

  assert.deepEqual(Object.keys(markets.json().markets), ['BTC/USDT:USDT']);
  assert.deepEqual(Object.keys(contracts.json().markets), ['BTC/USDT:USDT']);
  assert.deepEqual(Object.keys(marginMarkets.json().markets), ['BTC/USDT']);
  assert.equal(trades.json().trades[0].since, 1700000000000);
  assert.equal(trades.json().trades[0].paginate, true);
  assert.equal(trades.json().trades[0].paginationCalls, 3);
  assert.equal(trades.json().trades[0].paginationDirection, 'forward');
  assert.equal(funding.json().fundingRates[0].limit, 20);
  assert.equal(funding.json().fundingRates[0].paginationCalls, 3);
  assert.equal(spotFunding.statusCode, 400);
  assert.equal(spotFunding.json().error, 'invalid_market_type');
  assert.equal(interest.json().history[0].timeframe, '1h');
  assert.equal(markPrice.json().markPrice.markPrice, 123.4);
  assert.equal(markCandles.json().candles[0][4], true);
  assert.equal(markCandles.json().candles[0][5], 'mark');
  assert.equal(indexCandles.json().price, 'index');
  assert.equal(indexCandles.json().candles[0][5], 'index');
});

test('gateway token bucket returns 429 with a retry window after the configured capacity', async (t) => {
  const { ccxt } = createFakeCcxt();
  const app = buildApp({ config: testConfig({ rateLimitRequests: 1 }), ccxt, logger: false }).fastify;
  t.after(() => app.close());

  const first = await app.inject({ method: 'GET', url: '/v1/exchanges', headers: authorization });
  const second = await app.inject({ method: 'GET', url: '/v1/exchanges', headers: authorization });
  assert.equal(first.statusCode, 200);
  assert.equal(second.statusCode, 429);
  assert.ok(Number(second.headers['retry-after']) >= 1);
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
