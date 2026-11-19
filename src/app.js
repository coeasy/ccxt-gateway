import Fastify from 'fastify';
import fastifyWebsocket from '@fastify/websocket';
import { bearerToken, secureTokenEquals } from './auth.js';
import { AsyncTTLCache } from './cache.js';
import { ExchangeManager, ExchangeManagerError } from './exchange-manager.js';
import { MarketStreamHub } from './market-stream-hub.js';
import { TokenBucketLimiter } from './rate-limit.js';
import { registerWebSocket } from './websocket.js';

const EXCHANGE_PARAM = { type: 'object', required: ['exchange'], properties: { exchange: { type: 'string', pattern: '^[a-z0-9]+$' } } };
const SYMBOL_QUERY = { type: 'object', required: ['symbol'], additionalProperties: false, properties: { symbol: { type: 'string', minLength: 1, maxLength: 64 } } };
const LIMIT_QUERY = { type: 'object', required: ['symbol'], additionalProperties: false, properties: { symbol: { type: 'string', minLength: 1, maxLength: 64 }, limit: { type: 'integer', minimum: 1, maximum: 1000 } } };
const OHLCV_QUERY = { type: 'object', required: ['symbol', 'timeframe'], additionalProperties: false, properties: { symbol: { type: 'string', minLength: 1, maxLength: 64 }, timeframe: { type: 'string', minLength: 1, maxLength: 16 }, since: { type: 'integer', minimum: 0 }, limit: { type: 'integer', minimum: 1, maximum: 1000 } } };

function routeError(error, reply, request, fastify) {
  if (error instanceof ExchangeManagerError) {
    return reply.code(error.statusCode).send({ error: error.code, message: error.message });
  }
  const exchange = request.params?.exchange;
  fastify.log.error({ exchange, errorType: error?.name ?? 'Error' }, 'Exchange request failed');
  return reply.code(502).send({ error: 'exchange_request_failed', message: 'The exchange request failed' });
}

async function callExchange(exchangeManager, exchangeId, method, args, { cache, ttlMs } = {}) {
  const execute = () => exchangeManager.withExchange(exchangeId, async (exchange) => {
    if (typeof exchange[method] !== 'function' || exchange.has?.[method] === false) {
      throw new ExchangeManagerError(`Exchange "${exchangeId}" does not support ${method}`, 501, 'method_not_supported');
    }
    return exchange[method](...args);
  });
  if (!cache || !ttlMs) return execute();
  return cache.getOrCreate(JSON.stringify([exchangeId, method, args]), ttlMs, execute);
}

export function buildApp({ config, ccxt, logger = true } = {}) {
  if (typeof config?.apiKey !== 'string' || config.apiKey.length < 32) {
    throw new Error('API_KEY must contain at least 32 characters');
  }
  if (!ccxt) throw new Error('A CCXT module must be supplied');

  const fastify = Fastify({
    logger: logger === true ? {
      level: config.nodeEnv === 'test' ? 'silent' : 'info',
      redact: { paths: ['req.headers.authorization', 'req.headers["sec-websocket-protocol"]'], censor: '[REDACTED]' },
    } : logger,
    bodyLimit: 64 * 1024,
    trustProxy: false,
  });
  const exchangeManager = new ExchangeManager({
    ccxt,
    credentials: config.exchangeCredentials,
    idleTtlMs: config.exchangeIdleTtlMs,
    maxInstances: config.exchangeMaxInstances,
    timeoutMs: config.exchangeTimeoutMs,
    logger: fastify.log,
  });
  const streamHub = new MarketStreamHub({
    exchangeManager,
    idleGraceMs: config.wsIdleGraceMs,
    maxStreams: config.wsMaxStreams,
    logger: fastify.log,
  });
  const cache = new AsyncTTLCache({ maxEntries: config.cacheMaxEntries });
  const limiter = new TokenBucketLimiter({ capacity: config.rateLimitRequests, windowMs: config.rateLimitWindowMs });

  fastify.register(async (websocketScope) => {
    await websocketScope.register(fastifyWebsocket, { options: { maxPayload: 16 * 1024 } });
    registerWebSocket(websocketScope, {
      hub: streamHub,
      apiKey: config.apiKey,
      maxClients: config.wsMaxClients,
      limiter,
    });
  });

  fastify.get('/health/live', async () => ({ status: 'ok' }));
  fastify.get('/health/ready', async () => ({ status: 'ready' }));

  const authenticate = async (request, reply) => {
    if (!secureTokenEquals(bearerToken(request), config.apiKey)) {
      return reply.code(401).send({ error: 'unauthorized', message: 'A valid bearer token is required' });
    }
  };
  fastify.addHook('onRequest', async (request, reply) => {
    const path = request.url.split('?', 1)[0];
    if (path === '/v1' || path.startsWith('/v1/')) {
      const result = limiter.consume(request.ip || request.socket.remoteAddress || 'unknown');
      if (!result.allowed) {
        reply.header('retry-after', String(result.retryAfterSeconds));
        return reply.code(429).send({ error: 'rate_limit_exceeded', message: 'Gateway request limit exceeded' });
      }
      return authenticate(request, reply);
    }
  });

  fastify.get('/v1/exchanges', async () => ({ exchanges: exchangeManager.listExchanges() }));

  fastify.get('/v1/:exchange/markets', { schema: { params: EXCHANGE_PARAM } }, async (request, reply) => {
    try {
      const markets = await callExchange(exchangeManager, request.params.exchange, 'loadMarkets', [], { cache, ttlMs: config.cacheMarketsTtlMs });
      return { exchange: request.params.exchange, markets };
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.get('/v1/:exchange/ticker', { schema: { params: EXCHANGE_PARAM, querystring: SYMBOL_QUERY } }, async (request, reply) => {
    try {
      const ticker = await callExchange(exchangeManager, request.params.exchange, 'fetchTicker', [request.query.symbol], { cache, ttlMs: config.cacheTickerTtlMs });
      return { exchange: request.params.exchange, symbol: request.query.symbol, ticker };
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.get('/v1/:exchange/orderbook', { schema: { params: EXCHANGE_PARAM, querystring: LIMIT_QUERY } }, async (request, reply) => {
    try {
      const orderbook = await callExchange(exchangeManager, request.params.exchange, 'fetchOrderBook', [request.query.symbol, request.query.limit], { cache, ttlMs: config.cacheOrderbookTtlMs });
      return { exchange: request.params.exchange, symbol: request.query.symbol, orderbook };
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.get('/v1/:exchange/trades', { schema: { params: EXCHANGE_PARAM, querystring: LIMIT_QUERY } }, async (request, reply) => {
    try {
      const trades = await callExchange(exchangeManager, request.params.exchange, 'fetchTrades', [request.query.symbol, undefined, request.query.limit], { cache, ttlMs: config.cacheTradesTtlMs });
      return { exchange: request.params.exchange, symbol: request.query.symbol, trades };
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.get('/v1/:exchange/ohlcv', { schema: { params: EXCHANGE_PARAM, querystring: OHLCV_QUERY } }, async (request, reply) => {
    try {
      const candles = await callExchange(exchangeManager, request.params.exchange, 'fetchOHLCV', [request.query.symbol, request.query.timeframe, request.query.since, request.query.limit], { cache, ttlMs: config.cacheOhlcvTtlMs });
      return { exchange: request.params.exchange, symbol: request.query.symbol, timeframe: request.query.timeframe, candles };
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.get('/v1/:exchange/balance', { schema: { params: EXCHANGE_PARAM } }, async (request, reply) => {
    if (!exchangeManager.hasCredentials(request.params.exchange)) {
      return reply.code(403).send({ error: 'credentials_not_configured', message: 'No exchange credentials are configured' });
    }
    try {
      const balance = await callExchange(exchangeManager, request.params.exchange, 'fetchBalance', []);
      return { exchange: request.params.exchange, balance };
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.post('/v1/:exchange/order', {
    schema: {
      params: EXCHANGE_PARAM,
      body: {
        type: 'object', additionalProperties: false, required: ['symbol', 'type', 'side', 'amount'],
        properties: {
          symbol: { type: 'string', minLength: 1, maxLength: 64 },
          type: { type: 'string', enum: ['limit', 'market'] },
          side: { type: 'string', enum: ['buy', 'sell'] },
          amount: { type: 'number', exclusiveMinimum: 0 },
          price: { type: 'number', exclusiveMinimum: 0 },
          params: { type: 'object', additionalProperties: true },
        },
      },
    },
  }, async (request, reply) => {
    if (!exchangeManager.hasCredentials(request.params.exchange)) {
      return reply.code(403).send({ error: 'credentials_not_configured', message: 'No exchange credentials are configured' });
    }
    const { symbol, type, side, amount, price, params = {} } = request.body;
    if (type === 'limit' && price === undefined) {
      return reply.code(400).send({ error: 'price_required', message: 'Limit orders require a price' });
    }
    try {
      const order = await callExchange(exchangeManager, request.params.exchange, 'createOrder', [symbol, type, side, amount, price, params]);
      return reply.code(201).send({ exchange: request.params.exchange, order });
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.delete('/v1/:exchange/order/:id', {
    schema: {
      params: { type: 'object', required: ['exchange', 'id'], properties: { exchange: { type: 'string', pattern: '^[a-z0-9]+$' }, id: { type: 'string', minLength: 1, maxLength: 128 } } },
      querystring: SYMBOL_QUERY,
    },
  }, async (request, reply) => {
    if (!exchangeManager.hasCredentials(request.params.exchange)) {
      return reply.code(403).send({ error: 'credentials_not_configured', message: 'No exchange credentials are configured' });
    }
    try {
      const order = await callExchange(exchangeManager, request.params.exchange, 'cancelOrder', [request.params.id, request.query.symbol]);
      return { exchange: request.params.exchange, order };
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.addHook('onClose', async () => {
    await streamHub.close();
    await exchangeManager.close();
    cache.clear();
  });

  return { fastify, exchangeManager, streamHub };
}
