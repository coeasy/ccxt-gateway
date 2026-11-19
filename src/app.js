import Fastify from 'fastify';
import fastifyWebsocket from '@fastify/websocket';
import fastifySwagger from '@fastify/swagger';
import fastifySwaggerUI from '@fastify/swagger-ui';
import { bearerToken, secureTokenEquals } from './auth.js';
import { AsyncTTLCache } from './cache.js';
import { ExchangeManager, ExchangeManagerError } from './exchange-manager.js';
import { HistoryStore } from './history-store.js';
import { IdempotencyStore } from './idempotency.js';
import { MarketStreamHub } from './market-stream-hub.js';
import { RequestMetrics } from './metrics.js';
import { TokenBucketLimiter } from './rate-limit.js';
import { registerWebSocket } from './websocket.js';

const EXCHANGE_PARAM = { type: 'object', required: ['exchange'], properties: { exchange: { type: 'string', pattern: '^[a-z0-9]+$' } } };
const SYMBOL_QUERY = { type: 'object', required: ['symbol'], additionalProperties: false, properties: { symbol: { type: 'string', minLength: 1, maxLength: 64 } } };
const LIMIT_QUERY = { type: 'object', required: ['symbol'], additionalProperties: false, properties: { symbol: { type: 'string', minLength: 1, maxLength: 64 }, limit: { type: 'integer', minimum: 1, maximum: 1000 } } };
const PAGINATION_PROPERTIES = {
  paginate: { type: 'boolean', default: false },
  paginationCalls: { type: 'integer', minimum: 1, maximum: 5, default: 5 },
};
const TRADES_QUERY = { type: 'object', required: ['symbol'], additionalProperties: false, properties: { symbol: { type: 'string', minLength: 1, maxLength: 64 }, since: { type: 'integer', minimum: 0 }, limit: { type: 'integer', minimum: 1, maximum: 1000 }, ...PAGINATION_PROPERTIES } };
const OHLCV_QUERY = { type: 'object', required: ['symbol', 'timeframe'], additionalProperties: false, properties: { symbol: { type: 'string', minLength: 1, maxLength: 64 }, timeframe: { type: 'string', minLength: 1, maxLength: 16 }, since: { type: 'integer', minimum: 0 }, limit: { type: 'integer', minimum: 1, maximum: 1000 }, price: { type: 'string', enum: ['mark', 'index', 'premiumIndex'] }, ...PAGINATION_PROPERTIES } };
const MARK_PRICE_OHLCV_QUERY = {
  ...OHLCV_QUERY,
  properties: Object.fromEntries(Object.entries(OHLCV_QUERY.properties).filter(([key]) => key !== 'price')),
};
const FUNDING_HISTORY_QUERY = { type: 'object', required: ['symbol'], additionalProperties: false, properties: { symbol: { type: 'string', minLength: 1, maxLength: 64 }, since: { type: 'integer', minimum: 0 }, limit: { type: 'integer', minimum: 1, maximum: 1000 }, ...PAGINATION_PROPERTIES } };
const OPEN_INTEREST_HISTORY_QUERY = { type: 'object', required: ['symbol'], additionalProperties: false, properties: { symbol: { type: 'string', minLength: 1, maxLength: 64 }, timeframe: { type: 'string', minLength: 1, maxLength: 16 }, since: { type: 'integer', minimum: 0 }, limit: { type: 'integer', minimum: 1, maximum: 1000 }, ...PAGINATION_PROPERTIES } };
const MARKET_TYPE = { type: 'string', enum: ['spot', 'margin', 'swap', 'future', 'option', 'contract'] };
const REST_METHODS = Object.freeze({
  markets: 'loadMarkets', ticker: 'fetchTicker', tickers: 'fetchTickers', orderbook: 'fetchOrderBook',
  trades: 'fetchTrades', ohlcv: 'fetchOHLCV', fundingRate: 'fetchFundingRate',
  fundingRateHistory: 'fetchFundingRateHistory', openInterest: 'fetchOpenInterest',
  openInterestHistory: 'fetchOpenInterestHistory', markPrice: 'fetchMarkPrice',
  balance: 'fetchBalance', createOrder: 'createOrder', cancelOrder: 'cancelOrder',
  openOrders: 'fetchOpenOrders', order: 'fetchOrder', orders: 'fetchOrders', myTrades: 'fetchMyTrades',
  positions: 'fetchPositions', tradingFee: 'fetchTradingFee',
});
const WEBSOCKET_METHODS = Object.freeze({
  ticker: 'watchTicker', orderbook: 'watchOrderBook', trades: 'watchTrades',
  ohlcv: 'watchOHLCV', fundingRate: 'watchFundingRate', markPrice: 'watchMarkPrice',
});

function methodStatus(exchange, method) {
  if (typeof exchange[method] !== 'function') return 'unsupported';
  const capability = exchange.has?.[method];
  if (capability === true) return 'supported';
  return capability === 'emulated' ? 'emulated' : 'unsupported';
}

function websocketStatus(exchange, method) {
  return typeof exchange[method] === 'function' && exchange.has?.[method] === true
    ? 'supported'
    : 'unsupported';
}

function parseSymbols(value) {
  const symbols = [...new Set(value.split(',').map((symbol) => symbol.trim()))];
  if (symbols.length === 0 || symbols.length > 50 || symbols.some((symbol) => !symbol || symbol.length > 64)) {
    throw new ExchangeManagerError('Provide between 1 and 50 valid symbols', 400, 'invalid_symbols');
  }
  return symbols;
}

function paginationParams(query, { forwardSince = false } = {}) {
  if (!query.paginate) return {};
  return {
    paginate: true,
    paginationCalls: query.paginationCalls ?? 5,
    ...(forwardSince && query.since !== undefined ? { paginationDirection: 'forward' } : {}),
  };
}

function selectMarkets(markets, { type, base, quote }) {
  const matchesType = (market) => {
    if (!market || typeof market !== 'object') return false;
    if (!type) return true;
    if (type === 'margin') return market.margin === true;
    if (type === 'contract') return market.contract === true || ['swap', 'future', 'option'].includes(market.type);
    return market.type === type;
  };
  return Object.fromEntries(Object.entries(markets).filter(([, market]) => (
    matchesType(market)
    && (!base || market.base?.toUpperCase() === base.toUpperCase())
    && (!quote || market.quote?.toUpperCase() === quote.toUpperCase())
  )));
}

function routeError(error, reply, request, fastify) {
  if (error instanceof ExchangeManagerError) {
    return reply.code(error.statusCode).send({ error: error.code, message: error.message });
  }
  if (error?.code?.startsWith('idempotency_')) return reply.code(error.statusCode ?? 409).send({ error: error.code, message: error.message });
  const exchange = request.params?.exchange;
  if (exchange) fastify.gatewayMetrics.exchangeErrors += 1;
  fastify.log.error({ exchange, errorType: error?.name ?? 'Error' }, 'Exchange request failed');
  return reply.code(502).send({ error: 'exchange_request_failed', message: 'The exchange request failed' });
}

async function callExchange(exchangeManager, exchangeId, method, args, { cache, ttlMs, marketType } = {}) {
  const execute = () => exchangeManager.withExchange(exchangeId, async (exchange) => {
    if (marketType === 'contract') {
      const markets = await exchange.loadMarkets();
      const market = markets?.[args[0]];
      if (!market || !['swap', 'future'].includes(market.type)) {
        throw new ExchangeManagerError('The symbol is not an available swap or future market', 400, 'invalid_market_type');
      }
    }
    const capability = exchange.has?.[method];
    if (typeof exchange[method] !== 'function' || (capability !== true && capability !== 'emulated')) {
      throw new ExchangeManagerError(`Exchange "${exchangeId}" does not support ${method}`, 501, 'method_not_supported');
    }
    return exchange[method](...args);
  });
  if (!cache || !ttlMs) return execute();
  return cache.getOrCreate(JSON.stringify([exchangeId, method, args, marketType ?? null]), ttlMs, execute);
}

export async function buildApp({ config, ccxt, logger = true, historyStore: providedHistoryStore } = {}) {
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
  const metrics = new RequestMetrics();
  const idempotency = new IdempotencyStore();
  const historyStore = providedHistoryStore ?? new HistoryStore(config.databaseUrl);
  fastify.decorate('gatewayMetrics', metrics);

  await fastify.register(fastifySwagger, {
    openapi: {
      openapi: '3.0.3',
      info: { title: 'CCXT Gateway API', version: '0.1.0', description: 'REST routes use a bearer API key. WebSocket clients authenticate in their first JSON frame.' },
      components: { securitySchemes: { bearerAuth: { type: 'http', scheme: 'bearer' } } },
      security: [{ bearerAuth: [] }],
    },
  });
  await fastify.register(fastifySwaggerUI, { routePrefix: '/docs' });

  fastify.register(async (websocketScope) => {
    await websocketScope.register(fastifyWebsocket, { options: { maxPayload: 16 * 1024 } });
    registerWebSocket(websocketScope, {
      hub: streamHub,
      apiKey: config.apiKey,
      maxClients: config.wsMaxClients,
      limiter,
    });
  });

  fastify.addHook('onRequest', async (request) => { request.gatewayStartedAt = performance.now(); });
  fastify.addHook('onResponse', async (request, reply) => {
    const route = request.routeOptions?.url ?? '/unmatched';
    metrics.observe(request.method, route, reply.statusCode, Math.max(0, performance.now() - request.gatewayStartedAt) / 1000);
  });
  fastify.addHook('onSend', async (request, reply, payload) => {
    reply.header('x-request-id', request.id);
    return payload;
  });

  fastify.get('/health/live', { schema: { security: [] } }, async () => ({ status: 'ok' }));
  fastify.get('/health/ready', { schema: { security: [] } }, async (request, reply) => {
    if (!(await historyStore.ping())) return reply.code(503).send({ status: 'not_ready', storage: 'unavailable' });
    return { status: 'ready', storage: historyStore.enabled ? 'connected' : 'disabled' };
  });
  fastify.get('/version', { schema: { security: [] } }, async () => ({ version: '0.1.0', commit: config.gitSha }));
  fastify.get('/openapi.json', { schema: { hide: true, security: [] } }, async () => fastify.swagger());
  fastify.get('/metrics', async (request, reply) => {
    if (!secureTokenEquals(bearerToken(request), config.apiKey)) return reply.code(401).send({ error: 'unauthorized', message: 'A valid bearer token is required' });
    return reply.type('text/plain; version=0.0.4; charset=utf-8').send(metrics.render({ cacheEntries: cache.values.size, exchangeInstances: exchangeManager.size, websocketStreams: streamHub.streams.size }));
  });
  fastify.addHook('onReady', async () => historyStore.initialize());
  fastify.setErrorHandler((error, request, reply) => {
    if (error.validation) return reply.code(400).send({ error: 'validation_error', message: 'Request validation failed', details: error.validation.map(({ instancePath, message }) => ({ path: instancePath, message })) });
    const status = Number.isInteger(error.statusCode) ? error.statusCode : 500;
    const code = status === 404 ? 'not_found' : status === 401 ? 'unauthorized' : 'request_failed';
    if (status >= 500) fastify.log.error({ errorType: error?.name ?? 'Error' }, 'Request failed');
    return reply.code(status).send({ error: code, message: status >= 500 ? 'The request failed' : error.message });
  });

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

  fastify.get('/v1/:exchange/markets', {
    schema: {
      params: EXCHANGE_PARAM,
      querystring: {
        type: 'object', additionalProperties: false,
        properties: {
          type: MARKET_TYPE,
          base: { type: 'string', minLength: 1, maxLength: 16 },
          quote: { type: 'string', minLength: 1, maxLength: 16 },
        },
      },
    },
  }, async (request, reply) => {
    try {
      const markets = await callExchange(exchangeManager, request.params.exchange, 'loadMarkets', [], { cache, ttlMs: config.cacheMarketsTtlMs });
      return {
        exchange: request.params.exchange,
        filters: { type: request.query.type ?? null, base: request.query.base ?? null, quote: request.query.quote ?? null },
        markets: selectMarkets(markets, request.query),
      };
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.get('/v1/:exchange/capabilities', { schema: { params: EXCHANGE_PARAM } }, async (request, reply) => {
    try {
      return await exchangeManager.withExchange(request.params.exchange, async (exchange) => {
        const markets = await exchange.loadMarkets();
        const marketTypes = new Set();
        for (const market of Object.values(markets)) {
          if (market?.type) marketTypes.add(market.type);
          if (market?.margin === true) marketTypes.add('margin');
          if (market?.contract === true || ['swap', 'future', 'option'].includes(market?.type)) marketTypes.add('contract');
        }
        return {
          exchange: request.params.exchange,
          marketCount: Object.keys(markets).length,
          marketTypes: [...marketTypes].sort(),
          timeframes: Object.keys(exchange.timeframes ?? {}).sort(),
          rest: Object.fromEntries(Object.entries(REST_METHODS).map(([name, method]) => [name, methodStatus(exchange, method)])),
          websocket: Object.fromEntries(Object.entries(WEBSOCKET_METHODS).map(([name, method]) => [name, websocketStatus(exchange, method)])),
        };
      });
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.get('/v1/:exchange/ticker', { schema: { params: EXCHANGE_PARAM, querystring: SYMBOL_QUERY } }, async (request, reply) => {
    try {
      const ticker = await callExchange(exchangeManager, request.params.exchange, 'fetchTicker', [request.query.symbol], { cache, ttlMs: config.cacheTickerTtlMs });
      return { exchange: request.params.exchange, symbol: request.query.symbol, ticker };
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.get('/v1/:exchange/tickers', {
    schema: {
      params: EXCHANGE_PARAM,
      querystring: {
        type: 'object', required: ['symbols'], additionalProperties: false,
        properties: { symbols: { type: 'string', minLength: 3, maxLength: 3249 } },
      },
    },
  }, async (request, reply) => {
    try {
      const symbols = parseSymbols(request.query.symbols);
      const tickers = await callExchange(exchangeManager, request.params.exchange, 'fetchTickers', [symbols], { cache, ttlMs: config.cacheTickerTtlMs });
      return { exchange: request.params.exchange, symbols, tickers };
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.get('/v1/:exchange/orderbook', { schema: { params: EXCHANGE_PARAM, querystring: LIMIT_QUERY } }, async (request, reply) => {
    try {
      const orderbook = await callExchange(exchangeManager, request.params.exchange, 'fetchOrderBook', [request.query.symbol, request.query.limit], { cache, ttlMs: config.cacheOrderbookTtlMs });
      return { exchange: request.params.exchange, symbol: request.query.symbol, orderbook };
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.get('/v1/:exchange/trades', { schema: { params: EXCHANGE_PARAM, querystring: TRADES_QUERY } }, async (request, reply) => {
    try {
      const trades = await callExchange(exchangeManager, request.params.exchange, 'fetchTrades', [request.query.symbol, request.query.since, request.query.limit, paginationParams(request.query, { forwardSince: true })], { cache, ttlMs: config.cacheTradesTtlMs });
      try { await historyStore.saveTrades(request.params.exchange, request.query.symbol, trades); }
      catch (error) { metrics.storageErrors += 1; fastify.log.error({ errorType: error?.name ?? 'Error' }, 'Failed to archive exchange trades'); }
      return { exchange: request.params.exchange, symbol: request.query.symbol, trades };
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.get('/v1/:exchange/ohlcv', { schema: { params: EXCHANGE_PARAM, querystring: OHLCV_QUERY } }, async (request, reply) => {
    try {
      const candles = await callExchange(exchangeManager, request.params.exchange, 'fetchOHLCV', [request.query.symbol, request.query.timeframe, request.query.since, request.query.limit, { ...(request.query.price ? { price: request.query.price } : {}), ...paginationParams(request.query) }], { cache, ttlMs: config.cacheOhlcvTtlMs, marketType: request.query.price ? 'contract' : undefined });
      try { await historyStore.saveCandles(request.params.exchange, request.query.symbol, request.query.timeframe, candles, request.query.price ?? 'last'); }
      catch (error) { metrics.storageErrors += 1; fastify.log.error({ errorType: error?.name ?? 'Error' }, 'Failed to archive exchange candles'); }
      return { exchange: request.params.exchange, symbol: request.query.symbol, timeframe: request.query.timeframe, price: request.query.price ?? 'last', candles };
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  const ARCHIVE_QUERY = {
    type: 'object', required: ['exchange', 'symbol'], additionalProperties: false,
    properties: {
      exchange: { type: 'string', pattern: '^[a-z0-9]+$' }, symbol: { type: 'string', minLength: 1, maxLength: 64 },
      timeframe: { type: 'string', minLength: 1, maxLength: 16 }, price: { type: 'string', enum: ['last', 'mark', 'index', 'premiumIndex'], default: 'last' },
      since: { type: 'integer', minimum: 0 }, until: { type: 'integer', minimum: 0 }, limit: { type: 'integer', minimum: 1, maximum: 1000, default: 1000 },
    },
  };
  fastify.get('/v1/archive/ohlcv', { schema: { querystring: { ...ARCHIVE_QUERY, required: ['exchange', 'symbol', 'timeframe'] } } }, async (request, reply) => {
    if (!historyStore.enabled) return reply.code(503).send({ error: 'storage_not_configured', message: 'Set DATABASE_URL to enable archived market data' });
    if (request.query.until !== undefined && request.query.since !== undefined && request.query.until < request.query.since) return reply.code(400).send({ error: 'invalid_time_range', message: 'until must be greater than or equal to since' });
    try {
      const candles = await historyStore.query('ohlcv', { ...request.query, timeframe: `${request.query.timeframe}:${request.query.price ?? 'last'}` });
      return { source: 'archive', exchange: request.query.exchange, symbol: request.query.symbol, timeframe: request.query.timeframe, price: request.query.price ?? 'last', candles };
    } catch (error) { fastify.log.error({ errorType: error?.name ?? 'Error' }, 'History query failed'); return reply.code(503).send({ error: 'storage_unavailable', message: 'Historical storage is temporarily unavailable' }); }
  });
  fastify.get('/v1/archive/trades', {
    schema: { querystring: { type: 'object', required: ['exchange', 'symbol'], additionalProperties: false, properties: { exchange: ARCHIVE_QUERY.properties.exchange, symbol: ARCHIVE_QUERY.properties.symbol, since: ARCHIVE_QUERY.properties.since, until: ARCHIVE_QUERY.properties.until, limit: ARCHIVE_QUERY.properties.limit } } },
  }, async (request, reply) => {
    if (!historyStore.enabled) return reply.code(503).send({ error: 'storage_not_configured', message: 'Set DATABASE_URL to enable archived market data' });
    if (request.query.until !== undefined && request.query.since !== undefined && request.query.until < request.query.since) return reply.code(400).send({ error: 'invalid_time_range', message: 'until must be greater than or equal to since' });
    try {
      const trades = await historyStore.query('trades', request.query);
      return { source: 'archive', exchange: request.query.exchange, symbol: request.query.symbol, trades };
    } catch (error) { fastify.log.error({ errorType: error?.name ?? 'Error' }, 'History query failed'); return reply.code(503).send({ error: 'storage_unavailable', message: 'Historical storage is temporarily unavailable' }); }
  });

  fastify.get('/v1/:exchange/funding-rate', { schema: { params: EXCHANGE_PARAM, querystring: SYMBOL_QUERY } }, async (request, reply) => {
    try {
      const fundingRate = await callExchange(exchangeManager, request.params.exchange, 'fetchFundingRate', [request.query.symbol], { cache, ttlMs: config.cacheTickerTtlMs, marketType: 'contract' });
      return { exchange: request.params.exchange, symbol: request.query.symbol, fundingRate };
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.get('/v1/:exchange/funding-rate-history', { schema: { params: EXCHANGE_PARAM, querystring: FUNDING_HISTORY_QUERY } }, async (request, reply) => {
    try {
      const fundingRates = await callExchange(exchangeManager, request.params.exchange, 'fetchFundingRateHistory', [request.query.symbol, request.query.since, request.query.limit, paginationParams(request.query)], { marketType: 'contract' });
      return { exchange: request.params.exchange, symbol: request.query.symbol, fundingRates };
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.get('/v1/:exchange/open-interest', { schema: { params: EXCHANGE_PARAM, querystring: SYMBOL_QUERY } }, async (request, reply) => {
    try {
      const openInterest = await callExchange(exchangeManager, request.params.exchange, 'fetchOpenInterest', [request.query.symbol], { cache, ttlMs: config.cacheTickerTtlMs, marketType: 'contract' });
      return { exchange: request.params.exchange, symbol: request.query.symbol, openInterest };
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.get('/v1/:exchange/open-interest-history', { schema: { params: EXCHANGE_PARAM, querystring: OPEN_INTEREST_HISTORY_QUERY } }, async (request, reply) => {
    try {
      const history = await callExchange(exchangeManager, request.params.exchange, 'fetchOpenInterestHistory', [request.query.symbol, request.query.timeframe ?? '5m', request.query.since, request.query.limit, paginationParams(request.query)], { marketType: 'contract' });
      return { exchange: request.params.exchange, symbol: request.query.symbol, timeframe: request.query.timeframe ?? '5m', history };
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.get('/v1/:exchange/mark-price', { schema: { params: EXCHANGE_PARAM, querystring: SYMBOL_QUERY } }, async (request, reply) => {
    try {
      const markPrice = await callExchange(exchangeManager, request.params.exchange, 'fetchMarkPrice', [request.query.symbol], { cache, ttlMs: config.cacheTickerTtlMs, marketType: 'contract' });
      return { exchange: request.params.exchange, symbol: request.query.symbol, markPrice };
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.get('/v1/:exchange/mark-price-ohlcv', { schema: { params: EXCHANGE_PARAM, querystring: MARK_PRICE_OHLCV_QUERY } }, async (request, reply) => {
    try {
      const candles = await callExchange(exchangeManager, request.params.exchange, 'fetchOHLCV', [request.query.symbol, request.query.timeframe, request.query.since, request.query.limit, { price: 'mark', ...paginationParams(request.query) }], { marketType: 'contract' });
      return { exchange: request.params.exchange, symbol: request.query.symbol, timeframe: request.query.timeframe, price: 'mark', candles };
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

  const PRIVATE_QUERY = { type: 'object', additionalProperties: false, properties: { symbol: { type: 'string', minLength: 1, maxLength: 64 }, since: { type: 'integer', minimum: 0 }, limit: { type: 'integer', minimum: 1, maximum: 1000, default: 100 } } };
  const privateGuard = (exchange, reply) => {
    if (exchangeManager.hasCredentials(exchange)) return true;
    reply.code(403).send({ error: 'credentials_not_configured', message: 'No exchange credentials are configured' });
    return false;
  };
  const privateRoute = async (request, reply, method, args) => {
    if (!privateGuard(request.params.exchange, reply)) return reply;
    try {
      const value = await callExchange(exchangeManager, request.params.exchange, method, args);
      const responseKey = method === 'fetchPositions' ? 'positions' : method === 'fetchTradingFee' ? 'fee' : method === 'fetchMyTrades' ? 'trades' : 'orders';
      return { exchange: request.params.exchange, [responseKey]: value };
    } catch (error) { return routeError(error, reply, request, fastify); }
  };

  fastify.get('/v1/:exchange/open-orders', { schema: { params: EXCHANGE_PARAM, querystring: PRIVATE_QUERY } }, async (request, reply) => privateRoute(request, reply, 'fetchOpenOrders', [request.query.symbol]));
  fastify.get('/v1/:exchange/order/:order_id', {
    schema: { params: { type: 'object', required: ['exchange', 'order_id'], properties: { exchange: { type: 'string', pattern: '^[a-z0-9]+$' }, order_id: { type: 'string', minLength: 1, maxLength: 128 } }, additionalProperties: false }, querystring: SYMBOL_QUERY },
  }, async (request, reply) => {
    if (!privateGuard(request.params.exchange, reply)) return reply;
    try { return { exchange: request.params.exchange, order: await callExchange(exchangeManager, request.params.exchange, 'fetchOrder', [request.params.order_id, request.query.symbol]) }; }
    catch (error) { return routeError(error, reply, request, fastify); }
  });
  fastify.get('/v1/:exchange/orders/history', { schema: { params: EXCHANGE_PARAM, querystring: { ...PRIVATE_QUERY, required: ['symbol'] } } }, async (request, reply) => privateRoute(request, reply, 'fetchOrders', [request.query.symbol, request.query.since, request.query.limit]));
  fastify.get('/v1/:exchange/my-trades', { schema: { params: EXCHANGE_PARAM, querystring: { ...PRIVATE_QUERY, required: ['symbol'] } } }, async (request, reply) => privateRoute(request, reply, 'fetchMyTrades', [request.query.symbol, request.query.since, request.query.limit]));
  fastify.get('/v1/:exchange/positions', {
    schema: { params: EXCHANGE_PARAM, querystring: { type: 'object', additionalProperties: false, properties: { symbols: { type: 'string', minLength: 1, maxLength: 3249 } } } },
  }, async (request, reply) => {
    let symbols;
    try { symbols = request.query.symbols ? parseSymbols(request.query.symbols) : undefined; }
    catch (error) { return routeError(error, reply, request, fastify); }
    return privateRoute(request, reply, 'fetchPositions', [symbols]);
  });
  fastify.get('/v1/:exchange/trading-fee', { schema: { params: EXCHANGE_PARAM, querystring: SYMBOL_QUERY } }, async (request, reply) => privateRoute(request, reply, 'fetchTradingFee', [request.query.symbol]));

  fastify.post('/v1/:exchange/order', {
    schema: {
      params: EXCHANGE_PARAM,
      headers: { type: 'object', required: ['idempotency-key'], properties: { 'idempotency-key': { type: 'string', minLength: 8, maxLength: 128, pattern: '^[A-Za-z0-9._:-]+$' } }, additionalProperties: true },
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
    if (!config.tradingEnabled) return reply.code(403).send({ error: 'trading_disabled', message: 'Set TRADING_ENABLED=true to enable order submission' });
    const { symbol, type, side, amount, price, params = {} } = request.body;
    if (type === 'market' && !config.marketOrdersEnabled) return reply.code(403).send({ error: 'market_orders_disabled', message: 'Set MARKET_ORDERS_ENABLED=true to enable market orders' });
    if (type === 'limit' && price === undefined) {
      return reply.code(400).send({ error: 'price_required', message: 'Limit orders require a price' });
    }
    try {
      const idempotencyKey = request.headers['idempotency-key'];
      const orderPayload = { exchange: request.params.exchange, symbol, type, side, amount, price: price ?? null, params };
      const order = await idempotency.execute(`${request.params.exchange}:${idempotencyKey}`, orderPayload, async () => {
        const created = await callExchange(exchangeManager, request.params.exchange, 'createOrder', [symbol, type, side, amount, price, params]);
        const idempotencyHash = IdempotencyStore.fingerprint(idempotencyKey);
        const auditPayload = { symbol, type, side, amount, price: price ?? null, requestId: request.id };
        fastify.log.info({ event: 'order.created', exchange: request.params.exchange, orderId: created?.id, idempotencyHash, ...auditPayload }, 'Order audit');
        try { await historyStore.audit('order.created', request.params.exchange, created?.id, idempotencyHash, auditPayload); }
        catch (error) { fastify.log.error({ errorType: error?.name ?? 'Error', exchange: request.params.exchange }, 'Persistent order audit write failed'); }
        return created;
      });
      return reply.code(201).send({ exchange: request.params.exchange, order });
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.delete('/v1/:exchange/order/:order_id', {
    schema: {
      params: { type: 'object', required: ['exchange', 'order_id'], properties: { exchange: { type: 'string', pattern: '^[a-z0-9]+$' }, order_id: { type: 'string', minLength: 1, maxLength: 128 } } },
      querystring: SYMBOL_QUERY,
    },
  }, async (request, reply) => {
    if (!exchangeManager.hasCredentials(request.params.exchange)) {
      return reply.code(403).send({ error: 'credentials_not_configured', message: 'No exchange credentials are configured' });
    }
    try {
      const order = await callExchange(exchangeManager, request.params.exchange, 'cancelOrder', [request.params.order_id, request.query.symbol]);
      const keyHash = IdempotencyStore.fingerprint(`cancel:${request.params.exchange}:${request.params.order_id}:${request.query.symbol}`);
      const auditPayload = { symbol: request.query.symbol, requestId: request.id };
      fastify.log.info({ event: 'order.cancelled', exchange: request.params.exchange, orderId: request.params.order_id, ...auditPayload }, 'Order audit');
      try { await historyStore.audit('order.cancelled', request.params.exchange, request.params.order_id, keyHash, auditPayload); }
      catch (error) { metrics.storageErrors += 1; fastify.log.error({ errorType: error?.name ?? 'Error' }, 'Persistent order audit write failed'); }
      return { exchange: request.params.exchange, order };
    } catch (error) { return routeError(error, reply, request, fastify); }
  });

  fastify.addHook('onClose', async () => {
    await streamHub.close();
    await exchangeManager.close();
    cache.clear();
    await historyStore.close();
  });

  return { fastify, exchangeManager, streamHub };
}
