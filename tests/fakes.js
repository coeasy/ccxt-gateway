export function createFakeCcxt({ watchDelayMs = 5 } = {}) {
  const metrics = { instances: 0, closes: 0, tickerCalls: 0, watchCalls: 0, activeWatches: 0, maxConcurrentWatches: 0, orderCalls: 0, balances: 0 };

  class FakeExchange {
    constructor(options) {
      this.options = options;
      this.has = {
        loadMarkets: true,
        fetchTicker: true,
        fetchTickers: true,
        fetchOrderBook: true,
        fetchTrades: true,
        fetchOHLCV: true,
        fetchFundingRate: true,
        fetchFundingRateHistory: true,
        fetchOpenInterest: true,
        fetchOpenInterestHistory: true,
        fetchMarkPrice: true,
        fetchBalance: true,
        createOrder: true,
        cancelOrder: true,
        watchTicker: true,
        watchFundingRate: true,
        watchMarkPrice: true,
      };
      metrics.instances += 1;
    }

    async loadMarkets() {
      return {
        'BTC/USDT': { symbol: 'BTC/USDT', type: 'spot', base: 'BTC', quote: 'USDT', margin: true },
        'BTC/USDT:USDT': { symbol: 'BTC/USDT:USDT', type: 'swap', base: 'BTC', quote: 'USDT', contract: true },
      };
    }
    async fetchTicker(symbol) {
      metrics.tickerCalls += 1;
      return { symbol, last: 123.45 };
    }
    async fetchTickers(symbols) {
      return Object.fromEntries(symbols.map((symbol) => [symbol, { symbol, last: 123.45 }]));
    }
    async fetchOrderBook(symbol, limit) { return { symbol, limit: limit ?? 20, bids: [], asks: [] }; }
    async fetchTrades(symbol, since, limit, params) { return [{ symbol, since: since ?? null, limit: limit ?? 100, paginate: params?.paginate ?? false, paginationCalls: params?.paginationCalls ?? null, paginationDirection: params?.paginationDirection ?? null }]; }
    async fetchOHLCV(symbol, timeframe, since, limit, params) { return [[since ?? 0, symbol, timeframe, limit ?? 100, params?.paginate ?? false, params?.price ?? null]]; }
    async fetchFundingRate(symbol) { return { symbol, fundingRate: 0.0001 }; }
    async fetchFundingRateHistory(symbol, since, limit, params) { return [{ symbol, since: since ?? null, limit: limit ?? null, paginate: params?.paginate ?? false, paginationCalls: params?.paginationCalls ?? null }]; }
    async fetchOpenInterest(symbol) { return { symbol, openInterestAmount: 10 }; }
    async fetchOpenInterestHistory(symbol, timeframe, since, limit, params) { return [{ symbol, timeframe, since: since ?? null, limit: limit ?? null, paginate: params?.paginate ?? false }]; }
    async fetchMarkPrice(symbol) { return { symbol, markPrice: 123.4 }; }
    async fetchBalance() {
      metrics.balances += 1;
      return { USDT: { free: 10, used: 0, total: 10 } };
    }
    async createOrder(symbol, type, side, amount, price, params) {
      metrics.orderCalls += 1;
      return { id: 'order-1', symbol, type, side, amount, price: price ?? null, params };
    }
    async cancelOrder(id, symbol) { return { id, symbol, status: 'canceled' }; }
    async watchTicker(symbol) {
      metrics.watchCalls += 1;
      metrics.activeWatches += 1;
      metrics.maxConcurrentWatches = Math.max(metrics.maxConcurrentWatches, metrics.activeWatches);
      try {
        await new Promise((resolve) => setTimeout(resolve, watchDelayMs));
        return { symbol, last: 123.45, timestamp: Date.now() };
      } finally {
        metrics.activeWatches -= 1;
      }
    }
    async unWatchTicker() {}
    async watchFundingRate(symbol) {
      await new Promise((resolve) => setTimeout(resolve, watchDelayMs));
      return { symbol, fundingRate: 0.0001 };
    }
    async unWatchFundingRate() {}
    async watchMarkPrice(symbol) {
      await new Promise((resolve) => setTimeout(resolve, watchDelayMs));
      return { symbol, markPrice: 123.4 };
    }
    async unWatchMarkPrice() {}
    async close() { metrics.closes += 1; }
  }

  return {
    metrics,
    ccxt: {
      exchanges: ['binance', 'kraken'],
      binance: FakeExchange,
      kraken: FakeExchange,
      pro: { binance: FakeExchange, kraken: FakeExchange },
    },
  };
}

export function testConfig(overrides = {}) {
  return {
    host: '127.0.0.1',
    port: 0,
    apiKey: 'test-api-key-0123456789abcdef-0123456789',
    exchangeCredentials: {},
    exchangeIdleTtlMs: 60_000,
    exchangeMaxInstances: 8,
    exchangeTimeoutMs: 1000,
    wsIdleGraceMs: 5,
    wsMaxClients: 10,
    wsMaxStreams: 100,
    cacheMaxEntries: 100,
    cacheTickerTtlMs: 1000,
    cacheOrderbookTtlMs: 1000,
    cacheTradesTtlMs: 1000,
    cacheOhlcvTtlMs: 1000,
    cacheMarketsTtlMs: 1000,
    rateLimitRequests: 100,
    rateLimitWindowMs: 60_000,
    nodeEnv: 'test',
    ...overrides,
  };
}
