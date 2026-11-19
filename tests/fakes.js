export function createFakeCcxt({ watchDelayMs = 5 } = {}) {
  const metrics = { instances: 0, closes: 0, tickerCalls: 0, watchCalls: 0, activeWatches: 0, maxConcurrentWatches: 0, orderCalls: 0, balances: 0 };

  class FakeExchange {
    constructor(options) {
      this.options = options;
      this.has = {
        loadMarkets: true,
        fetchTicker: true,
        fetchOrderBook: true,
        fetchTrades: true,
        fetchOHLCV: true,
        fetchBalance: true,
        createOrder: true,
        cancelOrder: true,
      };
      metrics.instances += 1;
    }

    async loadMarkets() { return { 'BTC/USDT': { symbol: 'BTC/USDT' } }; }
    async fetchTicker(symbol) {
      metrics.tickerCalls += 1;
      return { symbol, last: 123.45 };
    }
    async fetchOrderBook(symbol, limit) { return { symbol, limit: limit ?? 20, bids: [], asks: [] }; }
    async fetchTrades(symbol, since, limit) { return [{ symbol, since: since ?? null, limit: limit ?? 100 }]; }
    async fetchOHLCV(symbol, timeframe, since, limit) { return [[since ?? 0, symbol, timeframe, limit ?? 100]]; }
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
    nodeEnv: 'test',
    ...overrides,
  };
}
