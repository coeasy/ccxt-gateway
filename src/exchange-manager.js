export class ExchangeManagerError extends Error {
  constructor(message, statusCode = 500, code = 'exchange_manager_error') {
    super(message);
    this.name = 'ExchangeManagerError';
    this.statusCode = statusCode;
    this.code = code;
  }
}

export class ExchangeManager {
  constructor({
    ccxt,
    credentials = {},
    idleTtlMs = 60_000,
    maxInstances = 32,
    timeoutMs = 30_000,
    logger = console,
    now = Date.now,
    setTimer = setTimeout,
    clearTimer = clearTimeout,
  }) {
    if (!ccxt || !Array.isArray(ccxt.exchanges)) throw new TypeError('A CCXT module is required');
    this.ccxt = ccxt;
    this.credentials = credentials;
    this.idleTtlMs = idleTtlMs;
    this.maxInstances = maxInstances;
    this.timeoutMs = timeoutMs;
    this.logger = logger;
    this.now = now;
    this.setTimer = setTimer;
    this.clearTimer = clearTimer;
    this.entries = new Map();
    this.closed = false;
  }

  get size() {
    return this.entries.size;
  }

  listExchanges() {
    return [...this.ccxt.exchanges];
  }

  async acquire(exchangeId) {
    if (this.closed) throw new ExchangeManagerError('Exchange manager is shutting down', 503, 'manager_closed');
    if (!this.ccxt.exchanges.includes(exchangeId)) {
      throw new ExchangeManagerError(`Unsupported exchange "${exchangeId}"`, 404, 'unsupported_exchange');
    }

    let entry = this.entries.get(exchangeId);
    if (!entry) {
      if (this.entries.size >= this.maxInstances) await this.#evictOldestIdle();
      if (this.entries.size >= this.maxInstances) {
        throw new ExchangeManagerError('Exchange instance capacity reached', 503, 'exchange_capacity_reached');
      }
      entry = this.#createEntry(exchangeId);
      this.entries.set(exchangeId, entry);
    }

    if (entry.idleTimer) {
      this.clearTimer(entry.idleTimer);
      entry.idleTimer = null;
    }
    entry.references += 1;
    entry.lastUsedAt = this.now();

    try {
      const exchange = await entry.ready;
      if (entry.closing || this.closed) throw new ExchangeManagerError('Exchange is closing', 503, 'exchange_closing');
      let released = false;
      return {
        exchange,
        release: () => {
          if (released) return;
          released = true;
          this.#release(exchangeId, entry);
        },
      };
    } catch (error) {
      this.#release(exchangeId, entry);
      throw error;
    }
  }

  async withExchange(exchangeId, operation) {
    const lease = await this.acquire(exchangeId);
    try {
      return await operation(lease.exchange);
    } finally {
      lease.release();
    }
  }

  hasCredentials(exchangeId) {
    const credentials = this.credentials[exchangeId];
    return Boolean(credentials?.apiKey && credentials?.secret);
  }

  async close() {
    if (this.closed) return;
    this.closed = true;
    const entries = [...this.entries.entries()];
    await Promise.all(entries.map(([exchangeId, entry]) => this.#closeEntry(exchangeId, entry)));
  }

  #createEntry(exchangeId) {
    const entry = {
      references: 0,
      lastUsedAt: this.now(),
      idleTimer: null,
      closing: false,
      closePromise: null,
      ready: null,
    };
    entry.ready = Promise.resolve().then(() => {
      const Constructor = this.ccxt.pro?.[exchangeId] ?? this.ccxt[exchangeId];
      if (typeof Constructor !== 'function') {
        throw new ExchangeManagerError(`CCXT has no constructor for "${exchangeId}"`, 404, 'exchange_not_available');
      }
      return new Constructor({
        enableRateLimit: true,
        timeout: this.timeoutMs,
        ...(this.credentials[exchangeId] ?? {}),
      });
    }).catch((error) => {
      if (this.entries.get(exchangeId) === entry) this.entries.delete(exchangeId);
      throw error;
    });
    return entry;
  }

  #release(exchangeId, entry) {
    if (entry.references > 0) entry.references -= 1;
    entry.lastUsedAt = this.now();
    if (entry.references !== 0 || entry.closing || this.closed) return;
    entry.idleTimer = this.setTimer(() => {
      entry.idleTimer = null;
      if (entry.references === 0) void this.#closeEntry(exchangeId, entry);
    }, this.idleTtlMs);
    entry.idleTimer.unref?.();
  }

  async #evictOldestIdle() {
    const candidate = [...this.entries.entries()]
      .filter(([, entry]) => entry.references === 0 && !entry.closing)
      .sort((a, b) => a[1].lastUsedAt - b[1].lastUsedAt)[0];
    if (candidate) await this.#closeEntry(candidate[0], candidate[1]);
  }

  async #closeEntry(exchangeId, entry) {
    if (entry.closePromise) return entry.closePromise;
    entry.closing = true;
    entry.closePromise = (async () => {
      if (entry.idleTimer) {
        this.clearTimer(entry.idleTimer);
        entry.idleTimer = null;
      }
      if (this.entries.get(exchangeId) === entry) this.entries.delete(exchangeId);
      try {
        const exchange = await entry.ready;
        await exchange.close?.();
      } catch (error) {
        this.logger.warn?.({ errorType: error?.name ?? 'Error', exchange: exchangeId }, 'Exchange close failed');
      }
    })();
    return entry.closePromise;
  }
}
