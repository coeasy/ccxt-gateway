const STOPPED = Symbol('stream-stopped');

const STREAMS = Object.freeze({
  ticker: { method: 'watchTicker', unwatch: 'unWatchTicker' },
  orderbook: { method: 'watchOrderBook', unwatch: 'unWatchOrderBook' },
  trades: { method: 'watchTrades', unwatch: 'unWatchTrades' },
  ohlcv: { method: 'watchOHLCV', unwatch: 'unWatchOHLCV' },
});

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function streamKey(options) {
  const limit = options.channel === 'ticker' ? null : options.limit ?? null;
  return JSON.stringify([options.exchange, options.channel, options.symbol, options.timeframe ?? null, limit]);
}

function watchArguments(stream) {
  switch (stream.channel) {
    case 'ticker': return [stream.symbol];
    case 'orderbook': return [stream.symbol, stream.limit];
    case 'trades': return [stream.symbol, undefined, stream.limit];
    case 'ohlcv': return [stream.symbol, stream.timeframe, undefined, stream.limit];
    default: return [];
  }
}

export class MarketStreamHub {
  constructor({ exchangeManager, idleGraceMs = 10_000, maxStreams = 1_000, logger = console, maxBufferedBytes = 1_000_000 }) {
    this.exchangeManager = exchangeManager;
    this.idleGraceMs = idleGraceMs;
    this.maxStreams = maxStreams;
    this.logger = logger;
    this.maxBufferedBytes = maxBufferedBytes;
    this.streams = new Map();
    this.pending = new Map();
    this.closed = false;
  }

  async subscribe(socket, options, subscriptionId = 'default') {
    if (this.closed) throw new Error('Stream hub is shutting down');
    if (!STREAMS[options.channel]) throw new Error(`Unsupported stream channel "${options.channel}"`);

    const key = streamKey(options);
    let stream = this.streams.get(key);
    if (stream?.stopped) {
      await (stream.stopTask ?? stream.task);
      stream = this.streams.get(key);
    }
    if (!stream) {
      let pending = this.pending.get(key);
      if (!pending) {
        const activeCount = [...this.streams.values()].filter((item) => !item.stopped).length;
        if (activeCount + this.pending.size >= this.maxStreams) throw new Error('WebSocket stream capacity reached');
        pending = this.#createStream(key, options);
        this.pending.set(key, pending);
      }
      try {
        stream = await pending;
      } finally {
        if (this.pending.get(key) === pending) this.pending.delete(key);
      }
    }
    if (stream.stopped) return this.subscribe(socket, options, subscriptionId);

    if (stream.timer) {
      clearTimeout(stream.timer);
      stream.timer = null;
    }
    let ids = stream.subscribers.get(socket);
    if (!ids) {
      ids = new Set();
      stream.subscribers.set(socket, ids);
    }
    ids.add(subscriptionId);
    return () => this.unsubscribe(socket, stream, subscriptionId);
  }

  unsubscribe(socket, stream, subscriptionId = 'default') {
    const ids = stream.subscribers.get(socket);
    ids?.delete(subscriptionId);
    if (ids?.size === 0) stream.subscribers.delete(socket);
    this.#scheduleStopIfIdle(stream);
  }

  #scheduleStopIfIdle(stream) {
    if (stream.subscribers.size > 0 || stream.stopped || stream.timer) return;
    stream.timer = setTimeout(() => {
      stream.timer = null;
      if (stream.subscribers.size === 0) void this.#stop(stream);
    }, this.idleGraceMs);
    stream.timer.unref?.();
  }

  async close() {
    if (this.closed) return;
    this.closed = true;
    await Promise.allSettled([...this.pending.values()]);
    const streams = [...this.streams.values()];
    await Promise.all(streams.map((stream) => this.#stop(stream)));
  }

  async #createStream(key, options) {
    const lease = await this.exchangeManager.acquire(options.exchange);
    if (this.closed) {
      lease.release();
      throw new Error('Stream hub is shutting down');
    }
    const definition = STREAMS[options.channel];
    if (typeof lease.exchange[definition.method] !== 'function') {
      lease.release();
      throw new Error(`Exchange "${options.exchange}" does not support ${definition.method}`);
    }
    let resolveStop;
    const stream = {
      ...options,
      key,
      lease,
      method: definition.method,
      unwatch: definition.unwatch,
      subscribers: new Map(),
      timer: null,
      stopped: false,
      stopPromise: new Promise((resolve) => { resolveStop = resolve; }),
      resolveStop,
      failures: 0,
      task: null,
      stopTask: null,
    };
    this.streams.set(key, stream);
    stream.task = this.#run(stream).finally(() => {
      if (this.streams.get(key) === stream) this.streams.delete(key);
    });
    return stream;
  }

  #broadcast(stream, message) {
    for (const [socket, ids] of stream.subscribers) {
      if (socket.readyState !== 1) {
        for (const id of ids) this.unsubscribe(socket, stream, id);
        continue;
      }
      if (socket.bufferedAmount > this.maxBufferedBytes) {
        socket.close(1013, 'Client is too slow');
        for (const id of ids) this.unsubscribe(socket, stream, id);
        continue;
      }
      let encoded;
      try {
        encoded = JSON.stringify({ ...message, subscriptionIds: [...ids] });
      } catch {
        socket.close(1011, 'Unable to encode upstream data');
        for (const id of ids) this.unsubscribe(socket, stream, id);
        continue;
      }
      socket.send(encoded, (error) => {
        if (error) this.logger.debug?.({ errorType: error?.name ?? 'Error' }, 'WebSocket send failed');
      });
    }
  }

  async #run(stream) {
    while (!stream.stopped) {
      try {
        const updatePromise = Promise.resolve().then(() => stream.lease.exchange[stream.method](...watchArguments(stream)));
        const update = await Promise.race([updatePromise, stream.stopPromise]);
        if (update === STOPPED || stream.stopped) break;
        stream.failures = 0;
        this.#broadcast(stream, {
          type: 'data',
          exchange: stream.exchange,
          channel: stream.channel,
          symbol: stream.symbol,
          timeframe: stream.timeframe,
          data: update,
        });
      } catch (error) {
        if (stream.stopped) break;
        stream.failures += 1;
        this.logger.warn?.({ errorType: error?.name ?? 'Error', exchange: stream.exchange, channel: stream.channel }, 'Market stream failed');
        this.#broadcast(stream, {
          type: 'error',
          exchange: stream.exchange,
          channel: stream.channel,
          symbol: stream.symbol,
          error: 'upstream_stream_error',
          retrying: true,
        });
        await Promise.race([sleep(Math.min(10_000, 250 * (2 ** Math.min(stream.failures, 6)))), stream.stopPromise]);
      }
    }
  }

  async #stop(stream) {
    if (stream.stopTask) return stream.stopTask;
    stream.stopped = true;
    if (stream.timer) {
      clearTimeout(stream.timer);
      stream.timer = null;
    }
    stream.resolveStop(STOPPED);
    const unwatch = stream.lease.exchange[stream.unwatch];
    stream.stopTask = (async () => {
      try {
        if (typeof unwatch === 'function') {
          await Promise.race([
            Promise.resolve().then(() => unwatch.call(stream.lease.exchange, ...watchArguments(stream))),
            sleep(2_000),
          ]);
        }
      } catch (error) {
        this.logger.debug?.({ errorType: error?.name ?? 'Error', exchange: stream.exchange, channel: stream.channel }, 'Exchange unsubscribe failed');
      } finally {
        stream.lease.release();
      }
      await stream.task;
    })();
    return stream.stopTask;
  }
}
