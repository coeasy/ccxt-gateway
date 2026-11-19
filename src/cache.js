export class AsyncTTLCache {
  constructor({ maxEntries = 10_000, now = Date.now } = {}) {
    this.maxEntries = maxEntries;
    this.now = now;
    this.values = new Map();
    this.inflight = new Map();
  }

  async getOrCreate(key, ttlMs, factory) {
    const cached = this.values.get(key);
    if (cached && cached.expiresAt > this.now()) {
      this.values.delete(key);
      this.values.set(key, cached);
      return cached.value;
    }
    if (cached) this.values.delete(key);
    let task = this.inflight.get(key);
    if (!task) {
      task = Promise.resolve().then(factory);
      this.inflight.set(key, task);
      try {
        const value = await task;
        if (this.inflight.get(key) === task) {
          this.inflight.delete(key);
          this.values.set(key, { value, expiresAt: this.now() + ttlMs });
          while (this.values.size > this.maxEntries) this.values.delete(this.values.keys().next().value);
        }
        return value;
      } catch (error) {
        if (this.inflight.get(key) === task) this.inflight.delete(key);
        throw error;
      }
    }
    return task;
  }

  clear() {
    this.values.clear();
  }
}
