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
      task = Promise.resolve().then(factory).then((value) => {
        if (this.inflight.get(key) === task) {
          this.inflight.delete(key);
          if (ttlMs > 0 && this.maxEntries > 0) {
            this.values.set(key, { value, expiresAt: this.now() + ttlMs });
            while (this.values.size > this.maxEntries) this.values.delete(this.values.keys().next().value);
          }
        }
        return value;
      }, (error) => {
        if (this.inflight.get(key) === task) this.inflight.delete(key);
        throw error;
      });
      this.inflight.set(key, task);
    }
    return task;
  }

  clear() {
    this.values.clear();
  }
}
