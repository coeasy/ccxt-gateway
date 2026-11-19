import { createHash } from 'node:crypto';

export class IdempotencyConflictError extends Error {
  constructor() {
    super('Idempotency-Key was already used with a different order payload');
    this.statusCode = 409;
    this.code = 'idempotency_conflict';
  }
}

export class IdempotencyCapacityError extends Error {
  constructor() {
    super('Idempotency store capacity reached; retry after existing keys expire');
    this.statusCode = 503;
    this.code = 'idempotency_capacity_reached';
  }
}

export class IdempotencyStore {
  constructor({ ttlMs = 86_400_000, maxEntries = 10_000, now = Date.now } = {}) {
    this.ttlMs = ttlMs;
    this.maxEntries = maxEntries;
    this.now = now;
    this.entries = new Map();
  }

  static fingerprint(value) {
    const canonical = (item) => {
      if (Array.isArray(item)) return item.map(canonical);
      if (item && typeof item === 'object') return Object.fromEntries(Object.keys(item).sort().map((key) => [key, canonical(item[key])]));
      return item;
    };
    return createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
  }

  async execute(key, payload, factory) {
    const fingerprint = IdempotencyStore.fingerprint(payload);
    const now = this.now();
    for (const [entryKey, entry] of this.entries) if (entry.expiresAt <= now) this.entries.delete(entryKey);
    const current = this.entries.get(key);
    if (current) {
      if (current.fingerprint !== fingerprint) throw new IdempotencyConflictError();
      this.entries.delete(key);
      this.entries.set(key, current);
      return current.promise;
    }
    if (this.entries.size >= this.maxEntries) throw new IdempotencyCapacityError();
    const promise = Promise.resolve().then(factory);
    this.entries.set(key, { fingerprint, expiresAt: now + this.ttlMs, promise });
    return promise;
  }
}
