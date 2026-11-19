export class TokenBucketLimiter {
  constructor({ capacity = 120, windowMs = 60_000, maxClients = 20_000, now = Date.now } = {}) {
    this.capacity = capacity;
    this.refillPerMs = capacity / windowMs;
    this.maxClients = maxClients;
    this.now = now;
    this.buckets = new Map();
  }

  consume(client) {
    const now = this.now();
    const bucket = this.buckets.get(client) ?? { tokens: this.capacity, updatedAt: now };
    bucket.tokens = Math.min(this.capacity, bucket.tokens + (now - bucket.updatedAt) * this.refillPerMs);
    bucket.updatedAt = now;
    const allowed = bucket.tokens >= 1;
    if (allowed) bucket.tokens -= 1;
    this.buckets.delete(client);
    this.buckets.set(client, bucket);
    while (this.buckets.size > this.maxClients) this.buckets.delete(this.buckets.keys().next().value);
    return {
      allowed,
      retryAfterSeconds: Math.max(1, Math.ceil((1 - bucket.tokens) / this.refillPerMs / 1000)),
    };
  }
}
