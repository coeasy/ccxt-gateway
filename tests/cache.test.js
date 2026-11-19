import test from 'node:test';
import assert from 'node:assert/strict';
import { AsyncTTLCache } from '../src/cache.js';

test('coalesces concurrent work, caches the result, and retries failures', async () => {
  const cache = new AsyncTTLCache();
  let calls = 0;
  const factory = async () => {
    calls += 1;
    await new Promise((resolve) => setTimeout(resolve, 5));
    if (calls === 1) throw new Error('temporary failure');
    return { value: calls };
  };

  const failed = await Promise.allSettled([
    cache.getOrCreate('failed', 100, factory),
    cache.getOrCreate('failed', 100, factory),
  ]);
  assert.deepEqual(failed.map((result) => result.status), ['rejected', 'rejected']);
  assert.equal(calls, 1);

  const [left, right] = await Promise.all([
    cache.getOrCreate('ok', 100, factory),
    cache.getOrCreate('ok', 100, factory),
  ]);
  assert.equal(left, right);
  assert.equal(calls, 2);
  assert.equal(await cache.getOrCreate('ok', 100, factory), left);
  assert.equal(calls, 2);
});

test('does not retain results when caching is disabled by capacity or TTL', async () => {
  let calls = 0;
  const factory = async () => ++calls;
  const cache = new AsyncTTLCache({ maxEntries: 0 });

  assert.equal(await cache.getOrCreate('k', 10, factory), 1);
  assert.equal(await cache.getOrCreate('k', 10, factory), 2);
  assert.equal(await cache.getOrCreate('ttl-zero', 0, factory), 3);
  assert.equal(await cache.getOrCreate('ttl-zero', 0, factory), 4);
});
