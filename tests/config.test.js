import test from 'node:test';
import assert from 'node:assert/strict';
import { loadConfig } from '../src/config.js';

test('loads required API key and validated defaults', () => {
  const config = loadConfig({ API_KEY: ' 12345678901234567890123456789012 ' });
  assert.equal(config.apiKey, '12345678901234567890123456789012');
  assert.equal(config.host, '0.0.0.0');
  assert.equal(config.port, 8080);
  assert.deepEqual(config.exchangeCredentials, {});
  assert.equal(config.cacheMaxEntries, 10_000);
  assert.equal(config.rateLimitRequests, 120);
});

test('rejects missing API key and malformed exchange credentials', () => {
  assert.throws(() => loadConfig({}), /API_KEY is required/);
  assert.throws(() => loadConfig({ API_KEY: 'short' }), /at least 32 characters/);
  assert.throws(() => loadConfig({ API_KEY: '12345678901234567890123456789012', EXCHANGE_CREDENTIALS_JSON: '{' }), /valid JSON/);
  assert.throws(() => loadConfig({ API_KEY: '12345678901234567890123456789012', EXCHANGE_CREDENTIALS_JSON: '[]' }), /JSON object/);
});

test('rejects invalid numeric settings and credential field types', () => {
  const apiKey = '12345678901234567890123456789012';
  assert.throws(() => loadConfig({ API_KEY: apiKey, PORT: '0' }), /PORT must be a positive integer/);
  assert.throws(() => loadConfig({ API_KEY: apiKey, PORT: '65536' }), /must not exceed 65535/);
  assert.throws(() => loadConfig({ API_KEY: apiKey, RATE_LIMIT_REQUESTS: '0' }), /positive integer/);
  assert.throws(() => loadConfig({ API_KEY: apiKey, EXCHANGE_CREDENTIALS_JSON: '{"binance":{"secret":42}}' }), /must be a string/);
});
