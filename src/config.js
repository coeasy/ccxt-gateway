const DEFAULTS = Object.freeze({
  host: '0.0.0.0',
  port: 8080,
  exchangeIdleTtlMs: 60_000,
  exchangeMaxInstances: 32,
  exchangeTimeoutMs: 30_000,
  wsIdleGraceMs: 10_000,
  wsMaxClients: 500,
  wsMaxStreams: 1_000,
});

function positiveInteger(value, name, fallback) {
  if (value === undefined || value === '') return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

function parseCredentials(value) {
  if (value === undefined || value === '') return Object.freeze({});
  let parsed;
  try {
    parsed = JSON.parse(value);
  } catch {
    throw new Error('EXCHANGE_CREDENTIALS_JSON must contain valid JSON');
  }
  if (parsed === null || Array.isArray(parsed) || typeof parsed !== 'object') {
    throw new Error('EXCHANGE_CREDENTIALS_JSON must be a JSON object');
  }
  for (const [exchangeId, credentials] of Object.entries(parsed)) {
    if (!/^[a-z0-9]+$/.test(exchangeId) || credentials === null || Array.isArray(credentials) || typeof credentials !== 'object') {
      throw new Error(`Invalid credentials entry for exchange "${exchangeId}"`);
    }
    for (const field of ['apiKey', 'secret', 'password', 'uid', 'token']) {
      if (credentials[field] !== undefined && typeof credentials[field] !== 'string') {
        throw new Error(`${exchangeId}.${field} must be a string`);
      }
    }
  }
  return Object.freeze(parsed);
}

export function loadConfig(env = process.env) {
  const apiKey = env.API_KEY?.trim();
  if (!apiKey) throw new Error('API_KEY is required');
  if (apiKey.length < 32) throw new Error('API_KEY must be at least 32 characters');
  const port = positiveInteger(env.PORT, 'PORT', DEFAULTS.port);
  if (port > 65_535) throw new Error('PORT must not exceed 65535');

  return Object.freeze({
    host: env.HOST?.trim() || DEFAULTS.host,
    port,
    apiKey,
    exchangeCredentials: parseCredentials(env.EXCHANGE_CREDENTIALS_JSON),
    exchangeIdleTtlMs: positiveInteger(env.EXCHANGE_IDLE_TTL_MS, 'EXCHANGE_IDLE_TTL_MS', DEFAULTS.exchangeIdleTtlMs),
    exchangeMaxInstances: positiveInteger(env.EXCHANGE_MAX_INSTANCES, 'EXCHANGE_MAX_INSTANCES', DEFAULTS.exchangeMaxInstances),
    exchangeTimeoutMs: positiveInteger(env.EXCHANGE_TIMEOUT_MS, 'EXCHANGE_TIMEOUT_MS', DEFAULTS.exchangeTimeoutMs),
    wsIdleGraceMs: positiveInteger(env.WS_IDLE_GRACE_MS, 'WS_IDLE_GRACE_MS', DEFAULTS.wsIdleGraceMs),
    wsMaxClients: positiveInteger(env.WS_MAX_CLIENTS, 'WS_MAX_CLIENTS', DEFAULTS.wsMaxClients),
    wsMaxStreams: positiveInteger(env.WS_MAX_STREAMS, 'WS_MAX_STREAMS', DEFAULTS.wsMaxStreams),
    nodeEnv: env.NODE_ENV || 'development',
  });
}
