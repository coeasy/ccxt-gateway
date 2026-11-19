CREATE TABLE IF NOT EXISTS market_history (
  kind TEXT NOT NULL CHECK (kind IN ('ohlcv', 'trades')),
  exchange TEXT NOT NULL,
  symbol TEXT NOT NULL,
  timeframe TEXT NOT NULL DEFAULT '',
  ts BIGINT NOT NULL,
  item_key TEXT NOT NULL,
  payload JSONB NOT NULL,
  PRIMARY KEY (kind, exchange, symbol, timeframe, ts, item_key)
);
CREATE INDEX IF NOT EXISTS market_history_lookup_idx
  ON market_history (kind, exchange, symbol, timeframe, ts);
CREATE TABLE IF NOT EXISTS gateway_audit_events (
  id BIGSERIAL PRIMARY KEY,
  event_type TEXT NOT NULL,
  exchange TEXT NOT NULL,
  order_id TEXT,
  idempotency_hash TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
