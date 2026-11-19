import { createHash } from 'node:crypto';
import pg from 'pg';

const { Pool } = pg;

const SCHEMA = `
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
`;

function tradeKey(trade) {
  if (trade?.id !== undefined && trade.id !== null) return String(trade.id).slice(0, 256);
  return createHash('sha256').update(JSON.stringify([trade?.timestamp, trade?.price, trade?.amount, trade?.side])).digest('hex');
}

export class HistoryStore {
  constructor(databaseUrl) {
    this.pool = databaseUrl ? new Pool({ connectionString: databaseUrl, max: 10, idleTimeoutMillis: 30_000 }) : null;
  }

  get enabled() { return this.pool !== null; }

  async ping() {
    if (!this.pool) return true;
    try { await this.pool.query('SELECT 1'); return true; }
    catch { return false; }
  }

  async initialize() {
    if (this.pool) {
      for (const statement of SCHEMA.split(';').map((part) => part.trim()).filter(Boolean)) {
        await this.pool.query(statement);
      }
    }
  }

  async saveCandles(exchange, symbol, timeframe, candles, price = 'last') {
    if (!this.pool || !Array.isArray(candles) || candles.length === 0) return;
    const rows = candles.filter((row) => Array.isArray(row) && row[0] !== null && row[0] !== undefined && Number.isSafeInteger(Number(row[0])))
      .map((row) => ['ohlcv', exchange, symbol, `${timeframe}:${price}`, Number(row[0]), 'candle', JSON.stringify(row)]);
    if (!rows.length) return;
    await this.pool.query(
      `INSERT INTO market_history (kind, exchange, symbol, timeframe, ts, item_key, payload)
       SELECT x.kind, x.exchange, x.symbol, x.timeframe, x.ts, x.item_key, x.payload::jsonb
       FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::bigint[], $6::text[], $7::text[])
       AS x(kind, exchange, symbol, timeframe, ts, item_key, payload)
       ON CONFLICT (kind, exchange, symbol, timeframe, ts, item_key) DO UPDATE SET payload = EXCLUDED.payload`,
      rows[0].map((_, index) => rows.map((row) => row[index])),
    );
  }

  async saveTrades(exchange, symbol, trades) {
    if (!this.pool || !Array.isArray(trades) || trades.length === 0) return;
    const rows = trades.filter((trade) => trade?.timestamp !== null && trade?.timestamp !== undefined && Number.isSafeInteger(Number(trade.timestamp)))
      .map((trade) => ['trades', exchange, symbol, '', Number(trade.timestamp), tradeKey(trade), JSON.stringify(trade)]);
    if (!rows.length) return;
    await this.pool.query(
      `INSERT INTO market_history (kind, exchange, symbol, timeframe, ts, item_key, payload)
       SELECT x.kind, x.exchange, x.symbol, x.timeframe, x.ts, x.item_key, x.payload::jsonb
       FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::bigint[], $6::text[], $7::text[])
       AS x(kind, exchange, symbol, timeframe, ts, item_key, payload)
       ON CONFLICT (kind, exchange, symbol, timeframe, ts, item_key) DO UPDATE SET payload = EXCLUDED.payload`,
      rows[0].map((_, index) => rows.map((row) => row[index])),
    );
  }

  async query(kind, { exchange, symbol, timeframe = '', since = null, until = null, limit = 1000 }) {
    if (!this.pool) return null;
    const { rows } = await this.pool.query(
      `SELECT payload FROM market_history
       WHERE kind = $1 AND exchange = $2 AND symbol = $3 AND timeframe = $4
         AND ($5::bigint IS NULL OR ts >= $5) AND ($6::bigint IS NULL OR ts <= $6)
       ORDER BY ts ASC LIMIT $7`,
      [kind, exchange, symbol, timeframe, since, until, limit],
    );
    return rows.map((row) => row.payload);
  }

  async audit(eventType, exchange, orderId, idempotencyHash, payload) {
    if (!this.pool) return;
    await this.pool.query(
      'INSERT INTO gateway_audit_events (event_type, exchange, order_id, idempotency_hash, payload) VALUES ($1, $2, $3, $4, $5::jsonb)',
      [eventType, exchange, orderId ?? null, idempotencyHash ?? null, JSON.stringify(payload)],
    );
  }

  async close() { await this.pool?.end(); }
}
