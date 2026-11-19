from __future__ import annotations

import hashlib
import json
from typing import Any

SCHEMA = """
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
"""


def _trade_key(trade: dict[str, Any]) -> str:
    if trade.get("id") is not None:
        return str(trade["id"])[:256]
    value = [trade.get("timestamp"), trade.get("price"), trade.get("amount"), trade.get("side")]
    encoded = json.dumps(value, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


class HistoryStore:
    def __init__(self, database_url: str | None) -> None:
        self.database_url = database_url
        self.pool: Any | None = None

    @property
    def enabled(self) -> bool:
        return self.database_url is not None

    async def initialize(self) -> None:
        if not self.database_url:
            return
        import asyncpg

        self.pool = await asyncpg.create_pool(
            self.database_url, min_size=1, max_size=10, command_timeout=10
        )
        async with self.pool.acquire() as connection:
            for statement in SCHEMA.split(";"):
                if statement.strip():
                    await connection.execute(statement)

    async def ping(self) -> bool:
        if not self.enabled:
            return True
        if self.pool is None:
            return False
        try:
            await self.pool.fetchval("SELECT 1")
            return True
        except Exception:
            return False

    async def save_candles(
        self, exchange: str, symbol: str, timeframe: str, candles: Any, price: str = "last"
    ) -> None:
        if not self.pool or not isinstance(candles, list):
            return
        rows = [
            (
                "ohlcv",
                exchange,
                symbol,
                f"{timeframe}:{price}",
                int(row[0]),
                "candle",
                json.dumps(row, separators=(",", ":")),
            )
            for row in candles
            if isinstance(row, (list, tuple)) and row and row[0] is not None and int(row[0]) >= 0
        ]
        if rows:
            await self.pool.executemany(
                """INSERT INTO market_history (
                       kind, exchange, symbol, timeframe, ts, item_key, payload
                   )
                   VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                   ON CONFLICT (kind, exchange, symbol, timeframe, ts, item_key)
                   DO UPDATE SET payload = EXCLUDED.payload""",
                rows,
            )

    async def save_trades(self, exchange: str, symbol: str, trades: Any) -> None:
        if not self.pool or not isinstance(trades, list):
            return
        rows = [
            (
                "trades",
                exchange,
                symbol,
                "",
                int(trade["timestamp"]),
                _trade_key(trade),
                json.dumps(trade, separators=(",", ":"), allow_nan=False),
            )
            for trade in trades
            if isinstance(trade, dict) and trade.get("timestamp") is not None
        ]
        if rows:
            await self.pool.executemany(
                """INSERT INTO market_history (
                       kind, exchange, symbol, timeframe, ts, item_key, payload
                   )
                   VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                   ON CONFLICT (kind, exchange, symbol, timeframe, ts, item_key)
                   DO UPDATE SET payload = EXCLUDED.payload""",
                rows,
            )

    async def query(
        self,
        kind: str,
        exchange: str,
        symbol: str,
        timeframe: str = "",
        since: int | None = None,
        until: int | None = None,
        limit: int = 1000,
    ) -> list[Any] | None:
        if not self.pool:
            return None
        rows = await self.pool.fetch(
            """SELECT payload::text AS payload FROM market_history
               WHERE kind = $1 AND exchange = $2 AND symbol = $3 AND timeframe = $4
                 AND ($5::bigint IS NULL OR ts >= $5) AND ($6::bigint IS NULL OR ts <= $6)
               ORDER BY ts ASC LIMIT $7""",
            kind,
            exchange,
            symbol,
            timeframe,
            since,
            until,
            limit,
        )
        return [json.loads(row["payload"]) for row in rows]

    async def audit(
        self,
        event_type: str,
        exchange: str,
        order_id: str | None,
        idempotency_hash: str,
        payload: dict[str, Any],
    ) -> None:
        if self.pool:
            await self.pool.execute(
                """INSERT INTO gateway_audit_events (
                       event_type, exchange, order_id, idempotency_hash, payload
                   ) VALUES ($1, $2, $3, $4, $5::jsonb)""",
                event_type,
                exchange,
                order_id,
                idempotency_hash,
                json.dumps(payload, separators=(",", ":"), allow_nan=False),
            )

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None
