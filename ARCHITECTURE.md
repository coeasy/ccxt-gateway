# Architecture and request flow

The repository contains two standalone implementations with the same service boundary:

- `src/` is the Node.js/Fastify implementation.
- `python/app/` is the Python/FastAPI implementation.
- `tests/` and `python/tests/` cover each implementation independently.
- `deploy/`, `Dockerfile`, and `python/Dockerfile` describe separate deployments.

## REST market-data request

1. The HTTP layer assigns a request ID, applies the per-process token bucket, checks the bearer key, and validates route input.
2. Public market-data handlers build a cache key from the exchange, CCXT method, arguments, and product type. Identical in-flight reads share one upstream call; successful results enter a bounded TTL cache.
3. The exchange manager leases the exchange's Async REST client. It reuses the client while active, reference-counts leases, and closes idle clients after the configured delay. Python closes an evicted client outside the manager lock so slow cleanup cannot block unrelated acquisitions.
4. The handler validates optional CCXT capabilities and contract-market type, performs the read, and records OHLCV/trades when PostgreSQL is enabled.
5. The HTTP layer returns normalized errors and records route metrics. Private account and order calls bypass the market-data cache.

## WebSocket market-data request

1. A socket authenticates in its first frame and obeys the client limit and token bucket.
2. A subscription key combines exchange, channel, symbol, timeframe, and any meaningful depth limit. The stream hub coalesces concurrent setup and runs one CCXT Pro watch loop per key.
3. Updates are fanned out to local subscribers. Slow clients are disconnected instead of allowing unbounded buffering; watch failures are reported and retried with capped backoff.
4. Removing the last subscriber starts an idle grace timer. The hub then unwatches, stops the loop, and releases its exchange lease. Shutdown stops streams before closing exchange clients.

## Storage and shutdown

PostgreSQL is optional. Successful REST candle/trade reads are archived on request; there is no background collector or historical backfill. Order actions write audit records when storage is enabled. Health readiness checks storage connectivity when configured.

Shutdown order is deliberate: stop WebSocket streams, finish/cancel cached in-flight reads, close exchange clients, then close storage. Rate-limit buckets, caches, idempotency records, exchange connections, and WebSocket streams are process-local, so replicas do not share them.

## Implementation boundaries

Route registration and HTTP error serialization stay at each framework entry point (`src/app.js` and `python/app/main.py`). Exchange lifecycle, stream lifecycle, cache, idempotency, metrics, rate limiting, and history storage are separate modules. Keep new endpoint behavior in the route layer and cross-cutting state changes in the corresponding module so both implementations remain reviewable and behaviorally aligned.
