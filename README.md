# ccxt-gateway

`coeasy/ccxt-gateway` ships two independent gateway implementations in one repository. Each can be built and deployed as its own service:

| Version | Location | Runtime | Container build context | Dockerfile |
| --- | --- | --- | --- | --- |
| Node.js | repository root | Fastify + CCXT Pro | `/` | `/Dockerfile` |
| Python | [`python/`](python/README.md) | FastAPI + CCXT Async + CCXT Pro | `/python` | `/Dockerfile` |

Both expose authenticated REST and WebSocket APIs, reuse exchange connections, coalesce identical market-data requests, apply gateway rate limits, and close resources during shutdown. REST schemas are published at `/openapi.json` with Swagger UI at `/docs`; `/metrics` exposes authenticated Prometheus metrics and `/version` reports the running build. Their dependency locks and build instructions are separate. See the [MIT License](LICENSE).

## Start the Node.js version

```sh
npm ci
# Set API_KEY to a random value of at least 32 characters.
npm start
```

Run `npm run check` for the Node unit and WebSocket tests.

## Start the Python version

```sh
cd python
python -m pip install -r requirements-dev.lock
# Set API_KEY in the shell or deployment environment first.
python -m uvicorn app.main:create_app --factory --host 0.0.0.0 --port 8080 --workers 1
```

Run `python -m ruff check app tests` and `python -m pytest -m "not integration"` from `python/`. The live test is read-only and checks public Kraken REST and WebSocket market data; run it with `RUN_LIVE_INTEGRATION=1 python -m pytest -m integration`.

## Deployment

Build and deploy the Node and Python folders as **two separate Northflank services**. Both listen on `PORT` (default `8080`) and expose `/health/live` and `/health/ready`. Configure each service's `API_KEY` as a runtime secret. Add `EXCHANGE_CREDENTIALS_JSON` only when private account/order routes are needed. Never put exchange credentials in build arguments.

The detailed service settings and scale constraints are in [`deploy/northflank.md`](deploy/northflank.md). The project has one GitHub Actions workflow that tests both implementations, runs a public live-exchange check, and builds both containers.

Set `DATABASE_URL` to a PostgreSQL-compatible database to enable durable market-history and order-audit storage. Successful REST requests to `/ohlcv` and `/trades` are archived; reads are available through `/v1/archive/ohlcv` and `/v1/archive/trades`. This is request-driven collection, not a backfill or continuous collector. Without a database, the gateway runs normally and archive reads return `503 storage_not_configured`. The SQL schema is in [`migrations/001_market_history.sql`](migrations/001_market_history.sql).

Order submission is disabled by default. Set `TRADING_ENABLED=true` to enable limit orders; market orders also require `MARKET_ORDERS_ENABLED=true`. Every order request must include an `Idempotency-Key` header. Deduplication is process-local; use a single replica until idempotency is backed by a shared store. Keep exchange keys restricted to trading and read access, without withdrawal permission.

## API

Both services provide the same route shape. Send `Authorization: Bearer <API_KEY>` on REST requests. WebSocket clients authenticate in their first JSON frame, so the key does not appear in the URL.

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health/live` | Liveness probe |
| `GET` | `/health/ready` | Readiness probe |
| `GET` | `/version` | Build version and commit identifier |
| `GET` | `/metrics` | Prometheus metrics (Bearer token required) |
| `GET` | `/openapi.json` | OpenAPI 3 document; `/docs` serves Swagger UI |
| `GET` | `/v1/exchanges` | Supported CCXT exchange IDs |
| `GET` | `/v1/{exchange}/capabilities` | Product types, timeframes, REST method status, and WebSocket channel status |
| `GET` | `/v1/{exchange}/markets?type=spot&base=BTC&quote=USDT` | Filter exchange markets by product type and currencies (`spot`, `margin`, `swap`, `future`, `option`, `contract`) |
| `GET` | `/v1/{exchange}/ticker?symbol=BTC%2FUSDT` | Ticker |
| `GET` | `/v1/{exchange}/tickers?symbols=BTC%2FUSDT,ETH%2FUSDT` | Batch tickers for up to 50 symbols when the exchange supports it |
| `GET` | `/v1/{exchange}/orderbook?symbol=BTC%2FUSDT&limit=20` | Order book |
| `GET` | `/v1/{exchange}/trades?symbol=BTC%2FUSDT&since=1700000000000&limit=100&paginate=true&paginationCalls=5` | Trades from a UTC millisecond timestamp; optional bounded CCXT pagination |
| `GET` | `/v1/{exchange}/ohlcv?symbol=BTC%2FUSDT%3AUSDT&timeframe=1h&price=mark&since=1700000000000&paginate=true&paginationCalls=5` | OHLCV candles for last, mark, index, or premium index prices |
| `GET` | `/v1/{exchange}/funding-rate?symbol=BTC%2FUSDT%3AUSDT` | Current contract funding rate |
| `GET` | `/v1/{exchange}/funding-rate-history?symbol=BTC%2FUSDT%3AUSDT&since=1700000000000&limit=100` | Contract funding rate history |
| `GET` | `/v1/{exchange}/open-interest?symbol=BTC%2FUSDT%3AUSDT` | Current contract open interest |
| `GET` | `/v1/{exchange}/open-interest-history?symbol=BTC%2FUSDT%3AUSDT&timeframe=1h&since=1700000000000` | Contract open interest history |
| `GET` | `/v1/{exchange}/mark-price?symbol=BTC%2FUSDT%3AUSDT` | Current contract mark price |
| `GET` | `/v1/{exchange}/mark-price-ohlcv?symbol=BTC%2FUSDT%3AUSDT&timeframe=1h&since=1700000000000` | Historical mark price candles |
| `GET` | `/v1/{exchange}/balance` | Private balance; requires configured credentials |
| `GET` | `/v1/{exchange}/open-orders?symbol=BTC%2FUSDT` | Private open orders |
| `GET` | `/v1/{exchange}/order/{order_id}?symbol=BTC%2FUSDT` | Private order detail |
| `GET` | `/v1/{exchange}/orders/history?symbol=BTC%2FUSDT&since=1700000000000&limit=100` | Private order history |
| `GET` | `/v1/{exchange}/my-trades?symbol=BTC%2FUSDT&since=1700000000000&limit=100` | Private account trades |
| `GET` | `/v1/{exchange}/positions?symbols=BTC%2FUSDT%3AUSDT` | Private contract positions |
| `GET` | `/v1/{exchange}/trading-fee?symbol=BTC%2FUSDT` | Private trading fee |
| `POST` | `/v1/{exchange}/order` | Submit a private order (requires `Idempotency-Key` and explicit trading enablement) |
| `DELETE` | `/v1/{exchange}/order/{order_id}?symbol=BTC%2FUSDT` | Cancel a private order |
| `GET` | `/v1/archive/ohlcv?exchange=binance&symbol=BTC%2FUSDT&timeframe=1h&since=1700000000000` | Read locally archived candles |
| `GET` | `/v1/archive/trades?exchange=binance&symbol=BTC%2FUSDT&since=1700000000000` | Read locally archived trades |

WebSocket clients connect to `/ws`, authenticate with `{"op":"auth","token":"<API_KEY>"}`, then subscribe with `{"op":"subscribe","id":"btc","exchange":"binance","channel":"ticker","symbol":"BTC/USDT"}`. Supported channels are `ticker`, `orderbook`, `trades`, `ohlcv`, `fundingRate`, and `markPrice`. Contract pairs use CCXT's unified contract symbol when required, such as `BTC/USDT:USDT`; discover available pairs with `/markets?type=swap` or `/markets?type=future`.

Historical endpoints pass the requested timestamp and limit to the exchange. Add `paginate=true` to supported history routes to enable CCXT pagination, with `paginationCalls` capped at 5; CCXT documents this facility as experimental and exchange-dependent. With `DATABASE_URL`, successful REST candle and trade responses are also archived locally, but exchange retention limits still apply until data has been collected. A unified symbol selects the exact market; availability of spot, margin, swap, dated futures, options, and each data method varies by exchange. Contract-only endpoints return a not-supported error when that exchange does not implement the underlying CCXT method.

The capabilities response reports each method as `supported`, `emulated`, or `unsupported`, based on the installed CCXT exchange implementation. Use it before invoking optional methods such as batch tickers or contract metrics.

## Resource and scaling model

- Each process caches a REST client and a Pro client per exchange. Requests and active streams hold leases; an idle timer closes both clients after the last lease is released.
- Equivalent public REST requests share a bounded, short-lived cache entry. Private balances and orders are never cached.
- REST and WebSocket commands use a bounded per-peer token bucket. WebSocket client and stream counts are capped.
- `/metrics` reports request counts and latency histograms, exchange/storage errors, cache entries, active exchange instances, and shared streams. It requires the API bearer token.
- WebSocket subscriptions are shared within each process. Keep each Northflank service at one replica unless a shared fan-out layer is added; replicas do not share their in-memory streams or rate-limit buckets.
- Configure CCXT Pro according to its licensing terms. The live integration test only reads public market data and does not place orders.
