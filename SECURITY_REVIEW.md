# Architecture and security review record

Three review passes were performed over the two independently deployable implementations. Findings below were fixed before release candidate preparation.

## Round 1 — API boundaries, authentication, and cache scope

- Checked gateway credentials, exchange credentials, error handling, and private routes in both implementations. REST requires a bearer token; WebSocket keys travel in the first frame; exchange credentials are accepted only through runtime configuration.
- Found that the first Python manager draft required a Pro constructor for ordinary REST use. Changed it to create the Async REST client on demand and the Pro client only when a WebSocket stream is requested. An exchange without Pro support can still serve REST.
- Found that the Python WebSocket adapter initially used CCXT's JavaScript method names. Changed it to CCXT Python's snake-case methods (`watch_ticker`, `watch_order_book`, `watch_trades`, `watch_ohlcv`) and verified those names against the installed CCXT Pro package.
- Added bounded TTL caches to both implementations for public markets, tickers, books, trades, and candles. Private balances and orders bypass caches.
- Added per-peer token-bucket limits to REST and WebSocket commands in both implementations. Client, per-client subscription, upstream stream, and outbound queue limits remain bounded.
- Kept upstream exceptions out of client responses and logs; malformed WebSocket commands return an error frame without discarding a valid connection.

## Round 2 — concurrency, reuse, and shutdown

- Reviewed the exchange lease counter, instance cap, idle expiry, in-flight construction, stream sharing, reconnect backoff, subscriber removal, and shutdown order.
- Found a Python stream-creation cancellation path that could retain a client lease. Stream creation now releases its lease on every failed or cancelled path.
- Verified a single Python Pro watch task fans out to multiple local clients, and that REST and Pro clients are reused separately under one exchange entry.
- Both apps close active streams before closing exchange clients, clear cache state on shutdown, and use one process worker in their deployment commands because their fan-out and limits are in memory.
- Exchange API secrets are redacted from Node logs; Python logs only exchange IDs and error types. Neither implementation logs authorization values or request bodies.

## Round 3 — release and deployment configuration

- Retained the existing Node deployment at the repository root and added a separate Python build context, pinned Python runtime/development requirements, Dockerfile, health check, and MIT license copy.
- Added one CI workflow for Node 22/24, Python 3.12/3.13, a read-only live Kraken REST/WebSocket integration gate, and both Docker builds.
- Added a tag release job: `v*` GitHub Releases are published only after unit tests, live exchange verification, and both container builds pass.
- Added a Northflank profile for two separate single-replica services, each with its own build context and Dockerfile. Account-specific service IDs and secrets remain deployment settings.
- Local automated checks: Node test suite and Python non-live suite. Live exchange, remote GitHub CI, Docker builds, and release publication are reported separately from their actual execution results; the workflow is the required release gate for those remote checks.
