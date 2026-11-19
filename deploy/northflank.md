# Northflank deployment profile

Deploy **two separate Combined Services** from this repository so the Python and Node versions can scale, configure, and roll back independently. Both may use the `main` branch; each service points at a different Docker build context and Dockerfile.

| Service | Build context | Dockerfile | Container port | Replicas |
| --- | --- | --- | ---: | ---: |
| Node.js | `/` | `/Dockerfile` | `8080` | `1` |
| Python | `/python` | `/Dockerfile` | `8080` | `1` |

For each service, select **Combined Service**, connect `coeasy/ccxt-gateway`, choose **Dockerfile** build, and enable deployment after a successful build. Configure HTTP health probes on `/health/live` and `/health/ready`; allow at least 10 seconds for startup.

## Runtime secrets and settings

Required for both services:

- `API_KEY`: random secret of at least 32 characters.
- `PORT`: optional; the platform assigned port is honored, with `8080` as fallback.
- `EXCHANGE_CREDENTIALS_JSON`: optional JSON object of server-side exchange keys. Only configure it if private balance/order routes are needed. Use keys without withdrawal permissions.
- `DATABASE_URL`: optional PostgreSQL connection string for archived candles/trades and durable order audit events. Both containers run schema initialization on startup.
- `TRADING_ENABLED`: defaults to `false`; set to `true` to allow limit orders.
- `MARKET_ORDERS_ENABLED`: defaults to `false`; additionally set to `true` to allow market orders.
- `GIT_SHA`: optional commit identifier returned by `/version`.

Both implementations support exchange reuse limits (`EXCHANGE_MAX_INSTANCES`, `EXCHANGE_TIMEOUT_MS`), WebSocket client/stream limits (`WS_MAX_CLIENTS`, `WS_MAX_STREAMS`), cache capacity (`CACHE_MAX_ENTRIES`), and gateway rate limits (`RATE_LIMIT_REQUESTS`). Their TTL and rate-window units use different environment names: Node uses `*_MS`; Python uses `*_SECONDS`.

Node cache settings: `CACHE_TICKER_TTL_MS`, `CACHE_ORDERBOOK_TTL_MS`, `CACHE_TRADES_TTL_MS`, `CACHE_OHLCV_TTL_MS`, and `CACHE_MARKETS_TTL_MS`.

Python cache settings: `CACHE_TICKER_TTL_SECONDS`, `CACHE_ORDERBOOK_TTL_SECONDS`, `CACHE_TRADES_TTL_SECONDS`, `CACHE_OHLCV_TTL_SECONDS`, and `CACHE_MARKETS_TTL_SECONDS`.

Node lifecycle settings: `EXCHANGE_IDLE_TTL_MS`, `WS_IDLE_GRACE_MS`, `RATE_LIMIT_WINDOW_MS`.

Python lifecycle settings: `EXCHANGE_IDLE_TTL_SECONDS`, `WS_IDLE_GRACE_SECONDS`, `RATE_LIMIT_WINDOW_SECONDS`.

Set each implementation's variables as runtime secrets/variables. Do not pass secrets as build arguments. The rate limiter and WebSocket fan-out are process-local; start each service with one replica. Horizontal scaling requires shared pub/sub and a shared rate-limit store.

Order idempotency is also process-local. Keep order submission on one replica until a shared idempotency backend is added. `/metrics` is protected by the service's `API_KEY`; configure monitoring to send it as a Bearer token.

## Health checks

Configure these HTTP probes against the service port:

| Probe | Path | Initial delay | Interval | Timeout | Failure threshold |
| --- | --- | ---: | ---: | ---: | ---: |
| Startup | `/health/ready` | 5 s | 5 s | 3 s | 12 |
| Liveness | `/health/live` | 10 s | 30 s | 3 s | 3 |
| Readiness | `/health/ready` | 5 s | 10 s | 3 s | 3 |

Northflank project/service IDs, custom domains, secrets, and networking belong to the account configuration and are not embedded in this repository.
