# ccxt-gateway

A small Node.js service that exposes authenticated REST and WebSocket access to CCXT exchanges. One CCXT Pro instance is shared per exchange, and idle instances are closed automatically.

## Requirements

- Node.js 22 or newer
- An exchange supported by CCXT
- For account or order endpoints, exchange API credentials with only the required permissions

## Run locally

```sh
npm ci
cp .env.example .env
# Set API_KEY to a long random value before starting the service.
npm start
```

The server listens on `0.0.0.0:8080` by default. Every `/v1` endpoint requires `Authorization: Bearer <API_KEY>`. The health endpoints are public for platform probes.

### Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `API_KEY` | required | Gateway bearer token for REST and WebSocket clients (minimum 32 characters) |
| `HOST` | `0.0.0.0` | Bind address |
| `PORT` | `8080` | HTTP and WebSocket port; respects the platform's assigned port |
| `EXCHANGE_CREDENTIALS_JSON` | `{}` | Server-side exchange credentials, keyed by CCXT exchange ID |
| `EXCHANGE_IDLE_TTL_MS` | `60000` | Close a connection after this many idle milliseconds |
| `EXCHANGE_MAX_INSTANCES` | `32` | Maximum exchange instances retained at once |
| `EXCHANGE_TIMEOUT_MS` | `30000` | CCXT REST timeout |
| `WS_IDLE_GRACE_MS` | `10000` | Keep an upstream stream briefly after its last client unsubscribes |
| `WS_MAX_CLIENTS` | `500` | Maximum connected WebSocket clients |
| `WS_MAX_STREAMS` | `1000` | Maximum distinct upstream WebSocket subscriptions |

Example credentials configuration (store this as a runtime secret):

```json
{"binance":{"apiKey":"...","secret":"..."}}
```

Exchange credentials are only read by the server. The gateway never accepts them in API requests. Give exchange keys only the permissions the service needs; do not enable withdrawals.

## REST API

All responses are JSON. Use URL-encoded symbols such as `BTC%2FUSDT`.

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health/live` | Liveness probe |
| `GET` | `/health/ready` | Readiness probe |
| `GET` | `/v1/exchanges` | Supported CCXT exchange IDs |
| `GET` | `/v1/:exchange/markets` | Load exchange markets |
| `GET` | `/v1/:exchange/ticker?symbol=BTC%2FUSDT` | Unified ticker |
| `GET` | `/v1/:exchange/orderbook?symbol=BTC%2FUSDT&limit=20` | Order book |
| `GET` | `/v1/:exchange/trades?symbol=BTC%2FUSDT&limit=100` | Recent public trades |
| `GET` | `/v1/:exchange/ohlcv?symbol=BTC%2FUSDT&timeframe=1m&limit=100` | OHLCV candles |
| `GET` | `/v1/:exchange/balance` | Private balance; requires configured exchange credentials |
| `POST` | `/v1/:exchange/order` | Place a private order; requires exchange credentials |
| `DELETE` | `/v1/:exchange/order/:id?symbol=BTC%2FUSDT` | Cancel a private order; requires exchange credentials |

Example:

```sh
curl -H "Authorization: Bearer $API_KEY" \
  "http://localhost:8080/v1/binance/ticker?symbol=BTC%2FUSDT"
```

Limit orders require a price. Order bodies accept `symbol`, `type` (`limit` or `market`), `side` (`buy` or `sell`), `amount`, optional `price`, and optional exchange `params`.

## WebSocket API

Connect to `/ws`, then authenticate with the first JSON message. The token is sent in a frame, never in the URL:

```json
{"op":"auth","token":"<API_KEY>"}
```

After receiving `{"type":"ready"}`, subscribe:

```json
{"op":"subscribe","id":"btc-ticker","exchange":"binance","channel":"ticker","symbol":"BTC/USDT"}
```

Channels are `ticker`, `orderbook`, `trades`, and `ohlcv`. For `ohlcv`, include `timeframe`; `limit` is optional. Updates contain `type`, `exchange`, `channel`, `symbol`, and `data`, plus `subscriptionIds` for that connection. Unsubscribe with `{"op":"unsubscribe","id":"btc-ticker"}`. Send `{"op":"ping","id":"1"}` for an application-level heartbeat.

The gateway shares each upstream exchange/channel/symbol subscription among local clients. A stream is unsubscribed after the last client leaves and the grace period expires. Reconnect errors are reported to clients while the gateway retries with bounded backoff.

## Connection lifecycle

- Concurrent requests for the same exchange share one in-flight constructor and one CCXT instance.
- REST calls and active WebSocket streams hold leases on the instance.
- When the final lease is released, an idle timer starts; a later request cancels it.
- At capacity, the oldest unused instance is closed before a new one is created. If all instances are busy, new exchange use returns `503`.
- Shutdown closes upstream streams and exchange instances.

## Tests and container

```sh
npm test
npm run check
docker build -t ccxt-gateway .
docker run --rm -p 8080:8080 -e API_KEY="$(openssl rand -hex 32)" ccxt-gateway
```

Tests use an in-memory fake exchange and do not place orders or contact exchanges.

## Northflank setup

The repository is ready for a Northflank combined service that builds from its root `Dockerfile`:

1. Connect the GitHub repository and select the `main` branch.
2. Create a **Combined** service and choose **Dockerfile** as the build type. Set the build context to the repository root and the Dockerfile path to `/Dockerfile`.
3. Set runtime environment variables `API_KEY`, `EXCHANGE_CREDENTIALS_JSON` (only if needed), and optionally `PORT`. Store secrets as runtime variables or secret files, never as build arguments.
4. Expose the application port as HTTP. The container exposes `8080`, and the process also reads Northflank's assigned `PORT` value.
5. Add HTTP liveness and readiness probes on `/health/live` and `/health/ready` at the application port. Allow at least 10 seconds for startup.
6. Keep one instance when using in-memory WebSocket fan-out. Multiple replicas each maintain their own upstream CCXT streams; horizontal fan-out needs shared pub/sub first.

Northflank service IDs, project IDs, networking, and secrets are account-specific, so they are set on the service rather than committed as a misleading account-independent manifest. See [Build and deploy your code](https://northflank.com/docs/v1/application/getting-started/build-and-deploy-your-code), [Build with a Dockerfile](https://northflank.com/docs/v1/application/build/build-with-a-dockerfile), and [Configure health checks](https://northflank.com/docs/v1/application/observe/configure-health-checks).

The GitHub Actions workflow runs the test/check suite on Node.js 22 and 24 and builds the Docker image for pushes and pull requests.
