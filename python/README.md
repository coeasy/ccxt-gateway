# Python gateway

FastAPI gateway using `ccxt.async_support` for REST and `ccxt.pro` for WebSocket market streams. This folder is a standalone Northflank build context and has its own pinned runtime and development dependency locks.

```sh
python -m pip install -r requirements-dev.lock
# Set API_KEY to a random value of at least 32 characters.
python -m uvicorn app.main:create_app --factory --host 0.0.0.0 --port 8080 --workers 1
```

Run repeatable checks with `python -m ruff check app tests` and `python -m pytest -m "not integration"`. The opt-in read-only Binance test checks both Async REST and CCXT Pro WebSocket market data:

```sh
RUN_LIVE_INTEGRATION=1 python -m pytest -m integration
```

Build its image from this directory with `docker build -t coeasy/ccxt-gateway-python .`. The service listens on `PORT` (default `8080`) and exposes `/health/live` and `/health/ready`. It uses one worker because WebSocket subscribers, caches, exchange connections, and rate-limit buckets are held in process memory.

Set `API_KEY` as a runtime secret. Add `EXCHANGE_CREDENTIALS_JSON` only when private routes are required. API credentials never belong in the image or build arguments. Python settings use seconds for TTLs and rate windows; see the root [README](../README.md) and [Northflank profile](../deploy/northflank.md).
